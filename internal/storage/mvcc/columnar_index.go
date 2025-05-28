/*
Copyright 2025 Stoolap Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// Ordering function type for comparing values
type compareFunc func(a, b storage.ColumnValue) int

// btreeNode represents a node in the B-tree
type btreeNode struct {
	keys     []storage.ColumnValue         // Keys stored in this node
	rowIDs   []*fastmap.Int64Map[struct{}] // Row IDs for each key using fast map for O(1) operations
	children []*btreeNode                  // Child nodes (nil for leaf nodes)
	isLeaf   bool                          // Whether this is a leaf node
	compare  compareFunc                   // Function for comparing keys

	// Lazy deletion tracking
	pendingDeletions int  // Count of keys with empty rowIDs maps (marked for deletion)
	toBeCompacted    bool // Flag indicating this node needs compaction
}

// Find returns the index where the key should be inserted
func (n *btreeNode) findIndex(key storage.ColumnValue) int {
	// Binary search for the key
	return sort.Search(len(n.keys), func(i int) bool {
		return n.compare(n.keys[i], key) >= 0
	})
}

// insert inserts a key and rowID into the B-tree node
func (n *btreeNode) insert(key storage.ColumnValue, rowID int64) {
	i := n.findIndex(key)

	// If key already exists, just add the rowID to the map
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		n.rowIDs[i].Put(rowID, struct{}{})
		return
	}

	// Insert new key and initialize a new map for the rowID
	n.keys = append(n.keys, nil)
	// Create a new map for the rowIDs - start with a small capacity as most keys have few rows
	newMap := fastmap.NewInt64Map[struct{}](4)
	newMap.Put(rowID, struct{}{})

	// Append empty values then copy to shift elements
	n.rowIDs = append(n.rowIDs, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.rowIDs[i+1:], n.rowIDs[i:])

	// Set the values at the insertion point
	n.keys[i] = key
	n.rowIDs[i] = newMap
}

// remove removes a rowID from a key in the B-tree node
func (n *btreeNode) remove(key storage.ColumnValue, rowID int64) (bool, bool) {
	i := n.findIndex(key)
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		// O(1) removal from the Int64Map
		deleted := n.rowIDs[i].Del(rowID)

		// If the key was found and deleted
		if deleted {
			// If no more rowIDs for this key, mark for lazy deletion
			if n.rowIDs[i].Len() == 0 {
				// Instead of immediate removal with costly slice operations,
				// just mark this position for eventual compaction
				n.pendingDeletions++
				n.toBeCompacted = true
				return true, true // Return true for both deletion and needing compaction
			}
			return true, false // Deletion occurred but no compaction needed
		}
	}
	return false, false // No deletion occurred
}

// findLeftBoundary finds the left (starting) boundary of a range search
// Returns the index of the first element that should be included
func (n *btreeNode) findLeftBoundary(min storage.ColumnValue, includeMin bool) int {
	// Handle NULL min value case
	if min == nil {
		return 0 // Include all from beginning
	}

	// Use binary search to efficiently find the position
	// This is a specialized version of sort.Search for our boundary cases
	left, right := 0, len(n.keys)-1

	// Special case for tiny arrays: linear scan might be faster
	if right < 8 {
		for i := 0; i <= right; i++ {
			compare := n.compare(n.keys[i], min)
			if includeMin && compare >= 0 {
				return i
			} else if !includeMin && compare > 0 {
				return i
			}
		}
		return right + 1
	}

	// Standard binary search with custom comparison function
	for left <= right {
		mid := left + (right-left)/2
		compare := n.compare(n.keys[mid], min)

		if includeMin {
			// Looking for first key >= min
			if compare < 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		} else {
			// Looking for first key > min
			if compare <= 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		}
	}

	// Left is now the index of the first key that should be included
	return left
}

// findRightBoundary finds the right (ending) boundary of a range search
// Returns the index after the last element that should be included
func (n *btreeNode) findRightBoundary(max storage.ColumnValue, includeMax bool) int {
	// Handle NULL max value case
	if max == nil {
		return len(n.keys) // Include all to the end
	}

	// Use binary search to efficiently find the position
	left, right := 0, len(n.keys)-1

	// Special case for tiny arrays: linear scan might be faster
	if right < 8 {
		for i := 0; i <= right; i++ {
			compare := n.compare(n.keys[i], max)
			if includeMax && compare > 0 {
				return i
			} else if !includeMax && compare >= 0 {
				return i
			}
		}
		return right + 1
	}

	// Standard binary search with custom comparison function
	for left <= right {
		mid := left + (right-left)/2
		compare := n.compare(n.keys[mid], max)

		if includeMax {
			// Looking for first key > max
			if compare <= 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		} else {
			// Looking for first key >= max
			if compare < 0 {
				left = mid + 1
			} else {
				// This could be a candidate, look to the left
				right = mid - 1
			}
		}
	}

	// Left is now the index of the first key that should be excluded
	return left
}

// rangeSearch finds all rowIDs in a range [min, max]
func (n *btreeNode) rangeSearch(min, max storage.ColumnValue, includeMin, includeMax bool, result *[]int64) {
	if len(n.keys) == 0 {
		return
	}

	// Get the start and end boundaries using optimized functions
	startIdx := n.findLeftBoundary(min, includeMin)
	endIdx := n.findRightBoundary(max, includeMax)

	// Early return if no overlap (completely outside range)
	if startIdx >= len(n.keys) || endIdx <= 0 || startIdx >= endIdx {
		return
	}

	// Fast path: If start and end are close, we can estimate the number of IDs
	if endIdx-startIdx < 64 { // Small range optimization
		// Estimate result capacity to reduce reallocations
		estimatedCapacity := 0
		for i := startIdx; i < endIdx; i++ {
			estimatedCapacity += n.rowIDs[i].Len()
		}

		// Pre-allocate result capacity if needed
		if estimatedCapacity > 0 && cap(*result)-len(*result) < estimatedCapacity {
			newResult := make([]int64, len(*result), len(*result)+estimatedCapacity)
			copy(newResult, *result)
			*result = newResult
		}
	}

	// Collect all rowIDs in the range
	for i := startIdx; i < endIdx; i++ {
		// For efficiency, use the ForEach method of Int64Map
		n.rowIDs[i].ForEach(func(id int64, _ struct{}) bool {
			*result = append(*result, id)
			return true // Continue iteration
		})
	}
}

// equalSearch finds all rowIDs with the given key
func (n *btreeNode) equalSearch(key storage.ColumnValue, result *[]int64) {
	i := n.findIndex(key)
	if i < len(n.keys) && n.compare(n.keys[i], key) == 0 {
		// Fast path: Pre-allocate space if needed
		currentLen := len(*result)
		rowCount := n.rowIDs[i].Len()

		if rowCount > 0 {
			// Ensure capacity
			if cap(*result)-currentLen < rowCount {
				newResult := make([]int64, currentLen, currentLen+rowCount)
				copy(newResult, *result)
				*result = newResult
			}

			// Use fast iteration to append all IDs
			n.rowIDs[i].ForEach(func(id int64, _ struct{}) bool {
				*result = append(*result, id)
				return true // Continue iteration
			})
		}
	}
}

// getAll returns all rowIDs in the node
func (n *btreeNode) getAll(result *[]int64) {
	// Estimate total capacity needed
	totalRowCount := 0
	for _, idMap := range n.rowIDs {
		totalRowCount += idMap.Len()
	}

	// Ensure capacity
	currentLen := len(*result)
	if cap(*result)-currentLen < totalRowCount {
		newResult := make([]int64, currentLen, currentLen+totalRowCount)
		copy(newResult, *result)
		*result = newResult
	}

	// Add all IDs
	for _, idMap := range n.rowIDs {
		idMap.ForEach(func(id int64, _ struct{}) bool {
			*result = append(*result, id)
			return true
		})
	}
}

// btree implements a B-tree data structure optimized for columnar indexes
type btreeColumnar struct {
	root                *btreeNode  // Root node of the B-tree
	compare             compareFunc // Function for comparing keys
	size                int         // Number of keys in the tree
	totalDeletions      int         // Total number of pending deletions across all nodes
	compactionThreshold float64     // Threshold ratio at which compaction is triggered (deletions/size)
}

// newBTree creates a new B-tree with the given compare function
func newBTree(compare compareFunc) *btreeColumnar {
	return &btreeColumnar{
		root: &btreeNode{
			keys:             make([]storage.ColumnValue, 0),
			rowIDs:           make([]*fastmap.Int64Map[struct{}], 0),
			children:         nil,
			isLeaf:           true,
			compare:          compare,
			pendingDeletions: 0,
			toBeCompacted:    false,
		},
		compare:             compare,
		size:                0,
		totalDeletions:      0,
		compactionThreshold: 0.25, // Default: compact when 25% of entries are marked for deletion
	}
}

// Insert adds a key and rowID to the B-tree
func (t *btreeColumnar) Insert(key storage.ColumnValue, rowID int64) {
	t.root.insert(key, rowID)
	t.size++
}

// Remove removes a key and rowID from the B-tree
func (t *btreeColumnar) Remove(key storage.ColumnValue, rowID int64) bool {
	deleted, needsCompaction := t.root.remove(key, rowID)
	if deleted {
		t.size--

		// Track deletions for compaction
		if needsCompaction {
			t.totalDeletions++

			// Check if compaction threshold is reached
			if float64(t.totalDeletions)/float64(t.size+t.totalDeletions) >= t.compactionThreshold {
				t.compact()
			}
		}
	}
	return deleted
}

// compact performs the physical removal of keys marked for deletion
func (t *btreeColumnar) compact() {
	if t.totalDeletions == 0 || t.root == nil {
		return
	}

	// Compact the root node
	t.compactNode(t.root)

	// Reset tracking counters
	t.totalDeletions = 0
}

// SetCompactionThreshold sets the threshold ratio at which compaction is triggered
// The threshold should be between 0 and 1, representing the ratio of deletions to total entries
func (t *btreeColumnar) SetCompactionThreshold(threshold float64) {
	if threshold < 0 {
		threshold = 0
	} else if threshold > 1 {
		threshold = 1
	}
	t.compactionThreshold = threshold
}

// GetCompactionThreshold gets the current compaction threshold
func (t *btreeColumnar) GetCompactionThreshold() float64 {
	return t.compactionThreshold
}

// ForceCompaction forces an immediate compaction of the tree
func (t *btreeColumnar) ForceCompaction() {
	t.compact()
}

// GetPendingDeletions returns the total number of pending deletions
func (t *btreeColumnar) GetPendingDeletions() int {
	return t.totalDeletions
}

// compactNode removes all keys with empty rowIDs maps from a node
func (t *btreeColumnar) compactNode(node *btreeNode) {
	if node == nil || !node.toBeCompacted || node.pendingDeletions == 0 {
		return
	}

	// Create new slices with proper capacity (removing pending deletions)
	newSize := len(node.keys) - node.pendingDeletions
	newKeys := make([]storage.ColumnValue, 0, newSize)
	newRowIDs := make([]*fastmap.Int64Map[struct{}], 0, newSize)

	// Copy only valid entries (those with non-empty rowIDs maps)
	for i := 0; i < len(node.keys); i++ {
		if node.rowIDs[i].Len() > 0 {
			newKeys = append(newKeys, node.keys[i])
			newRowIDs = append(newRowIDs, node.rowIDs[i])
		}
	}

	// Replace the node's data with the compacted data
	node.keys = newKeys
	node.rowIDs = newRowIDs
	node.pendingDeletions = 0
	node.toBeCompacted = false

	// Recursively compact children (if node is not a leaf)
	if !node.isLeaf {
		for _, child := range node.children {
			t.compactNode(child)
		}
	}
}

// ValueCount returns the number of occurrences of a key
func (t *btreeColumnar) ValueCount(key storage.ColumnValue) int {
	i := t.root.findIndex(key)
	if i < len(t.root.keys) && t.compare(t.root.keys[i], key) == 0 {
		return t.root.rowIDs[i].Len()
	}
	return 0
}

// RangeSearch finds all rowIDs in a range [min, max]
func (t *btreeColumnar) RangeSearch(min, max storage.ColumnValue, includeMin, includeMax bool) []int64 {
	// Estimate capacity based on range size and tree characteristics
	estimatedCapacity := t.estimateRangeSize(min, max)
	result := make([]int64, 0, estimatedCapacity)

	// Perform the range search with the optimized boundaries
	t.root.rangeSearch(min, max, includeMin, includeMax, &result)

	// If result is much smaller than capacity, consider trimming
	result = slices.Clip(result)

	return result
}

// estimateRangeSize estimates the number of rows in a given range
// This helps allocate appropriate capacity for the result slice
func (t *btreeColumnar) estimateRangeSize(min, max storage.ColumnValue) int {
	// Default reasonable capacity
	defaultCapacity := 100

	// If tree is empty, return minimum capacity
	if t.size == 0 || t.root == nil || len(t.root.keys) == 0 {
		return defaultCapacity
	}

	// For unbounded ranges (no min or max), use a proportion of the tree size
	if min == nil && max == nil {
		return t.size
	}

	// Try to get a rough range size estimate
	if min != nil && max != nil {
		// Find the start and end boundaries
		startIdx := t.root.findLeftBoundary(min, true)
		endIdx := t.root.findRightBoundary(max, true)

		// If we can determine the boundaries
		if startIdx < len(t.root.keys) && endIdx <= len(t.root.keys) && startIdx < endIdx {
			// Count the actual number of rowIDs in this range
			estimateCap := 0
			for i := startIdx; i < endIdx && i < len(t.root.keys); i++ {
				estimateCap += t.root.rowIDs[i].Len()
			}

			// Use actual count with some buffer
			return estimateCap + 10
		}

		// If we have some range information but can't determine exact count
		range_ratio := float64(endIdx-startIdx) / float64(len(t.root.keys))
		if range_ratio > 0 && range_ratio <= 1.0 {
			return int(float64(t.size)*range_ratio) + 10
		}
	}

	// If we have only min or only max, use half the tree size as an estimate
	if (min != nil && max == nil) || (min == nil && max != nil) {
		return t.size/2 + 10
	}

	// Fallback to a reasonable default
	return defaultCapacity
}

// EqualSearch finds all rowIDs with the given key
func (t *btreeColumnar) EqualSearch(key storage.ColumnValue) []int64 {
	result := make([]int64, 0, 1) // Start with a reasonable capacity
	t.root.equalSearch(key, &result)
	return result
}

// GetAll returns all rowIDs in the B-tree
func (t *btreeColumnar) GetAll() []int64 {
	result := make([]int64, 0, t.size)
	t.root.getAll(&result)
	return result
}

// Size returns the number of keys in the B-tree
func (t *btreeColumnar) Size() int {
	return t.size
}

// Clear removes all entries from the B-tree
func (t *btreeColumnar) Clear() {
	t.root = &btreeNode{
		keys:             make([]storage.ColumnValue, 0),
		rowIDs:           make([]*fastmap.Int64Map[struct{}], 0),
		children:         nil,
		isLeaf:           true,
		compare:          t.compare,
		pendingDeletions: 0,
		toBeCompacted:    false,
	}
	t.size = 0
	t.totalDeletions = 0
}

// compareColumnValues compares two ColumnValue objects
// This is the key function that allows us to use a single B-tree for all data types
func compareColumnValues(aVal, bVal storage.ColumnValue) int {
	cmp, err := aVal.Compare(bVal)
	if err != nil {
		return 0 // Error handling - should not happen in normal operation
	}

	return cmp
}

// ColumnarIndex represents a column-oriented index optimized for range queries
// using a B-tree data structure for efficient lookups and range scans
type ColumnarIndex struct {
	// name is the index name
	name string

	// columnName is the name of the column this index is for
	columnName string

	// columnID is the position of the column in the schema
	columnID int

	// dataType is the type of the column
	dataType storage.DataType

	// Single B-tree that works directly with ColumnValue objects
	// This eliminates unnecessary conversions between types
	valueTree *btreeColumnar

	// nullRows tracks rows with NULL in this column using a fast Int64Map
	nullRows *fastmap.Int64Map[struct{}]

	// Create a mutex for thread-safety
	mutex sync.RWMutex

	// tableName is the name of the table this index belongs to
	tableName string

	// versionStore is a reference to the MVCC version store
	versionStore *VersionStore

	// isUnique indicates if this is a unique index
	isUnique bool
}

// NewColumnarIndex creates a new ColumnarIndex
func NewColumnarIndex(name, tableName, columnName string,
	columnID int, dataType storage.DataType,
	versionStore *VersionStore, isUnique bool) *ColumnarIndex {

	idx := &ColumnarIndex{
		name:         name,
		columnName:   columnName,
		columnID:     columnID,
		dataType:     dataType,
		valueTree:    newBTree(compareColumnValues),     // Use single tree with ColumnValue comparator
		nullRows:     fastmap.NewInt64Map[struct{}](16), // Start with small capacity for NULL rows
		mutex:        sync.RWMutex{},
		tableName:    tableName,
		versionStore: versionStore,
		isUnique:     isUnique,
	}

	return idx
}

// Name returns the index name - implements IndexInterface
func (idx *ColumnarIndex) Name() string {
	return idx.name
}

// TableName returns the name of the table this index belongs to - implements IndexInterface
func (idx *ColumnarIndex) TableName() string {
	return idx.tableName
}

// IndexType returns the type of the index - implements IndexInterface
func (idx *ColumnarIndex) IndexType() storage.IndexType {
	return storage.ColumnarIndex
}

// ColumnNames returns the names of the columns this index is for - implements IndexInterface
func (idx *ColumnarIndex) ColumnNames() []string {
	return []string{idx.columnName}
}

// ColumnIDs returns the column IDs for this index - implements IndexInterface
func (idx *ColumnarIndex) ColumnIDs() []int {
	return []int{idx.columnID}
}

// DataType returns the data type of the column this index is for - implements IndexInterface
func (idx *ColumnarIndex) DataTypes() []storage.DataType {
	return []storage.DataType{idx.dataType}
}

// HasUniqueValue checks if a value already exists in a unique index
// This is a fast path check that doesn't modify the index
func (idx *ColumnarIndex) HasUniqueValue(value storage.ColumnValue) bool {
	// Skip NULL values - they don't violate uniqueness
	if value == nil || value.IsNull() {
		return false
	}

	// Only do this check for unique indexes
	if !idx.isUnique {
		return false
	}

	// Use a read lock since we're only reading
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Fast path: Just check if the value exists in the B-tree
	count := idx.valueTree.ValueCount(value)

	return count > 0
}

// Add adds a value to the index with the given row ID - implements IndexInterface
func (idx *ColumnarIndex) Add(values []storage.ColumnValue, rowID int64, refID int64) error {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
	}

	value := values[0]
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Handle NULL value - NULLs are always allowed, even with unique constraint
	if value == nil || value.IsNull() {
		// Use O(1) insert to track NULL values
		idx.nullRows.Put(rowID, struct{}{})
		return nil
	}

	// Fast path for non-unique indexes (most common case)
	if !idx.isUnique {
		// Store the ColumnValue directly in the B-tree
		idx.valueTree.Insert(value, rowID)
		return nil
	}

	// For unique indexes, check if the value already exists
	// Fast O(1) check with the optimized ValueCount method
	count := idx.valueTree.ValueCount(value)
	if count > 0 {
		// Return unique constraint violation
		return storage.NewUniqueConstraintError(idx.name, idx.columnName, value)
	}

	// If we get here, the value is unique, so insert it
	idx.valueTree.Insert(value, rowID)
	return nil
}

// AddBatch adds multiple entries to the index in a single batch operation
// This is more efficient than multiple individual Add operations
func (idx *ColumnarIndex) AddBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// For unique indexes, we need to check constraints first
	if idx.isUnique {
		// First collect all non-NULL values to check
		for _, values := range entries {
			// Skip invalid entries
			if len(values) != 1 {
				return fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
			}

			value := values[0]

			// Skip NULL values - they're always allowed
			if value == nil || value.IsNull() {
				continue
			}

			// Check uniqueness
			count := idx.valueTree.ValueCount(value)
			if count > 0 {
				return storage.NewUniqueConstraintError(idx.name, idx.columnName, value)
			}
		}
	}

	// Now actually add all the entries
	for rowID, values := range entries {
		if len(values) != 1 {
			continue // Skip invalid entries (already checked for unique indexes)
		}

		value := values[0]

		// Handle NULL values
		if value == nil || value.IsNull() {
			idx.nullRows.Put(rowID, struct{}{})
		} else {
			// Add to B-tree
			idx.valueTree.Insert(value, rowID)
		}
	}

	return nil
}

// Find finds all pairs where the column equals the given value - implements Index interface
func (idx *ColumnarIndex) Find(values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return nil, fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
	}

	// Get matching row IDs
	rowIDs := idx.GetRowIDsEqual(values)
	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// GetRowIDsEqual returns row IDs with the given values
func (idx *ColumnarIndex) GetRowIDsEqual(values []storage.ColumnValue) []int64 {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return []int64{}
	}

	value := values[0]
	// Fast path for common equality checks
	if value == nil || value.IsNull() {
		// NULL value check
		idx.mutex.RLock()
		defer idx.mutex.RUnlock()

		// Quick return for empty nullRows
		if idx.nullRows.Len() == 0 {
			return nil
		}

		// Allocate result slice with exact capacity needed
		nullCount := idx.nullRows.Len()
		result := make([]int64, 0, nullCount)

		// Efficiently collect all NULL row IDs
		idx.nullRows.ForEach(func(id int64, _ struct{}) bool {
			result = append(result, id)
			return true // Continue iteration
		})

		return result
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Use the value directly in the B-tree
	// No conversion needed - more efficient
	return idx.valueTree.EqualSearch(value)
}

// FindRange finds all row IDs where the column is in the given range
func (idx *ColumnarIndex) FindRange(min, max []storage.ColumnValue,
	minInclusive, maxInclusive bool) ([]storage.IndexEntry, error) {
	// Get row IDs in the range
	rowIDs := idx.GetRowIDsInRange(min, max, minInclusive, maxInclusive)
	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// GetRowIDsInRange returns row IDs with values in the given range
func (idx *ColumnarIndex) GetRowIDsInRange(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	var minValue, maxValue storage.ColumnValue
	if len(minValues) == 1 {
		minValue = minValues[0]
	}

	if len(maxValues) == 1 {
		maxValue = maxValues[0]
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Fast path - most common case
	if !(idx.dataType == storage.TIMESTAMP) ||
		!(maxValue != nil && !maxValue.IsNull() && includeMax) {
		// Use the valueTree directly with ColumnValue objects for best performance
		return idx.valueTree.RangeSearch(minValue, maxValue, includeMin, includeMax)
	}

	// Use the valueTree directly with ColumnValue objects
	return idx.valueTree.RangeSearch(minValue, maxValue, includeMin, includeMax)
}

// GetLatestBefore finds the most recent row IDs before a given timestamp
func (idx *ColumnarIndex) GetLatestBefore(timestamp time.Time) []int64 {
	if idx.dataType != storage.TIMESTAMP {
		return nil
	}

	tsValue := storage.GetPooledTimestampValue(timestamp)

	// Use range query with max value as the timestamp and no min value
	return idx.GetRowIDsInRange(nil, []storage.ColumnValue{tsValue}, false, true) // Up to and including timestamp
}

// GetRecentTimeRange finds row IDs within a recent time window (e.g., last hour, day)
func (idx *ColumnarIndex) GetRecentTimeRange(duration time.Duration) []int64 {
	if idx.dataType != storage.TIMESTAMP {
		return nil
	}

	now := time.Now()
	startTime := now.Add(-duration)

	// Convert to timestamp values
	startValue := storage.GetPooledTimestampValue(startTime)
	endValue := storage.GetPooledTimestampValue(now)

	// Get rows in the recent time range
	return idx.GetRowIDsInRange([]storage.ColumnValue{startValue}, []storage.ColumnValue{endValue}, true, true)
}

// Remove removes a value from the index - implements Index interface
func (idx *ColumnarIndex) Remove(values []storage.ColumnValue, rowID int64, refID int64) error {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
	}

	value := values[0]
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Handle NULL value - O(1) removal
	if value == nil || value.IsNull() {
		// Simply delete from the map - no iteration needed
		idx.nullRows.Del(rowID)
		return nil
	}

	// Remove directly from the valueTree - now O(1) with our optimized B-tree
	idx.valueTree.Remove(value, rowID)

	return nil
}

// RemoveBatch removes multiple entries from the index in a single batch operation
// This is more efficient than multiple individual Remove operations
func (idx *ColumnarIndex) RemoveBatch(entries map[int64][]storage.ColumnValue) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Process all entries at once
	for rowID, values := range entries {
		// Skip invalid entries
		if len(values) != 1 {
			return fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
		}

		value := values[0]

		// Handle NULL values
		if value == nil || value.IsNull() {
			idx.nullRows.Del(rowID)
		} else {
			// Remove from B-tree
			idx.valueTree.Remove(value, rowID)
		}
	}

	// Force compaction after large batch operations if needed
	if idx.valueTree.totalDeletions > 1000 || // Hard limit for very large deletions
		float64(idx.valueTree.totalDeletions)/float64(idx.valueTree.size+idx.valueTree.totalDeletions) >= idx.valueTree.compactionThreshold {
		idx.valueTree.compact()
	}

	return nil
}

// GetFilteredRowIDs returns row IDs that match the given expression
func (idx *ColumnarIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	if expr == nil {
		return nil
	}

	// Fast path for SimpleExpression with equality or NULL checks (most common cases)
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Only handle expressions for this column
		if simpleExpr.Column == idx.columnName {
			// Handle specific operators
			switch simpleExpr.Operator {
			case storage.EQ:
				// Convert the expression value to a ColumnValue with the correct type
				valueCol := storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataType)
				defer storage.PutPooledColumnValue(valueCol)
				return idx.GetRowIDsEqual([]storage.ColumnValue{valueCol})

			case storage.GT, storage.GTE, storage.LT, storage.LTE:
				// Range query - simple inequality
				var minValue, maxValue storage.ColumnValue
				var includeMin, includeMax bool

				if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
					minValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataType)
					defer storage.PutPooledColumnValue(minValue)
					includeMin = simpleExpr.Operator == storage.GTE
				} else {
					maxValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataType)
					defer storage.PutPooledColumnValue(maxValue)
					includeMax = simpleExpr.Operator == storage.LTE
				}

				return idx.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

			case storage.ISNULL:
				// NULL check - fast path with Int64Map
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()
				if idx.nullRows.Len() == 0 {
					return nil
				}

				// Use pre-sized allocation and fast iteration
				nullCount := idx.nullRows.Len()
				result := make([]int64, 0, nullCount)

				idx.nullRows.ForEach(func(id int64, _ struct{}) bool {
					result = append(result, id)
					return true
				})

				return result

			case storage.ISNOTNULL:
				// NOT NULL check - get all non-NULL values
				idx.mutex.RLock()
				defer idx.mutex.RUnlock()
				return idx.valueTree.GetAll()
			}
		}
	}

	// Handle RangeExpression for range queries
	if rangeExpr, ok := expr.(*expression.RangeExpression); ok {
		var minValue, maxValue storage.ColumnValue

		if rangeExpr.MinValue != nil {
			minValue = storage.ValueToPooledColumnValue(rangeExpr.MinValue, idx.dataType)
			defer storage.PutPooledColumnValue(minValue)
		}

		if rangeExpr.MaxValue != nil {
			maxValue = storage.ValueToPooledColumnValue(rangeExpr.MaxValue, idx.dataType)
			defer storage.PutPooledColumnValue(maxValue)
		}

		return idx.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, rangeExpr.IncludeMin, rangeExpr.IncludeMax)
	}

	// Handle NullCheckExpression for IS NULL and IS NOT NULL queries
	if nullCheckExpr, ok := expr.(*expression.NullCheckExpression); ok && nullCheckExpr.GetColumnName() == idx.columnName {
		if nullCheckExpr.IsNull() {
			// IS NULL check - get all NULL values
			idx.mutex.RLock()
			defer idx.mutex.RUnlock()

			// If no NULL values, return empty slice
			if idx.nullRows.Len() == 0 {
				return nil
			}

			// Allocate with exact capacity needed
			nullCount := idx.nullRows.Len()
			result := make([]int64, 0, nullCount)

			// Collect all NULL row IDs
			idx.nullRows.ForEach(func(id int64, _ struct{}) bool {
				result = append(result, id)
				return true
			})

			return result
		} else {
			// IS NOT NULL check - get all non-NULL values
			idx.mutex.RLock()
			defer idx.mutex.RUnlock()

			// Return all values in the tree (which are non-NULL by definition)
			return idx.valueTree.GetAll()
		}
	}

	// Handle BetweenExpression for BETWEEN queries
	if betweenExpr, ok := expr.(*expression.BetweenExpression); ok && betweenExpr.Column == idx.columnName {
		// Convert lower and upper bounds to appropriate column values
		lowerValue := storage.ValueToPooledColumnValue(betweenExpr.LowerBound, idx.dataType)
		defer storage.PutPooledColumnValue(lowerValue)

		upperValue := storage.ValueToPooledColumnValue(betweenExpr.UpperBound, idx.dataType)
		defer storage.PutPooledColumnValue(upperValue)

		// Use the index's range function with inclusivity based on the BetweenExpression's Inclusive field
		return idx.GetRowIDsInRange([]storage.ColumnValue{lowerValue}, []storage.ColumnValue{upperValue},
			betweenExpr.Inclusive, betweenExpr.Inclusive)
	}

	// Also check for AndExpression with range conditions
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		// Check if both expressions are for this column and represent a range
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		if ok1 && ok2 && expr1.Column == idx.columnName && expr2.Column == idx.columnName {
			// Fast path for range queries
			// Check for range patterns like (col > X AND col < Y)
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool
			var hasRange bool

			// Check if expr1 is a lower bound and expr2 is an upper bound
			if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
				(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr1.Value, idx.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr1.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr2.Value, idx.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr2.Operator == storage.LTE
				hasRange = true
			}

			// Check if expr2 is a lower bound and expr1 is an upper bound
			if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
				(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr2.Value, idx.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr2.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr1.Value, idx.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr1.Operator == storage.LTE
				hasRange = true
			}

			if hasRange {
				return idx.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)
			}
		}
	}

	// For other cases, return nil to indicate we can't use this index
	return nil
}

// Build builds or rebuilds the index from the version store
func (idx *ColumnarIndex) Build() error {
	// Clear existing data
	idx.mutex.Lock()

	// Clear B-tree and nullRows
	if idx.valueTree != nil {
		idx.valueTree.Clear()
	} else {
		idx.valueTree = newBTree(compareColumnValues)
	}

	// Clear the nullRows map or create a new one if needed
	if idx.nullRows != nil {
		idx.nullRows.Clear()
	} else {
		idx.nullRows = fastmap.NewInt64Map[struct{}](16)
	}
	idx.mutex.Unlock()

	// Get all visible versions from the version store
	if idx.versionStore == nil {
		return nil
	}

	// Use transaction ID 0 to see all committed data
	visibleVersions := idx.versionStore.GetAllVisibleVersions(0)

	// Process each visible row
	visibleVersions.ForEach(func(rowID int64, version *RowVersion) bool {
		if version.IsDeleted() {
			return true
		}

		// Get the value at the specified column ID
		if idx.columnID < len(version.Data) {
			value := version.Data[idx.columnID]

			// Add to index
			idx.Add([]storage.ColumnValue{value}, rowID, 0)
		} else {
			// Column doesn't exist in this row, treat as NULL
			idx.Add(nil, rowID, 0)
		}

		return true
	})

	return nil
}

// FindWithOperator finds all row IDs that match the operation - implements IndexInterface
func (idx *ColumnarIndex) FindWithOperator(op storage.Operator, values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	// For columnar index, we only support single column
	if len(values) != 1 {
		return nil, fmt.Errorf("expected 1 value for column %s, got %d values", idx.columnName, len(values))
	}

	value := values[0]
	var rowIDs []int64

	switch op {
	case storage.EQ:
		rowIDs = idx.GetRowIDsEqual(values)

	case storage.GT, storage.GTE, storage.LT, storage.LTE:
		var minValue, maxValue storage.ColumnValue
		var includeMin, includeMax bool

		if op == storage.GT || op == storage.GTE {
			minValue = value
			includeMin = op == storage.GTE
		} else {
			maxValue = value
			includeMax = op == storage.LTE
		}

		rowIDs = idx.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

	case storage.ISNULL:
		idx.mutex.RLock()
		if idx.nullRows.Len() > 0 {
			// Fast path with Int64Map
			nullCount := idx.nullRows.Len()
			rowIDs = make([]int64, 0, nullCount)

			idx.nullRows.ForEach(func(id int64, _ struct{}) bool {
				rowIDs = append(rowIDs, id)
				return true
			})
		}
		idx.mutex.RUnlock()

	case storage.ISNOTNULL:
		idx.mutex.RLock()
		rowIDs = idx.valueTree.GetAll()
		idx.mutex.RUnlock()
	}

	if len(rowIDs) == 0 {
		return nil, nil
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

func (idx *ColumnarIndex) IsUnique() bool {
	return idx.isUnique
}

// Close releases resources held by the index - implements Index interface
func (idx *ColumnarIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Clear valueTree and nullRows to free memory
	if idx.valueTree != nil {
		idx.valueTree.Clear()
		idx.valueTree = nil
	}

	idx.nullRows = nil

	return nil
}

// ForceCompaction forces an immediate compaction of the index
func (idx *ColumnarIndex) ForceCompaction() {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.valueTree != nil {
		idx.valueTree.ForceCompaction()
	}
}

// SetCompactionThreshold sets the threshold ratio at which compaction is triggered
func (idx *ColumnarIndex) SetCompactionThreshold(threshold float64) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.valueTree != nil {
		idx.valueTree.SetCompactionThreshold(threshold)
	}
}

// GetPendingDeletions returns the total number of pending deletions
func (idx *ColumnarIndex) GetPendingDeletions() int {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	if idx.valueTree != nil {
		return idx.valueTree.GetPendingDeletions()
	}
	return 0
}
