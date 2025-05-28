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
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// Pool for int64 set maps to reduce allocations
var int64SetPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[struct{}](1000)
	},
}

// GetInt64Set gets a set from the pool
func GetInt64Set() *fastmap.Int64Map[struct{}] {
	return int64SetPool.Get().(*fastmap.Int64Map[struct{}])
}

// PutInt64Set returns a set to the pool
func PutInt64Set(s *fastmap.Int64Map[struct{}]) {
	if s == nil {
		return
	}
	s.Clear()
	int64SetPool.Put(s)
}

// MultiColumnarIndex is an enhanced index that supports queries across multiple columns
// while also providing uniqueness constraint enforcement
type MultiColumnarIndex struct {
	name        string
	tableName   string
	columnNames []string
	columnIDs   []int
	dataTypes   []storage.DataType

	// Individual column indices for efficient per-column operations
	columnIndices []*ColumnarIndex

	// Map for efficient uniqueness checking
	// The key is a binary representation of all column values
	uniqueValues map[string]int64 // Value is the rowID

	vs *VersionStore // Version store for MVCC operations

	isUnique bool

	mutex sync.RWMutex
}

// NewMultiColumnarIndex creates a new index for both uniqueness constraint enforcement
// and efficient query operations using individual ColumnarIndex instances for each column
func NewMultiColumnarIndex(
	name string,
	tableName string,
	columnNames []string,
	columnIDs []int,
	dataTypes []storage.DataType,
	vs *VersionStore,
	isUnique bool) *MultiColumnarIndex {

	idx := &MultiColumnarIndex{
		name:          name,
		tableName:     tableName,
		columnNames:   columnNames,
		columnIDs:     columnIDs,
		dataTypes:     dataTypes,
		vs:            vs,
		isUnique:      isUnique,
		uniqueValues:  make(map[string]int64),
		columnIndices: make([]*ColumnarIndex, len(columnNames)),
	}

	// Create individual ColumnarIndex instances for each column
	for i, colName := range columnNames {
		colIdx := NewColumnarIndex(
			fmt.Sprintf("%s_%s", name, colName),
			tableName,
			colName,
			columnIDs[i],
			dataTypes[i],
			vs,
			false, // Not enforcing uniqueness at individual column level
		)
		idx.columnIndices[i] = colIdx
	}

	return idx
}

// Storage.Index interface implementation

// Name returns the index name
func (idx *MultiColumnarIndex) Name() string {
	return idx.name
}

// TableName returns the table this index belongs to
func (idx *MultiColumnarIndex) TableName() string {
	return idx.tableName
}

// ColumnNames returns the names of indexed columns
func (idx *MultiColumnarIndex) ColumnNames() []string {
	return idx.columnNames
}

// ColumnIDs returns the IDs of indexed columns
func (idx *MultiColumnarIndex) ColumnIDs() []int {
	return idx.columnIDs
}

// DataTypes returns the data types of indexed columns
func (idx *MultiColumnarIndex) DataTypes() []storage.DataType {
	return idx.dataTypes
}

// IndexType returns the type of the index
func (idx *MultiColumnarIndex) IndexType() storage.IndexType {
	if idx.isUnique {
		return "unique" // Using string literal directly as storage.UniqueIndex is not defined
	}
	return storage.ColumnarIndex
}

// IsUnique returns whether this is a unique index
func (idx *MultiColumnarIndex) IsUnique() bool {
	return idx.isUnique
}

// HasUniqueValues checks if the provided values already exist in the unique index
func (idx *MultiColumnarIndex) HasUniqueValues(values []storage.ColumnValue) bool {
	if !idx.isUnique || values == nil || len(values) == 0 {
		return false
	}

	// Quick check that we have the expected number of values
	if len(values) != len(idx.columnIDs) {
		return false
	}

	// Generate the key for these values
	key := idx.createKey(values)
	if key == "" {
		return false // NULL values don't violate uniqueness
	}

	// Check if this key exists in the uniqueness map
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	_, exists := idx.uniqueValues[key]
	return exists
}

// createKey creates a unique key from column values with minimal allocations
func (idx *MultiColumnarIndex) createKey(values []storage.ColumnValue) string {
	if values == nil {
		return ""
	}

	// Skip if any value is NULL - NULL values don't violate uniqueness
	for _, val := range values {
		if val == nil || val.IsNull() {
			return ""
		}
	}

	// Get a buffer from the pool
	buf := common.GetBufferPool()
	defer common.PutBufferPool(buf)

	// Simple binary encoding of values
	// Format: <value1-length><value1-bytes><value2-length><value2-bytes>...
	for i, val := range values {
		// Different serialization based on type
		switch idx.dataTypes[i] {
		case storage.INTEGER:
			intVal := val.AsInterface().(int64)
			// Write 8 bytes for int64, big-endian
			buf.B = append(buf.B, byte((intVal>>56)&0xFF))
			buf.B = append(buf.B, byte((intVal>>48)&0xFF))
			buf.B = append(buf.B, byte((intVal>>40)&0xFF))
			buf.B = append(buf.B, byte((intVal>>32)&0xFF))
			buf.B = append(buf.B, byte((intVal>>24)&0xFF))
			buf.B = append(buf.B, byte((intVal>>16)&0xFF))
			buf.B = append(buf.B, byte((intVal>>8)&0xFF))
			buf.B = append(buf.B, byte(intVal&0xFF))

		case storage.FLOAT:
			// Store string representation of float for consistency
			strVal, _ := val.AsString()
			// Write length prefix and then string bytes
			buf.B = append(buf.B, byte(len(strVal)))
			buf.B = append(buf.B, strVal...)

		case storage.TEXT:
			strVal := val.AsInterface().(string)
			// For longer strings, use 2 bytes for length
			if len(strVal) > 255 {
				buf.B = append(buf.B, 255) // Marker for long string
				buf.B = append(buf.B, byte((len(strVal)>>8)&0xFF))
				buf.B = append(buf.B, byte(len(strVal)&0xFF))
			} else {
				buf.B = append(buf.B, byte(len(strVal)))
			}
			buf.B = append(buf.B, strVal...)

		case storage.BOOLEAN:
			if val.AsInterface().(bool) {
				buf.B = append(buf.B, 1)
			} else {
				buf.B = append(buf.B, 0)
			}

		default:
			// For other types, use string representation
			strVal, _ := val.AsString()
			buf.B = append(buf.B, byte(len(strVal)))
			buf.B = append(buf.B, strVal...)
		}

		// Add a type marker byte between values for safety
		if i < len(values)-1 {
			buf.B = append(buf.B, byte(idx.dataTypes[i]))
		}
	}

	// Convert to string - this is the only unavoidable allocation
	return string(buf.B)
}

// Add adds values to the index, enforcing uniqueness and updating column indices
func (idx *MultiColumnarIndex) Add(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for unique constraint, got %d",
			len(idx.columnIDs), len(values))
	}

	// First check uniqueness if required
	if idx.isUnique {
		key := idx.createKey(values)
		if key != "" { // Skip NULL values for uniqueness check
			idx.mutex.Lock()
			defer idx.mutex.Unlock()

			// Check for uniqueness violation
			if _, exists := idx.uniqueValues[key]; exists {
				return storage.NewUniqueConstraintError(
					idx.name,
					strings.Join(idx.columnNames, ","),
					values[0],
				)
			}

			// Add the key to the uniqueness map
			idx.uniqueValues[key] = rowID
		}
	}

	// Update individual column indices for query support
	for i, colIdx := range idx.columnIndices {
		err := colIdx.Add([]storage.ColumnValue{values[i]}, rowID, refID)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddBatch adds multiple entries in batch mode
func (idx *MultiColumnarIndex) AddBatch(entries map[int64][]storage.ColumnValue) error {
	// When adding in batch, first check uniqueness constraints (if applicable)
	if idx.isUnique {
		idx.mutex.Lock()
		defer idx.mutex.Unlock()

		// First check all entries for uniqueness
		keysToAdd := make(map[string]int64)
		for rowID, values := range entries {
			// Validate column count
			if len(values) != len(idx.columnIDs) {
				return fmt.Errorf("expected %d values for unique constraint, got %d",
					len(idx.columnIDs), len(values))
			}

			key := idx.createKey(values)
			if key == "" {
				continue // Skip NULL values
			}

			// Check against existing keys
			if _, exists := idx.uniqueValues[key]; exists {
				return storage.NewUniqueConstraintError(
					idx.name,
					strings.Join(idx.columnNames, ","),
					values[0],
				)
			}

			// Check against other entries in this batch
			if _, exists := keysToAdd[key]; exists {
				return storage.NewUniqueConstraintError(
					idx.name,
					strings.Join(idx.columnNames, ","),
					values[0],
				)
			}

			keysToAdd[key] = rowID
		}

		// If all checks pass, add all entries to uniqueness map
		for key, rowID := range keysToAdd {
			idx.uniqueValues[key] = rowID
		}
	}

	// Process all entries for column indices
	for i, colIdx := range idx.columnIndices {
		// Prepare batch data for this column
		columnBatch := make(map[int64][]storage.ColumnValue)
		for rowID, values := range entries {
			if len(values) != len(idx.columnIDs) {
				continue // Skip invalid entries
			}
			columnBatch[rowID] = []storage.ColumnValue{values[i]}
		}

		// Add to column index
		if err := colIdx.AddBatch(columnBatch); err != nil {
			return err
		}
	}

	return nil
}

// Remove removes values from the index
func (idx *MultiColumnarIndex) Remove(values []storage.ColumnValue, rowID int64, refID int64) error {
	// Validate column count
	if len(values) != len(idx.columnIDs) {
		return fmt.Errorf("expected %d values for unique constraint, got %d",
			len(idx.columnIDs), len(values))
	}

	// First update uniqueness map if applicable
	if idx.isUnique {
		key := idx.createKey(values)
		if key != "" { // Only remove non-NULL values
			idx.mutex.Lock()
			// Only remove if rowID matches (important for concurrent operations)
			if existingRowID, exists := idx.uniqueValues[key]; exists && existingRowID == rowID {
				delete(idx.uniqueValues, key)
			}
			idx.mutex.Unlock()
		}
	}

	// Update individual column indices
	for i, colIdx := range idx.columnIndices {
		if err := colIdx.Remove([]storage.ColumnValue{values[i]}, rowID, refID); err != nil {
			return err
		}
	}

	return nil
}

// RemoveBatch removes multiple entries in batch mode
func (idx *MultiColumnarIndex) RemoveBatch(entries map[int64][]storage.ColumnValue) error {
	// First update uniqueness map if applicable
	if idx.isUnique {
		idx.mutex.Lock()
		for rowID, values := range entries {
			// Validate column count
			if len(values) != len(idx.columnIDs) {
				continue // Skip invalid entries
			}

			key := idx.createKey(values)
			if key == "" {
				continue // Skip NULL values
			}

			// Only remove if rowID matches
			if existingRowID, exists := idx.uniqueValues[key]; exists && existingRowID == rowID {
				delete(idx.uniqueValues, key)
			}
		}
		idx.mutex.Unlock()
	}

	// Update individual column indices
	for i, colIdx := range idx.columnIndices {
		// Prepare batch data for this column
		columnBatch := make(map[int64][]storage.ColumnValue)
		for rowID, values := range entries {
			if len(values) != len(idx.columnIDs) {
				continue // Skip invalid entries
			}
			columnBatch[rowID] = []storage.ColumnValue{values[i]}
		}

		// Remove from column index
		if err := colIdx.RemoveBatch(columnBatch); err != nil {
			return err
		}
	}

	return nil
}

// Find returns rows where column values exactly match the given values
func (idx *MultiColumnarIndex) Find(values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	if len(values) == 0 || len(values) > len(idx.columnIDs) {
		return nil, fmt.Errorf("expected between 1 and %d values, got %d", len(idx.columnIDs), len(values))
	}

	// Use column index for most selective column (usually the first one)
	// For exact matches, we can use any of the column indices
	colIdx := 0 // Start with first column by default
	if len(values) < len(idx.columnIndices) {
		// If partial match, use the last specified column
		colIdx = len(values) - 1
	}

	// Get initial result set
	rowIDs := idx.columnIndices[colIdx].GetRowIDsEqual([]storage.ColumnValue{values[colIdx]})
	if len(rowIDs) == 0 {
		return nil, nil
	}

	// If using all columns, verify the multi-column constraint
	if len(values) == len(idx.columnIDs) && idx.isUnique {
		key := idx.createKey(values)
		if key == "" {
			// NULL values - no uniqueness check needed
			// Convert to index entries
			result := make([]storage.IndexEntry, len(rowIDs))
			for i, id := range rowIDs {
				result[i] = storage.IndexEntry{RowID: id}
			}
			return result, nil
		}

		// Check uniqueness map for exact match
		idx.mutex.RLock()
		if matchRowID, exists := idx.uniqueValues[key]; exists {
			// Return only the matching row ID
			idx.mutex.RUnlock()
			return []storage.IndexEntry{{RowID: matchRowID}}, nil
		}
		idx.mutex.RUnlock()
	}

	// For multi-column queries, filter the initial result set using other columns
	if len(values) > 1 {
		// Get a map from the pool and convert rowIDs to the map for O(1) lookups
		rowIDMap := GetInt64Set()

		for _, id := range rowIDs {
			rowIDMap.Put(id, struct{}{})
		}

		// Filter using each column's index
		for i := 0; i < len(values); i++ {
			if i == colIdx {
				continue // Skip the column we already used
			}

			// Get matches for this column
			colMatches := idx.columnIndices[i].GetRowIDsEqual([]storage.ColumnValue{values[i]})
			if len(colMatches) == 0 {
				PutInt64Set(rowIDMap)
				return nil, nil // No matches for this column
			}

			// Get a new map from the pool for this iteration
			newRowIDMap := GetInt64Set()

			// Intersect with current result set
			for _, id := range colMatches {
				if rowIDMap.Has(id) {
					newRowIDMap.Put(id, struct{}{})
				}
			}

			// If intersection is empty, early return
			if newRowIDMap.Len() == 0 {
				PutInt64Set(newRowIDMap)
				PutInt64Set(rowIDMap)
				return nil, nil
			}

			// Update current result set and release old map
			PutInt64Set(rowIDMap)
			rowIDMap = newRowIDMap
		}

		defer PutInt64Set(rowIDMap)

		// Convert back to slice with proper pre-allocation
		rowIDs = make([]int64, 0, rowIDMap.Len())
		rowIDMap.Keys()(func(rowID int64) bool {
			rowIDs = append(rowIDs, rowID)
			return true
		})
	}

	// Convert to index entries
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// FindRange returns rows with values in the specified range
func (idx *MultiColumnarIndex) FindRange(min, max []storage.ColumnValue,
	minInclusive, maxInclusive bool) ([]storage.IndexEntry, error) {

	// Validate input
	if (min != nil && len(min) > len(idx.columnIDs)) || (max != nil && len(max) > len(idx.columnIDs)) {
		return nil, fmt.Errorf("expected no more than %d values for range query", len(idx.columnIDs))
	}

	// Default to first column for range query if no specific column is specified
	colIdx := 0

	// If min/max specify values, ensure consistent length
	minLen := 0
	if min != nil {
		minLen = len(min)
	}

	maxLen := 0
	if max != nil {
		maxLen = len(max)
	}

	// Use the most specific (longest) constraint for initial filtering
	specificLen := minLen
	if maxLen > minLen {
		specificLen = maxLen
	}

	if specificLen > 0 {
		// Get initial result set from most specific column for range query
		colIdx = specificLen - 1 // Use the last column with a constraint

		// Prepare column-specific min/max
		var colMin, colMax []storage.ColumnValue
		if minLen > colIdx {
			colMin = []storage.ColumnValue{min[colIdx]}
		}
		if maxLen > colIdx {
			colMax = []storage.ColumnValue{max[colIdx]}
		}

		// Get rows in range for selected column
		rowIDs := idx.columnIndices[colIdx].GetRowIDsInRange(colMin, colMax, minInclusive, maxInclusive)
		if len(rowIDs) == 0 {
			return nil, nil // No results match the range
		}

		// For multi-column range query, we need to filter with other columns
		if specificLen > 1 {
			// Get a map from the pool and convert rowIDs to the map for O(1) lookups
			rowIDMap := GetInt64Set()

			for _, id := range rowIDs {
				rowIDMap.Put(id, struct{}{})
			}

			// Filter using each column's range constraint
			for i := 0; i < specificLen; i++ {
				if i == colIdx {
					continue // Skip the column we already used
				}

				// Prepare column-specific min/max
				var colMin, colMax []storage.ColumnValue
				if minLen > i {
					colMin = []storage.ColumnValue{min[i]}
				}
				if maxLen > i {
					colMax = []storage.ColumnValue{max[i]}
				}

				// Skip if no constraint for this column
				if colMin == nil && colMax == nil {
					continue
				}

				// Get matches for this column's range
				colMatches := idx.columnIndices[i].GetRowIDsInRange(colMin, colMax, minInclusive, maxInclusive)
				if len(colMatches) == 0 {
					PutInt64Set(rowIDMap)
					return nil, nil // No matches for this column
				}

				// Get a new map from the pool for this iteration
				newRowIDMap := GetInt64Set()

				// Intersect with current result set
				for _, id := range colMatches {
					if rowIDMap.Has(id) {
						newRowIDMap.Put(id, struct{}{})
					}
				}

				// If intersection is empty, early return
				if newRowIDMap.Len() == 0 {
					PutInt64Set(newRowIDMap)
					PutInt64Set(rowIDMap)
					return nil, nil
				}

				// Update current result set and release old map
				PutInt64Set(rowIDMap)
				rowIDMap = newRowIDMap
			}

			defer PutInt64Set(rowIDMap)

			// Convert back to slice with proper pre-allocation
			rowIDs = make([]int64, 0, rowIDMap.Len())
			rowIDMap.Keys()(func(rowID int64) bool {
				rowIDs = append(rowIDs, rowID)
				return true
			})
		}

		// Convert to index entries
		result := make([]storage.IndexEntry, len(rowIDs))
		for i, id := range rowIDs {
			result[i] = storage.IndexEntry{RowID: id}
		}

		return result, nil
	}

	// If no specific range was provided, return all rows
	// This is an uncommon case, but supported for completeness
	rowIDs := idx.columnIndices[0].GetRowIDsInRange(nil, nil, false, false)
	result := make([]storage.IndexEntry, len(rowIDs))
	for i, id := range rowIDs {
		result[i] = storage.IndexEntry{RowID: id}
	}

	return result, nil
}

// FindWithOperator finds all rows that match the operator and values
func (idx *MultiColumnarIndex) FindWithOperator(op storage.Operator, values []storage.ColumnValue) ([]storage.IndexEntry, error) {
	if len(values) == 0 || len(values) > len(idx.columnIDs) {
		return nil, fmt.Errorf("expected between 1 and %d values, got %d", len(idx.columnIDs), len(values))
	}

	// For multi-column equality, use Find method
	if op == storage.EQ && len(values) == len(idx.columnIDs) {
		return idx.Find(values)
	}

	// For other operations, use column-specific operator
	colIdx := len(values) - 1 // Use the last specified column
	entries, err := idx.columnIndices[colIdx].FindWithOperator(op, []storage.ColumnValue{values[colIdx]})
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// For multi-column queries, filter the initial result set using other columns
	if len(values) > 1 {
		// Extract row IDs
		rowIDs := make([]int64, len(entries))
		for i, entry := range entries {
			rowIDs[i] = entry.RowID
		}

		// Get a map from the pool and convert rowIDs to the map for O(1) lookups
		rowIDMap := GetInt64Set()

		for _, id := range rowIDs {
			rowIDMap.Put(id, struct{}{})
		}

		// Filter using each column's index
		for i := 0; i < len(values)-1; i++ { // Skip the last column we already used
			// Get matches for this column
			colMatches := idx.columnIndices[i].GetRowIDsEqual([]storage.ColumnValue{values[i]})
			if len(colMatches) == 0 {
				PutInt64Set(rowIDMap)
				return nil, nil // No matches for this column
			}

			// Get a new map from the pool for this iteration
			newRowIDMap := GetInt64Set()

			// Intersect with current result set
			for _, id := range colMatches {
				if rowIDMap.Has(id) {
					newRowIDMap.Put(id, struct{}{})
				}
			}

			// If intersection is empty, early return
			if newRowIDMap.Len() == 0 {
				PutInt64Set(newRowIDMap)
				PutInt64Set(rowIDMap)
				return nil, nil
			}

			// Update current result set and release old map
			PutInt64Set(rowIDMap)
			rowIDMap = newRowIDMap
		}

		defer PutInt64Set(rowIDMap)

		// Convert back to slice of IndexEntry with proper pre-allocation
		result := make([]storage.IndexEntry, 0, rowIDMap.Len())
		rowIDMap.Keys()(func(rowID int64) bool {
			result = append(result, storage.IndexEntry{RowID: rowID})
			return true
		})
		return result, nil
	}

	return entries, nil
}

// GetRowIDsEqual returns row IDs with values exactly matching the given values
func (idx *MultiColumnarIndex) GetRowIDsEqual(values []storage.ColumnValue) []int64 {
	if len(values) == 0 || len(values) > len(idx.columnIDs) {
		return nil
	}

	// Find the first valid value to start with (handle nil values properly)
	colIdx := 0 // Start with first column by default

	// Find the first non-nil value to start with
	for i := 0; i < len(values); i++ {
		if values[i] != nil && !values[i].IsNull() {
			colIdx = i
			break
		}
	}

	// Get initial result set
	rowIDs := idx.columnIndices[colIdx].GetRowIDsEqual([]storage.ColumnValue{values[colIdx]})

	if len(rowIDs) == 0 {
		return nil
	}

	// If using all columns, verify the multi-column constraint
	if len(values) == len(idx.columnIDs) && idx.isUnique {
		key := idx.createKey(values)
		if key == "" {
			// NULL values - no uniqueness check needed
			return rowIDs
		}

		// Check uniqueness map for exact match
		idx.mutex.RLock()
		if matchRowID, exists := idx.uniqueValues[key]; exists {
			// Return only the matching row ID
			idx.mutex.RUnlock()
			return []int64{matchRowID}
		}
		idx.mutex.RUnlock()
	}

	// For multi-column queries, filter the initial result set using other columns
	if len(values) > 1 {
		// Get a map from the pool and convert rowIDs to the map for O(1) lookups
		rowIDMap := GetInt64Set()

		for _, id := range rowIDs {
			rowIDMap.Put(id, struct{}{})
		}

		// Filter using each column's index
		for i := 0; i < len(values); i++ {
			if i == colIdx {
				continue // Skip the column we already used
			}

			// Get matches for this column
			colMatches := idx.columnIndices[i].GetRowIDsEqual([]storage.ColumnValue{values[i]})
			if len(colMatches) == 0 {
				PutInt64Set(rowIDMap)
				return nil // No matches for this column
			}

			// Get a new map from the pool for this iteration
			newRowIDMap := GetInt64Set()

			// Intersect with current result set
			for _, id := range colMatches {
				if rowIDMap.Has(id) {
					newRowIDMap.Put(id, struct{}{})
				}
			}

			// If intersection is empty, early return
			if newRowIDMap.Len() == 0 {
				PutInt64Set(newRowIDMap)
				PutInt64Set(rowIDMap)
				return nil
			}

			// Update current result set and release old map
			PutInt64Set(rowIDMap)
			rowIDMap = newRowIDMap
		}

		defer PutInt64Set(rowIDMap)

		// Convert back to slice with proper pre-allocation
		rowIDs = make([]int64, 0, rowIDMap.Len())
		rowIDMap.Keys()(func(rowID int64) bool {
			rowIDs = append(rowIDs, rowID)
			return true
		})
	}

	return rowIDs
}

// GetRowIDsInRange returns row IDs with values in the given range
func (idx *MultiColumnarIndex) GetRowIDsInRange(minValues, maxValues []storage.ColumnValue,
	includeMin, includeMax bool) []int64 {

	// Validate input
	if (minValues != nil && len(minValues) > len(idx.columnIDs)) || (maxValues != nil && len(maxValues) > len(idx.columnIDs)) {
		return nil
	}

	// Default to first column for range query if no specific column is specified
	colIdx := 0
	min := minValues
	max := maxValues

	// If min/max specify values, ensure consistent length
	minLen := 0
	if min != nil {
		minLen = len(min)
	}

	maxLen := 0
	if max != nil {
		maxLen = len(max)
	}

	// Use the most specific (longest) constraint for initial filtering
	specificLen := minLen
	if maxLen > minLen {
		specificLen = maxLen
	}

	if specificLen > 0 {
		// Get initial result set from most specific column for range query
		colIdx = specificLen - 1 // Use the last column with a constraint

		// Prepare column-specific min/max
		var colMin, colMax []storage.ColumnValue
		if minLen > colIdx {
			colMin = []storage.ColumnValue{min[colIdx]}
		}
		if maxLen > colIdx {
			colMax = []storage.ColumnValue{max[colIdx]}
		}

		// Get rows in range for selected column
		rowIDs := idx.columnIndices[colIdx].GetRowIDsInRange(colMin, colMax, includeMin, includeMax)
		if len(rowIDs) == 0 {
			return nil // No results match the range
		}

		// For multi-column range query, we need to filter with other columns
		if specificLen > 1 {
			// Use the optimized sorted IDs intersection instead of maps
			// This is much more efficient than the previous implementation
			for i := 0; i < specificLen; i++ {
				if i == colIdx {
					continue // Skip the column we already used
				}

				// Prepare column-specific min/max
				var colMin, colMax []storage.ColumnValue
				if minLen > i {
					colMin = []storage.ColumnValue{min[i]}
				}
				if maxLen > i {
					colMax = []storage.ColumnValue{max[i]}
				}

				// Skip if no constraint for this column
				if colMin == nil && colMax == nil {
					continue
				}

				// Get matches for this column's range
				colMatches := idx.columnIndices[i].GetRowIDsInRange(colMin, colMax, includeMin, includeMax)
				if len(colMatches) == 0 {
					return nil // No matches for this column
				}

				// Use the fast intersection function from columnar_iterator.go
				rowIDs = intersectSortedIDs(rowIDs, colMatches)

				// If intersection is empty, early return
				if len(rowIDs) == 0 {
					return nil
				}
			}
		}

		return rowIDs
	}

	// If no specific range was provided, return all rows
	return idx.columnIndices[0].GetRowIDsInRange(nil, nil, false, false)
}

// GetRowIDsFromColumnarIndex is a specialized function for getting row IDs directly from this index
// by applying expressions to it. It is used by GetFilteredRowIDs to provide fast paths for performance.
func (idx *MultiColumnarIndex) GetRowIDsFromColumnarIndex(expr storage.Expression) []int64 {
	// Implementation that provides direct fast paths instead of delegating to individual column indices
	if expr == nil {
		return nil
	}

	// For multi-column AND expressions with simple equality conditions, we can use specialized fast paths
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		// Check if this is a multi-column equality expression where each expression matches a column
		// in this index - this is the most common case for multi-column indexes
		columnExprMap := make(map[string]*expression.SimpleExpression)
		rangeExprs := make(map[string]*expression.SimpleExpression)
		allSimple := true

		// Check if all sub-expressions are simple expressions on our columns
		// and separate into equality and range conditions
		for _, subExpr := range andExpr.Expressions {
			if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
				// Check if this column is part of our index
				foundColumn := false
				for _, colName := range idx.columnNames {
					if simpleExpr.Column == colName {
						if simpleExpr.Operator == storage.EQ {
							columnExprMap[colName] = simpleExpr
						} else if simpleExpr.Operator == storage.GT ||
							simpleExpr.Operator == storage.GTE ||
							simpleExpr.Operator == storage.LT ||
							simpleExpr.Operator == storage.LTE {
							// Store range expressions separately
							rangeExprs[colName] = simpleExpr
						}
						foundColumn = true
						break
					}
				}
				if !foundColumn {
					allSimple = false
					break
				}
			} else {
				allSimple = false
				break
			}
		}

		// If we have equality expressions for multiple columns in our index,
		// we can use GetRowIDsEqual for a direct fast path
		if allSimple && len(columnExprMap) > 1 {
			// Collect values in the right order for our multi-column index
			values := make([]storage.ColumnValue, len(idx.columnNames))
			valuesProvided := 0

			for i, colName := range idx.columnNames {
				if expr, exists := columnExprMap[colName]; exists {
					values[i] = storage.ValueToPooledColumnValue(expr.Value, idx.dataTypes[i])
					defer storage.PutPooledColumnValue(values[i])
					valuesProvided++
				} else {
					// This is a partial match (not all columns provided)
					// We'll trim the values array later
					values[i] = nil
				}
			}

			// Only use this fast path if we have at least 2 columns specified
			if valuesProvided >= 2 {
				// For partial matches, trim the values array to only include specified columns
				if valuesProvided < len(idx.columnNames) {
					trimmedValues := make([]storage.ColumnValue, valuesProvided)
					valueIndex := 0
					for i, colName := range idx.columnNames {
						if _, exists := columnExprMap[colName]; exists {
							trimmedValues[valueIndex] = values[i]
							valueIndex++
						}
					}
					values = trimmedValues
				}

				// First get results using the equality conditions
				result := idx.GetRowIDsEqual(values)

				// If we also have range conditions, filter the results further
				if len(rangeExprs) > 0 && len(result) > 0 {
					// Process each range expression to further filter the results
					for colName, rangeExpr := range rangeExprs {
						for i, indexColName := range idx.columnNames {
							if colName == indexColName {
								// Convert the expression to column-specific range
								var minValue, maxValue storage.ColumnValue
								var includeMin, includeMax bool

								// Setup range based on operator type
								if rangeExpr.Operator == storage.GT || rangeExpr.Operator == storage.GTE {
									minValue = storage.ValueToPooledColumnValue(rangeExpr.Value, idx.dataTypes[i])
									defer storage.PutPooledColumnValue(minValue)
									includeMin = rangeExpr.Operator == storage.GTE
								} else {
									maxValue = storage.ValueToPooledColumnValue(rangeExpr.Value, idx.dataTypes[i])
									defer storage.PutPooledColumnValue(maxValue)
									includeMax = rangeExpr.Operator == storage.LTE
								}

								// Get the range condition results
								rangeResult := idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

								// Intersect with our current results
								if len(rangeResult) > 0 {
									result = intersectSortedIDs(result, rangeResult)
									if len(result) == 0 {
										return nil // Empty result after intersection, exit early
									}
								} else {
									return nil // No matches for this range condition
								}
								break
							}
						}
					}
				}

				return result
			}
		}

		// Special case for multi-column AND with range conditions
		// If we have range expressions on multiple columns in our index
		if allSimple && len(rangeExprs) > 0 {
			var result []int64
			var firstCol bool = true

			// Process each column's range conditions
			for colName, rangeExpr := range rangeExprs {
				for i, indexColName := range idx.columnNames {
					if colName == indexColName {
						// Convert the expression to column-specific range
						var minValue, maxValue storage.ColumnValue
						var includeMin, includeMax bool

						// Setup range based on operator type
						if rangeExpr.Operator == storage.GT || rangeExpr.Operator == storage.GTE {
							minValue = storage.ValueToPooledColumnValue(rangeExpr.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(minValue)
							includeMin = rangeExpr.Operator == storage.GTE
						} else {
							maxValue = storage.ValueToPooledColumnValue(rangeExpr.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(maxValue)
							includeMax = rangeExpr.Operator == storage.LTE
						}

						// Get the range condition results
						rangeResult := idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

						// For first column, initialize result; otherwise intersect
						if firstCol {
							result = rangeResult
							firstCol = false
						} else if len(rangeResult) > 0 {
							result = intersectSortedIDs(result, rangeResult)
							if len(result) == 0 {
								return nil // Empty result after intersection, exit early
							}
						} else {
							return nil // No matches for this range condition
						}
						break
					}
				}
			}

			// If we also have equality conditions, filter the results further
			if len(columnExprMap) > 0 && len(result) > 0 {
				for colName, eqExpr := range columnExprMap {
					for i, indexColName := range idx.columnNames {
						if colName == indexColName {
							// Get equality results
							eqValue := storage.ValueToPooledColumnValue(eqExpr.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(eqValue)
							eqResult := idx.columnIndices[i].GetRowIDsEqual([]storage.ColumnValue{eqValue})

							// Intersect with our current results
							if len(eqResult) > 0 {
								result = intersectSortedIDs(result, eqResult)
								if len(result) == 0 {
									return nil // Empty result after intersection
								}
							} else {
								return nil // No matches for this equality condition
							}
							break
						}
					}
				}
			}

			return result
		}
	}

	// Delegate to the regular path
	return idx.getFilteredRowIDsImpl(expr)
}

// unionSortedIDs efficiently combines two sorted arrays of int64s into a single sorted array
// with no duplicates, similar to the SQL UNION operation
func unionSortedIDs(a, b []int64) []int64 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	// Pre-allocate result with a reasonable capacity
	result := make([]int64, 0, len(a)+len(b)/2) // Assuming ~50% overlap as a heuristic

	// Use a merge algorithm similar to merge sort to efficiently combine the arrays
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else if a[i] > b[j] {
			result = append(result, b[j])
			j++
		} else {
			// Equal values - add only once and advance both indices
			result = append(result, a[i])
			i++
			j++
		}
	}

	// Add any remaining elements
	if i < len(a) {
		result = append(result, a[i:]...)
	}
	if j < len(b) {
		result = append(result, b[j:]...)
	}

	return result
}

// getFilteredRowIDsImpl is the implementation logic for GetFilteredRowIDs
// This exists as a separate method to make the fast-path logic in GetRowIDsFromColumnarIndex cleaner
func (idx *MultiColumnarIndex) getFilteredRowIDsImpl(expr storage.Expression) []int64 {
	// Special optimization for AndExpressions with multiple columns
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		return idx.handleAndExpression(andExpr)
	}
	if expr == nil {
		return nil
	}

	// Fast path for SimpleExpression with equality or NULL checks (most common cases)
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Find the column index that matches this expression
		for i, colName := range idx.columnNames {
			if simpleExpr.Column == colName {
				// Direct fast path based on operator for better performance
				switch simpleExpr.Operator {
				case storage.EQ:
					// Convert the expression value to a ColumnValue with the correct type
					valueCol := storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[i])
					defer storage.PutPooledColumnValue(valueCol)
					return idx.columnIndices[i].GetRowIDsEqual([]storage.ColumnValue{valueCol})

				case storage.GT, storage.GTE, storage.LT, storage.LTE:
					// Range query - simple inequality with optimized fast path
					var minValue, maxValue storage.ColumnValue
					var includeMin, includeMax bool

					if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
						minValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[i])
						defer storage.PutPooledColumnValue(minValue)
						includeMin = simpleExpr.Operator == storage.GTE
					} else {
						maxValue = storage.ValueToPooledColumnValue(simpleExpr.Value, idx.dataTypes[i])
						defer storage.PutPooledColumnValue(maxValue)
						includeMax = simpleExpr.Operator == storage.LTE
					}

					return idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

				case storage.ISNULL, storage.ISNOTNULL:
					// Use the optimized NULL check fast path
					return idx.columnIndices[i].GetFilteredRowIDs(simpleExpr)

				default:
					// For other operators, delegate to the column index implementation
					return idx.columnIndices[i].GetFilteredRowIDs(simpleExpr)
				}
			}
		}
	}

	// Fast path for RangeExpression (optimized range queries)
	if rangeExpr, ok := expr.(*expression.RangeExpression); ok {
		// Find the column index that matches this range expression
		for i, colName := range idx.columnNames {
			if rangeExpr.Column == colName {
				// Direct fast path using GetRowIDsInRange for better performance
				var minValue, maxValue storage.ColumnValue

				if rangeExpr.MinValue != nil {
					minValue = storage.ValueToPooledColumnValue(rangeExpr.MinValue, idx.dataTypes[i])
					defer storage.PutPooledColumnValue(minValue)
				}

				if rangeExpr.MaxValue != nil {
					maxValue = storage.ValueToPooledColumnValue(rangeExpr.MaxValue, idx.dataTypes[i])
					defer storage.PutPooledColumnValue(maxValue)
				}

				return idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue},
					rangeExpr.IncludeMin, rangeExpr.IncludeMax)
			}
		}
	}

	// Fast path for BetweenExpression (BETWEEN queries)
	if betweenExpr, ok := expr.(*expression.BetweenExpression); ok {
		// Find the column index that matches this between expression
		for i, colName := range idx.columnNames {
			if betweenExpr.Column == colName {
				// Convert lower and upper bounds to appropriate column values
				lowerValue := storage.ValueToPooledColumnValue(betweenExpr.LowerBound, idx.dataTypes[i])
				defer storage.PutPooledColumnValue(lowerValue)

				upperValue := storage.ValueToPooledColumnValue(betweenExpr.UpperBound, idx.dataTypes[i])
				defer storage.PutPooledColumnValue(upperValue)

				// Use the optimized range fast path with proper inclusivity
				return idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{lowerValue}, []storage.ColumnValue{upperValue},
					betweenExpr.Inclusive, betweenExpr.Inclusive)
			}
		}
	}

	// Fast path for AndExpression with range conditions on a single column
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		if len(andExpr.Expressions) == 2 {
			// Check if both expressions are SimpleExpressions for the same column
			expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
			expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

			if ok1 && ok2 && expr1.Column == expr2.Column {
				// Find the column index
				for i, colName := range idx.columnNames {
					if expr1.Column == colName {
						// Check for range patterns like (col > X AND col < Y)
						var minValue, maxValue storage.ColumnValue
						var includeMin, includeMax bool
						var hasRange bool

						// Check if expr1 is a lower bound and expr2 is an upper bound
						if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
							(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {
							minValue = storage.ValueToPooledColumnValue(expr1.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(minValue)
							includeMin = expr1.Operator == storage.GTE
							maxValue = storage.ValueToPooledColumnValue(expr2.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(maxValue)
							includeMax = expr2.Operator == storage.LTE
							hasRange = true
						}

						// Check if expr2 is a lower bound and expr1 is an upper bound
						if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
							(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {
							minValue = storage.ValueToPooledColumnValue(expr2.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(minValue)
							includeMin = expr2.Operator == storage.GTE
							maxValue = storage.ValueToPooledColumnValue(expr1.Value, idx.dataTypes[i])
							defer storage.PutPooledColumnValue(maxValue)
							includeMax = expr1.Operator == storage.LTE
							hasRange = true
						}

						if hasRange {
							return idx.columnIndices[i].GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)
						}
					}
				}
			}
		}

		// For other AND expressions, delegate to the standard path for intersection
		var result []int64
		var firstResult bool = true

		for _, subExpr := range andExpr.Expressions {
			// Get results for this sub-expression
			subResult := idx.getFilteredRowIDsImpl(subExpr)
			if len(subResult) == 0 {
				return nil // Empty intersection
			}

			// For first result, initialize result set
			if firstResult {
				result = subResult
				firstResult = false
				continue
			}

			// Use the optimized sorted intersection function
			// This is much more efficient than using maps
			result = intersectSortedIDs(result, subResult)

			// If intersection is empty, early return
			if len(result) == 0 {
				return nil
			}
		}

		return result
	}

	// Handle OR expressions - union results from each sub-expression
	if orExpr, ok := expr.(*expression.OrExpression); ok {
		var result []int64
		var firstExpr = true

		for _, subExpr := range orExpr.Expressions {
			// Get results for this sub-expression
			subResult := idx.getFilteredRowIDsImpl(subExpr)

			// For first expression, initialize result
			if firstExpr {
				result = subResult
				firstExpr = false
				continue
			}

			// For subsequent expressions, use optimized union operation
			if len(subResult) > 0 {
				result = unionSortedIDs(result, subResult)
			}
		}

		return result
	}

	// For unsupported expressions, return nil
	return nil
}

// GetFilteredRowIDs returns row IDs that match the given expression
func (idx *MultiColumnarIndex) GetFilteredRowIDs(expr storage.Expression) []int64 {
	// Fast path for IS NOT NULL/IS NULL expressions
	if nullCheckExpr, ok := expr.(*expression.NullCheckExpression); ok {
		columnName := nullCheckExpr.GetColumnName()

		// Find which column in our multi-column index this expression is for
		colIdx := -1
		for i, colName := range idx.columnNames {
			if colName == columnName {
				colIdx = i
				break
			}
		}

		// If we found the column, delegate to the single-column index
		if colIdx >= 0 && colIdx < len(idx.columnIndices) {
			result := idx.columnIndices[colIdx].GetFilteredRowIDs(nullCheckExpr)
			return result
		}
	}

	// Try to use other specialized fast paths if possible
	result := idx.GetRowIDsFromColumnarIndex(expr)
	if result != nil {
		return result
	}

	return idx.getFilteredRowIDsImpl(expr)
}

// Build builds or rebuilds the index from the version store
func (idx *MultiColumnarIndex) Build() error {
	// Clear uniqueness map if using unique constraint
	if idx.isUnique {
		idx.mutex.Lock()
		idx.uniqueValues = make(map[string]int64)
		idx.mutex.Unlock()
	}

	// Build each column index
	for _, colIdx := range idx.columnIndices {
		if err := colIdx.Build(); err != nil {
			return err
		}
	}

	// If using unique constraint, rebuild the uniqueness map
	if idx.isUnique && idx.vs != nil {
		// Get all visible versions from the version store
		visibleVersions := idx.vs.GetAllVisibleVersions(0)

		// Process each visible row
		visibleVersions.ForEach(func(rowID int64, version *RowVersion) bool {
			if version.IsDeleted() {
				return true
			}

			// Extract values for all columns in this index
			values := make([]storage.ColumnValue, len(idx.columnIDs))
			for i, colID := range idx.columnIDs {
				if colID < len(version.Data) {
					values[i] = version.Data[colID]
				} else {
					// Column doesn't exist in this row, treat as NULL
					values[i] = nil
				}
			}

			// Create uniqueness key and add to map
			key := idx.createKey(values)
			if key != "" {
				idx.mutex.Lock()
				idx.uniqueValues[key] = rowID
				idx.mutex.Unlock()
			}

			return true
		})
	}

	return nil
}

// handleAndExpression is a specialized function for handling AND expressions
// that optimizes for expressions involving multiple columns in the multi-column index
func (idx *MultiColumnarIndex) handleAndExpression(andExpr *expression.AndExpression) []int64 {
	// First, organize expressions by column for better pushdown
	// Use a regular map here as this is a string map with infrequent operations
	columnExprs := make(map[string][]storage.Expression)

	// Classify expressions by their column
	for _, subExpr := range andExpr.Expressions {
		// For SimpleExpression, we can determine the column directly
		if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
			columnExprs[simpleExpr.Column] = append(columnExprs[simpleExpr.Column], simpleExpr)
		} else if nullExpr, ok := subExpr.(*expression.NullCheckExpression); ok {
			columnExprs[nullExpr.GetColumnName()] = append(columnExprs[nullExpr.GetColumnName()], nullExpr)
		} else if rangeExpr, ok := subExpr.(*expression.RangeExpression); ok {
			columnExprs[rangeExpr.Column] = append(columnExprs[rangeExpr.Column], rangeExpr)
		} else if betweenExpr, ok := subExpr.(*expression.BetweenExpression); ok {
			columnExprs[betweenExpr.Column] = append(columnExprs[betweenExpr.Column], betweenExpr)
		}
		// Other expression types will be handled by the fallback path
	}

	// Check if we have expressions for columns in our index
	var columnsToUse []int
	for i, colName := range idx.columnNames {
		if _, exists := columnExprs[colName]; exists {
			columnsToUse = append(columnsToUse, i)
		}
	}

	// If we have expressions for multiple columns that are part of this index,
	// apply each column's conditions separately using the column indices
	if len(columnsToUse) > 1 {
		var result []int64
		var firstCol = true

		// For each column in our index that has conditions
		for _, colIdx := range columnsToUse {
			colName := idx.columnNames[colIdx]
			colExprs := columnExprs[colName]

			// If multiple expressions for this column, combine them with AND
			var colResult []int64
			if len(colExprs) > 1 {
				// Create a combined AND expression for just this column
				colAndExpr := &expression.AndExpression{
					Expressions: colExprs,
				}
				// Get results using the column's index
				colResult = idx.columnIndices[colIdx].GetFilteredRowIDs(colAndExpr)
			} else if len(colExprs) == 1 {
				// Single expression, use directly
				colResult = idx.columnIndices[colIdx].GetFilteredRowIDs(colExprs[0])
			}

			// If any column returns no results, the intersection will be empty
			if len(colResult) == 0 {
				return nil
			}

			// Build the result through intersection
			if firstCol {
				result = colResult
				firstCol = false
			} else {
				// Use optimized intersection of sorted IDs
				result = intersectSortedIDs(result, colResult)

				// Early exit if intersection becomes empty
				if len(result) == 0 {
					return nil
				}
			}
		}

		// Check for expressions not related to our columns
		var otherExprs []storage.Expression
		for _, subExpr := range andExpr.Expressions {
			isHandled := false

			// Check if this expression was for a column we already processed
			if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
				for _, colName := range idx.columnNames {
					if simpleExpr.Column == colName {
						isHandled = true
						break
					}
				}
			} else if nullExpr, ok := subExpr.(*expression.NullCheckExpression); ok {
				for _, colName := range idx.columnNames {
					if nullExpr.GetColumnName() == colName {
						isHandled = true
						break
					}
				}
			} else if rangeExpr, ok := subExpr.(*expression.RangeExpression); ok {
				for _, colName := range idx.columnNames {
					if rangeExpr.Column == colName {
						isHandled = true
						break
					}
				}
			} else if betweenExpr, ok := subExpr.(*expression.BetweenExpression); ok {
				for _, colName := range idx.columnNames {
					if betweenExpr.Column == colName {
						isHandled = true
						break
					}
				}
			}

			// If not handled yet, add to other expressions
			if !isHandled {
				otherExprs = append(otherExprs, subExpr)
			}
		}

		// If we have other expressions, apply them to our current result
		if len(otherExprs) > 0 && len(result) > 0 {
			// Process each expression separately
			for _, otherExpr := range otherExprs {
				// Process using standard method
				otherResult := idx.standardGetFilteredRowIDs(otherExpr)
				if len(otherResult) == 0 {
					return nil
				}

				// Intersect with current result
				result = intersectSortedIDs(result, otherResult)
				if len(result) == 0 {
					return nil
				}
			}
		}

		return result
	}

	// If we only have expressions for one or zero of our columns,
	// fall back to the standard implementation
	return idx.standardGetFilteredRowIDs(andExpr)
}

// standardGetFilteredRowIDs is the standard implementation for filtering rows
// when we don't have specialized optimizations
func (idx *MultiColumnarIndex) standardGetFilteredRowIDs(expr storage.Expression) []int64 {
	// Handle AND expressions
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		var result []int64
		var firstResult bool = true

		for _, subExpr := range andExpr.Expressions {
			// Get results for this sub-expression
			subResult := idx.getFilteredRowIDsImpl(subExpr)
			if len(subResult) == 0 {
				return nil // Empty intersection
			}

			// For first result, initialize result set
			if firstResult {
				result = subResult
				firstResult = false
				continue
			}

			// Use the optimized sorted intersection function
			// This is much more efficient than using maps
			result = intersectSortedIDs(result, subResult)

			// If intersection is empty, early return
			if len(result) == 0 {
				return nil
			}
		}

		return result
	}

	// Handle OR expressions - union results from each sub-expression
	if orExpr, ok := expr.(*expression.OrExpression); ok {
		var result []int64
		var firstExpr = true

		for _, subExpr := range orExpr.Expressions {
			// Get results for this sub-expression
			subResult := idx.getFilteredRowIDsImpl(subExpr)

			// For first expression, initialize result
			if firstExpr {
				result = subResult
				firstExpr = false
				continue
			}

			// For subsequent expressions, use optimized union operation
			if len(subResult) > 0 {
				result = unionSortedIDs(result, subResult)
			}
		}

		return result
	}

	// For specific column expressions, delegate to the appropriate single column index
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		for i, colName := range idx.columnNames {
			if simpleExpr.Column == colName {
				return idx.columnIndices[i].GetFilteredRowIDs(expr)
			}
		}
	} else if nullExpr, ok := expr.(*expression.NullCheckExpression); ok {
		for i, colName := range idx.columnNames {
			if nullExpr.GetColumnName() == colName {
				return idx.columnIndices[i].GetFilteredRowIDs(expr)
			}
		}
	} else if rangeExpr, ok := expr.(*expression.RangeExpression); ok {
		for i, colName := range idx.columnNames {
			if rangeExpr.Column == colName {
				return idx.columnIndices[i].GetFilteredRowIDs(expr)
			}
		}
	} else if betweenExpr, ok := expr.(*expression.BetweenExpression); ok {
		for i, colName := range idx.columnNames {
			if betweenExpr.Column == colName {
				return idx.columnIndices[i].GetFilteredRowIDs(expr)
			}
		}
	}

	// For unsupported expressions, return empty result
	return nil
}

// Close releases any resources
func (idx *MultiColumnarIndex) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Close column indices
	for _, colIdx := range idx.columnIndices {
		if err := colIdx.Close(); err != nil {
			return err
		}
	}

	// Clear uniqueness map
	idx.uniqueValues = nil
	return nil
}
