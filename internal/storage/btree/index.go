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
// Package btree implements B-tree indexes for high-cardinality columns.
package btree

import (
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Index represents a B-tree index for one or more columns
type Index struct {
	// columnNames are the names of the columns this index is for
	columnNames []string
	// btree is the B-tree data structure
	btree *BTree
	// rowCount is the number of rows in the column
	rowCount int64
	// dataTypes are the data types of the columns
	dataTypes []storage.DataType
	// mutex protects concurrent access to the index
	mutex sync.RWMutex
}

// NewIndex creates a new B-tree index for one or more columns
func NewIndex(columnNames []string, dataTypes []storage.DataType, rowCount int64) *Index {
	// For multi-column indexes, we use the data type of the first column for the B-tree
	// since the keys will be composite strings
	primaryDataType := storage.TEXT
	if len(dataTypes) > 0 {
		primaryDataType = dataTypes[0]
	}

	return &Index{
		columnNames: columnNames,
		btree:       NewBTree(primaryDataType, DefaultOrder, DefaultLeafCapacity),
		rowCount:    rowCount,
		dataTypes:   dataTypes,
	}
}

// NewSingleColumnIndex creates a new B-tree index for a single column
func NewSingleColumnIndex(columnName string, dataType storage.DataType, rowCount int64) *Index {
	return NewIndex([]string{columnName}, []storage.DataType{dataType}, rowCount)
}

// AddValue adds a value to the index at the specified position
func (idx *Index) AddValue(value interface{}, position int64) error {
	// For backward compatibility with single column indexes
	if len(idx.columnNames) == 1 {
		return idx.AddValues([]interface{}{value}, position)
	}

	return errors.New("AddValue only supports single-column indexes, use AddValues for multi-column indexes")
}

// AddValues adds multiple column values to the index at the specified position
func (idx *Index) AddValues(values []interface{}, position int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Validate position is not negative
	if position < 0 {
		return errors.New("position cannot be negative")
	}

	// If position is beyond current rowCount, update rowCount instead of failing
	if position >= idx.rowCount {
		idx.rowCount = position + 1
	}

	// Validate that we have the right number of values
	if len(values) != len(idx.columnNames) {
		return errors.New("number of values does not match number of columns in index")
	}

	// Create a composite key by concatenating all values
	key := idx.createCompositeKey(values)

	// Insert into the B-tree
	idx.btree.insertNonLocked(key, position)

	return nil
}

// createCompositeKey creates a composite key from multiple column values
func (idx *Index) createCompositeKey(values []interface{}) string {
	// For a single value, just convert it directly
	if len(values) == 1 {
		return storage.ConvertValueToString(values[0])
	}

	// For multiple values, concatenate them with a separator
	var keyParts []string
	for _, value := range values {
		// Convert value to string format based on data type
		valueStr := storage.ConvertValueToString(value)
		// Escape any separator characters in the value
		valueStr = strings.ReplaceAll(valueStr, "|", "\\|")
		keyParts = append(keyParts, valueStr)
	}

	// Join with a separator that's unlikely to appear in normal data
	return strings.Join(keyParts, "|")
}

// GetMatchingPositions returns a slice of positions matching the specified condition
func (idx *Index) GetMatchingPositions(op storage.Operator, value interface{}) ([]int64, error) {
	// For backward compatibility with single-column operations
	if len(idx.columnNames) == 1 {
		return idx.GetMatchingPositionsMulti(op, []interface{}{value})
	}

	return nil, errors.New("for multi-column indexes, use GetMatchingPositionsMulti")
}

// GetMatchingPositionsMulti returns a slice of positions matching the specified condition on multiple columns
func (idx *Index) GetMatchingPositionsMulti(op storage.Operator, values []interface{}) ([]int64, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Validate input for multi-column operations
	if len(values) != len(idx.columnNames) && op != storage.ISNULL && op != storage.ISNOTNULL {
		return nil, errors.New("number of values does not match number of columns in index")
	}

	// Handle NULL checks first - this is where we need to correctly identify NULL values
	if op == storage.ISNULL {
		// Find NULL values - in BTree, an empty string key represents NULL
		// We use empty string as the NULL marker in the index
		results, err := idx.btree.Search("")
		if err != nil {
			if err == ErrKeyNotFound {
				// No NULL values found, return empty slice
				return []int64{}, nil
			}
			return nil, err
		}
		return results, nil
	} else if op == storage.ISNOTNULL {
		// Find non-NULL values by collecting all positions with non-empty keys
		// This is a more direct approach that doesn't rely on calculating complements

		// Create a map to deduplicate positions
		nonNullPositions := make(map[int64]bool)

		// Iterate through all keys in the BTree and collect positions
		idx.btree.IterateKeys(func(key string, positions []int64) bool {
			// Skip the NULL key (empty string)
			if key == "" {
				return true
			}

			// Add all positions for this key
			for _, pos := range positions {
				nonNullPositions[pos] = true
			}

			return true
		})

		// Convert the map to a slice
		result := make([]int64, 0, len(nonNullPositions))
		for pos := range nonNullPositions {
			result = append(result, pos)
		}

		return result, nil
	}

	// Create composite key for multi-column operations
	key := idx.createCompositeKey(values)

	switch op {
	case storage.EQ:
		return idx.btree.Search(key)
	case storage.NE:
		// For not equals, find all values except this one
		return idx.getNotEqualPositions(key)
	case storage.LT:
		// Find all values less than the key
		return idx.btree.RangeSearch("", key)
	case storage.LTE:
		// Find all values less than or equal to the key
		// For this, we need to include the key itself, so we increment the end of the range
		return idx.btree.RangeSearch("", idx.nextKey(key))
	case storage.GT:
		// Find all values greater than the key
		return idx.btree.RangeSearch(idx.nextKey(key), "\uffff")
	case storage.GTE:
		// Find all values greater than or equal to the key
		return idx.btree.RangeSearch(key, "\uffff")
	case storage.LIKE:
		// For multi-column indexes, LIKE is only useful with a prefix search on the first column
		if strings.HasSuffix(key, "%") && !strings.Contains(key[:len(key)-1], "%") {
			prefix := key[:len(key)-1]
			return idx.btree.PrefixSearch(prefix)
		}
		// For other LIKE patterns, we can't use the B-tree effectively
		return nil, errors.New("complex LIKE patterns not supported by B-tree index")
	case storage.IN:
		// For IN with multi-column indexes, we use the composite key approach
		return idx.getMultiColumnInPositions(values)
	case storage.NOTIN:
		// For NOT IN with multi-column indexes
		return idx.getMultiColumnNotInPositions(values)
	default:
		return nil, fmt.Errorf("operator %v not supported by B-tree index", op)
	}
}

// getMultiColumnInPositions handles IN operations for multi-column indexes
func (idx *Index) getMultiColumnInPositions(values interface{}) ([]int64, error) {
	// Check if values is a slice of slices (for multi-column IN operations)
	valuesList, ok := values.([]interface{})
	if !ok {
		return nil, errors.New("IN operator for multi-column indexes requires a slice of value sets")
	}

	// For single-column indexes, handle as before
	if len(idx.columnNames) == 1 {
		return idx.getInPositions(values)
	}

	// Create a set to deduplicate positions
	resultSet := make(map[int64]bool)

	// For each value set in the IN clause, create a composite key and find matches
	for _, valueSet := range valuesList {
		valSetSlice, ok := valueSet.([]interface{})
		if !ok || len(valSetSlice) != len(idx.columnNames) {
			return nil, errors.New("each value set in multi-column IN must match the number of index columns")
		}

		// Create composite key for this value set
		key := idx.createCompositeKey(valSetSlice)

		// Search for matches
		positions, err := idx.btree.Search(key)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		// Add positions to result set
		for _, pos := range positions {
			resultSet[pos] = true
		}
	}

	// Convert the set to a slice
	result := make([]int64, 0, len(resultSet))
	for pos := range resultSet {
		result = append(result, pos)
	}

	return result, nil
}

// getMultiColumnNotInPositions handles NOT IN operations for multi-column indexes
func (idx *Index) getMultiColumnNotInPositions(values interface{}) ([]int64, error) {
	// Get positions where the value is in the set
	inPositions, err := idx.getMultiColumnInPositions(values)
	if err != nil {
		return nil, err
	}

	// Create a map of positions to exclude
	excludeMap := make(map[int64]bool)
	for _, pos := range inPositions {
		excludeMap[pos] = true
	}

	// Create a list of all positions except those in the exclude map
	result := make([]int64, 0, int(idx.rowCount)-len(excludeMap))
	for i := int64(0); i < idx.rowCount; i++ {
		if !excludeMap[i] {
			result = append(result, i)
		}
	}

	return result, nil
}

// getNotEqualPositions returns all positions where the value is not equal to the key
func (idx *Index) getNotEqualPositions(key string) ([]int64, error) {
	// Find positions with the specified key
	equalPositions, err := idx.btree.Search(key)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}

	// Create a map of positions to exclude
	excludeMap := make(map[int64]bool)
	for _, pos := range equalPositions {
		excludeMap[pos] = true
	}

	// Create a list of all positions except those in the exclude map
	result := make([]int64, 0, int(idx.rowCount)-len(excludeMap))
	for i := int64(0); i < idx.rowCount; i++ {
		if !excludeMap[i] {
			result = append(result, i)
		}
	}

	return result, nil
}

// getInPositions returns all positions where the value is in the set
func (idx *Index) getInPositions(value interface{}) ([]int64, error) {
	// Check if value is a slice
	values, ok := value.([]interface{})
	if !ok {
		return nil, errors.New("IN operator requires a slice of values")
	}

	if len(values) == 0 {
		// Empty set, no matches
		return []int64{}, nil
	}

	// Create a set to deduplicate positions
	resultSet := make(map[int64]bool)

	// Search for each value and add positions to the result set
	for _, val := range values {
		key := storage.ConvertValueToString(val)
		positions, err := idx.btree.Search(key)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		for _, pos := range positions {
			resultSet[pos] = true
		}
	}

	// Convert the set to a slice
	result := make([]int64, 0, len(resultSet))
	for pos := range resultSet {
		result = append(result, pos)
	}

	return result, nil
}

// RemoveValuePosition removes a specific position from the positions list for a key
func (idx *Index) RemoveValuePosition(key string, position int64) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Use the btree to find the node containing this key
	node, err := idx.btree.searchNode(key)
	if err != nil {
		return err
	}

	// Find the key in the node
	keyIndex := -1
	for i, k := range node.keys {
		if k == key {
			keyIndex = i
			break
		}
	}

	if keyIndex < 0 {
		return nil // Key not found
	}

	// Get the positions list for this key
	positions := node.values[keyIndex]

	// Find and remove the position
	for i, pos := range positions {
		if pos == position {
			// Remove this position by replacing it with the last element and shrinking the slice
			if i < len(positions)-1 {
				positions[i] = positions[len(positions)-1]
			}
			positions = positions[:len(positions)-1]

			// Update the node's values
			node.values[keyIndex] = positions

			break
		}
	}

	return nil
}

// nextKey returns the lexicographically next key after the given key
// This is used for implementing LE by using LT with the next key
func (idx *Index) nextKey(key string) string {
	// Special case for empty string
	if key == "" {
		return "0"
	}

	// Add a null byte to the end of the key
	return key + "\u0000"
}

// Cardinality returns the cardinality of the index (number of distinct values)
func (idx *Index) Cardinality() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.btree.Size()
}

// RowCount returns the number of rows in the column
func (idx *Index) RowCount() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.rowCount
}

// Save saves the B-tree index to disk
func (idx *Index) Save(dirPath string) error {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Create a directory name for the index
	// For multi-column indexes, create a hashed directory name to avoid issues with special chars
	columnsKey := strings.Join(idx.columnNames, "_")
	indexDirName := fmt.Sprintf("index_%x", md5.Sum([]byte(columnsKey)))

	// Create directory if it doesn't exist
	indexDir := filepath.Join(dirPath, indexDirName)
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return err
	}

	// Note: The actual index metadata is saved by the MetadataManager in Table.CreateIndex method
	// using the binser.IndexMetadata structure.

	// Save the B-tree
	return idx.btree.SaveToDisk(indexDir, "index")
}

// LoadFromDir loads a B-tree index from disk
func (idx *Index) LoadFromDir(dirPath string) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Check if the directory exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return fmt.Errorf("index directory does not exist: %s", dirPath)
	}

	// Load the B-tree
	btree, err := LoadBTreeFromDisk(dirPath, "index")
	if err != nil {
		return fmt.Errorf("failed to load B-tree: %w", err)
	}

	// Replace the existing B-tree
	idx.btree = btree

	// Set the row count based on the B-tree size
	idx.rowCount = btree.Size()

	return nil
}

// Load loads a B-tree index from disk
func Load(dirPath, indexKey string) (*Index, error) {
	indexDir := filepath.Join(dirPath, indexKey)

	// Check if the index directory exists
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return nil, errors.New("b-tree index does not exist")
	}

	// Note: The actual index metadata is expected to be loaded by the calling code
	// via the MetadataManager. Here we just need the basic information to create the index.
	// We'll try to extract the information from the indexKey which follows our naming convention:
	// - For single column: it's the column name itself
	// - For multi-column: it's in format "index_<md5hash>" where the hash is from joined column names

	var columnNames []string
	var dataTypes []storage.DataType
	var rowCount int64

	// Check if it's a multi-column index (starts with "index_")
	if strings.HasPrefix(indexKey, "index_") {
		// For now, we need to assume some defaults since we don't know the exact columns
		columnNames = []string{"multiple_columns"}
		dataTypes = []storage.DataType{storage.TEXT} // Default to TEXT
		rowCount = 0                                 // Will need to be determined from data
	} else {
		// Single column index
		columnNames = []string{indexKey}
		dataTypes = []storage.DataType{storage.TEXT} // Default to TEXT
		rowCount = 0                                 // Will need to be determined from data
	}

	// Load the B-tree
	btree, err := LoadFromDisk(indexDir, "index")
	if err != nil {
		return nil, fmt.Errorf("failed to load B-tree: %w", err)
	}

	// Create the index with the column names and data types
	idx := &Index{
		columnNames: columnNames,
		rowCount:    rowCount,
		dataTypes:   dataTypes,
		btree:       btree,
	}

	return idx, nil
}

// ShouldCreateBTreeIndex determines if a B-tree index should be created
// for this column based on cardinality analysis
func ShouldCreateBTreeIndex(values []interface{}, threshold float64) bool {
	if len(values) == 0 {
		return false
	}

	// Count distinct values
	valueSet := make(map[string]struct{})
	for _, v := range values {
		key := storage.ConvertValueToString(v)
		valueSet[key] = struct{}{}
	}

	// Calculate cardinality ratio
	cardinalityRatio := float64(len(valueSet)) / float64(len(values))

	// Create B-tree index if cardinality is above threshold
	return cardinalityRatio >= threshold
}
