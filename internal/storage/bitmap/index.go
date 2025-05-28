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
// Package bitmap implements bitmap indexes for low-cardinality columns.
//
// The bitmap index functionality is implemented and integrated with the storage engine
// to provide efficient filtering for low-cardinality columns. This includes automatic
// creation and update of bitmap indexes during data insertion, as well as their use
// in query filtering through the FilteredScanner.
package bitmap

import (
	"crypto/md5"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/binser"
)

// Index represents a bitmap index for one or more columns
type Index struct {
	// columnNames are the names of the columns this index is for
	columnNames []string
	// valueMap maps values to their corresponding bitmaps
	valueMap map[string]*Bitmap
	// rowCount is the number of rows in the column
	rowCount int64
	// dataTypes are the data types of the columns
	dataTypes []storage.DataType
	// mutex protects concurrent access to the bitmap index
	mutex sync.RWMutex
}

// NewIndex creates a new bitmap index for one or more columns
func NewIndex(columnNames []string, dataTypes []storage.DataType, rowCount int64) *Index {
	return &Index{
		columnNames: columnNames,
		valueMap:    make(map[string]*Bitmap),
		rowCount:    rowCount,
		dataTypes:   dataTypes,
	}
}

// NewSingleColumnIndex creates a new bitmap index for a single column
func NewSingleColumnIndex(columnName string, dataType storage.DataType, rowCount int64) *Index {
	return NewIndex([]string{columnName}, []storage.DataType{dataType}, rowCount)
}

// ColumnNames returns the column names this index is for
func (idx *Index) ColumnNames() []string {
	return idx.columnNames
}

// DataTypes returns the data types of the columns
func (idx *Index) DataTypes() []storage.DataType {
	return idx.dataTypes
}

// RowCount returns the number of rows in the index
func (idx *Index) RowCount() int64 {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	return idx.rowCount
}

// Clone creates a copy of the bitmap index
func (idx *Index) Clone() (*Index, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	newIdx := &Index{
		columnNames: make([]string, len(idx.columnNames)),
		valueMap:    make(map[string]*Bitmap, len(idx.valueMap)),
		rowCount:    idx.rowCount,
		dataTypes:   make([]storage.DataType, len(idx.dataTypes)),
	}

	copy(newIdx.columnNames, idx.columnNames)
	copy(newIdx.dataTypes, idx.dataTypes)

	for value, bitmap := range idx.valueMap {
		newIdx.valueMap[value] = bitmap.Clone()
	}

	return newIdx, nil
}

// AddValue adds a value at a specific position
func (idx *Index) AddValue(value interface{}, pos int64) error {
	if pos < 0 {
		return fmt.Errorf("position cannot be negative: %d", pos)
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Convert the value to a string representation
	strValue := storage.ConvertValueToString(value)

	// Get or create the bitmap for this value
	bitmap, exists := idx.valueMap[strValue]
	if !exists {
		bitmap = New(idx.rowCount)
		idx.valueMap[strValue] = bitmap
	}

	// Ensure the bitmap is large enough
	if pos >= bitmap.Size() {
		bitmap.Resize(pos + 1)
	}

	// Set the bit
	bitmap.Set(pos, true)

	// Update rowCount if needed
	if pos >= idx.rowCount {
		idx.rowCount = pos + 1
	}

	return nil
}

// AddValues adds multiple values to the index at the given position
// For single-column indexes, it uses only the first value
// For multi-column indexes, it creates a composite key from all values
func (idx *Index) AddValues(values []interface{}, pos int64) error {
	if pos < 0 {
		return fmt.Errorf("position cannot be negative: %d", pos)
	}

	// For single column indexes, just add the first value
	if len(idx.columnNames) == 1 && len(values) > 0 {
		return idx.AddValue(values[0], pos)
	}

	// For multi-column indexes, create a composite key
	if len(values) != len(idx.columnNames) {
		return fmt.Errorf("number of values (%d) does not match number of columns (%d)", len(values), len(idx.columnNames))
	}

	// Create a composite key from all values
	var compositeKey string
	for i, value := range values {
		if i > 0 {
			compositeKey += "|" // Use pipe as a separator
		}
		compositeKey += storage.ConvertValueToString(value)
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Get or create the bitmap for this composite value
	bitmap, exists := idx.valueMap[compositeKey]
	if !exists {
		bitmap = New(idx.rowCount)
		idx.valueMap[compositeKey] = bitmap
	}

	// Ensure the bitmap is large enough
	if pos >= bitmap.Size() {
		bitmap.Resize(pos + 1)
	}

	// Set the bit
	bitmap.Set(pos, true)

	// Update rowCount if needed
	if pos >= idx.rowCount {
		idx.rowCount = pos + 1
	}

	return nil
}

// ExtendEmpty extends all bitmaps by the specified number of rows
func (idx *Index) ExtendEmpty(n int64) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Update rowCount
	idx.rowCount += n

	// Resize all bitmaps
	for _, bitmap := range idx.valueMap {
		bitmap.Resize(idx.rowCount)
	}
}

// GetMatchingPositions returns positions where bitmap index matches the operation
func (idx *Index) GetMatchingPositions(op storage.Operator, value interface{}) (*Bitmap, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Special handling for NULL operations which need to check empty string value
	if op == storage.ISNULL || op == storage.ISNOTNULL {
		// For NULL, we use the empty string as the value in the bitmap index
		nullBitmap, nullExists := idx.valueMap[""]

		if op == storage.ISNULL {
			// IS NULL operator: return positions with NULL values
			if nullExists {
				// Return a copy of the NULL bitmap
				return nullBitmap.Clone(), nil
			}
			// No NULL values found, return empty bitmap
			return New(idx.rowCount), nil
		} else { // ISNOTNULL
			// Create a result bitmap with exactly the right size
			result := New(idx.rowCount)

			// For IS NOT NULL, we want to include all positions that have any non-null value
			// Iterate through all values except the empty string (NULL)
			for value, bitmap := range idx.valueMap {
				if value == "" {
					continue // Skip NULL values
				}

				// Add all positions from this value's bitmap to the result
				for i := int64(0); i < bitmap.Size() && i < idx.rowCount; i++ {
					hasValue, _ := bitmap.Get(i)
					if hasValue {
						result.Set(i, true)
					}
				}
			}

			return result, nil
		}
	}

	// For other operators, process normally
	// Convert value to string representation
	strValue := storage.ConvertValueToString(value)

	// Create a bitmap for the result
	result := New(idx.rowCount)

	switch op {
	case storage.EQ:
		// Get the bitmap for the value
		bitmap, exists := idx.valueMap[strValue]
		if !exists {
			// No match, return empty bitmap
			return result, nil
		}
		return bitmap.Clone(), nil

	case storage.NE:
		// Start with all bits set
		resultBitmap := New(idx.rowCount)
		resultBitmap.SetAll(true)

		// Subtract the matching bitmap (if it exists)
		bitmap, exists := idx.valueMap[strValue]
		if exists {
			// Clear bits that match
			for i := int64(0); i < bitmap.Size(); i++ {
				val, _ := bitmap.Get(i)
				if val {
					resultBitmap.Set(i, false)
				}
			}
		}
		return resultBitmap, nil

	case storage.IN:
		// Get list of values for IN operator
		values, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("IN operator requires a slice of values")
		}

		// Convert values to string representation
		strValues := make([]string, len(values))
		for i, v := range values {
			strValues[i] = storage.ConvertValueToString(v)
		}

		// Create a bitmap for the result
		// Start with empty bitmap
		resultBitmap := New(idx.rowCount)

		// Union all matching bitmaps
		for _, v := range strValues {
			bitmap, exists := idx.valueMap[v]
			if !exists {
				continue
			}

			// Set bits that match this value
			for i := int64(0); i < bitmap.Size(); i++ {
				val, _ := bitmap.Get(i)
				if val {
					resultBitmap.Set(i, true)
				}
			}
		}
		return resultBitmap, nil

	case storage.NOTIN:
		// Get list of values for NOT IN operator
		values, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("NOT IN operator requires a slice of values")
		}

		// Convert values to string representation
		strValues := make([]string, len(values))
		for i, v := range values {
			strValues[i] = storage.ConvertValueToString(v)
		}

		// Create a bitmap for the result
		// Start with all bits set
		resultBitmap := New(idx.rowCount)
		resultBitmap.SetAll(true)

		// Subtract all matching bitmaps
		for _, v := range strValues {
			bitmap, exists := idx.valueMap[v]
			if !exists {
				continue
			}

			// Clear bits that match this value
			for i := int64(0); i < bitmap.Size(); i++ {
				val, _ := bitmap.Get(i)
				if val {
					resultBitmap.Set(i, false)
				}
			}
		}
		return resultBitmap, nil

	case storage.LIKE:
		// Handle LIKE operator for text data with proper SQL wildcards
		// Delegate to the dedicated function for LIKE pattern matching
		return idx.GetMatchingLike(strValue)

	case storage.GT, storage.GTE, storage.LT, storage.LTE:
		// Handle range comparison operators
		resultBitmap := New(idx.rowCount)

		// Get the data type of the first column
		dataType := storage.TEXT // Default to TEXT
		if len(idx.dataTypes) > 0 {
			dataType = idx.dataTypes[0]
		}

		// Process each value in the index
		for strVal, bitmap := range idx.valueMap {
			// Skip empty/null values
			if strVal == "" {
				continue
			}

			// Determine whether the value matches the operator condition
			match := false

			// Compare based on data type
			switch dataType {
			case storage.INTEGER:
				// Parse both values as integers
				indexVal, err := strconv.ParseInt(strVal, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				compareVal, err := strconv.ParseInt(strValue, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				// Apply the appropriate comparison
				switch op {
				case storage.GT:
					match = indexVal > compareVal
				case storage.GTE:
					match = indexVal >= compareVal
				case storage.LT:
					match = indexVal < compareVal
				case storage.LTE:
					match = indexVal <= compareVal
				}

			case storage.FLOAT:
				// Parse both values as floats
				indexVal, err := strconv.ParseFloat(strVal, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				compareVal, err := strconv.ParseFloat(strValue, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				// Apply the appropriate comparison
				switch op {
				case storage.GT:
					match = indexVal > compareVal
				case storage.GTE:
					match = indexVal >= compareVal
				case storage.LT:
					match = indexVal < compareVal
				case storage.LTE:
					match = indexVal <= compareVal
				}

			case storage.BOOLEAN:
				// Convert to bool
				indexVal := strVal == "true" || strVal == "1"
				compareVal := strValue == "true" || strValue == "1"

				// Apply the appropriate comparison (less/greater ordering: false < true)
				switch op {
				case storage.GT:
					match = indexVal && !compareVal // true > false only
				case storage.GTE:
					match = indexVal || !compareVal // true >= false or false >= false (always true except when comparing false > true)
				case storage.LT:
					match = !indexVal && compareVal // false < true only
				case storage.LTE:
					match = !indexVal || compareVal // false <= true or true <= true (always true except when comparing true < false)
				}

			case storage.TIMESTAMP:
				// Try to parse timestamps
				indexTime, err := storage.ParseTimestamp(strVal)
				if err != nil {
					continue // Skip if not a valid timestamp
				}

				compareTime, err := storage.ParseTimestamp(strValue)
				if err != nil {
					continue // Skip if not a valid timestamp
				}

				// Apply the appropriate comparison
				switch op {
				case storage.GT:
					match = indexTime.After(compareTime)
				case storage.GTE:
					match = indexTime.After(compareTime) || indexTime.Equal(compareTime)
				case storage.LT:
					match = indexTime.Before(compareTime)
				case storage.LTE:
					match = indexTime.Before(compareTime) || indexTime.Equal(compareTime)
				}

			default:
				// For all other types, do a string comparison
				switch op {
				case storage.GT:
					match = strVal > strValue
				case storage.GTE:
					match = strVal >= strValue
				case storage.LT:
					match = strVal < strValue
				case storage.LTE:
					match = strVal <= strValue
				}
			}

			// If this value matches the condition, add it to the result
			if match {
				// OR this bitmap with the result
				for i := int64(0); i < bitmap.Size(); i++ {
					val, _ := bitmap.Get(i)
					if val {
						resultBitmap.Set(i, true)
					}
				}
			}
		}

		return resultBitmap, nil

	default:
		return nil, fmt.Errorf("unsupported operator for bitmap index: %v", op)
	}
}

// All returns a bitmap with all positions
func (idx *Index) All() *Bitmap {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	result := New(idx.rowCount)
	result.SetAll(true)
	return result
}

// None returns an empty bitmap
func (idx *Index) None() *Bitmap {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	return New(idx.rowCount)
}

// GetBitmap returns the bitmap for a specific value
func (idx *Index) GetBitmap(value string) (*Bitmap, bool) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	bitmap, exists := idx.valueMap[value]
	return bitmap, exists
}

// GetValueMap returns a copy of the value map (for range operations)
func (idx *Index) GetValueMap() map[string]*Bitmap {
	// This is intended to be called when the index is already locked
	// via LockForRead/UnlockRead to avoid double locking
	valueCopy := make(map[string]*Bitmap, len(idx.valueMap))
	for k, v := range idx.valueMap {
		valueCopy[k] = v
	}
	return valueCopy
}

// GetMatchingRange returns positions where bitmap index matches the range
func (idx *Index) GetMatchingRange(min, max interface{}, minInclusive, maxInclusive bool) (*Bitmap, error) {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Early check for nil values
	if min == nil && max == nil {
		return nil, errors.New("both min and max cannot be nil in range query")
	}

	// Create a result bitmap
	resultBitmap := New(idx.rowCount)

	// Get the data type of the first column
	dataType := storage.TEXT // Default to TEXT
	if len(idx.dataTypes) > 0 {
		dataType = idx.dataTypes[0]
	}

	// Convert min/max to string representation if not nil
	var minStr, maxStr string
	if min != nil {
		minStr = storage.ConvertValueToString(min)
	}
	if max != nil {
		maxStr = storage.ConvertValueToString(max)
	}

	// Process each value in the index
	for strVal, bitmap := range idx.valueMap {
		// Skip empty/null values
		if strVal == "" {
			continue
		}

		// Determine whether the value is in range
		inRange := true

		// Check min boundary if provided
		if min != nil {
			var cmp int

			// Compare based on data type
			switch dataType {
			case storage.INTEGER:
				// Parse both values as integers
				indexVal, err := strconv.ParseInt(strVal, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				minVal, err := strconv.ParseInt(minStr, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				if indexVal < minVal {
					cmp = -1
				} else if indexVal > minVal {
					cmp = 1
				} else {
					cmp = 0
				}

			case storage.FLOAT:
				// Parse both values as floats
				indexVal, err := strconv.ParseFloat(strVal, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				minVal, err := strconv.ParseFloat(minStr, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				if indexVal < minVal {
					cmp = -1
				} else if indexVal > minVal {
					cmp = 1
				} else {
					cmp = 0
				}

			case storage.BOOLEAN:
				// Convert to bool
				indexVal := strVal == "true" || strVal == "1"
				minVal := minStr == "true" || minStr == "1"

				// Compare booleans (false < true)
				if !indexVal && minVal {
					cmp = -1 // false < true
				} else if indexVal && !minVal {
					cmp = 1 // true > false
				} else {
					cmp = 0 // same values
				}

			case storage.TIMESTAMP:
				// Try to parse timestamps
				indexTime, err := storage.ParseTimestamp(strVal)
				if err != nil {
					continue // Skip if not a valid timestamp
				}

				minTime, err := storage.ParseTimestamp(minStr)
				if err != nil {
					continue // Skip if not a valid timestamp
				}

				// Compare times
				if indexTime.Before(minTime) {
					cmp = -1
				} else if indexTime.After(minTime) {
					cmp = 1
				} else {
					cmp = 0
				}

			default:
				// For all other types, do a string comparison
				cmp = strings.Compare(strVal, minStr)
			}

			if minInclusive {
				inRange = inRange && (cmp >= 0) // value >= min
			} else {
				inRange = inRange && (cmp > 0) // value > min
			}
		}

		// Check max boundary if provided and we're still in range
		if max != nil && inRange {
			var cmp int

			// Compare based on data type
			switch dataType {
			case storage.INTEGER:
				// Parse both values as integers
				indexVal, err := strconv.ParseInt(strVal, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				maxVal, err := strconv.ParseInt(maxStr, 10, 64)
				if err != nil {
					continue // Skip if not a valid integer
				}

				if indexVal < maxVal {
					cmp = -1
				} else if indexVal > maxVal {
					cmp = 1
				} else {
					cmp = 0
				}

			case storage.FLOAT:
				// Parse both values as floats
				indexVal, err := strconv.ParseFloat(strVal, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				maxVal, err := strconv.ParseFloat(maxStr, 64)
				if err != nil {
					continue // Skip if not a valid float
				}

				if indexVal < maxVal {
					cmp = -1
				} else if indexVal > maxVal {
					cmp = 1
				} else {
					cmp = 0
				}

			case storage.BOOLEAN:
				// Convert to bool
				indexVal := strVal == "true" || strVal == "1"
				maxVal := maxStr == "true" || maxStr == "1"

				// Compare booleans (false < true)
				if !indexVal && maxVal {
					cmp = -1 // false < true
				} else if indexVal && !maxVal {
					cmp = 1 // true > false
				} else {
					cmp = 0 // same values
				}

			case storage.TIMESTAMP:
				// Try to parse timestamps
				indexTime, err := time.Parse(time.RFC3339, strVal)
				if err != nil {
					// Try date format
					indexTime, err = time.Parse("2006-01-02", strVal)
					if err != nil {
						continue // Skip if not a valid timestamp
					}
				}

				maxTime, err := time.Parse(time.RFC3339, maxStr)
				if err != nil {
					// Try date format
					maxTime, err = time.Parse("2006-01-02", maxStr)
					if err != nil {
						continue // Skip if not a valid timestamp
					}
				}

				// Compare times
				if indexTime.Before(maxTime) {
					cmp = -1
				} else if indexTime.After(maxTime) {
					cmp = 1
				} else {
					cmp = 0
				}

			default:
				// For all other types, do a string comparison
				cmp = strings.Compare(strVal, maxStr)
			}

			if maxInclusive {
				inRange = inRange && (cmp <= 0) // value <= max
			} else {
				inRange = inRange && (cmp < 0) // value < max
			}
		}

		// If this value is in range, add it to the result
		if inRange {
			// OR this bitmap with the result
			for i := int64(0); i < bitmap.Size(); i++ {
				val, _ := bitmap.Get(i)
				if val {
					resultBitmap.Set(i, true)
				}
			}
		}
	}

	return resultBitmap, nil
}

// LockForRead locks the index for reading
func (idx *Index) LockForRead() {
	idx.mutex.RLock()
}

// UnlockRead unlocks the index after reading
func (idx *Index) UnlockRead() {
	idx.mutex.RUnlock()
}

// SaveFile saves a bitmap index to disk
func (idx *Index) Save(indexDir string) error {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Create the index directory if it doesn't exist
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return err
	}

	// Note: The actual index metadata is saved by the MetadataManager in Table.CreateIndex method
	// using the binser.IndexMetadata structure.

	// Create a binary manifest of all bitmap files
	valuesList := make([]string, 0, len(idx.valueMap))
	fileList := make([]string, 0, len(idx.valueMap))

	// Save each bitmap with a safe filename
	for value, bitmap := range idx.valueMap {
		// Create a safe filename - use a hash for the filename to avoid file system issues
		// with special characters in values
		safeFilename := fmt.Sprintf("%x.bitmap", md5.Sum([]byte(value)))
		filename := filepath.Join(indexDir, safeFilename)

		// Add to manifest
		valuesList = append(valuesList, value)
		fileList = append(fileList, safeFilename)

		// Serialize bitmap
		data := bitmap.ToByteSlice()

		// Save to file
		if err := os.WriteFile(filename, data, 0644); err != nil {
			return err
		}
	}

	// Create a binary writer for the manifest
	writer := binser.NewWriter()
	defer writer.Release()

	// Write the values array
	writer.WriteArrayHeader(len(valuesList))
	for _, value := range valuesList {
		writer.WriteString(value)
	}

	// Write the file list array
	writer.WriteArrayHeader(len(fileList))
	for _, file := range fileList {
		writer.WriteString(file)
	}

	// Write only the binary manifest
	if err := os.WriteFile(filepath.Join(indexDir, "manifest.bin"), writer.Bytes(), 0644); err != nil {
		return err
	}

	return nil
}

// LoadFromDir loads a bitmap index from disk
func (idx *Index) LoadFromDir(indexDir string) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Check if the index directory exists
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return fmt.Errorf("index directory does not exist: %s", indexDir)
	}

	// Clear existing data
	idx.valueMap = make(map[string]*Bitmap)

	// Only load the binary manifest
	binaryManifestPath := filepath.Join(indexDir, "manifest.bin")

	// Values and fileList to be populated from binary manifest
	var values []string
	var fileList []string

	// Check if binary manifest exists
	if _, err := os.Stat(binaryManifestPath); err == nil {
		// Read binary manifest
		binaryManifestBytes, err := os.ReadFile(binaryManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read binary manifest: %w", err)
		}

		// Deserialize using binser
		reader := binser.NewReader(binaryManifestBytes)

		// Read values array
		valuesCount, err := reader.ReadArrayHeader()
		if err != nil {
			return fmt.Errorf("failed to read values array header: %w", err)
		}

		values = make([]string, valuesCount)
		for i := 0; i < valuesCount; i++ {
			value, err := reader.ReadString()
			if err != nil {
				return fmt.Errorf("failed to read value at index %d: %w", i, err)
			}
			values[i] = value
		}

		// Read file list array
		filesCount, err := reader.ReadArrayHeader()
		if err != nil {
			return fmt.Errorf("failed to read files array header: %w", err)
		}

		fileList = make([]string, filesCount)
		for i := 0; i < filesCount; i++ {
			file, err := reader.ReadString()
			if err != nil {
				return fmt.Errorf("failed to read file at index %d: %w", i, err)
			}
			fileList[i] = file
		}
	} else {
		// No manifest found
		return fmt.Errorf("manifest not found in %s", indexDir)
	}

	// Load bitmaps using manifest values and fileList
	for i, value := range values {
		if i >= len(fileList) {
			break // Safety check
		}

		filename := filepath.Join(indexDir, fileList[i])
		data, err := os.ReadFile(filename)
		if err != nil {
			// Log error but continue with other bitmaps
			log.Printf("Warning: Failed to read bitmap file %s: %v\n", filename, err)
			continue
		}

		bitmap, err := FromByteSlice(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize bitmap: %w", err)
		}
		idx.valueMap[value] = bitmap

		// Update row count based on the largest bitmap size
		if bitmap.Size() > idx.rowCount {
			idx.rowCount = bitmap.Size()
		}
	}

	return nil
}

// Load loads a bitmap index from disk
func Load(indexDir string, columnName string) (*Index, error) {
	// Check if the index directory exists
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("index directory does not exist: %s", indexDir)
	}

	// Create a new index
	idx := &Index{
		columnNames: []string{columnName},
		valueMap:    make(map[string]*Bitmap),
		dataTypes:   []storage.DataType{storage.TEXT}, // Default to TEXT if not specified
	}

	// Load the bitmaps from the manifest file
	if err := loadBitmapsFromManifest(indexDir, idx); err != nil {
		return nil, err
	}

	// Set rowCount based on bitmap size
	for _, bitmap := range idx.valueMap {
		idx.rowCount = bitmap.Size()
		break
	}

	return idx, nil
}

// loadBitmapsFromManifest loads bitmap data from the manifest file
func loadBitmapsFromManifest(indexDir string, idx *Index) error {
	// Only load the binary manifest
	binaryManifestPath := filepath.Join(indexDir, "manifest.bin")

	// Values and fileList to be populated from binary manifest
	var values []string
	var fileList []string

	// Check if binary manifest exists
	if _, err := os.Stat(binaryManifestPath); err == nil {
		// Read binary manifest
		binaryManifestBytes, err := os.ReadFile(binaryManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read binary manifest: %w", err)
		}

		// Deserialize using binser
		reader := binser.NewReader(binaryManifestBytes)

		// Read values array
		valuesCount, err := reader.ReadArrayHeader()
		if err != nil {
			return fmt.Errorf("failed to read values array header: %w", err)
		}

		values = make([]string, valuesCount)
		for i := 0; i < valuesCount; i++ {
			value, err := reader.ReadString()
			if err != nil {
				return fmt.Errorf("failed to read value at index %d: %w", i, err)
			}
			values[i] = value
		}

		// Read file list array
		filesCount, err := reader.ReadArrayHeader()
		if err != nil {
			return fmt.Errorf("failed to read files array header: %w", err)
		}

		fileList = make([]string, filesCount)
		for i := 0; i < filesCount; i++ {
			file, err := reader.ReadString()
			if err != nil {
				return fmt.Errorf("failed to read file at index %d: %w", i, err)
			}
			fileList[i] = file
		}
	} else {
		// No manifest found
		return fmt.Errorf("manifest not found in %s", indexDir)
	}

	// Load bitmaps using manifest values and fileList
	for i, value := range values {
		if i >= len(fileList) {
			break // Safety check
		}

		filename := filepath.Join(indexDir, fileList[i])
		data, err := os.ReadFile(filename)
		if err != nil {
			// Log error but continue with other bitmaps
			log.Printf("Warning: Failed to read bitmap file %s: %v\n", filename, err)
			continue
		}

		bitmap, err := FromByteSlice(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize bitmap: %w", err)
		}
		idx.valueMap[value] = bitmap
	}

	return nil
}

// ShouldCreateBitmapIndex determines if a bitmap index should be created
// for this column based on cardinality analysis
func ShouldCreateBitmapIndex(values []interface{}, threshold float64) bool {
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

	// Create bitmap index if cardinality is below threshold
	return cardinalityRatio <= threshold
}
