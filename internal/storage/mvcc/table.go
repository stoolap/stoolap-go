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
package mvcc

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// Per-table auto-increment counters are now implemented in the VersionStore

// MVCCTable is a wrapper that provides MVCC isolation for tables
type MVCCTable struct {
	txnID        int64
	versionStore *VersionStore
	txnVersions  *TransactionVersionStore
	engine       *MVCCEngine
}

// Name returns the table name
func (mt *MVCCTable) Name() string {
	return mt.versionStore.tableName
}

// GetCurrentAutoIncrementValue returns the current auto-increment value directly
func (mt *MVCCTable) GetCurrentAutoIncrementValue() int64 {
	if mt.versionStore != nil {
		return mt.versionStore.GetCurrentAutoIncrementValue()
	}
	return 0
}

// Schema returns the table schema
func (mt *MVCCTable) Schema() storage.Schema {
	// Get the schema from the engine
	if mt.versionStore == nil {
		return storage.Schema{}
	}

	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		// Return empty schema on error
		return storage.Schema{}
	}

	return schema
}

// CleanUp prepares an MVCCTable to be returned to a pool
// This helps reduce GC pressure by reusing MVCCTable objects
func (mt *MVCCTable) CleanUp() {
	// Only clean up transaction-specific data
	// Keep the reference to the underlying table and version store,
	// as these might be shared with the engine
	if mt.txnVersions != nil {
		mt.txnVersions.Rollback()
		mt.txnVersions = nil
	}
}

// Insert adds a new row to the table with MVCC isolation
func (mt *MVCCTable) Insert(row storage.Row) error {
	// Validate row before inserting
	if err := mt.validateRow(row); err != nil {
		return err
	}

	// Get schema for extracting row ID
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Check if we have a primary key column and it's NULL or unset
	pkInfo := getOrCreatePKInfo(schema)
	var explicitPKValue int64
	var hasExplicitPK bool = false

	// Check if we need to generate a primary key value
	if pkInfo.hasPK && pkInfo.singleIntPK && pkInfo.singlePKIndex >= 0 && pkInfo.singlePKIndex < len(row) {
		colVal := row[pkInfo.singlePKIndex]
		if colVal != nil && !colVal.IsNull() {
			// Check if we have an explicit primary key value
			if intVal, ok := colVal.AsInt64(); ok {
				explicitPKValue = intVal
				hasExplicitPK = true
				// We'll update the auto-increment counter AFTER we ensure the row doesn't exist
			}
		} else if pkInfo.singleIntPK && mt.versionStore != nil {
			// If we have a single INTEGER primary key but no value provided,
			// we need to generate one and set it in the row
			nextID := mt.versionStore.GetNextAutoIncrementID()
			row[pkInfo.singlePKIndex] = storage.NewIntegerValue(nextID)
		}
	}

	// Extract or generate the row ID
	rowID := mt.extractRowPK(schema, row)

	// Fast path: Check if this rowID has been seen in this transaction
	// This avoids the expensive full Get operation
	if mt.txnVersions.HasLocallySeen(rowID) {
		// Need to do full check to handle deleted rows properly
		if _, exists := mt.txnVersions.Get(rowID); exists {
			return storage.NewPrimaryKeyConstraintError(rowID)
		}
	}

	// Ultra-fast path: First check if the row exists at all in the version store
	// This avoids mutex acquisition and visibility checks for the common case
	// where the row doesn't exist at all
	if !mt.versionStore.QuickCheckRowExistence(rowID) {
		// Row definitely doesn't exist, skip expensive visibility check
	} else {
		// Row might exist, do the full visibility check
		if _, exists := mt.versionStore.GetVisibleVersion(rowID, mt.txnID); exists {
			return storage.NewPrimaryKeyConstraintError(rowID)
		}
	}

	// NOW update the auto-increment counter if needed - AFTER all existence checks
	if hasExplicitPK && mt.versionStore != nil && explicitPKValue > mt.versionStore.GetCurrentAutoIncrementValue() {
		mt.versionStore.SetAutoIncrementCounter(explicitPKValue)
	}

	// Check unique columnar index constraints
	if err := mt.CheckUniqueConstraints(row); err != nil {
		return err
	}

	// Add to transaction's local version store using the numeric row ID
	mt.txnVersions.Put(rowID, row, false)

	return nil
}

// CheckUniqueConstraints checks if a row violates any unique columnar index constraints
// If originalRow is provided, only checks columns that have changed from the original
// It returns ErrUniqueConstraint if any constraints are violated
func (mt *MVCCTable) CheckUniqueConstraints(row storage.Row, originalRow ...storage.Row) error {
	// Skip if version store is not available
	if mt.versionStore == nil {
		return fmt.Errorf("version store not available")
	}

	// Quick check for empty indexes
	mt.versionStore.indexMutex.RLock()
	if len(mt.versionStore.indexes) == 0 {
		mt.versionStore.indexMutex.RUnlock()
		return nil
	}

	// Get all unique indexes
	uniqueIndexes := make([]storage.Index, 0)

	// Collect all unique indexes regardless of index implementation (ColumnarIndex or MultiColumnarIndex)
	for _, index := range mt.versionStore.indexes {
		if index.IsUnique() {
			uniqueIndexes = append(uniqueIndexes, index)
		}
	}
	mt.versionStore.indexMutex.RUnlock()

	// If there are no unique indexes, return early
	if len(uniqueIndexes) == 0 {
		return nil
	}

	// Get schema to map column names to positions
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Create a map of column names to positions for faster lookups
	colPosMap := make(map[string]int, len(schema.Columns))
	for i, col := range schema.Columns {
		colPosMap[col.Name] = i
	}

	// Determine if we're updating (have an original row) or inserting
	isUpdate := len(originalRow) > 0 && originalRow[0] != nil

	// Check each unique index
	for _, idx := range uniqueIndexes {
		// Get column names and positions for this index
		colNames := idx.ColumnNames()

		// Skip if no column names (shouldn't happen)
		if len(colNames) == 0 {
			continue
		}

		// Get the values for the indexed columns
		values := make([]storage.ColumnValue, len(colNames))
		allNull := true

		// Track if any columns in this index have changed (for updates)
		columnChanged := false

		for i, colName := range colNames {
			// Get position in the row
			pos, exists := colPosMap[colName]
			if !exists || pos >= len(row) {
				// Skip if column not found or out of bounds
				continue
			}

			// Get value
			values[i] = row[pos]

			// Check if all values are NULL (NULL values don't violate uniqueness)
			if values[i] != nil && !values[i].IsNull() {
				allNull = false
			}

			// For updates, check if this column has changed
			if isUpdate && pos < len(originalRow[0]) {
				origValue := originalRow[0][pos]
				currValue := row[pos]

				// Check if the value has changed
				if (origValue == nil && currValue != nil) ||
					(origValue != nil && currValue == nil) ||
					(origValue != nil && currValue != nil && !origValue.Equals(currValue)) {
					columnChanged = true
				}
			}
		}

		// Skip if all values are NULL (uniqueness constraints don't apply to NULL values)
		if allNull {
			continue
		}

		// For updates, skip if none of the columns in this index have changed
		if isUpdate && !columnChanged {
			continue
		}

		// For multi-column indexes, we need to check both global and local versions
		// First check local transaction versions
		for localRow := range mt.txnVersions.localVersions.Values() {
			if localRow.IsDeleted || localRow.Data == nil {
				continue // Skip deleted rows
			}

			// Compare values for this index's columns
			match := true

			for i, colName := range colNames {
				pos, exists := colPosMap[colName]
				if !exists || pos >= len(localRow.Data) {
					match = false
					break
				}

				localValue := localRow.Data[pos]
				// Skip NULL values
				if localValue == nil || localValue.IsNull() || values[i] == nil || values[i].IsNull() {
					match = false
					break
				}

				// If values don't match, this row doesn't conflict
				if !localValue.Equals(values[i]) {
					match = false
					break
				}
			}

			// If all columns match, we have a uniqueness violation
			if match {
				return storage.NewUniqueConstraintError(idx.Name(), colNames[0], values[0])
			}
		}

		// Check for uniqueness in the index
		// For single-column indexes
		if columnarIdx, ok := idx.(*ColumnarIndex); ok {
			// Use the HasUniqueValue method if available
			if len(values) > 0 && values[0] != nil && !values[0].IsNull() {
				if columnarIdx.HasUniqueValue(values[0]) {
					return storage.NewUniqueConstraintError(idx.Name(), colNames[0], values[0])
				}
			}
		} else if multiColumnarIdx, ok := idx.(*MultiColumnarIndex); ok {
			if len(values) > 1 && values != nil {
				// Check if the multi-columnar index has this combination of values
				if multiColumnarIdx.HasUniqueValues(values) {
					return storage.NewUniqueConstraintError(idx.Name(), strings.Join(colNames, ", "), values[0])
				}
			}
		}
	}

	return nil
}

// CheckIfAnyRowExists checks if any of the provided rowIDs already exist
// Returns the first existing rowID and true if any exist, otherwise 0 and false
// This avoids unnecessary allocations since we only need to know if ANY row exists
func (mt *MVCCTable) CheckIfAnyRowExists(rowIDs []int64) (int64, bool) {
	// First check local versions
	for _, rowID := range rowIDs {
		if mt.txnVersions.HasLocallySeen(rowID) {
			// Do full check for those that are locally seen
			if _, exists := mt.txnVersions.Get(rowID); exists {
				return rowID, true
			}
		}
	}

	// Then check global versions
	if mt.versionStore != nil {
		// First do a quick check without locking to filter out definite non-existent rows
		var possibleRowIDs []int64
		for _, rowID := range rowIDs {
			if mt.versionStore.QuickCheckRowExistence(rowID) {
				possibleRowIDs = append(possibleRowIDs, rowID)
			}
		}

		// If any rows might exist, do a full visibility check on just those rows
		if len(possibleRowIDs) > 0 {
			// Use a closure to capture the result
			var foundRowID int64
			var found bool

			mt.versionStore.IterateVisibleVersions(possibleRowIDs, mt.txnID,
				func(rowID int64, _ RowVersion) bool {
					foundRowID = rowID
					found = true
					return false // Stop iteration as soon as we find one
				})

			if found {
				return foundRowID, true
			}
		}
	}

	return 0, false
}

// InsertBatch adds multiple rows to the table with MVCC isolation
// This is an optimized version that reduces redundant checks for batch operations
func (mt *MVCCTable) InsertBatch(rows []storage.Row) error {
	// Quick exit for empty batch
	if len(rows) == 0 {
		return nil
	}

	// For small batches, just use the single row method to avoid overhead
	if len(rows) <= 3 {
		for _, row := range rows {
			if err := mt.Insert(row); err != nil {
				return err
			}
		}
		return nil
	}

	// Get schema information once for all rows
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Cache column types and constraints for faster validation
	columnTypes := make([]storage.DataType, len(schema.Columns))
	nullableFlags := make([]bool, len(schema.Columns))
	for i, col := range schema.Columns {
		columnTypes[i] = col.Type
		nullableFlags[i] = col.Nullable
	}

	// Check if we have a primary key column to handle auto-increment
	pkInfo := getOrCreatePKInfo(schema)
	var maxExplicitPK int64

	// Validate all rows using cached schema info
	for i, row := range rows {
		// Perform fast validation with cached schema info
		if err := validateRowFast(row, columnTypes, nullableFlags); err != nil {
			return fmt.Errorf("validation error in row %d: %w", i, err)
		}

		// Check for explicit primary key values to update auto-increment counter
		if pkInfo.hasPK && pkInfo.singleIntPK && pkInfo.singlePKIndex >= 0 && pkInfo.singlePKIndex < len(row) {
			colVal := row[pkInfo.singlePKIndex]
			if colVal != nil && !colVal.IsNull() {
				if intVal, ok := colVal.AsInt64(); ok {
					// Keep track of the maximum explicit PK value
					if intVal > maxExplicitPK {
						maxExplicitPK = intVal
					}
				}
			} else if pkInfo.singleIntPK && mt.versionStore != nil {
				// If we have a single INTEGER primary key but no value provided,
				// we need to generate one and set it in the row
				nextID := mt.versionStore.GetNextAutoIncrementID()
				row[pkInfo.singlePKIndex] = storage.NewIntegerValue(nextID)
			}
		}
	}

	// Note: We'll update the auto-increment counter AFTER checking row existence

	// Extract all row IDs first - reuse the schema we already fetched
	rowIDs := make([]int64, len(rows))
	for i, row := range rows {
		rowIDs[i] = mt.extractRowPK(schema, row)
	}

	// Check if any row already exists (optimized to avoid allocations)
	if existingRowID, exists := mt.CheckIfAnyRowExists(rowIDs); exists {
		return storage.NewPrimaryKeyConstraintError(existingRowID)
	}

	// NOW update the auto-increment counter if needed - AFTER checking existence
	if maxExplicitPK > 0 && mt.versionStore != nil && maxExplicitPK > mt.versionStore.GetCurrentAutoIncrementValue() {
		mt.versionStore.SetAutoIncrementCounter(maxExplicitPK)
	}

	for i, row := range rows {
		if err := mt.CheckUniqueConstraints(row); err != nil {
			return fmt.Errorf("uniqueness constraint violated in row %d: %w", i, err)
		}
	}

	// If we got here, none of the rows exist, so insert them all
	// Use the batch insertion method for better performance
	mt.txnVersions.PutRowsBatch(rowIDs, rows, false)

	return nil
}

// validateRow checks if a row is valid for the table's schema
func (mt *MVCCTable) validateRow(row storage.Row) error {
	// Check if the version store is nil
	if mt == nil || mt.versionStore == nil {
		return errors.New("invalid table: version store is nil")
	}

	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Check if row is nil
	if row == nil {
		return errors.New("invalid row: row is nil")
	}

	// Check column count
	if len(row) != len(schema.Columns) {
		return fmt.Errorf("invalid column count: expected %d, got %d", len(schema.Columns), len(row))
	}

	// Validate column types and NULL constraints
	for i, col := range schema.Columns {
		// Check if row[i] is nil
		if i >= len(row) || row[i] == nil {
			return fmt.Errorf("nil value at index %d (column '%s')", i, col.Name)
		}

		// Check NULL constraint
		if !col.Nullable && row[i].IsNull() {
			return fmt.Errorf("NULL value in non-nullable column '%s'", col.Name)
		}

		// Check type compatibility
		if !row[i].IsNull() {
			// First check the actual type - strict validation
			actualType := row[i].Type()
			if actualType != col.Type {
				return fmt.Errorf("type mismatch in column '%s': expected %v, got %v",
					col.Name, col.Type, actualType)
			}

			// Then also verify conversion works
			switch col.Type {
			case storage.TypeInteger:
				if _, ok := row[i].AsInt64(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Integer", col.Name)
				}
			case storage.TypeFloat:
				if _, ok := row[i].AsFloat64(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Float", col.Name)
				}
			case storage.TypeString:
				if _, ok := row[i].AsString(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected String", col.Name)
				}
			case storage.TypeBoolean:
				if _, ok := row[i].AsBoolean(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Boolean", col.Name)
				}
			case storage.TypeTimestamp:
				if _, ok := row[i].AsTimestamp(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected Timestamp", col.Name)
				}
			case storage.TypeJSON:
				if _, ok := row[i].AsJSON(); !ok {
					return fmt.Errorf("type mismatch in column '%s': expected JSON", col.Name)
				}
			default:
				// For unknown or future types, just check the type matches
				if row[i].Type() != col.Type {
					return fmt.Errorf("type mismatch in column '%s': expected %v, got %v",
						col.Name, col.Type, row[i].Type())
				}
			}
		}
	}

	return nil
}

// validateRowFast performs a fast validation for batch operations
// using pre-computed schema type information for better performance
func validateRowFast(row storage.Row, columnTypes []storage.DataType, nullableFlags []bool) error {
	// Quick nil check
	if row == nil {
		return errors.New("invalid row: row is nil")
	}

	// Check column count
	if len(row) != len(columnTypes) {
		return fmt.Errorf("invalid column count: expected %d, got %d", len(columnTypes), len(row))
	}

	// Fast validation using cached type information
	for i, colType := range columnTypes {
		// Check if value is nil
		if i >= len(row) || row[i] == nil {
			return fmt.Errorf("nil value at index %d", i)
		}

		// Check NULL constraint
		if !nullableFlags[i] && row[i].IsNull() {
			return fmt.Errorf("NULL value in non-nullable column at index %d", i)
		}

		// Type check for non-NULL values
		if !row[i].IsNull() {
			// Fast type check - just compare the type ID
			// This avoids multiple method calls and string formatting
			actualType := row[i].Type()
			if actualType != colType {
				return fmt.Errorf("type mismatch at index %d: expected %v, got %v",
					i, colType, actualType)
			}
		}
	}

	return nil
}

// Update updates rows that match the expression
func (mt *MVCCTable) Update(where storage.Expression, setter func(storage.Row) (storage.Row, bool)) (int, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)

	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize with our fast expressions
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return 0, nil
		}

		// For equality operator with PK, we can do direct update (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Direct lookup by ID
			row, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return 0, nil
			}

			// Apply the setter function
			updatedRow, uniqueCheck := setter(row)

			// Check unique columnar index constraints
			if uniqueCheck {
				if err := mt.CheckUniqueConstraints(updatedRow, row); err != nil {
					return 0, err
				}
			}
			// Store the updated row
			mt.txnVersions.Put(pkInfo.ID, updatedRow, false)

			// Return count of 1 row updated
			return 1, nil
		}

		// For other operators, we'll use the optimized expression
		// but still need to perform a full scan
		if pkInfo.Expr != nil {
			where = pkInfo.Expr // Use optimized expression for evaluation
		}

		if len(pkInfos) == 2 {
			simpleExpr1 := pkInfos[0].Expr
			simpleExpr2 := pkInfos[1].Expr

			if (simpleExpr1.Operator == storage.GT || simpleExpr1.Operator == storage.GTE) &&
				(simpleExpr2.Operator == storage.LT || simpleExpr2.Operator == storage.LTE) {

				// Get range bounds with adjustments for inclusive/exclusive
				lowerBound := simpleExpr1.Int64Value
				if simpleExpr1.Operator == storage.GTE {
					lowerBound-- // Adjust for inclusive lower bound
				}

				upperBound := simpleExpr2.Int64Value
				if simpleExpr2.Operator == storage.LT {
					upperBound-- // Adjust for exclusive upper bound
				}

				// Build list of all potential IDs in the range
				visibleIDs := make([]int64, 0, upperBound-lowerBound)
				for id := lowerBound + 1; id <= upperBound; id++ {
					visibleIDs = append(visibleIDs, id)
				}

				// Process updates in batch
				updateCount := 0

				// First process locally visible rows (common case)
				localRows := make(map[int64]storage.Row)
				for _, id := range visibleIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists {
							localRows[id] = row
						}
					}
				}

				// Then get global versions for remaining IDs
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(visibleIDs, mt.txnID)
				defer ReturnVisibleVersionMap(globalRows)

				// Update local rows
				for id, row := range localRows {
					// Apply the setter function
					updatedRow, uniqueCheck := setter(row)

					// Check unique columnar index constraints
					if uniqueCheck {
						if err := mt.CheckUniqueConstraints(updatedRow, row); err != nil {
							return 0, err
						}
					}

					// Store the updated row
					mt.txnVersions.Put(id, updatedRow, false)
					updateCount++
				}

				var err error
				// Process global rows that weren't in local cache
				globalRows.ForEach(func(rowID int64, version *RowVersion) bool {
					if _, isLocal := localRows[rowID]; !isLocal && !version.IsDeleted {
						// Apply the setter function
						updatedRow, uniqueCheck := setter(version.Data)

						// Check unique columnar index constraints
						if uniqueCheck {
							if err = mt.CheckUniqueConstraints(updatedRow, version.Data); err != nil {
								return false
							}
						}

						// Store the updated row
						mt.txnVersions.Put(rowID, updatedRow, false)
						updateCount++
					}

					return true
				})

				if err != nil {
					return 0, err
				}

				return updateCount, nil
			}
		}
	}
	// Prepare filter expression (optimized if possible)
	var filterExpr storage.Expression = where

	if filterExpr != nil {
		filterExpr = filterExpr.PrepareForSchema(schema)

		// Get filtered row IDs using columnar indexes
		rowIDs := mt.GetFilteredRowIDs(filterExpr)

		if len(rowIDs) > 0 {
			// Processing update using columnar-index filtered row IDs
			updateCount := 0

			// Process in batches for better memory usage
			batchSize := 1000
			for i := 0; i < len(rowIDs); i += batchSize {
				end := i + batchSize
				if end > len(rowIDs) {
					end = len(rowIDs)
				}

				// Get this batch of rows
				batchIDs := rowIDs[i:end]
				versions := mt.versionStore.GetVisibleVersionsByIDs(batchIDs, mt.txnID)

				var err error
				// Process each visible row in this batch
				versions.ForEach(func(rowID int64, version *RowVersion) bool {
					if !version.IsDeleted {
						// Apply the setter function
						updatedRow, uniqueCheck := setter(version.Data)

						// Check unique columnar index constraints
						if uniqueCheck {
							if err = mt.CheckUniqueConstraints(updatedRow, version.Data); err != nil {
								return false
							}
						}

						// Store the updated row
						mt.txnVersions.Put(rowID, updatedRow, false)
						updateCount++
					}

					return true
				})

				// Free the versions map
				ReturnVisibleVersionMap(versions)

				if err != nil {
					return 0, err
				}
			}

			return updateCount, nil
		}
	}

	// Fall back to the general case if columnar index optimization didn't work
	processedKeys := GetProcessedKeysMap()
	defer PutProcessedKeysMap(processedKeys)

	// Count of rows updated
	updateCount := 0

	// PART 2: Process global versions with batch limiting
	processCount, err := mt.processGlobalVersions(filterExpr, processedKeys, func(rowID int64, row storage.Row) error {
		// Apply the setter function
		updatedRow, uniqueCheck := setter(row)

		// Check unique columnar index constraints
		if uniqueCheck {
			if err := mt.CheckUniqueConstraints(updatedRow, row); err != nil {
				return err
			}
		}

		// Store the updated row
		mt.txnVersions.Put(rowID, updatedRow, false)

		return nil
	}, 0) // Process in batches all at once
	if err != nil {
		return 0, err
	}

	updateCount += processCount

	// PART 3: Process rows only visible in local versions for small updates
	if updateCount < 100 {
		// Skip this step for large updates to optimize memory usage
		allRows := mt.txnVersions.GetAllVisibleRows()

		// Prepare expression for schema evaluation
		if filterExpr != nil {
			// Get schema for expression preparation
			schema, schemaErr := mt.versionStore.GetTableSchema()
			if schemaErr != nil {
				return 0, fmt.Errorf("failed to get schema for filter preparation: %w", schemaErr)
			}

			filterExpr = filterExpr.PrepareForSchema(schema)
		}

		// Prepare a batch of rows for processing
		const batchCapacity = 100
		var batchRows []storage.Row
		var batchRowIDs []int64

		// Collect unprocessed rows first
		allRows.ForEach(func(rowID int64, row storage.Row) bool {
			// Skip already processed rows
			if processedKeys.Has(rowID) {
				return true
			}

			// Skip if already marked as deleted locally
			if localVersion, exists := mt.txnVersions.localVersions.Get(rowID); exists && localVersion.IsDeleted {
				return true
			}

			// Add to batch
			if batchRows == nil {
				batchRows = make([]storage.Row, 0, batchCapacity)
				batchRowIDs = make([]int64, 0, batchCapacity)
			}

			batchRows = append(batchRows, row)
			batchRowIDs = append(batchRowIDs, rowID)

			return true
		})

		// Return the map to the pool
		PutRowMap(allRows)

		// Process batch of rows
		if len(batchRows) > 0 {
			var matches []bool
			if filterExpr != nil {
				// Evaluate each row with the prepared expression
				matches = make([]bool, len(batchRows))
				for i, row := range batchRows {
					matches[i] = filterExpr.EvaluateFast(row)
				}
			} else {
				// No filter, all rows match
				matches = make([]bool, len(batchRows))
				for i := range matches {
					matches[i] = true
				}
			}

			// Process matching rows
			var err error
			for i, match := range matches {
				if match {
					rowID := batchRowIDs[i]
					row := batchRows[i]

					// Apply the setter function
					updatedRow, uniqueCheck := setter(row)

					// Check unique columnar index constraints
					if uniqueCheck {
						if err = mt.CheckUniqueConstraints(updatedRow, row); err != nil {
							break
						}
					}

					// Store the updated row
					mt.txnVersions.Put(rowID, updatedRow, false)
					updateCount++
				}
			}

			if err != nil {
				return 0, err
			}
		}
	}

	return updateCount, nil
}

// ProcessedKeysPool is a pool for bool maps to reduce allocations
var processedKeysPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[struct{}](1000) // Default to 1000 capacity
	},
}

// GetProcessedKeysMap gets a map from the pool
func GetProcessedKeysMap() *fastmap.Int64Map[struct{}] {
	m := processedKeysPool.Get().(*fastmap.Int64Map[struct{}])

	return m
}

// PutProcessedKeysMap returns a map to the pool
func PutProcessedKeysMap(m *fastmap.Int64Map[struct{}]) {
	if m == nil {
		return
	}

	// Clear the map
	m.Clear()
	processedKeysPool.Put(m)
}

// processGlobalVersions processes visible versions from the global version store
// processedKeys: Map of keys already processed to avoid duplicates
// processor: Function to call for each matching row
// batchSize: Maximum number of rows to process (0 for unlimited)
// Returns the number of rows processed
func (mt *MVCCTable) processGlobalVersions(
	filter storage.Expression,
	processedKeys *fastmap.Int64Map[struct{}],
	processor func(int64, storage.Row) error,
	batchSize int,
) (int, error) {
	// Get visible versions from global store
	globalVersions := mt.versionStore.GetAllVisibleVersions(mt.txnID)
	defer ReturnVisibleVersionMap(globalVersions)

	processCount := 0
	var err error

	// Prepare filter expression for schema evaluation
	if filter != nil {
		// Get schema for expression preparation
		schema, schemaErr := mt.versionStore.GetTableSchema()
		if schemaErr != nil {
			return 0, fmt.Errorf("failed to get schema for filter preparation: %w", schemaErr)
		}

		filter = filter.PrepareForSchema(schema)
	}

	// Prepare a batch of rows for processing
	// This helps optimize memory locality for evaluation
	const batchCapacity = 1000
	batchRows := make([]storage.Row, 0, batchCapacity)
	batchRowIDs := make([]int64, 0, batchCapacity)

	// Helper function to process a batch of rows
	processBatch := func() error {
		if len(batchRows) == 0 {
			return nil
		}

		if filter != nil {
			// Evaluate and process in a single pass for better performance
			for i, row := range batchRows {
				// Fast path: directly evaluate and process in one loop
				if filter.EvaluateFast(row) {
					rowID := batchRowIDs[i]

					// Mark as processed
					processedKeys.Put(rowID, struct{}{})

					// Process this row
					if err = processor(rowID, row); err != nil {
						return err
					}

					processCount++

					// Check batch size limit
					if batchSize > 0 && processCount >= batchSize {
						return nil
					}
				}
			}
		} else {
			// No filter, process all rows
			for i, row := range batchRows {
				rowID := batchRowIDs[i]

				// Mark as processed
				processedKeys.Put(rowID, struct{}{})

				// Process this row
				if err = processor(rowID, row); err != nil {
					return err
				}

				processCount++

				// Check batch size limit
				if batchSize > 0 && processCount >= batchSize {
					return nil
				}
			}
		}

		// Clear the batch for reuse
		batchRows = batchRows[:0]
		batchRowIDs = batchRowIDs[:0]
		return nil
	}

	// Collect rows into batches for efficient evaluation
	globalVersions.ForEach(func(rowID int64, version *RowVersion) bool {
		// Skip if already processed
		if processedKeys.Has(rowID) {
			return true
		}

		// Skip deleted rows
		if version.IsDeleted {
			return true
		}

		// Add to batch for processing
		batchRows = append(batchRows, version.Data)
		batchRowIDs = append(batchRowIDs, rowID)

		// Process batch when it reaches capacity
		if len(batchRows) >= batchCapacity {
			err = processBatch()
			if err != nil || (batchSize > 0 && processCount >= batchSize) {
				return false
			}
		}

		return true
	})

	// Process any remaining rows in the last batch
	if err == nil && len(batchRows) > 0 {
		err = processBatch()
	}

	if err != nil {
		return 0, err
	}

	return processCount, nil
}

// Delete removes rows that match the expression
func (mt *MVCCTable) Delete(where storage.Expression) (int, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)
	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize with our fast expressions
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return 0, nil
		}

		// For equality operator with integer PK, we can do direct deletion (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Check if row exists and is visible
			row, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return 0, nil
			}

			// Mark the row as deleted with tombstone
			mt.txnVersions.Put(pkInfo.ID, row, true)

			// Return count of 1 row deleted
			return 1, nil
		}

		// For other operators, we'll use the optimized expression
		// but still need to perform a full scan
		if pkInfo.Expr != nil {
			where = pkInfo.Expr // Use optimized expression for evaluation
		}

		// Fast path for range deletion
		if len(pkInfos) == 2 {
			simpleExpr1 := pkInfos[0].Expr
			simpleExpr2 := pkInfos[1].Expr

			if (simpleExpr1.Operator == storage.GT || simpleExpr1.Operator == storage.GTE) &&
				(simpleExpr2.Operator == storage.LT || simpleExpr2.Operator == storage.LTE) {

				// Get range bounds
				lowerBound := simpleExpr1.Int64Value
				if simpleExpr1.Operator == storage.GTE {
					lowerBound-- // Adjust for inclusive lower bound
				}

				upperBound := simpleExpr2.Int64Value
				if simpleExpr2.Operator == storage.LT {
					upperBound-- // Adjust for exclusive upper bound
				}

				// Prepare list of all IDs in the range to check
				visibleIDs := make([]int64, 0, upperBound-lowerBound)
				for id := lowerBound + 1; id <= upperBound; id++ {
					visibleIDs = append(visibleIDs, id)
				}

				// Process deletions in batch
				deleteCount := 0

				// Process local versions first (fast path)
				for _, id := range visibleIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists && row != nil {
							// Mark as deleted with tombstone
							mt.txnVersions.Put(id, row, true)
							deleteCount++
						}
					}
				}

				// Then check global versions in bulk
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(visibleIDs, mt.txnID)
				defer ReturnVisibleVersionMap(globalRows)

				globalRows.ForEach(func(rowID int64, version *RowVersion) bool {
					if !version.IsDeleted {
						// Mark as deleted
						mt.txnVersions.Put(rowID, version.Data, true)
						deleteCount++
					}

					return true
				})

				return deleteCount, nil
			}
		}
	}

	// Prepare filter expression (optimized if possible)
	var filterExpr storage.Expression = where
	schema, err = mt.versionStore.GetTableSchema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	if filterExpr != nil {
		filterExpr = filterExpr.PrepareForSchema(schema)

		// Get filtered row IDs using columnar indexes
		rowIDs := mt.GetFilteredRowIDs(filterExpr)

		if len(rowIDs) > 0 {
			// Process deletions in batches for better memory usage
			deleteCount := 0
			batchSize := 1000

			for i := 0; i < len(rowIDs); i += batchSize {
				end := i + batchSize
				if end > len(rowIDs) {
					end = len(rowIDs)
				}

				// Get this batch of rows
				batchIDs := rowIDs[i:end]

				// First process locally visible rows (fast path)
				for _, id := range batchIDs {
					if mt.txnVersions.HasLocallySeen(id) {
						if row, exists := mt.txnVersions.Get(id); exists && row != nil {
							// Mark as deleted with tombstone
							mt.txnVersions.Put(id, row, true)
							deleteCount++
						}
					}
				}

				// Then check global versions in bulk
				globalRows := mt.versionStore.GetVisibleVersionsByIDs(batchIDs, mt.txnID)

				globalRows.ForEach(func(rowID int64, version *RowVersion) bool {
					if !version.IsDeleted {
						// Mark as deleted
						mt.txnVersions.Put(rowID, version.Data, true)
						deleteCount++
					}

					return true
				})

				// Free the versions map
				ReturnVisibleVersionMap(globalRows)
			}

			return deleteCount, nil
		}
	}

	// Use the pooled map for tracking processed keys
	processedKeys := GetProcessedKeysMap()
	defer PutProcessedKeysMap(processedKeys)

	// Count of rows deleted
	deleteCount := 0

	filterExpr = where
	if filterExpr != nil {
		filterExpr = filterExpr.PrepareForSchema(schema)
	}

	// PART 2: Process global versions with batch limiting
	processCount, _ := mt.processGlobalVersions(filterExpr, processedKeys, func(rowID int64, row storage.Row) error {
		// Mark as deleted in transaction's local versions
		mt.txnVersions.Put(rowID, row, true)
		return nil
	}, 0) // Process in batches all at once

	deleteCount += processCount

	// PART 3: Process rows only visible in local versions for small deletes
	if deleteCount < 100 {
		// Skip this step for large deletes to optimize memory usage
		allRows := mt.txnVersions.GetAllVisibleRows()

		// Prepare expression for schema evaluation
		if filterExpr != nil {
			// Get schema for expression preparation
			schema, schemaErr := mt.versionStore.GetTableSchema()
			if schemaErr != nil {
				return 0, fmt.Errorf("failed to get schema for filter preparation: %w", schemaErr)
			}

			filterExpr = filterExpr.PrepareForSchema(schema)
		}

		// Prepare a batch of rows for processing
		const batchCapacity = 100
		var batchRows []storage.Row
		var batchRowIDs []int64

		// Collect unprocessed rows first
		allRows.ForEach(func(rowID int64, row storage.Row) bool {
			// Skip already processed rows
			if processedKeys.Has(rowID) {
				return true
			}

			// Skip if already marked as deleted locally
			if localVersion, exists := mt.txnVersions.localVersions.Get(rowID); exists && localVersion.IsDeleted {
				return true
			}

			// Add to batch
			if batchRows == nil {
				batchRows = make([]storage.Row, 0, batchCapacity)
				batchRowIDs = make([]int64, 0, batchCapacity)
			}

			batchRows = append(batchRows, row)
			batchRowIDs = append(batchRowIDs, rowID)

			return true
		})

		// Return the map to the pool
		PutRowMap(allRows)

		// Process batch of rows
		if len(batchRows) > 0 {
			var matches []bool
			if filterExpr != nil {
				// Evaluate each row with the prepared expression
				matches = make([]bool, len(batchRows))
				for i, row := range batchRows {
					matches[i] = filterExpr.EvaluateFast(row)
				}
			} else {
				// No filter, all rows match
				matches = make([]bool, len(batchRows))
				for i := range matches {
					matches[i] = true
				}
			}

			// Process matching rows
			for i, match := range matches {
				if match {
					rowID := batchRowIDs[i]
					row := batchRows[i]

					// Mark as deleted
					mt.txnVersions.Put(rowID, row, true)
					deleteCount++
				}
			}
		}
	}

	return deleteCount, nil
}

// Cleaned up old schemaCache code as we're now using schemaPKInfo instead

// Optimized PK cache with more metadata for faster extraction
type schemaPKInfo struct {
	pkIndices     []int            // Primary key column indices
	hasPK         bool             // Whether this schema has a primary key
	pkType        storage.DataType // Primary key data type (for single PKs)
	singleIntPK   bool             // Special flag for single integer PKs (fast path)
	singlePKIndex int              // Index of the single PK column (for fast path)
}

// Cache of schema PK info to avoid expensive lookups
var schemaPKInfoCache sync.Map

// getOrCreatePKInfo gets or creates cached PK info for a schema
func getOrCreatePKInfo(schema storage.Schema) *schemaPKInfo {
	// Use table name as key instead of schema pointer
	// This is more stable across different schema instances with same content
	schemaKey := schema.TableName

	// Try fast path - check if already in cache
	if info, found := schemaPKInfoCache.Load(schemaKey); found {
		return info.(*schemaPKInfo)
	}

	// Create new PK info
	pkIndices := make([]int, 0, 4) // Most tables have few PK columns
	hasPK := false
	pkType := storage.NULL
	singleIntPK := false
	singlePKIndex := -1

	// Find primary key columns
	for i, col := range schema.Columns {
		if col.PrimaryKey {
			pkIndices = append(pkIndices, i)
			hasPK = true

			// Track if this is a single primary key
			if len(pkIndices) == 1 {
				pkType = col.Type
				singlePKIndex = i

				// Check if it's an integer PK (fastest path)
				if col.Type == storage.TypeInteger {
					singleIntPK = true
				}
			} else {
				// Multiple PKs - reset single PK flags
				singleIntPK = false
				singlePKIndex = -1
			}
		}
	}

	// If no primary key defined, use first column
	if !hasPK && len(schema.Columns) > 0 {
		pkIndices = append(pkIndices, 0)
		pkType = schema.Columns[0].Type
		singlePKIndex = 0
		if pkType == storage.TypeInteger {
			singleIntPK = true
		}
	}

	info := &schemaPKInfo{
		pkIndices:     pkIndices,
		hasPK:         hasPK,
		pkType:        pkType,
		singleIntPK:   singleIntPK,
		singlePKIndex: singlePKIndex,
	}

	// Store in cache (if another thread did this simultaneously, we'll just have a duplicate that GC will clean up)
	schemaPKInfoCache.Store(schemaKey, info)
	return info
}

// extractRowPK extracts the primary key from a row, optimized for performance
func (mt *MVCCTable) extractRowPK(schema storage.Schema, row storage.Row) int64 {
	// Get or create cached PK info
	pkInfo := getOrCreatePKInfo(schema)

	// If the table has a primary key, extract it
	if pkInfo.hasPK {
		// Fast path: Single integer primary key
		if pkInfo.singleIntPK && pkInfo.singlePKIndex >= 0 && pkInfo.singlePKIndex < len(row) {
			colVal := row[pkInfo.singlePKIndex]
			if colVal != nil && !colVal.IsNull() {
				if intVal, ok := colVal.AsInt64(); ok {
					return intVal
				}
			}
		}
	}

	// If we reach here, we need to generate a synthetic row ID
	// This happens when:
	// 1. The table has no primary key
	// 2. The primary key is not a single integer column
	// 3. The primary key is NULL or not provided
	if mt.versionStore != nil {
		return mt.versionStore.GetNextAutoIncrementID()
	} else {
		panic("Error: VersionStore not initialized properly when generating row ID")
	}
}

// Scan returns a scanner for rows in the table
func (mt *MVCCTable) Scan(columnIndices []int, where storage.Expression) (storage.Scanner, error) {
	// Get schema directly from the version store
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Fast path for primary key operations
	pkInfos := GetPKOperationInfo(where, schema)
	pkInfo := pkInfos[0] // Use the first PK info for simplicity

	// Check if we can optimize this with our fast expression
	if pkInfo.Valid {
		// Special case for empty result (contradictory conditions)
		if pkInfo.EmptyResult {
			return newEmptyScanner(), nil
		}

		// For equality operator, we can do direct row lookup (fastest path)
		if pkInfo.Operator == storage.EQ && pkInfo.ID != 0 {
			// Direct lookup by ID
			row, exists := mt.txnVersions.Get(pkInfo.ID)
			if !exists {
				// Row doesn't exist or isn't visible to this transaction
				return newEmptyScanner(), nil
			}

			// Return a scanner with just this single row
			return newSingleRowScanner(row, schema, columnIndices), nil
		}

		if pkInfo.Expr != nil {
			where = pkInfo.Expr
		}

		if len(pkInfos) == 2 {
			simpleExpr1 := pkInfos[0].Expr
			simpleExpr2 := pkInfos[1].Expr

			if (simpleExpr1.Operator == storage.GT || simpleExpr1.Operator == storage.GTE) &&
				(simpleExpr2.Operator == storage.LT || simpleExpr2.Operator == storage.LTE) {

				lowerBound := simpleExpr1.Int64Value
				upperBound := simpleExpr2.Int64Value

				// Adjust bounds based on inclusive/exclusive operators
				startID := lowerBound
				if simpleExpr1.Operator == storage.GT {
					startID = lowerBound + 1 // exclusive lower bound
				}

				endID := upperBound
				inclusiveEnd := simpleExpr2.Operator == storage.LTE

				// Create and return optimized range scanner
				return NewRangeScanner(
					mt.versionStore,
					startID,
					endID,
					inclusiveEnd,
					mt.txnID,
					schema,
					columnIndices,
				), nil
			}
		}
	}

	// Check if we can use columnar indexes for optimization
	var filterExpr storage.Expression
	if where != nil {
		filterExpr = where
		filterExpr = filterExpr.PrepareForSchema(schema)

		// Try to optimize with columnar indexes using the new direct row ID approach
		rowIDs, usedIndex := mt.GetFilteredRowIDsWithIndex(filterExpr)
		if len(rowIDs) > 0 {
			// Check if we're using a multi-column index
			if multiColIndex, isMulti := usedIndex.(*MultiColumnarIndex); isMulti {
				// Create the optimized multi-columnar iterator for better performance
				return NewMultiColumnarIndexIterator(
					mt.versionStore,
					rowIDs,
					mt.txnID,
					schema,
					columnIndices,
					multiColIndex.ColumnNames(),
					multiColIndex.ColumnIDs(),
				), nil
			}

			// Otherwise use standard columnar iterator for single-column index
			return NewColumnarIndexIterator(
				mt.versionStore,
				rowIDs,
				mt.txnID,
				schema,
				columnIndices,
			), nil
		}
	}

	// Regular path for all other queries
	// Make sure expression is properly prepared with schema before scanning
	if filterExpr != nil {
		filterExpr = filterExpr.PrepareForSchema(schema)
	}

	// Get all visible rows with proper visibility checks for deleted rows
	visibleRows := mt.txnVersions.GetAllVisibleRows()

	// Create and return a scanner with the visible rows
	return NewMVCCScanner(visibleRows, schema, columnIndices, filterExpr), nil
}

// CreateColumn adds a column to the table
func (mt *MVCCTable) CreateColumn(name string, dataType storage.DataType, nullable bool) error {
	// Use the engine to perform the operation
	return mt.engine.CreateColumn(mt.versionStore.tableName, name, dataType, nullable)
}

// DropColumn removes a column from the table
func (mt *MVCCTable) DropColumn(name string) error {
	// Use the engine to perform the operation
	return mt.engine.DropColumn(mt.versionStore.tableName, name)
}

// CreateIndex creates an index on the table
// This method maintains the custom indexName provided by the user when creating the index
func (mt *MVCCTable) CreateIndex(indexName string, columns []string, isUnique bool) error {
	// Check if an index already exists for this column
	if mt.versionStore == nil {
		return fmt.Errorf("invalid table: version store is nil")
	}

	// First check if we've already got a matching index - either by column name or by index name
	mt.versionStore.indexMutex.RLock()
	// Check if there's an index with this name already
	indexExists := false
	for _, idx := range mt.versionStore.indexes {
		if idx.Name() == indexName {
			indexExists = true
			break
		}
	}
	mt.versionStore.indexMutex.RUnlock()

	// If the index already exists, just return (IF NOT EXISTS case)
	if indexExists {
		return nil
	}

	// Get schema information
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Create different types of indexes based on column count
	var index storage.Index

	if len(columns) == 1 {
		// Single-column index case
		columnName := columns[0]

		// Find column ID and data type
		columnID := -1
		var dataType storage.DataType
		var isPrimaryKey bool

		for i, col := range schema.Columns {
			if col.Name == columnName {
				columnID = i
				dataType = col.Type
				isPrimaryKey = col.PrimaryKey
				break
			}
		}

		if columnID == -1 {
			return fmt.Errorf("column %s not found in schema", columnName)
		}

		// Prevent creating indexes on primary key column
		if isPrimaryKey {
			return fmt.Errorf("cannot create index on primary key column %s", columnName)
		}

		// Create a standard columnar index
		index = NewColumnarIndex(indexName, mt.versionStore.tableName,
			columnName, columnID, dataType, mt.versionStore, isUnique)
	} else {
		// Multi-column index case
		// Collect column IDs and data types
		columnIDs := make([]int, len(columns))
		dataTypes := make([]storage.DataType, len(columns))
		hasPrimaryKey := false

		for i, columnName := range columns {
			found := false
			for j, col := range schema.Columns {
				if col.Name == columnName {
					columnIDs[i] = j
					dataTypes[i] = col.Type
					found = true
					if col.PrimaryKey {
						hasPrimaryKey = true
					}
					break
				}
			}
			if !found {
				return fmt.Errorf("column %s not found in schema", columnName)
			}
		}

		// Prevent creating indexes that include primary key columns
		if hasPrimaryKey {
			return fmt.Errorf("cannot create index including primary key column")
		}

		// Create a multi-column index
		index = NewMultiColumnarIndex(
			indexName,
			mt.versionStore.tableName,
			columns,
			columnIDs,
			dataTypes,
			mt.versionStore,
			isUnique,
		)
	}

	// Build the index from existing data
	if err := index.Build(); err != nil {
		if closeableIndex, ok := index.(interface{ Close() error }); ok {
			closeableIndex.Close()
		}
		return err
	}

	// Register the index in the version store
	mt.versionStore.indexMutex.Lock()
	mt.versionStore.indexes[indexName] = index
	mt.versionStore.indexMutex.Unlock()

	// Record this operation in the WAL if the engine has persistence enabled
	if mt.engine != nil && mt.engine.persistence != nil && mt.engine.persistence.IsEnabled() {
		// SerializeIndexMetadata already supports multi-column indexes
		indexData, err := SerializeIndexMetadata(index)
		if err != nil {
			log.Printf("Warning: Failed to serialize index metadata for WAL: %v\n", err)
		} else {
			// Record the index creation in WAL
			err = mt.engine.persistence.RecordIndexOperation(mt.versionStore.tableName, WALCreateIndex, indexData)
			if err != nil {
				log.Printf("Warning: Failed to record index creation in WAL: %v\n", err)
			}
		}
	}

	return nil
}

// DropIndex removes an index from the table
func (mt *MVCCTable) DropIndex(indexName string) error {
	if mt.versionStore == nil {
		return fmt.Errorf("invalid table: version store is nil")
	}

	// First try a direct lookup by column name
	mt.versionStore.indexMutex.RLock()
	_, exists := mt.versionStore.indexes[indexName]

	// If not found by column name, look for a match by index name
	if !exists {
		var indexIdentifier string
		for colName, idx := range mt.versionStore.indexes {
			if idx.Name() == indexName {
				exists = true
				indexIdentifier = colName
				break
			}
		}
		mt.versionStore.indexMutex.RUnlock()

		if exists {
			// Drop the columnar index by column name
			return mt.DropColumnarIndex(indexIdentifier)
		}
	} else {
		mt.versionStore.indexMutex.RUnlock()
		// Drop the index by column name
		return mt.DropColumnarIndex(indexName)
	}

	return fmt.Errorf("index %s not found", indexName)
}

// Close closes the table
func (mt *MVCCTable) Close() error {
	// Just release resources
	mt.versionStore = nil
	mt.txnVersions = nil
	return nil
}

// PKOperationInfo contains the result of a primary key operation check
// with optimized information for fast path execution
type PKOperationInfo struct {
	// Primary key ID (for integer PKs)
	ID int64

	// Optimized expression for direct execution
	Expr *expression.SimpleExpression

	// Original comparison operator
	Operator storage.Operator

	// Whether this is a valid operation for fast path
	Valid bool

	// Special marker for empty result (contradictory conditions)
	EmptyResult bool
}

// singleRowScanner is a simple scanner that returns a single row
type singleRowScanner struct {
	row           storage.Row
	schema        storage.Schema
	columnIndices []int
	done          bool
}

// newSingleRowScanner creates a new scanner for a single row
func newSingleRowScanner(row storage.Row, schema storage.Schema, columnIndices []int) storage.Scanner {
	return &singleRowScanner{
		row:           row,
		schema:        schema,
		columnIndices: columnIndices,
		done:          false,
	}
}

// Next advances to the next row, returns false after first row
func (s *singleRowScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}

// Row returns the current row, with projection if needed
func (s *singleRowScanner) Row() storage.Row {
	if len(s.columnIndices) == 0 {
		return s.row
	}

	// Project the row to only include the requested columns
	projectedRow := make(storage.Row, len(s.columnIndices))
	for i, idx := range s.columnIndices {
		if idx < len(s.row) {
			projectedRow[i] = s.row[idx]
		}
	}
	return projectedRow
}

// Err returns any error that occurred
func (s *singleRowScanner) Err() error {
	return nil
}

// Close releases resources
func (s *singleRowScanner) Close() error {
	s.row = nil
	return nil
}

// emptyScanner is a scanner that returns no rows
type emptyScanner struct{}

// newEmptyScanner creates a new empty scanner
func newEmptyScanner() storage.Scanner {
	return &emptyScanner{}
}

// Next always returns false for empty scanner
func (s *emptyScanner) Next() bool {
	return false
}

// Row always returns nil for empty scanner
func (s *emptyScanner) Row() storage.Row {
	return nil
}

// Err always returns nil for empty scanner
func (s *emptyScanner) Err() error {
	return nil
}

// Close does nothing for empty scanner
func (s *emptyScanner) Close() error {
	return nil
}

// RowCount returns the number of rows in the table
func (mt *MVCCTable) RowCount() int {
	// Create set of all row IDs
	processedKeys := GetProcessedKeysMap()
	defer PutProcessedKeysMap(processedKeys)

	rowCount := 0

	// First grab visible versions from the version store
	// We don't need the full data, just how many rows are visible
	visibleVersions := mt.versionStore.GetAllVisibleVersions(mt.txnID)
	visibleVersions.ForEach(func(rowID int64, version *RowVersion) bool {
		if !version.IsDeleted {
			processedKeys.Put(rowID, struct{}{})
			rowCount++
		}
		return true
	})
	ReturnVisibleVersionMap(visibleVersions)

	// Apply local changes that might override global versions
	mt.txnVersions.localVersions.ForEach(func(rowID int64, version RowVersion) bool {
		if version.IsDeleted {
			// If deleted locally, remove from count
			if processedKeys.Has(rowID) {
				processedKeys.Del(rowID)
				rowCount--
			}
		} else if !processedKeys.Has(rowID) {
			// If not already counted and not deleted
			processedKeys.Put(rowID, struct{}{})
			rowCount++
		}

		return true
	})

	return rowCount
}

// Commit merges the transaction's local changes to the global version store
func (mt *MVCCTable) Commit() error {
	// Merge local changes into the global version store
	// The Commit method now automatically handles returning the object to the pool
	mt.txnVersions.Commit()
	mt.txnVersions = nil // Clear reference after commit
	return nil
}

// Rollback aborts the transaction and releases resources
func (mt *MVCCTable) Rollback() error {
	// Abort the transaction
	if mt.txnVersions != nil {
		mt.txnVersions.Rollback()
		mt.txnVersions = nil // Clear reference after rollback
	}
	return nil
}

// CreateColumnarIndex creates a columnar index for a column
// This provides HTAP capabilities by maintaining column-oriented indexes
// CreateColumnarIndex creates a columnar index for a column with optional uniqueness constraint
// If a custom name is provided, it will be used instead of the default generated name.
func (mt *MVCCTable) CreateColumnarIndex(columnName string, isUnique bool, customName ...string) error {
	// Check if version store is valid
	if mt.versionStore == nil {
		return fmt.Errorf("version store not available")
	}

	// Get schema to find column ID
	schema := mt.Schema()
	columnID := -1
	var dataType storage.DataType
	isPrimaryKey := false

	// Find column in schema
	for i, col := range schema.Columns {
		if col.Name == columnName {
			columnID = i
			dataType = col.Type
			isPrimaryKey = col.PrimaryKey
			break
		}
	}

	if columnID == -1 {
		return fmt.Errorf("column %s not found in schema", columnName)
	}

	// Prevent creating columnar indexes on primary key columns
	if isPrimaryKey {
		return fmt.Errorf("cannot create index on primary key column %s", columnName)
	}

	// Extract custom name if provided
	var indexName string
	if len(customName) > 0 && customName[0] != "" {
		indexName = customName[0]
	} else {
		// Use a standardized naming pattern, consistent with the rest of the system
		if isUnique {
			indexName = fmt.Sprintf("unique_columnar_%s_%s", mt.versionStore.tableName, columnName)
		} else {
			indexName = fmt.Sprintf("columnar_%s_%s", mt.versionStore.tableName, columnName)
		}
	}

	// The version store's CreateColumnarIndex method already handles locking properly
	index, err := mt.versionStore.CreateColumnarIndex(mt.versionStore.tableName, columnName, columnID, dataType, isUnique, indexName)
	if err != nil {
		return err
	}

	// Record the creation in the WAL if persistence is enabled
	if mt.engine != nil && mt.engine.persistence != nil && mt.engine.persistence.IsEnabled() {
		// Serialize the index metadata for WAL recording
		indexData, serErr := SerializeIndexMetadata(index)
		if serErr != nil {
			log.Printf("Warning: Failed to serialize index metadata for WAL: %v\n", serErr)
			return nil
		}

		// Record in WAL using the dedicated index operation method
		err = mt.engine.persistence.RecordIndexOperation(
			mt.versionStore.tableName,
			WALCreateIndex,
			indexData, // Use the properly serialized index metadata
		)

		if err != nil {
			// Log the error but don't fail the operation since the index was already created
			log.Printf("Warning: Failed to record index creation in WAL: %v\n", err)
		}
	}

	return nil
}

// GetColumnarIndex retrieves a columnar index for a column if it exists
func (mt *MVCCTable) GetColumnarIndex(indexIdentifier string) (storage.Index, error) {
	// Check if the version store exists
	if mt.versionStore == nil {
		return nil, fmt.Errorf("version store not available")
	}

	total := 17 + len(mt.versionStore.tableName) + len(indexIdentifier)
	var columnarName strings.Builder
	columnarName.Grow(total)
	columnarName.WriteString("unique_columnar_")
	columnarName.WriteString(mt.versionStore.tableName)
	columnarName.WriteString("_")
	columnarName.WriteString(indexIdentifier)

	// Try multiple standard naming patterns for backward compatibility
	possibleIndexNames := []string{
		indexIdentifier, // Try the provided name first
		// Try standard index naming patterns
		columnarName.String()[7:],
		columnarName.String(),
	}

	// Try all possible names
	for _, possibleName := range possibleIndexNames {
		// Try to get the index from the version store using this name
		index, err := mt.versionStore.GetColumnarIndex(possibleName)
		if err == nil && index != nil {
			// Make sure it's a columnar index type
			if index.IndexType() != storage.ColumnarIndex {
				continue // Not the right type, try next name
			}
			return index, nil
		}
	}

	// If we get here, none of our standard patterns worked
	return nil, fmt.Errorf("columnar index for identifier %s not found", indexIdentifier)
}

// DropColumnarIndex removes a columnar index by index identifier (index name)
func (mt *MVCCTable) DropColumnarIndex(indexIdentifier string) error {
	// Check if version store is valid
	if mt.versionStore == nil {
		return fmt.Errorf("version store not available")
	}

	// Try multiple standard naming patterns for backward compatibility
	possibleIndexNames := []string{
		indexIdentifier, // Try the provided name first
		// Try standard index naming patterns
		fmt.Sprintf("columnar_%s_%s", mt.versionStore.tableName, indexIdentifier),
		fmt.Sprintf("unique_columnar_%s_%s", mt.versionStore.tableName, indexIdentifier),
	}

	mt.versionStore.indexMutex.RLock()
	var index storage.Index
	var keyToDelete string
	exists := false

	// Try all possible index names
	for _, possibleName := range possibleIndexNames {
		// First try direct map lookup
		if idx, found := mt.versionStore.indexes[possibleName]; found {
			index = idx
			keyToDelete = possibleName
			exists = true
			break
		}
	}

	// If still not found, search through all indexes for a name match
	// This is for backward compatibility
	if !exists {
		for key, idx := range mt.versionStore.indexes {
			// Try matching either by key or by index name
			if idx.Name() == indexIdentifier {
				index = idx
				keyToDelete = key // We need to delete using the map key
				exists = true
				break
			}
		}
	}
	mt.versionStore.indexMutex.RUnlock()

	if !exists {
		return fmt.Errorf("index '%s' not found", indexIdentifier)
	}

	// Remember the index name for logging and WAL
	indexName := index.Name()

	// Now acquire the write lock for the actual modification
	mt.versionStore.indexMutex.Lock()
	defer mt.versionStore.indexMutex.Unlock()

	// Remove the index from the map using the correct key
	delete(mt.versionStore.indexes, keyToDelete)

	// Attempt to close the index resources if it implements a Close method
	if closeableIndex, ok := index.(interface{ Close() error }); ok {
		_ = closeableIndex.Close() // Ignore errors during cleanup
	}

	// Record the drop in the WAL if persistence is enabled
	if mt.engine != nil && mt.engine.persistence != nil && mt.engine.persistence.IsEnabled() {
		// For drop operations, use the index name
		err := mt.engine.persistence.RecordIndexOperation(
			mt.versionStore.tableName,
			WALDropIndex,
			[]byte(indexName), // Store index name for drop operations
		)

		if err != nil {
			// Log the error but don't fail the operation since the index was already dropped
			log.Printf("Warning: Failed to record index drop in WAL: %v\n", err)
		}
	}

	return nil
}

// GetFilteredRowIDs extracts row IDs that match the expression using columnar indexes
// This is optimized for direct iteration over row IDs without materializing intermediate rows
func (mt *MVCCTable) GetFilteredRowIDs(expr storage.Expression) []int64 {
	rowIDs, _ := mt.GetFilteredRowIDsWithIndex(expr)
	return rowIDs
}

// GetFilteredRowIDsWithIndex is an extended version of GetFilteredRowIDs that also returns
// the index used to filter the rowIDs, which is useful for choosing the appropriate iterator
func (mt *MVCCTable) GetFilteredRowIDsWithIndex(expr storage.Expression) ([]int64, storage.Index) {
	// Fast exit for nil expression or nil version store
	if expr == nil || mt.versionStore == nil {
		return nil, nil
	}

	// Fetch schema and prepare expression (cached during a query)
	schema, err := mt.versionStore.GetTableSchema()
	if err != nil {
		return nil, nil
	}

	// Ensure the expression is prepared with schema
	expr = expr.PrepareForSchema(schema)

	// Ultra-fast path: check for equality on a single column (most common case)
	// This is a specialized version of the more general SimpleExpression check below
	// but directly checks for equality which is by far the most common operation
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok && simpleExpr.Operator == storage.EQ {
		// Try to get a direct columnar index for this column (most efficient)
		index, err := mt.GetColumnarIndex(simpleExpr.Column)
		if err == nil {
			// Special fast path for equality on exact match index
			return GetRowIDsFromColumnarIndex(simpleExpr, index), index
		}
	}

	// Fast path for other simple expressions on a single column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		// Get the index directly using our consistent method
		index, err := mt.GetColumnarIndex(simpleExpr.Column)
		if err == nil {
			// Use the optimized path for this index
			return GetRowIDsFromColumnarIndex(simpleExpr, index), index
		}
	}

	// Fast path for NullCheck expressions
	if nullExpr, ok := expr.(*expression.NullCheckExpression); ok {
		// Get the index directly using our consistent method
		index, err := mt.GetColumnarIndex(nullExpr.GetColumnName())
		if err == nil {
			// Use the optimized path for this index
			return GetRowIDsFromColumnarIndex(nullExpr, index), index
		}
	}

	// Fast path for range expressions
	if rangeExpr, ok := expr.(*expression.RangeExpression); ok {
		// Get the index directly using our consistent method
		index, err := mt.GetColumnarIndex(rangeExpr.Column)
		if err == nil {
			// Use the optimized path for this index
			return index.GetFilteredRowIDs(rangeExpr), index
		}
	}

	// Fast path for between expressions
	if betweenExpr, ok := expr.(*expression.BetweenExpression); ok {
		// Get the index directly using our consistent method
		index, err := mt.GetColumnarIndex(betweenExpr.Column)
		if err == nil {
			// Use the optimized path for this index
			return GetRowIDsFromColumnarIndex(betweenExpr, index), index
		}
	}

	// Fast path for OR expressions with optimized sorting approach
	if orExpr, ok := expr.(*expression.OrExpression); ok {
		// Check if we can optimize the OR expression
		if len(orExpr.Expressions) == 2 {
			expr1, ok1 := orExpr.Expressions[0].(*expression.SimpleExpression)
			expr2, ok2 := orExpr.Expressions[1].(*expression.SimpleExpression)

			// Check if both expressions are for the same column
			if ok1 && ok2 && expr1.Column == expr2.Column {
				// Get the index for this column
				index, err := mt.GetColumnarIndex(expr1.Column)
				if err == nil {
					// Get results for each condition
					rowIDs1 := GetRowIDsFromColumnarIndex(expr1, index)
					rowIDs2 := GetRowIDsFromColumnarIndex(expr2, index)

					// Union the results using the optimized unionSortedIDs function
					if len(rowIDs1) > 0 || len(rowIDs2) > 0 {
						result := unionSortedIDs(rowIDs1, rowIDs2)
						return result, index
					}
				}
			}
		}

		// For more complex OR expressions, try to extract column references
		columnRefs := extractColumnReferences(expr)
		if len(columnRefs) > 0 {
			// Check if we have a multi-column index that can handle this
			if len(columnRefs) > 1 {
				multiIndex := mt.findMultiColumnIndex(columnRefs)
				if multiIndex != nil {
					return multiIndex.GetFilteredRowIDs(expr), multiIndex
				}
			}

			// Process each condition to gather results
			var result []int64
			var firstResult = true
			var bestIndex storage.Index

			for _, subExpr := range orExpr.Expressions {
				// Extract columns from this sub-expression
				subColumns := extractColumnReferences(subExpr)
				if len(subColumns) == 0 {
					continue
				}

				// Try to find an index for this sub-expression
				var index storage.Index
				var rowIDs []int64

				if len(subColumns) == 1 {
					// Single column expression
					colIndex, err := mt.GetColumnarIndex(subColumns[0])
					if err == nil {
						rowIDs = GetRowIDsFromColumnarIndex(subExpr, colIndex)
						index = colIndex
					}
				} else {
					// Multi-column expression
					multiIndex := mt.findMultiColumnIndex(subColumns)
					if multiIndex != nil {
						rowIDs = multiIndex.GetFilteredRowIDs(subExpr)
						index = multiIndex
					}
				}

				if len(rowIDs) > 0 {
					if firstResult {
						// First result becomes our initial set
						result = rowIDs
						bestIndex = index
						firstResult = false
					} else {
						// Union with previous results
						result = unionSortedIDs(result, rowIDs)
					}
				}
			}

			if !firstResult {
				return result, bestIndex
			}
		}
	}

	// Fast path for simple AND expression with two conditions
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		if ok1 && ok2 {
			// Case 1: Conditions on the same column (e.g., x > 10 AND x < 20)
			if expr1.Column == expr2.Column {
				colName := expr1.Column

				// Get the index directly
				index, err := mt.GetColumnarIndex(colName)
				if err == nil {
					// Let the index handle the AND condition directly
					return index.GetFilteredRowIDs(andExpr), index
				}
			} else {
				// Case 2: Conditions on different columns (e.g., x > 10 AND y = true)
				// First check if we have a multi-column index that includes both columns
				multiIndex := mt.findMultiColumnIndex([]string{expr1.Column, expr2.Column})
				if multiIndex != nil {
					// If we have a multi-column index, use it directly
					return multiIndex.GetFilteredRowIDs(andExpr), multiIndex
				}

				// Otherwise fall back to using individual column indexes
				// Determine which expression is more likely to be selective
				// Equality conditions typically filter out more rows than range conditions
				var moreSelectiveExpr, lessSelectiveExpr *expression.SimpleExpression

				// Prioritize equality checks as they're more selective
				if expr1.Operator == storage.EQ && expr2.Operator != storage.EQ {
					moreSelectiveExpr = expr1
					lessSelectiveExpr = expr2
				} else if expr2.Operator == storage.EQ && expr1.Operator != storage.EQ {
					moreSelectiveExpr = expr2
					lessSelectiveExpr = expr1
				} else {
					// Default to the first expression as more selective if we can't determine
					moreSelectiveExpr = expr1
					lessSelectiveExpr = expr2
				}

				// Get indexes for both expressions
				indexSelective, err1 := mt.GetColumnarIndex(moreSelectiveExpr.Column)
				indexLessSelective, err2 := mt.GetColumnarIndex(lessSelectiveExpr.Column)

				// If we have both indexes, apply the more selective one first
				if err1 == nil && err2 == nil {
					// Get matching row IDs from more selective condition first
					rowIDs1 := GetRowIDsFromColumnarIndex(moreSelectiveExpr, indexSelective)
					if len(rowIDs1) == 0 {
						return nil, nil
					}

					// Get matching row IDs from less selective condition
					rowIDs2 := GetRowIDsFromColumnarIndex(lessSelectiveExpr, indexLessSelective)
					if len(rowIDs2) == 0 {
						return nil, nil
					}

					// Intersect the results and return with the more selective index
					// We choose this one because it's likely to provide better filtering
					return intersectSortedIDs(rowIDs1, rowIDs2), indexSelective
				}
			}
		}
	}

	// Try to extract column references to find usable indexes
	columnRefs := extractColumnReferences(expr)
	if len(columnRefs) == 0 {
		return nil, nil
	}

	// Check if we have a multi-column index that can handle this query
	if len(columnRefs) > 1 {
		multiIndex := mt.findMultiColumnIndex(columnRefs)
		if multiIndex != nil {
			// Get filtered row IDs using the multi-column index
			rowIDs := multiIndex.GetFilteredRowIDs(expr)
			if len(rowIDs) > 0 {
				return rowIDs, multiIndex
			}
		}
	}

	// Optimization: if there's only one column reference, retrieve directly
	if len(columnRefs) == 1 {
		colName := columnRefs[0]
		index, err := mt.GetColumnarIndex(colName)
		if err == nil {
			return GetRowIDsFromColumnarIndex(expr, index), index
		}
	}

	// Standard path for multiple conditions on different columns
	var matchingRowIDs []int64
	var foundIndex bool
	var usedIndex storage.Index

	// Create a list of available columnar indexes for these columns
	availableIndexColumns := make([]string, 0, len(columnRefs))
	for _, colName := range columnRefs {
		// Check if index exists using our consistent method
		if _, err := mt.GetColumnarIndex(colName); err == nil {
			availableIndexColumns = append(availableIndexColumns, colName)
		}
	}

	// Optimization: If no indexes are available, return early
	if len(availableIndexColumns) == 0 {
		return nil, nil
	}

	// Try to find the most selective index first (the one likely to have the fewest matches)
	// This makes the intersection operations more efficient by starting with smaller sets
	if len(availableIndexColumns) > 1 {
		// Simple heuristic - use equality conditions first if available
		var mostSelectiveCol string
		var foundEquality bool

		for _, colName := range availableIndexColumns {
			// Simple check for equality conditions
			if hasEqualityCondition(expr, colName) {
				mostSelectiveCol = colName
				foundEquality = true
				break
			}
		}

		// If we found an equality condition, process it first
		if foundEquality {
			index, err := mt.GetColumnarIndex(mostSelectiveCol)
			if err == nil {
				rowIDs := GetRowIDsFromColumnarIndex(expr, index)
				if len(rowIDs) > 0 {
					matchingRowIDs = rowIDs
					foundIndex = true
					usedIndex = index

					// Remove this column from the processing list
					for i := 0; i < len(availableIndexColumns); i++ {
						if availableIndexColumns[i] == mostSelectiveCol {
							availableIndexColumns = append(availableIndexColumns[:i], availableIndexColumns[i+1:]...)
							break
						}
					}
				}
			}
		}
	}

	// Process the remaining indexes
	for _, colName := range availableIndexColumns {
		// Skip if we've already processed this column
		if foundIndex && matchingRowIDs != nil && len(matchingRowIDs) == 0 {
			// Short circuit if we already have an empty result set
			return nil, nil
		}

		// Try to get a columnar index for this column
		index, err := mt.GetColumnarIndex(colName)
		if err != nil {
			continue
		}

		// Use the index to get matching row IDs directly
		rowIDs := GetRowIDsFromColumnarIndex(expr, index)

		if len(rowIDs) > 0 {
			if !foundIndex {
				// First index found
				matchingRowIDs = rowIDs
				foundIndex = true
				usedIndex = index
			} else {
				// Intersect with previous results using optimized intersection
				matchingRowIDs = intersectSortedIDs(matchingRowIDs, rowIDs)
			}
		}
	}

	return matchingRowIDs, usedIndex
}

// findMultiColumnIndex tries to find a multi-column index that includes all the specified columns
func (mt *MVCCTable) findMultiColumnIndex(columnNames []string) *MultiColumnarIndex {
	if len(columnNames) < 2 || mt.versionStore == nil {
		return nil
	}

	// Create a map for O(1) lookups of column names
	columnSet := make(map[string]bool, len(columnNames))
	for _, colName := range columnNames {
		columnSet[colName] = true
	}

	// Check all indexes
	mt.versionStore.indexMutex.RLock()
	defer mt.versionStore.indexMutex.RUnlock()

	for _, idx := range mt.versionStore.indexes {
		// Skip non-multi-columnar indexes
		multiIdx, isMulti := idx.(*MultiColumnarIndex)
		if !isMulti {
			continue
		}

		// Check if this index contains all our columns
		indexColumns := multiIdx.ColumnNames()

		// Check the number of columns first - if the index has fewer columns than our query,
		// it can't possibly contain all the required columns
		if len(indexColumns) < len(columnNames) {
			continue
		}

		// Check if all query columns are in this index
		allColumnsFound := true
		for _, colName := range columnNames {
			found := false
			for _, indexCol := range indexColumns {
				if colName == indexCol {
					found = true
					break
				}
			}
			if !found {
				allColumnsFound = false
				break
			}
		}

		if allColumnsFound {
			return multiIdx
		}
	}

	return nil
}

// GetRowsWithFilter uses columnar indexes to filter rows based on an expression
// Returns a map of row IDs to rows that match the filter
func (mt *MVCCTable) GetRowsWithFilter(expr storage.Expression) *fastmap.Int64Map[storage.Row] {
	// Start with an empty result
	result := fastmap.NewInt64Map[storage.Row](100)

	// If the expression is nil or version store is not available, return empty result
	if expr == nil || mt.versionStore == nil {
		return result
	}

	// Get filtered row IDs using our optimized method
	matchingRowIDs := mt.GetFilteredRowIDs(expr)

	// If we found matching rows, fetch them from the version store
	if len(matchingRowIDs) > 0 {
		// For efficiency, use the batch version retrieval if there are many rows
		if len(matchingRowIDs) > 10 {
			// Get visible versions in bulk for efficiency
			versions := mt.versionStore.GetVisibleVersionsByIDs(matchingRowIDs, mt.txnID)
			defer ReturnVisibleVersionMap(versions)

			// Process the visible versions
			versions.ForEach(func(rowID int64, version *RowVersion) bool {
				if !version.IsDeleted {
					result.Put(rowID, version.Data)
				}

				return true
			})
		} else {
			// For smaller sets, get rows individually
			for _, rowID := range matchingRowIDs {
				version, exists := mt.versionStore.GetVisibleVersion(rowID, mt.txnID)
				if exists && !version.IsDeleted {
					result.Put(rowID, version.Data)
				}
			}
		}
	}

	return result
}
