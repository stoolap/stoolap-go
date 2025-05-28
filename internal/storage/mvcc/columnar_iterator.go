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
	"slices"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// ColumnarIndexIterator provides direct and efficient iteration over columnar index matches
type ColumnarIndexIterator struct {
	versionStore  *VersionStore
	txnID         int64
	schema        storage.Schema
	columnIndices []int

	// For direct sorted iteration
	rowIDs    []int64
	idIndex   int
	batchSize int

	// For current row
	currentRow   storage.Row
	projectedRow storage.Row

	// For pre-fetching
	prefetchRowIDs []int64
	prefetchIndex  int
	prefetchMap    *fastmap.Int64Map[storage.Row]
}

// NewColumnarIndexIterator creates an efficient iterator over the matched row IDs
func NewColumnarIndexIterator(
	versionStore *VersionStore,
	rowIDs []int64,
	txnID int64,
	schema storage.Schema,
	columnIndices []int) storage.Scanner {

	// Sort row IDs for efficient access pattern and sequential prefetching
	SIMDSortInt64s(rowIDs)

	// Choose optimal batch size based on result set size
	batchSize := 100 // Default batch size
	if len(rowIDs) > 1000 {
		// For large result sets, use larger batches
		batchSize = 200
	} else if len(rowIDs) < 50 {
		// For tiny result sets, use smaller batches to avoid waste
		batchSize = max(10, len(rowIDs))
	}

	return &ColumnarIndexIterator{
		versionStore:  versionStore,
		txnID:         txnID,
		schema:        schema,
		columnIndices: columnIndices,
		rowIDs:        rowIDs,
		idIndex:       -1,
		batchSize:     batchSize,
		prefetchIndex: 0,
		projectedRow:  make(storage.Row, len(columnIndices)),
		prefetchMap:   GetRowMap(),
	}
}

// Next advances to the next matching row
func (it *ColumnarIndexIterator) Next() bool {
	// Check if we need to prefetch the next batch
	if it.prefetchIndex >= len(it.prefetchRowIDs) {
		// Calculate next batch to prefetch
		remainingRows := len(it.rowIDs) - it.idIndex - 1
		if remainingRows <= 0 {
			return false // No more rows
		}

		// Determine batch size (don't exceed remaining rows)
		// Optimize batch size based on remaining rows and data density
		batchSize := it.batchSize
		if remainingRows < batchSize {
			batchSize = remainingRows
		} else if remainingRows > batchSize*10 {
			// For large result sets, use a larger batch size to reduce the
			// number of fetches and improve performance
			batchSize = min(remainingRows/5, 500) // Cap at 500 to avoid excessive memory usage
		}

		// Clear previous batch data
		it.prefetchMap.Clear()

		// Prefetch batch of IDs with optimized slice handling
		endIdx := it.idIndex + 1 + batchSize
		if endIdx > len(it.rowIDs) {
			endIdx = len(it.rowIDs)
		}

		// Reuse the prefetch slice if possible to reduce allocations
		if cap(it.prefetchRowIDs) >= endIdx-(it.idIndex+1) {
			it.prefetchRowIDs = it.prefetchRowIDs[:0] // Reset length but keep capacity
			it.prefetchRowIDs = append(it.prefetchRowIDs, it.rowIDs[it.idIndex+1:endIdx]...)
		} else {
			it.prefetchRowIDs = it.rowIDs[it.idIndex+1 : endIdx]
		}
		it.prefetchIndex = 0

		// Batch fetch visible versions
		versions := it.versionStore.GetVisibleVersionsByIDs(it.prefetchRowIDs, it.txnID)

		// Track if any rows were found to determine early exit
		foundAny := false

		// Extract non-deleted rows
		versions.ForEach(func(rowID int64, version *RowVersion) bool {
			if !version.IsDeleted() {
				it.prefetchMap.Put(rowID, version.Data)
				foundAny = true
			}

			return true
		})

		// IMPORTANT: Free the versions map to avoid memory leak
		ReturnVisibleVersionMap(versions)

		// If nothing found, skip to next batch immediately rather than looping
		if !foundAny {
			// Advance the idIndex past this entire batch to avoid revisiting
			it.idIndex += len(it.prefetchRowIDs)
			// Try next batch directly, skipping the loop below
			return it.Next()
		}
	}

	// Fast path: Advance until we find a visible row
	for it.prefetchIndex < len(it.prefetchRowIDs) {
		rowID := it.prefetchRowIDs[it.prefetchIndex]
		it.prefetchIndex++
		it.idIndex++

		// Check if this row is in our prefetch map
		if row, exists := it.prefetchMap.Get(rowID); exists {
			// Use direct assignment for better performance
			it.currentRow = row
			return true
		}
	}

	// Try next batch
	return it.Next()
}

// Row returns the current row
func (it *ColumnarIndexIterator) Row() storage.Row {
	// Fast path for full row projection
	if len(it.columnIndices) == 0 {
		return it.currentRow
	}

	// Fast path for single column projection (common case)
	if len(it.columnIndices) == 1 {
		colIdx := it.columnIndices[0]
		if colIdx < len(it.currentRow) {
			it.projectedRow[0] = it.currentRow[colIdx]
		} else {
			// Column doesn't exist, set to nil
			it.projectedRow[0] = nil
		}
		return it.projectedRow
	}

	// Regular column projection for multiple columns
	for i, colIdx := range it.columnIndices {
		if colIdx < len(it.currentRow) {
			it.projectedRow[i] = it.currentRow[colIdx]
		} else {
			it.projectedRow[i] = nil
		}
	}
	return it.projectedRow
}

// Err returns any error
func (it *ColumnarIndexIterator) Err() error {
	return nil
}

// Close releases resources
func (it *ColumnarIndexIterator) Close() error {
	clear(it.projectedRow)
	it.projectedRow = it.projectedRow[:0]

	it.rowIDs = nil
	it.prefetchRowIDs = nil
	it.currentRow = nil

	it.prefetchMap.Clear()
	PutRowMap(it.prefetchMap)

	return nil
}

// Helper for direct access to row IDs from columnar index
func GetRowIDsFromColumnarIndex(expr storage.Expression, index storage.Index) []int64 {
	// First check for MultiColumnarIndex
	if multiIndex, ok := index.(*MultiColumnarIndex); ok {
		// MultiColumnarIndex has its own efficient implementations for filtering
		return multiIndex.GetFilteredRowIDs(expr)
	}

	// Extract the column name from the single-column index
	var indexColumnName string
	if bi, ok := index.(*ColumnarIndex); ok {
		indexColumnName = bi.columnName
	}

	// If the expression is a SimpleExpression, check if it matches the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok && indexColumnName != "" {
		// If the expression is for this specific column, extract only that condition
		if simpleExpr.Column == indexColumnName {
			// Use type assertion to handle different index implementations
			if concreteIndex, ok := index.(*ColumnarIndex); ok {
				return getRowIDsFromColumnarIndex(simpleExpr, concreteIndex)
			} else {
				// For any other index that implements FilteredRowIDs interface
				if filteredIndex, ok := index.(interface {
					GetFilteredRowIDs(expr storage.Expression) []int64
				}); ok {
					return filteredIndex.GetFilteredRowIDs(simpleExpr)
				}
			}
		}
	}

	// Fast path for RangeExpression directly on the indexed column
	if rangeExpr, ok := expr.(*expression.RangeExpression); ok && indexColumnName != "" {
		if rangeExpr.Column == indexColumnName {
			// Use type assertion to handle different index implementations
			if concreteIndex, ok := index.(*ColumnarIndex); ok {
				return getRowIDsFromRangeExpression(rangeExpr, concreteIndex)
			} else {
				// For any other index that implements FilteredRowIDs interface
				if filteredIndex, ok := index.(interface {
					GetFilteredRowIDs(expr storage.Expression) []int64
				}); ok {
					return filteredIndex.GetFilteredRowIDs(rangeExpr)
				}
			}
		}
	}

	// Fast path for BetweenExpression directly on the indexed column
	if betweenExpr, ok := expr.(*expression.BetweenExpression); ok && indexColumnName != "" {
		if betweenExpr.Column == indexColumnName {
			// Use type assertion to handle different index implementations
			if concreteIndex, ok := index.(*ColumnarIndex); ok {
				return getRowIDsFromBetweenExpression(betweenExpr, concreteIndex)
			} else {
				// For any other index that implements FilteredRowIDs interface
				if filteredIndex, ok := index.(interface {
					GetFilteredRowIDs(expr storage.Expression) []int64
				}); ok {
					return filteredIndex.GetFilteredRowIDs(betweenExpr)
				}
			}
		}
	}

	// Fast path for NullCheckExpression directly on the indexed column
	if nullExpr, ok := expr.(*expression.NullCheckExpression); ok && indexColumnName != "" {
		if nullExpr.GetColumnName() == indexColumnName {
			// Use type assertion to handle different index implementations
			if concreteIndex, ok := index.(*ColumnarIndex); ok {
				return getRowIDsFromNullCheckExpression(nullExpr, concreteIndex)
			} else {
				// For any other index that implements FilteredRowIDs interface
				if filteredIndex, ok := index.(interface {
					GetFilteredRowIDs(expr storage.Expression) []int64
				}); ok {
					return filteredIndex.GetFilteredRowIDs(nullExpr)
				}
			}
		}
	}

	// For AND expressions, extract the condition for this specific column if possible
	if andExpr, ok := expr.(*expression.AndExpression); ok && indexColumnName != "" {
		for _, subExpr := range andExpr.Expressions {
			if simpleExpr, ok := subExpr.(*expression.SimpleExpression); ok {
				if simpleExpr.Column == indexColumnName {
					// Use type assertion to handle different index implementations
					if concreteIndex, ok := index.(*ColumnarIndex); ok {
						return getRowIDsFromColumnarIndex(simpleExpr, concreteIndex)
					} else {
						// For any other index that implements FilteredRowIDs interface
						if filteredIndex, ok := index.(interface {
							GetFilteredRowIDs(expr storage.Expression) []int64
						}); ok {
							return filteredIndex.GetFilteredRowIDs(simpleExpr)
						}
					}
				}
			} else if rangeExpr, ok := subExpr.(*expression.RangeExpression); ok {
				if rangeExpr.Column == indexColumnName {
					// Use type assertion to handle different index implementations
					if concreteIndex, ok := index.(*ColumnarIndex); ok {
						return getRowIDsFromRangeExpression(rangeExpr, concreteIndex)
					} else {
						// For any other index that implements FilteredRowIDs interface
						if filteredIndex, ok := index.(interface {
							GetFilteredRowIDs(expr storage.Expression) []int64
						}); ok {
							return filteredIndex.GetFilteredRowIDs(rangeExpr)
						}
					}
				}
			} else if betweenExpr, ok := subExpr.(*expression.BetweenExpression); ok {
				if betweenExpr.Column == indexColumnName {
					// Use type assertion to handle different index implementations
					if concreteIndex, ok := index.(*ColumnarIndex); ok {
						return getRowIDsFromBetweenExpression(betweenExpr, concreteIndex)
					} else {
						// For any other index that implements FilteredRowIDs interface
						if filteredIndex, ok := index.(interface {
							GetFilteredRowIDs(expr storage.Expression) []int64
						}); ok {
							return filteredIndex.GetFilteredRowIDs(betweenExpr)
						}
					}
				}
			} else if nullExpr, ok := subExpr.(*expression.NullCheckExpression); ok {
				if nullExpr.GetColumnName() == indexColumnName {
					// Use type assertion to handle different index implementations
					if concreteIndex, ok := index.(*ColumnarIndex); ok {
						return getRowIDsFromNullCheckExpression(nullExpr, concreteIndex)
					} else {
						// For any other index that implements FilteredRowIDs interface
						if filteredIndex, ok := index.(interface {
							GetFilteredRowIDs(expr storage.Expression) []int64
						}); ok {
							return filteredIndex.GetFilteredRowIDs(nullExpr)
						}
					}
				}
			}
		}
	}

	// Fall back to the default behavior for more complex expressions
	if concreteIndex, ok := index.(*ColumnarIndex); ok {
		return getRowIDsFromColumnarIndex(expr, concreteIndex)
	} else {
		// For any other index that implements FilteredRowIDs interface
		if filteredIndex, ok := index.(interface {
			GetFilteredRowIDs(expr storage.Expression) []int64
		}); ok {
			return filteredIndex.GetFilteredRowIDs(expr)
		}
		return nil
	}
}

// Helper for columnar index implementation with RangeExpression
func getRowIDsFromRangeExpression(expr *expression.RangeExpression, index *ColumnarIndex) []int64 {
	// Optimize for range queries
	// Get the range bounds
	var minValue, maxValue storage.ColumnValue

	// Convert directly from the original values
	if expr.MinValue != nil {
		minValue = storage.ValueToPooledColumnValue(expr.MinValue, index.dataType)
		defer storage.PutPooledColumnValue(minValue)
	}
	if expr.MaxValue != nil {
		maxValue = storage.ValueToPooledColumnValue(expr.MaxValue, index.dataType)
		defer storage.PutPooledColumnValue(maxValue)
	}

	// Get the row IDs within the range
	return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, expr.IncludeMin, expr.IncludeMax)
}

// Helper for columnar index implementation with BetweenExpression
func getRowIDsFromBetweenExpression(expr *expression.BetweenExpression, index *ColumnarIndex) []int64 {
	// Optimize for BETWEEN queries
	// Get the range bounds from the BETWEEN expression
	var minValue, maxValue storage.ColumnValue

	// Convert lower bound to appropriate column value
	minValue = storage.ValueToPooledColumnValue(expr.LowerBound, index.dataType)
	defer storage.PutPooledColumnValue(minValue)

	// Convert upper bound to appropriate column value
	maxValue = storage.ValueToPooledColumnValue(expr.UpperBound, index.dataType)
	defer storage.PutPooledColumnValue(maxValue)

	// Get the row IDs within the range, inclusivity is determined by BetweenExpression's Inclusive field
	return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, expr.Inclusive, expr.Inclusive)
}

// Helper for columnar index implementation with NullCheckExpression
func getRowIDsFromNullCheckExpression(expr *expression.NullCheckExpression, index *ColumnarIndex) []int64 {
	// Determine if we're checking for NULL or NOT NULL
	isNull := expr.IsNull()

	if isNull {
		// For IS NULL, we need to get all rows with NULL values for this column
		index.mutex.RLock()
		defer index.mutex.RUnlock()

		if index.nullRows.Len() == 0 {
			return nil // No NULL rows
		}

		// Allocate with exact capacity to avoid resizing
		nullCount := index.nullRows.Len()
		result := make([]int64, 0, nullCount)

		// Extract all row IDs with NULL values
		index.nullRows.ForEach(func(id int64, _ struct{}) bool {
			result = append(result, id)
			return true
		})

		return result
	} else {
		// For IS NOT NULL, we need to get all rows with non-NULL values
		index.mutex.RLock()
		defer index.mutex.RUnlock()

		// Get all row IDs from the value tree (non-NULL values)
		return index.valueTree.GetAll()
	}
}

// Helper for columnar index implementation
func getRowIDsFromColumnarIndex(expr storage.Expression, index *ColumnarIndex) []int64 {
	// Ultra-fast path for direct equality comparison on the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok &&
		simpleExpr.Column == index.columnName && simpleExpr.Operator == storage.EQ {

		exprValue := storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
		defer storage.PutPooledColumnValue(exprValue)
		// Fast equality match using our B-tree implementation
		return index.GetRowIDsEqual([]storage.ColumnValue{exprValue})
	}

	// For simple expressions on the indexed column
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok &&
		simpleExpr.Column == index.columnName {

		switch simpleExpr.Operator {
		case storage.GT, storage.GTE, storage.LT, storage.LTE:
			// Range query
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool

			if simpleExpr.Operator == storage.GT || simpleExpr.Operator == storage.GTE {
				minValue = storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = simpleExpr.Operator == storage.GTE
			} else {
				maxValue = storage.ValueToPooledColumnValue(simpleExpr.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = simpleExpr.Operator == storage.LTE
			}

			// Use the index's range function
			return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)

		case storage.ISNULL, storage.ISNOTNULL:
			// Use direct implementation in the B-tree index
			return index.GetFilteredRowIDs(simpleExpr)
		}
	}

	// Fast path for AND expressions on a single column
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		expr1, ok1 := andExpr.Expressions[0].(*expression.SimpleExpression)
		expr2, ok2 := andExpr.Expressions[1].(*expression.SimpleExpression)

		// Check if both expressions are on the same column as the index
		if ok1 && ok2 &&
			expr1.Column == index.columnName &&
			expr2.Column == index.columnName {

			// Look for range patterns: (col > X AND col < Y) or similar
			var minValue, maxValue storage.ColumnValue
			var includeMin, includeMax bool
			var hasRange bool

			// Check if expr1 is a lower bound and expr2 is an upper bound
			if (expr1.Operator == storage.GT || expr1.Operator == storage.GTE) &&
				(expr2.Operator == storage.LT || expr2.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr1.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr1.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr2.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr2.Operator == storage.LTE
				hasRange = true
			}

			// Check if expr2 is a lower bound and expr1 is an upper bound
			if (expr2.Operator == storage.GT || expr2.Operator == storage.GTE) &&
				(expr1.Operator == storage.LT || expr1.Operator == storage.LTE) {
				minValue = storage.ValueToPooledColumnValue(expr2.Value, index.dataType)
				defer storage.PutPooledColumnValue(minValue)
				includeMin = expr2.Operator == storage.GTE
				maxValue = storage.ValueToPooledColumnValue(expr1.Value, index.dataType)
				defer storage.PutPooledColumnValue(maxValue)
				includeMax = expr1.Operator == storage.LTE
				hasRange = true
			}

			if hasRange {
				return index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, includeMin, includeMax)
			}
		}
	}

	// Let the index handle the expression directly
	return index.GetFilteredRowIDs(expr)
}

// Helper to intersect sorted ID lists efficiently
func intersectSortedIDs(a []int64, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	// Optimization: If one list is much smaller than the other, swap them
	// This improves cache locality when the sizes are very different
	if len(a) > len(b)*10 {
		// b is much smaller, keep it as the inner loop for better cache performance
		a, b = b, a
	}

	// Sort if not already sorted
	if !isSorted(a) {
		SIMDSortInt64s(a)
	}

	if !isSorted(b) {
		SIMDSortInt64s(b)
	}

	// Fast path for common case of no intersection
	if a[len(a)-1] < b[0] || b[len(b)-1] < a[0] {
		return nil // Ranges don't overlap at all
	}

	// Ensure 'a' is the smaller array for better performance
	if len(a) > len(b) {
		a, b = b, a
	}

	// Binary search approach for very different sized arrays
	result := make([]int64, 0, len(a))
	for _, val := range a {
		// Use binary search instead of linear search
		if _, ok := slices.BinarySearch(b, val); ok {
			result = append(result, val)
		}
	}
	return result
}

// Helper to check if int64 slice is sorted
func isSorted(ids []int64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			return false
		}
	}
	return true
}
