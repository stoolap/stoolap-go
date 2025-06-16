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
package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/storage"
)

// ArrayAggregateResult is an optimized aggregate result using arrays instead of maps
type ArrayAggregateResult struct {
	baseResult     storage.Result
	columns        []string
	functions      []*SqlFunction
	groupByColumns []string
	initialized    bool
	aliases        map[string]string

	// Array-based storage
	aggregatedRows []storage.Row
	currentIndex   int

	// Column indices for fast lookup
	baseColumns  []string
	baseColIndex map[string]int
}

// NewArrayAggregateResult creates a new array-based aggregate result
func NewArrayAggregateResult(baseResult storage.Result, columns []string, functions []*SqlFunction, groupByColumns []string, aliases map[string]string) *ArrayAggregateResult {
	baseColumns := baseResult.Columns()
	baseColIndex := make(map[string]int)

	// Build column index map
	for i, col := range baseColumns {
		baseColIndex[col] = i
		// Also map unqualified names
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			if _, exists := baseColIndex[unqualified]; !exists {
				baseColIndex[unqualified] = i
			}
		}
	}

	return &ArrayAggregateResult{
		baseResult:     baseResult,
		columns:        columns,
		functions:      functions,
		groupByColumns: groupByColumns,
		aliases:        aliases,
		currentIndex:   -1,
		baseColumns:    baseColumns,
		baseColIndex:   baseColIndex,
	}
}

// Columns returns the column names
func (r *ArrayAggregateResult) Columns() []string {
	return r.columns
}

// Next advances to the next row
func (r *ArrayAggregateResult) Next() bool {
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return false
		}
	}

	r.currentIndex++
	return r.currentIndex < len(r.aggregatedRows)
}

// Row returns the current row
func (r *ArrayAggregateResult) Row() storage.Row {
	if r.currentIndex >= 0 && r.currentIndex < len(r.aggregatedRows) {
		return r.aggregatedRows[r.currentIndex]
	}
	return nil
}

// Scan copies values to destinations
func (r *ArrayAggregateResult) Scan(dest ...interface{}) error {
	row := r.Row()
	if row == nil {
		return fmt.Errorf("no current row")
	}

	if len(dest) != len(row) {
		return fmt.Errorf("scan column count mismatch: %d != %d", len(dest), len(row))
	}

	for i, val := range row {
		if err := storage.ScanColumnValueToDestination(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// initialize processes the aggregation
func (r *ArrayAggregateResult) initialize() error {
	r.initialized = true

	// Group rows by group key
	type groupData struct {
		rows     []storage.Row
		firstRow storage.Row // For GROUP BY column values
	}
	groups := make(map[string]*groupData)

	// Collect all rows from base result
	for r.baseResult.Next() {
		row := r.baseResult.Row()
		if row == nil {
			continue
		}

		// Make a copy of the row
		rowCopy := make(storage.Row, len(row))
		copy(rowCopy, row)

		// Build group key
		groupKey := r.buildGroupKey(rowCopy)

		// Add row to group
		if group, exists := groups[groupKey]; exists {
			group.rows = append(group.rows, rowCopy)
		} else {
			groups[groupKey] = &groupData{
				rows:     []storage.Row{rowCopy},
				firstRow: rowCopy,
			}
		}
	}

	// Process each group
	r.aggregatedRows = make([]storage.Row, 0, len(groups))

	for _, group := range groups {
		resultRow := make(storage.Row, len(r.columns))

		// Process each column
		for i, col := range r.columns {
			// Check if it's a GROUP BY column
			isGroupBy := false
			for _, groupCol := range r.groupByColumns {
				if col == groupCol {
					isGroupBy = true

					// Check if it's a function expression
					if strings.Contains(groupCol, "(") && strings.Contains(groupCol, ")") && !strings.HasPrefix(groupCol, "(") {
						// For function expressions, we need to evaluate them
						funcNameEndIdx := strings.Index(groupCol, "(")
						if funcNameEndIdx > 0 {
							funcName := strings.TrimSpace(groupCol[:funcNameEndIdx])

							if funcName == "TIME_TRUNC" || funcName == "DATE_TRUNC" {
								// Extract arguments
								argStr := groupCol[funcNameEndIdx+1 : len(groupCol)-1]
								argParts := strings.Split(argStr, ",")
								if len(argParts) >= 2 {
									// Get duration string
									durationStr := strings.TrimSpace(argParts[0])
									if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
										durationStr = durationStr[1 : len(durationStr)-1]
									}

									// Get the timestamp column name
									timeColName := strings.TrimSpace(argParts[1])

									// Find the column value
									var timeVal storage.ColumnValue
									if idx, ok := r.baseColIndex[timeColName]; ok && idx < len(group.firstRow) {
										timeVal = group.firstRow[idx]
									}

									// Execute the function
									if timeVal != nil && !timeVal.IsNull() {
										registry := GetGlobalFunctionRegistry()
										if registry != nil {
											scalarFn := registry.GetScalarFunction(funcName)
											if scalarFn != nil {
												result, err := scalarFn.Evaluate(durationStr, timeVal.AsInterface())
												if err == nil {
													resultRow[i] = storage.NewDirectValueFromInterface(result)
													break
												}
											}
										}
									}
								}
							}
						}
						// If function evaluation failed, use NULL
						resultRow[i] = storage.NewDirectValueFromInterface(nil)
					} else {
						// Regular column - get value from first row
						if idx, ok := r.baseColIndex[groupCol]; ok && idx < len(group.firstRow) {
							resultRow[i] = group.firstRow[idx]
						} else {
							resultRow[i] = storage.NewDirectValueFromInterface(nil)
						}
					}
					break
				}
			}

			if !isGroupBy {
				// Check if it's an aggregate function
				found := false
				for _, fn := range r.functions {
					if fn.GetColumnName() == col {
						// Execute aggregate function
						val := r.executeAggregateFunction(fn, group.rows)
						resultRow[i] = val
						found = true
						break
					}
				}

				if !found {
					// Not a GROUP BY or aggregate, use NULL
					resultRow[i] = storage.NewDirectValueFromInterface(nil)
				}
			}
		}

		r.aggregatedRows = append(r.aggregatedRows, resultRow)
	}

	return nil
}

// buildGroupKey builds a group key from a row
func (r *ArrayAggregateResult) buildGroupKey(row storage.Row) string {
	if len(r.groupByColumns) == 0 {
		return "global"
	}

	groupValues := make([]string, len(r.groupByColumns))
	for i, groupCol := range r.groupByColumns {
		// Check if it's a function expression like TIME_TRUNC('15m', event_time)
		if strings.Contains(groupCol, "(") && strings.Contains(groupCol, ")") && !strings.HasPrefix(groupCol, "(") {
			// Parse function name
			funcNameEndIdx := strings.Index(groupCol, "(")
			if funcNameEndIdx > 0 {
				funcName := strings.TrimSpace(groupCol[:funcNameEndIdx])

				// Special handling for TIME_TRUNC/DATE_TRUNC
				if funcName == "TIME_TRUNC" || funcName == "DATE_TRUNC" {
					// Extract arguments
					argStr := groupCol[funcNameEndIdx+1 : len(groupCol)-1]
					argParts := strings.Split(argStr, ",")
					if len(argParts) >= 2 {
						// Get duration string
						durationStr := strings.TrimSpace(argParts[0])
						if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
							durationStr = durationStr[1 : len(durationStr)-1]
						}

						// Get the timestamp column name
						timeColName := strings.TrimSpace(argParts[1])

						// Find the column value
						var timeVal storage.ColumnValue
						if idx, ok := r.baseColIndex[timeColName]; ok && idx < len(row) {
							timeVal = row[idx]
						}

						// Execute the function
						if timeVal != nil && !timeVal.IsNull() {
							registry := GetGlobalFunctionRegistry()
							if registry != nil {
								scalarFn := registry.GetScalarFunction(funcName)
								if scalarFn != nil {
									result, err := scalarFn.Evaluate(durationStr, timeVal.AsInterface())
									if err == nil {
										groupValues[i] = fmt.Sprintf("%v", result)
										continue
									}
								}
							}
						}
					}
				}
			}
			// If function evaluation failed, use nil
			groupValues[i] = "nil"
		} else {
			// Regular column
			if idx, ok := r.baseColIndex[groupCol]; ok && idx < len(row) {
				val := row[idx]
				if val != nil && !val.IsNull() {
					groupValues[i] = fmt.Sprintf("%v", val.AsInterface())
				} else {
					groupValues[i] = "nil"
				}
			} else {
				groupValues[i] = "nil"
			}
		}
	}

	return strings.Join(groupValues, "|")
}

// executeAggregateFunction executes an aggregate function on a group of rows
func (r *ArrayAggregateResult) executeAggregateFunction(fn *SqlFunction, rows []storage.Row) storage.ColumnValue {
	// If no implementation is set, try to get it from registry
	if fn.Implementation == nil {
		registry := GetGlobalFunctionRegistry()
		if registry != nil {
			aggFunc := registry.GetAggregateFunction(fn.Name)
			if aggFunc != nil {
				fn.Implementation = aggFunc
			}
		}
	}

	switch impl := fn.Implementation.(type) {
	case contract.AggregateFunction:
		// Reset the accumulator
		impl.Reset()

		// Find column index for the function's column
		colIdx := -1
		if fn.Column != "" && fn.Column != "*" {
			if idx, ok := r.baseColIndex[fn.Column]; ok {
				colIdx = idx
			}
		}

		// Process each row
		for _, row := range rows {
			if fn.Column == "*" {
				// COUNT(*) - just count the row
				impl.Accumulate(1, fn.IsDistinct)
			} else if colIdx >= 0 && colIdx < len(row) {
				val := row[colIdx]
				if val != nil && (!val.IsNull() || fn.Name == "COUNT") {
					impl.Accumulate(val.AsInterface(), fn.IsDistinct)
				}
			}
		}

		// Get the final result
		result := impl.Result()

		// Convert to ColumnValue
		if cv, ok := result.(storage.ColumnValue); ok {
			return cv
		}
		return storage.NewDirectValueFromInterface(result)

	default:
		// Not an aggregate function
		return storage.NewDirectValueFromInterface(nil)
	}
}

// Close closes the result
func (r *ArrayAggregateResult) Close() error {
	return r.baseResult.Close()
}

// RowsAffected returns 0 for SELECT
func (r *ArrayAggregateResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns 0 for SELECT
func (r *ArrayAggregateResult) LastInsertID() int64 {
	return 0
}

// Context returns the context
func (r *ArrayAggregateResult) Context() context.Context {
	return r.baseResult.Context()
}

// WithAliases returns self
func (r *ArrayAggregateResult) WithAliases(aliases map[string]string) storage.Result {
	return r
}
