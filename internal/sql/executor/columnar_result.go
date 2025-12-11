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

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// ColumnarResult stores data in column-oriented format for better memory efficiency
// Instead of []map[string]ColumnValue, we use parallel arrays
type ColumnarResult struct {
	columnNames []string
	columns     [][]storage.ColumnValue // columns[col_idx][row_idx]
	rowCount    int
	current     int
	rowBuffer   storage.Row // Reusable buffer for Row() method
}

// NewColumnarResult creates a new columnar result from a regular result
func NewColumnarResult(result storage.Result) (*ColumnarResult, error) {
	columnNames := result.Columns()
	numCols := len(columnNames)

	// Pre-allocate columns with initial capacity
	columns := make([][]storage.ColumnValue, numCols)
	for i := range columns {
		columns[i] = make([]storage.ColumnValue, 0, 1000)
	}

	// Read all rows and store in columnar format
	rowCount := 0
	for result.Next() {
		row := result.Row()
		if row != nil && len(row) == numCols {
			for i, val := range row {
				columns[i] = append(columns[i], val)
			}
			rowCount++
		}
	}

	return &ColumnarResult{
		columnNames: columnNames,
		columns:     columns,
		rowCount:    rowCount,
		current:     -1,
		rowBuffer:   make(storage.Row, numCols), // Pre-allocate row buffer
	}, nil
}

// Columns returns the column names
func (r *ColumnarResult) Columns() []string {
	return r.columnNames
}

// Next advances to the next row
func (r *ColumnarResult) Next() bool {
	r.current++
	return r.current < r.rowCount
}

// Row returns the current row as storage.Row
func (r *ColumnarResult) Row() storage.Row {
	if r.current < 0 || r.current >= r.rowCount {
		return nil
	}

	// Create a new row to avoid issues with shared buffers in aggregations
	// This is less efficient but ensures correctness
	row := make(storage.Row, len(r.columns))
	for i, col := range r.columns {
		row[i] = col[r.current]
	}
	return row
}

// Scan copies column values to the provided destinations
func (r *ColumnarResult) Scan(dest ...interface{}) error {
	if r.current < 0 || r.current >= r.rowCount {
		return fmt.Errorf("no current row")
	}

	if len(dest) != len(r.columns) {
		return fmt.Errorf("scan column count mismatch: %d != %d", len(dest), len(r.columns))
	}

	for i, col := range r.columns {
		val := col[r.current]
		if err := mvcc.ScanValue(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the result
func (r *ColumnarResult) Close() error {
	// Reset for potential reuse
	r.current = -1
	return nil
}

// RowsAffected returns 0 for SELECT
func (r *ColumnarResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns 0 for SELECT
func (r *ColumnarResult) LastInsertID() int64 {
	return 0
}

// Context returns the context
func (r *ColumnarResult) Context() context.Context {
	return context.Background()
}

// WithAliases returns a new result with column aliases
func (r *ColumnarResult) WithAliases(aliases map[string]string) storage.Result {
	// Create new column names with aliases applied
	newColumnNames := make([]string, len(r.columnNames))
	for i, name := range r.columnNames {
		if alias, ok := aliases[name]; ok {
			newColumnNames[i] = alias
		} else {
			newColumnNames[i] = name
		}
	}

	// Return a new columnar result with updated column names
	// Note: We share the underlying column data for efficiency
	return &ColumnarResult{
		columnNames: newColumnNames,
		columns:     r.columns,
		rowCount:    r.rowCount,
		current:     -1,
		rowBuffer:   make(storage.Row, len(r.columns)), // New buffer for aliased result
	}
}

// Reset allows the result to be iterated again
func (r *ColumnarResult) Reset() {
	r.current = -1
}

// GetColumnByName returns a specific column's data by name (useful for optimization)
func (r *ColumnarResult) GetColumnByName(name string) ([]storage.ColumnValue, bool) {
	for i, colName := range r.columnNames {
		if colName == name {
			return r.columns[i], true
		}
	}
	return nil, false
}

// GetColumnByIndex returns a specific column's data by index (useful for optimization)
func (r *ColumnarResult) GetColumnByIndex(idx int) ([]storage.ColumnValue, bool) {
	if idx < 0 || idx >= len(r.columns) {
		return nil, false
	}
	return r.columns[idx], true
}

// FilterColumnar applies a filter predicate directly on a column (columnar operation)
// This is much more efficient than row-by-row filtering
func (r *ColumnarResult) FilterColumnar(columnName string, predicate func(storage.ColumnValue) bool) (*ColumnarResult, error) {
	// Find the column index
	colIdx := -1
	for i, name := range r.columnNames {
		if name == columnName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	// Pre-scan to count matching rows
	matchCount := 0
	column := r.columns[colIdx]
	for i := 0; i < r.rowCount; i++ {
		if predicate(column[i]) {
			matchCount++
		}
	}

	// Allocate new columns with exact size
	newColumns := make([][]storage.ColumnValue, len(r.columns))
	for i := range newColumns {
		newColumns[i] = make([]storage.ColumnValue, 0, matchCount)
	}

	// Copy matching rows
	for rowIdx := 0; rowIdx < r.rowCount; rowIdx++ {
		if predicate(column[rowIdx]) {
			for colIdx, col := range r.columns {
				newColumns[colIdx] = append(newColumns[colIdx], col[rowIdx])
			}
		}
	}

	return &ColumnarResult{
		columnNames: r.columnNames,
		columns:     newColumns,
		rowCount:    matchCount,
		current:     -1,
		rowBuffer:   make(storage.Row, len(r.columns)),
	}, nil
}

// ProjectColumnar creates a new result with only specified columns (columnar operation)
func (r *ColumnarResult) ProjectColumnar(columnNames []string) (*ColumnarResult, error) {
	// Map column names to indices
	nameToIdx := make(map[string]int)
	for i, name := range r.columnNames {
		nameToIdx[name] = i
	}

	// Build new columns array with only requested columns
	newColumns := make([][]storage.ColumnValue, len(columnNames))
	newColumnNames := make([]string, len(columnNames))

	for i, name := range columnNames {
		idx, ok := nameToIdx[name]
		if !ok {
			return nil, fmt.Errorf("column %s not found", name)
		}
		newColumns[i] = r.columns[idx]
		newColumnNames[i] = name
	}

	return &ColumnarResult{
		columnNames: newColumnNames,
		columns:     newColumns,
		rowCount:    r.rowCount,
		current:     -1,
		rowBuffer:   make(storage.Row, len(newColumns)),
	}, nil
}

// AggregateColumnar performs aggregation on a specific column (columnar operation)
func (r *ColumnarResult) AggregateColumnar(columnName string, aggFunc func([]storage.ColumnValue) storage.ColumnValue) (storage.ColumnValue, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	return aggFunc(col), nil
}

// CountNonNull returns the count of non-null values in a column (columnar operation)
func (r *ColumnarResult) CountNonNull(columnName string) (int64, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return 0, fmt.Errorf("column %s not found", columnName)
	}

	count := int64(0)
	for _, val := range col {
		if val != nil && !val.IsNull() {
			count++
		}
	}
	return count, nil
}

// MinColumnar finds the minimum value in a column (columnar operation)
func (r *ColumnarResult) MinColumnar(columnName string) (storage.ColumnValue, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	if len(col) == 0 {
		return storage.NewDirectValueFromInterface(nil), nil
	}

	var min storage.ColumnValue
	for _, val := range col {
		if val != nil && !val.IsNull() {
			if min == nil {
				min = val
			} else if cmp, err := val.Compare(min); err == nil && cmp < 0 {
				min = val
			}
		}
	}

	if min == nil {
		return storage.NewDirectValueFromInterface(nil), nil
	}
	return min, nil
}

// MaxColumnar finds the maximum value in a column (columnar operation)
func (r *ColumnarResult) MaxColumnar(columnName string) (storage.ColumnValue, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	if len(col) == 0 {
		return storage.NewDirectValueFromInterface(nil), nil
	}

	var max storage.ColumnValue
	for _, val := range col {
		if val != nil && !val.IsNull() {
			if max == nil {
				max = val
			} else if cmp, err := val.Compare(max); err == nil && cmp > 0 {
				max = val
			}
		}
	}

	if max == nil {
		return storage.NewDirectValueFromInterface(nil), nil
	}
	return max, nil
}

// SumColumnar calculates the sum of values in a column (columnar operation)
func (r *ColumnarResult) SumColumnar(columnName string) (storage.ColumnValue, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	var sum float64
	hasValue := false

	for _, val := range col {
		if val != nil && !val.IsNull() {
			// Try to get numeric value
			if i, ok := val.AsInt64(); ok {
				sum += float64(i)
				hasValue = true
			} else if f, ok := val.AsFloat64(); ok {
				sum += f
				hasValue = true
			}
		}
	}

	if !hasValue {
		return storage.NewDirectValueFromInterface(nil), nil
	}
	return storage.NewDirectValueFromInterface(sum), nil
}

// AvgColumnar calculates the average of values in a column (columnar operation)
func (r *ColumnarResult) AvgColumnar(columnName string) (storage.ColumnValue, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	var sum float64
	count := 0

	for _, val := range col {
		if val != nil && !val.IsNull() {
			// Try to get numeric value
			if i, ok := val.AsInt64(); ok {
				sum += float64(i)
				count++
			} else if f, ok := val.AsFloat64(); ok {
				sum += f
				count++
			}
		}
	}

	if count == 0 {
		return storage.NewDirectValueFromInterface(nil), nil
	}
	return storage.NewDirectValueFromInterface(sum / float64(count)), nil
}

// CountColumnar counts all rows including nulls (columnar operation)
func (r *ColumnarResult) CountColumnar() int64 {
	return int64(r.rowCount)
}

// DistinctCountColumnar counts distinct non-null values in a column
func (r *ColumnarResult) DistinctCountColumnar(columnName string) (int64, error) {
	col, ok := r.GetColumnByName(columnName)
	if !ok {
		return 0, fmt.Errorf("column %s not found", columnName)
	}

	seen := make(map[interface{}]bool)
	for _, val := range col {
		if val != nil && !val.IsNull() {
			seen[val.AsInterface()] = true
		}
	}

	return int64(len(seen)), nil
}

// Clone creates a deep copy of the columnar result
func (r *ColumnarResult) Clone() *ColumnarResult {
	newColumns := make([][]storage.ColumnValue, len(r.columns))
	for i, col := range r.columns {
		newCol := make([]storage.ColumnValue, len(col))
		copy(newCol, col)
		newColumns[i] = newCol
	}

	newColumnNames := make([]string, len(r.columnNames))
	copy(newColumnNames, r.columnNames)

	return &ColumnarResult{
		columnNames: newColumnNames,
		columns:     newColumns,
		rowCount:    r.rowCount,
		current:     -1,
		rowBuffer:   make(storage.Row, len(r.columns)),
	}
}
