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

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

// ColumnarResult stores data in column-oriented format for better memory efficiency
// Instead of []map[string]ColumnValue, we use parallel arrays
type ColumnarResult struct {
	columnNames []string
	columns     [][]storage.ColumnValue // columns[col_idx][row_idx]
	rowCount    int
	current     int
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

	// Construct row from columns
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
