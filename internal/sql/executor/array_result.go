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

// ArrayResult is a simple array-based result implementation
// It stores rows as arrays without any map overhead
type ArrayResult struct {
	columns []string
	rows    []storage.Row
	current int
}

// NewArrayResult creates a new array result
func NewArrayResult(columns []string, rows []storage.Row) *ArrayResult {
	return &ArrayResult{
		columns: columns,
		rows:    rows,
		current: -1,
	}
}

// NewArrayResultFromMaps converts map-based rows to array-based
// This is for migration purposes only
func NewArrayResultFromMaps(columns []string, mapRows []map[string]storage.ColumnValue) *ArrayResult {
	rows := make([]storage.Row, len(mapRows))
	for i, mapRow := range mapRows {
		row := make(storage.Row, len(columns))
		for j, col := range columns {
			if val, ok := mapRow[col]; ok {
				row[j] = val
			} else {
				row[j] = storage.NewDirectValueFromInterface(nil)
			}
		}
		rows[i] = row
	}
	return &ArrayResult{
		columns: columns,
		rows:    rows,
		current: -1,
	}
}

// Columns returns the column names
func (r *ArrayResult) Columns() []string {
	return r.columns
}

// Next advances to the next row
func (r *ArrayResult) Next() bool {
	r.current++
	return r.current < len(r.rows)
}

// Row returns the current row
func (r *ArrayResult) Row() storage.Row {
	if r.current < 0 || r.current >= len(r.rows) {
		return nil
	}
	return r.rows[r.current]
}

// Scan copies column values to the provided destinations
func (r *ArrayResult) Scan(dest ...interface{}) error {
	if r.current < 0 || r.current >= len(r.rows) {
		return fmt.Errorf("no current row")
	}

	row := r.rows[r.current]
	if len(dest) != len(row) {
		return fmt.Errorf("scan column count mismatch: %d != %d", len(dest), len(row))
	}

	for i, val := range row {
		if err := mvcc.ScanValue(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the result
func (r *ArrayResult) Close() error {
	return nil
}

// RowsAffected returns 0 for SELECT
func (r *ArrayResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns 0 for SELECT
func (r *ArrayResult) LastInsertID() int64 {
	return 0
}

// Context returns the context
func (r *ArrayResult) Context() context.Context {
	return context.Background()
}

// WithAliases returns a new result with column aliases
func (r *ArrayResult) WithAliases(aliases map[string]string) storage.Result {
	newColumns := make([]string, len(r.columns))
	for i, col := range r.columns {
		if alias, ok := aliases[col]; ok {
			newColumns[i] = alias
		} else {
			newColumns[i] = col
		}
	}

	return &ArrayResult{
		columns: newColumns,
		rows:    r.rows,
		current: -1,
	}
}

// Reset allows the result to be iterated again
func (r *ArrayResult) Reset() {
	r.current = -1
}
