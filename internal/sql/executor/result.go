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
	"errors"
	"fmt"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// ExecResult represents an execution result for DML operations
type ExecResult struct {
	rowsAffected int64
	lastInsertID int64
	ctx          context.Context

	// For memory result support (used in tests and simple queries)
	columns    []string
	rows       [][]interface{}
	currentRow int
	isMemory   bool
}

// IsMemory returns true if this is an in-memory result (for testing)
func (r *ExecResult) IsMemory() bool {
	return r.isMemory
}

// GetRows returns the raw rows (for testing)
func (r *ExecResult) GetRows() [][]interface{} {
	return r.rows
}

// NewExecMemoryResult creates a new in-memory result set
func NewExecMemoryResult(columns []string, rows [][]interface{}, ctx context.Context) *ExecResult {
	return &ExecResult{
		columns:    columns,
		rows:       rows,
		ctx:        ctx,
		currentRow: 0,
		isMemory:   true,
	}
}

// Columns returns the column names in the result
func (r *ExecResult) Columns() []string {
	if r.isMemory {
		return r.columns
	}
	return []string{}
}

// Next moves the cursor to the next row
func (r *ExecResult) Next() bool {
	if r.isMemory {
		r.currentRow++
		return r.currentRow <= len(r.rows)
	}
	return false
}

// Scan scans the current row into the specified variables
func (r *ExecResult) Scan(dest ...interface{}) error {
	if !r.isMemory {
		return errors.New("not a query result")
	}

	if r.currentRow <= 0 || r.currentRow > len(r.rows) {
		return errors.New("no row to scan")
	}

	row := r.rows[r.currentRow-1]
	if len(dest) != len(row) {
		return fmt.Errorf("expected %d destination arguments in Scan, got %d", len(row), len(dest))
	}

	for i, val := range row {
		// Use the centralized converter if value is a ColumnValue
		if colVal, ok := val.(storage.ColumnValue); ok {
			if err := storage.ScanColumnValueToDestination(colVal, dest[i]); err != nil {
				return fmt.Errorf("error scanning column %d: %w", i, err)
			}
			continue
		}

		// For non-ColumnValue types, convert to ColumnValue first
		colVal := storage.NewDirectValueFromInterface(val)
		if err := storage.ScanColumnValueToDestination(colVal, dest[i]); err != nil {
			return fmt.Errorf("error scanning column %d: %w", i, err)
		}
	}

	return nil
}

// Close closes the result set
func (r *ExecResult) Close() error {
	return nil
}

// RowsAffected returns the number of rows affected
func (r *ExecResult) RowsAffected() int64 {
	return r.rowsAffected
}

// LastInsertID returns the last insert ID
func (r *ExecResult) LastInsertID() int64 {
	return r.lastInsertID
}

// Context returns the result's context
func (r *ExecResult) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// WithAliases sets column aliases for this result
func (r *ExecResult) WithAliases(aliases map[string]string) storage.Result {
	if !r.isMemory {
		// If this isn't a memory result, just return self as there are no columns to alias
		return r
	}

	// For memory results, use NewAliasedResult
	return NewAliasedResult(r, aliases)
}

// Row implements the storage.Result interface
func (r *ExecResult) Row() storage.Row {
	if !r.isMemory || r.currentRow <= 0 || r.currentRow > len(r.rows) {
		// No row available
		return nil
	}

	// Get the current row
	row := r.rows[r.currentRow-1]

	// Convert to storage.Row
	result := make(storage.Row, len(row))
	for i, val := range row {
		// Check if it's already a ColumnValue
		if cv, ok := val.(storage.ColumnValue); ok {
			result[i] = cv
		} else {
			// Convert based on type
			result[i] = storage.NewDirectValueFromInterface(val)
		}
	}

	return result
}
