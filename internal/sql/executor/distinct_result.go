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

	"github.com/stoolap/stoolap/internal/storage"
)

// DistinctResult is a wrapper result that applies DISTINCT to filter duplicates
type DistinctResult struct {
	result       storage.Result  // The underlying result
	seenRows     map[string]bool // Tracks unique rows to avoid duplicates
	currentRow   storage.Row     // Current row to return
	currentValid bool            // Whether currentRow is valid
	closed       bool            // Whether the result is closed
	columnNames  []string        // Names of columns in the result
}

// NewDistinctResult creates a new distinct result
func NewDistinctResult(result storage.Result) *DistinctResult {
	// Get column names from the underlying result
	columnNames := result.Columns()

	return &DistinctResult{
		result:       result,
		seenRows:     make(map[string]bool),
		currentRow:   nil,
		currentValid: false,
		closed:       false,
		columnNames:  columnNames,
	}
}

// Next advances to the next distinct row
func (dr *DistinctResult) Next() bool {
	if dr.closed {
		return false
	}

	// Keep advancing until we find a distinct row or run out of rows
	for dr.result.Next() {
		// Get the current row from the wrapped result
		row := dr.result.Row()
		if row == nil {
			dr.currentValid = false
			return false
		}

		// Create a string key for the row to track uniqueness
		key := createRowKey(row)

		// Check if we've seen this row before
		if _, seen := dr.seenRows[key]; !seen {
			// This is a unique row - remember it
			dr.seenRows[key] = true
			dr.currentRow = row
			dr.currentValid = true
			return true
		}

		// This row is a duplicate, so skip it and continue
	}

	// No more rows
	dr.currentValid = false
	return false
}

// Values returns the values for the current distinct row
// This is deprecated; use Row() instead
func (dr *DistinctResult) Values() storage.Row {
	if !dr.currentValid {
		return nil
	}
	return dr.currentRow
}

// Row returns the current row
func (dr *DistinctResult) Row() storage.Row {
	if !dr.currentValid {
		return nil
	}
	return dr.currentRow
}

// Scan scans the current row into the specified variables
func (dr *DistinctResult) Scan(dest ...interface{}) error {
	if dr.closed || !dr.currentValid {
		return fmt.Errorf("no row available to scan")
	}

	if len(dest) != len(dr.currentRow) {
		return fmt.Errorf("expected %d destination arguments in Scan, got %d", len(dr.currentRow), len(dest))
	}

	for i, val := range dr.currentRow {
		// Use the centralized converter if value is a ColumnValue
		if err := storage.ScanColumnValueToDestination(val, dest[i]); err != nil {
			return fmt.Errorf("error scanning column %d: %w", i, err)
		}
	}

	return nil
}

// Columns returns the column names
func (dr *DistinctResult) Columns() []string {
	return dr.columnNames
}

// Close closes the result
func (dr *DistinctResult) Close() error {
	if dr.closed {
		return nil
	}
	dr.closed = true
	return dr.result.Close()
}

// RowsAffected returns the number of rows affected
func (dr *DistinctResult) RowsAffected() int64 {
	return dr.result.RowsAffected()
}

// LastInsertID returns the last insert ID
func (dr *DistinctResult) LastInsertID() int64 {
	return dr.result.LastInsertID()
}

// Context returns the result's context
func (dr *DistinctResult) Context() context.Context {
	return dr.result.Context()
}

// WithAliases sets column aliases for this result
func (dr *DistinctResult) WithAliases(aliases map[string]string) storage.Result {
	// This is just a pass-through to the underlying result
	result := dr.result.WithAliases(aliases)

	// Create a new distinct result wrapping the aliased result
	return NewDistinctResult(result)
}

// Create a string key for a row to use as a unique identifier
func createRowKey(row storage.Row) string {
	// Create a string representation of the row values
	var sb strings.Builder
	for i, val := range row {
		if i > 0 {
			sb.WriteString("|")
		}
		// Handle null values specially
		if val == nil || val.IsNull() {
			sb.WriteString("NULL")
		} else {
			// Convert the value to string for comparison
			// Use the String() method for consistent representation
			sb.WriteString(fmt.Sprintf("%v", val.AsInterface()))
		}
	}
	return sb.String()
}
