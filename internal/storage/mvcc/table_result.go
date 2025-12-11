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
	"context"
	"fmt"
	"reflect"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// TableResult is a result set from a table scan
type TableResult struct {
	schema          storage.Schema
	scanner         storage.Scanner
	columnIndices   []int
	columnNames     []string
	originalColumns []string
	rowsAffected    int64
	lastInsertID    int64
	ctx             context.Context
}

// NewTableResult creates a new result from a table scanner
func NewTableResult(ctx context.Context, scanner storage.Scanner, schema storage.Schema, columnIndices []int, originalColumns ...string) *TableResult {
	// If no column indices provided, use all columns
	if len(columnIndices) == 0 {
		columnIndices = make([]int, len(schema.Columns))
		for i := range schema.Columns {
			columnIndices[i] = i
		}
	}

	// Extract column names
	columnNames := make([]string, len(columnIndices))
	for i, colIdx := range columnIndices {
		if colIdx < len(schema.Columns) {
			columnNames[i] = schema.Columns[colIdx].Name
		} else {
			columnNames[i] = fmt.Sprintf("column%d", colIdx)
		}
	}

	return &TableResult{
		schema:          schema,
		scanner:         scanner,
		columnIndices:   columnIndices,
		columnNames:     columnNames,
		originalColumns: originalColumns,
		ctx:             ctx,
	}
}

// Columns returns the column names in the result
func (r *TableResult) Columns() []string {
	if len(r.originalColumns) > 0 {
		// If original columns are provided, return them
		return r.originalColumns
	}
	return r.columnNames
}

// Next moves to the next row
func (r *TableResult) Next() bool {
	return r.scanner.Next()
}

// Scan scans the current row into the specified variables
func (r *TableResult) Scan(dest ...interface{}) error {
	// Get the current row
	row := r.scanner.Row()

	// Validate the number of destination arguments matches column count
	if len(dest) > len(r.columnNames) {
		return fmt.Errorf("too many destination arguments: got %d, expected %d", len(dest), len(r.columnNames))
	}

	// Scan the values into the destination arguments
	for i, ptr := range dest {
		if i >= len(r.columnNames) {
			return fmt.Errorf("index out of range: %d", i)
		}

		// The scanner's Row() already handles the column projection based on columnIndices
		if i >= len(row) {
			return fmt.Errorf("index out of range: %d", i)
		}

		// Get the value from the row (already projected by scanner)
		value := row[i]
		if value == nil {
			// Set the destination to nil or default value
			scanNull(ptr)
			continue
		}

		// Fast path for DirectValue with valueRef for interface{} destinations
		if dv, ok := value.(*storage.DirectValue); ok && dv.AsInterface() != nil {
			if reflect.TypeOf(ptr).String() == "*interface {}" {
				// When the destination is interface{} and we have a valueRef,
				// we can directly assign it without any conversion or boxing
				*ptr.(*interface{}) = dv.AsInterface()
				continue
			}
		}

		// Special case: if this is an interface{} pointer that isn't already the specific type,
		// try to directly convert to common primitive types to avoid allocation
		if reflect.TypeOf(ptr).String() == "*interface {}" {
			// Try to use AsInterface if available (DirectValue implementation)
			if dv, ok := value.(*storage.DirectValue); ok {
				if iface := dv.AsInterface(); iface != nil {
					*ptr.(*interface{}) = iface
					continue
				}
			}

			// For common primitive types, we can avoid the interface{} boxing by using direct conversion
			switch value.Type() {
			case storage.INTEGER:
				if i, ok := value.AsInt64(); ok {
					// Store directly as int64 - this avoids an allocation!
					*ptr.(*interface{}) = i // No need for explicit conversion
					continue
				}
			case storage.FLOAT:
				if f, ok := value.AsFloat64(); ok {
					// Store directly as float64 - this avoids an allocation!
					*ptr.(*interface{}) = f // No need for explicit conversion
					continue
				}
			}
		}

		// Try the direct path first to avoid interface{} allocations when possible
		ptrType := reflect.TypeOf(ptr)
		if ptrType != nil && ptrType.Kind() == reflect.Ptr {
			ptrVal := reflect.ValueOf(ptr)
			if handled, err := scanDirect(value, ptrType, ptrVal); err != nil {
				return err
			} else if handled {
				continue
			}
		}

		// Fall back to regular scanning for other cases
		if err := scanValue(value, ptr); err != nil {
			return err
		}
	}

	return nil
}

// Row implements the storage.Result interface
// It returns the current row directly from the scanner without copying
func (r *TableResult) Row() storage.Row {
	// Simply return the scanner's row directly
	return r.scanner.Row()
}

// Close closes the result set
func (r *TableResult) Close() error {
	return r.scanner.Close()
}

// Context returns the result's context
func (r *TableResult) Context() context.Context {
	return r.ctx
}

// RowsAffected returns the number of rows affected
func (r *TableResult) RowsAffected() int64 {
	return r.rowsAffected
}

// LastInsertID returns the last inserted ID
func (r *TableResult) LastInsertID() int64 {
	return r.lastInsertID
}

// WithAliases implements the storage.Result interface
// It sets column aliases for the result and returns a new result with aliases applied
func (r *TableResult) WithAliases(aliases map[string]string) storage.Result {
	// Create a wrapper result with aliases instead of modifying this one
	return NewAliasedResult(r, aliases)
}
