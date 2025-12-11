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
package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"sync/atomic"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Rows represents a result set with optimized concurrency
type Rows struct {
	storageResult    storage.Result
	columnNames      []string
	ctx              context.Context  // Context for the query
	closed           atomic.Bool      // Track if the rows have been closed
	rowsRead         int              // Number of rows read so far
	multiResults     []storage.Result // For storing multiple result sets
	currentResultSet int              // Index of the current result set
	mu               sync.Mutex       // Mutex for thread safety
	valueBuffer      []driver.Value   // Reusable buffer for values to reduce GC pressure
	scanBuffer       []interface{}    // Reusable buffer for scan operations
	scanPtrBuffer    []interface{}    // Reusable buffer for scan pointers
}

// Columns returns the column names
func (r *Rows) Columns() []string {
	if r.columnNames == nil {
		r.columnNames = r.storageResult.Columns()
	}
	return r.columnNames
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	// Use atomic swap to ensure we close only once
	// This avoids race conditions between Close() and Next() at EOF
	if r.closed.Swap(true) {
		return nil
	}

	return r.storageResult.Close()
}

// Next moves to the next row with optimized buffer reuse
func (r *Rows) Next(dest []driver.Value) error {
	// Use mutex to ensure thread safety
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return io.EOF
	}

	// Check context cancellation efficiently
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
	}

	// Check if we're at the end of the result set
	if !r.storageResult.Next() {
		// Instead of setting closed flag here, we'll let the sql.Rows.Close() method
		// handle the cleanup by calling our Close() method which properly closes
		// the underlying result
		return io.EOF
	}

	r.rowsRead++

	// Get the raw row using Row() instead of Scan()
	row := r.storageResult.Row()
	if row == nil {
		return io.EOF
	}

	// Get column information
	columns := r.storageResult.Columns()

	// Convert ColumnValue objects to driver.Value
	for i := 0; i < len(columns); i++ {
		if i < len(row) {
			// Extract the appropriate value from ColumnValue
			colVal := row[i]
			if colVal == nil || colVal.IsNull() {
				dest[i] = nil
			} else {
				switch colVal.Type() {
				case storage.INTEGER:
					if v, ok := colVal.AsInt64(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				case storage.FLOAT:
					if v, ok := colVal.AsFloat64(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				case storage.TEXT:
					if v, ok := colVal.AsString(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				case storage.BOOLEAN:
					if v, ok := colVal.AsBoolean(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				case storage.TIMESTAMP:
					if v, ok := colVal.AsTimestamp(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				case storage.JSON:
					if v, ok := colVal.AsString(); ok {
						dest[i] = v
					} else {
						dest[i] = nil
					}
				default:
					// Fall back to generic interface extraction
					dest[i] = colVal.AsInterface()
				}
			}
		} else {
			dest[i] = nil
		}
	}

	return nil
}

// HasNextResultSet implements the driver.RowsNextResultSet interface
func (r *Rows) HasNextResultSet() bool {
	if r.closed.Load() {
		return false
	}

	return r.currentResultSet < len(r.multiResults)
}

// NextResultSet implements the driver.RowsNextResultSet interface
func (r *Rows) NextResultSet() error {
	if r.closed.Load() {
		return io.EOF
	}

	if !r.HasNextResultSet() {
		return io.EOF
	}

	// Move to the next result set
	r.currentResultSet++
	r.storageResult = r.multiResults[r.currentResultSet]
	r.columnNames = nil // Reset column names to force refresh
	r.rowsRead = 0      // Reset row counter

	return nil
}
