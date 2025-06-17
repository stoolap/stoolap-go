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

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// FilteredResult represents a result set that filters rows based on a WHERE clause
// This optimized version avoids map allocations by using array-based row storage
type FilteredResult struct {
	result    storage.Result
	whereExpr parser.Expression
	evaluator *Evaluator

	// Column information
	columns     []string
	colIndexMap map[string]int

	// Current row state - using array instead of map
	currentRow   storage.Row
	currentValid bool
	closed       bool

	// Evaluation statistics
	rowsScanned int
	rowsMatched int
}

// WithColumnAliases sets the column aliases on the FilteredResult's evaluator
func (f *FilteredResult) WithColumnAliases(aliases map[string]string) *FilteredResult {
	f.evaluator.WithColumnAliases(aliases)
	return f
}

// Columns returns the column names from the underlying result
func (f *FilteredResult) Columns() []string {
	if f.columns == nil {
		f.columns = f.result.Columns()
		f.buildColumnIndexMap()
	}
	return f.columns
}

// buildColumnIndexMap builds the column index mapping
func (f *FilteredResult) buildColumnIndexMap() {
	f.colIndexMap = make(map[string]int)
	for i, col := range f.columns {
		f.colIndexMap[col] = i
		// Also map unqualified names
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			if _, exists := f.colIndexMap[unqualified]; !exists {
				f.colIndexMap[unqualified] = i
			}
		}
	}
}

// Next advances to the next row that matches the filter
func (f *FilteredResult) Next() bool {
	if f.closed {
		return false
	}

	// Ensure columns are initialized
	if f.columns == nil {
		f.Columns()
	}

	// Fast path for no WHERE clause
	if f.whereExpr == nil {
		if f.result.Next() {
			f.rowsScanned++
			f.rowsMatched++
			f.currentRow = f.result.Row()
			f.currentValid = true
			return true
		}
		f.currentValid = false
		return false
	}

	// Find next matching row
	for f.result.Next() {
		f.rowsScanned++

		row := f.result.Row()
		if row == nil {
			continue
		}

		// Set row in evaluator using array method
		f.evaluator.SetRowArray(row, f.columns, f.colIndexMap)

		// Evaluate WHERE clause
		match, err := f.evaluator.Evaluate(f.whereExpr)
		if err != nil {
			continue
		}

		// Check if it's a boolean true value
		boolInterface := match.AsInterface()
		if b, ok := boolInterface.(bool); ok && b {
			f.currentRow = row
			f.currentValid = true
			f.rowsMatched++
			return true
		}
	}

	f.currentValid = false
	return false
}

// Row returns the current row
func (f *FilteredResult) Row() storage.Row {
	if !f.currentValid {
		return nil
	}
	return f.currentRow
}

// Scan copies column values from the current row into the provided variables
func (f *FilteredResult) Scan(dest ...interface{}) error {
	if !f.currentValid {
		return fmt.Errorf("no row to scan")
	}

	if len(dest) != len(f.currentRow) {
		return fmt.Errorf("column count mismatch: got %d, wanted %d", len(dest), len(f.currentRow))
	}

	// Copy values to destination
	for i, colValue := range f.currentRow {
		if dest[i] == nil {
			return fmt.Errorf("destination pointer is nil")
		}

		// Use the central utility function for consistent scanning
		if err := storage.ScanColumnValueToDestination(colValue, dest[i]); err != nil {
			if i < len(f.columns) {
				return fmt.Errorf("column %s: %w", f.columns[i], err)
			}
			return fmt.Errorf("column %d: %w", i, err)
		}
	}

	return nil
}

// Close closes the result set
func (f *FilteredResult) Close() error {
	if f.closed {
		return nil
	}

	f.closed = true
	f.currentRow = nil
	return f.result.Close()
}

// RowsAffected returns the number of rows affected by the operation
func (f *FilteredResult) RowsAffected() int64 {
	return f.result.RowsAffected()
}

// LastInsertID returns the last insert ID
func (f *FilteredResult) LastInsertID() int64 {
	return 0
}

// Context returns the result's context
func (f *FilteredResult) Context() context.Context {
	if f.result != nil {
		return f.result.Context()
	}
	return context.Background()
}

// GetMetrics returns execution metrics for this result
func (f *FilteredResult) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"rows_scanned": f.rowsScanned,
		"rows_matched": f.rowsMatched,
		"filter_ratio": float64(f.rowsMatched) / float64(max(1, f.rowsScanned)),
	}
}

// WithAliases implements the storage.Result interface
func (f *FilteredResult) WithAliases(aliases map[string]string) storage.Result {
	// Create a new evaluator with these aliases
	newEvaluator := NewEvaluator(f.Context(), GetGlobalFunctionRegistry())
	newEvaluator.WithColumnAliases(aliases)

	// First try to propagate aliases to the base result if it supports them
	var baseResult storage.Result = f.result
	if aliasable, ok := f.result.(interface {
		WithAliases(map[string]string) storage.Result
	}); ok {
		baseResult = aliasable.WithAliases(aliases)
	}

	// Create a new FilteredResult with the aliased base result
	return &FilteredResult{
		result:       baseResult,
		whereExpr:    f.whereExpr,
		evaluator:    newEvaluator,
		currentValid: false,
		closed:       false,
	}
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
