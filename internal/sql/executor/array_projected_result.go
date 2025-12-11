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

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/parser"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// ArrayProjectedResult is an optimized projection result that avoids map allocations
type ArrayProjectedResult struct {
	baseResult  storage.Result
	columns     []string
	expressions []parser.Expression
	evaluator   *Evaluator

	// Column indices for fast lookup
	baseColumns []string
	colIndexMap map[string]int

	// Current row state
	currentRow    storage.Row
	hasCurrentRow bool
}

// NewArrayProjectedResult creates a new array-based projected result
func NewArrayProjectedResult(ctx context.Context, baseResult storage.Result, columns []string, expressions []parser.Expression, registry contract.FunctionRegistry) *ArrayProjectedResult {
	baseColumns := baseResult.Columns()
	colIndexMap := make(map[string]int)

	// Build column index map
	for i, col := range baseColumns {
		colIndexMap[col] = i
		// Also map unqualified names
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			if _, exists := colIndexMap[unqualified]; !exists {
				colIndexMap[unqualified] = i
			}
		}
	}

	return &ArrayProjectedResult{
		baseResult:  baseResult,
		columns:     columns,
		expressions: expressions,
		evaluator:   NewEvaluator(ctx, registry),
		baseColumns: baseColumns,
		colIndexMap: colIndexMap,
	}
}

// Columns returns the projected column names
func (r *ArrayProjectedResult) Columns() []string {
	return r.columns
}

// Next advances to the next row
func (r *ArrayProjectedResult) Next() bool {
	if !r.baseResult.Next() {
		r.hasCurrentRow = false
		return false
	}

	baseRow := r.baseResult.Row()
	if baseRow == nil {
		r.hasCurrentRow = false
		return false
	}

	// Create projected row
	r.currentRow = make(storage.Row, len(r.expressions))

	// Set evaluator row data using array instead of map
	r.evaluator.SetRowArray(baseRow, r.baseColumns, r.colIndexMap)

	// Evaluate each expression
	for i, expr := range r.expressions {
		val, err := r.evaluator.Evaluate(expr)
		if err != nil {
			// On error, use NULL
			r.currentRow[i] = storage.NewDirectValueFromInterface(nil)
		} else {
			r.currentRow[i] = val
		}
	}

	r.hasCurrentRow = true
	return true
}

// Row returns the current projected row
func (r *ArrayProjectedResult) Row() storage.Row {
	if r.hasCurrentRow {
		return r.currentRow
	}
	return nil
}

// Scan copies values to destinations
func (r *ArrayProjectedResult) Scan(dest ...interface{}) error {
	if !r.hasCurrentRow {
		return fmt.Errorf("no current row")
	}

	if len(dest) != len(r.currentRow) {
		return fmt.Errorf("scan column count mismatch: %d != %d", len(dest), len(r.currentRow))
	}

	for i, val := range r.currentRow {
		if err := storage.ScanColumnValueToDestination(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the result
func (r *ArrayProjectedResult) Close() error {
	return r.baseResult.Close()
}

// RowsAffected returns 0 for SELECT
func (r *ArrayProjectedResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns 0 for SELECT
func (r *ArrayProjectedResult) LastInsertID() int64 {
	return 0
}

// Context returns the context
func (r *ArrayProjectedResult) Context() context.Context {
	return r.baseResult.Context()
}

// WithAliases returns self as columns are already projected
func (r *ArrayProjectedResult) WithAliases(aliases map[string]string) storage.Result {
	return r
}
