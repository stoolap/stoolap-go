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

	"github.com/stoolap/stoolap-go/internal/parser"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// HavingFilteredResult is a specialized result that filters aggregated rows based on a HAVING clause
type HavingFilteredResult struct {
	result     storage.Result
	havingExpr parser.Expression
	evaluator  *Evaluator

	// Current state
	currentRow   storage.Row
	currentValid bool
	colIndex     map[string]int // Column index map for array-based evaluation
}

// Columns returns the column names from the underlying result
func (h *HavingFilteredResult) Columns() []string {
	return h.result.Columns()
}

// Next advances to the next row that matches the HAVING clause
func (h *HavingFilteredResult) Next() bool {
	for h.result.Next() {
		// Get the current row
		row := h.result.Row()
		if row == nil {
			continue
		}

		// Use array-based evaluation if available
		columns := h.result.Columns()

		// Build column index map if not already built
		if h.colIndex == nil {
			h.colIndex = make(map[string]int)
			for i, col := range columns {
				h.colIndex[col] = i

				// Also add reverse alias mapping
				if h.evaluator.columnAliases != nil {
					if aggExpr, ok := h.evaluator.columnAliases[col]; ok {
						h.colIndex[aggExpr] = i
					}
				}
			}
		}

		// Set row data in evaluator using array
		h.evaluator.SetRowArray(row, columns, h.colIndex)

		// Evaluate HAVING clause
		result, err := h.evaluator.Evaluate(h.havingExpr)
		if err != nil {
			// Skip rows that cause evaluation errors
			continue
		}

		// Check if result is true
		var match bool
		if b, ok := result.AsInterface().(bool); ok {
			match = b
		}

		if match {
			h.currentRow = row
			h.currentValid = true
			return true
		}
	}

	h.currentValid = false
	return false
}

// Scan copies column values to the provided destinations
func (h *HavingFilteredResult) Scan(dest ...interface{}) error {
	if !h.currentValid {
		return fmt.Errorf("no current row")
	}
	return h.result.Scan(dest...)
}

// Row returns the current row
func (h *HavingFilteredResult) Row() storage.Row {
	if !h.currentValid {
		return nil
	}
	return h.currentRow
}

// Close closes the result
func (h *HavingFilteredResult) Close() error {
	return h.result.Close()
}

// RowsAffected returns the number of rows affected
func (h *HavingFilteredResult) RowsAffected() int64 {
	return h.result.RowsAffected()
}

// LastInsertID returns the last insert ID
func (h *HavingFilteredResult) LastInsertID() int64 {
	return h.result.LastInsertID()
}

// Context returns the context
func (h *HavingFilteredResult) Context() context.Context {
	return h.result.Context()
}

// WithAliases returns a new result with column aliases
func (h *HavingFilteredResult) WithAliases(aliases map[string]string) storage.Result {
	h.result = h.result.WithAliases(aliases)
	return h
}
