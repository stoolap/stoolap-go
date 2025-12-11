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

// HashJoinResult implements an efficient hash join without map allocations per row
type HashJoinResult struct {
	leftResult  storage.Result
	rightResult storage.Result
	joinType    parser.JoinType
	condition   parser.Expression
	evaluator   *Evaluator

	// Column information
	leftColumns  []string
	rightColumns []string
	allColumns   []string

	// Column index maps for fast lookup
	leftColIndex  map[string]int
	rightColIndex map[string]int
	allColIndex   map[string]int

	// Current row state
	currentRow        storage.Row
	hasCurrentRow     bool
	rightRows         []storage.Row // For nested loop join
	rightIndex        int
	rightMaterialized bool // Track if right side has been materialized
	leftRowMatched    bool // Track if current left row has any matches

	// Hash join state
	hashTable    map[interface{}][]storage.Row
	joinColLeft  int
	joinColRight int
	isHashJoin   bool
}

// NewHashJoinResult creates a new efficient join result
func NewHashJoinResult(leftResult, rightResult storage.Result, joinType parser.JoinType, condition parser.Expression, evaluator *Evaluator) (*HashJoinResult, error) {
	leftColumns := leftResult.Columns()
	rightColumns := rightResult.Columns()

	// Build combined column list
	allColumns := make([]string, 0, len(leftColumns)+len(rightColumns))
	allColumns = append(allColumns, leftColumns...)
	allColumns = append(allColumns, rightColumns...)

	// Build column index maps
	leftColIndex := make(map[string]int)
	for i, col := range leftColumns {
		leftColIndex[col] = i
		// Also map unqualified name
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			leftColIndex[unqualified] = i
		}
	}

	rightColIndex := make(map[string]int)
	for i, col := range rightColumns {
		rightColIndex[col] = i
		// Also map unqualified name
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			rightColIndex[unqualified] = i
		}
	}

	allColIndex := make(map[string]int)
	for i, col := range allColumns {
		allColIndex[col] = i
		// Also map unqualified name
		if idx := lastIndex(col, '.'); idx >= 0 {
			unqualified := col[idx+1:]
			// Only map if not already mapped (left table has priority)
			if _, exists := allColIndex[unqualified]; !exists {
				allColIndex[unqualified] = i
			}
		}
	}

	result := &HashJoinResult{
		leftResult:    leftResult,
		rightResult:   rightResult,
		joinType:      joinType,
		condition:     condition,
		evaluator:     evaluator,
		leftColumns:   leftColumns,
		rightColumns:  rightColumns,
		allColumns:    allColumns,
		leftColIndex:  leftColIndex,
		rightColIndex: rightColIndex,
		allColIndex:   allColIndex,
		rightIndex:    -1,
	}

	// Try to optimize as hash join if possible
	if condition != nil {
		if result.tryOptimizeHashJoin(condition) {
			// Build hash table
			if err := result.buildHashTable(); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// tryOptimizeHashJoin checks if we can use hash join optimization
func (h *HashJoinResult) tryOptimizeHashJoin(condition parser.Expression) bool {
	// Look for simple equality conditions like t1.id = t2.customer_id
	if infix, ok := condition.(*parser.InfixExpression); ok && infix.Operator == "=" {
		var leftCol, rightCol string
		var leftIsQualified, rightIsQualified bool

		// Check left side
		switch expr := infix.Left.(type) {
		case *parser.Identifier:
			leftCol = expr.Value
		case *parser.QualifiedIdentifier:
			leftCol = expr.Name.Value
			leftIsQualified = true
		}

		// Check right side
		switch expr := infix.Right.(type) {
		case *parser.Identifier:
			rightCol = expr.Value
		case *parser.QualifiedIdentifier:
			rightCol = expr.Name.Value
			rightIsQualified = true
		}

		if leftCol != "" && rightCol != "" {
			// For qualified identifiers, we need to ensure we're using the correct side
			// e.g., for e.dept_id = d.id, e.dept_id should be from left, d.id from right

			// Try to determine which side each column belongs to
			leftIdx := -1
			rightIdx := -1

			// Check if left expression should come from left result or right result
			if leftIsQualified {
				// Try left result first
				if idx, ok := h.findColumnIndex(leftCol, true); ok {
					leftIdx = idx
				} else if idx, ok := h.findColumnIndex(leftCol, false); ok {
					// It's actually from the right result, so swap
					rightIdx = idx
					// Now find the right expression in left result
					if idx2, ok := h.findColumnIndex(rightCol, true); ok {
						leftIdx = idx2
					}
				}
			} else {
				// Unqualified, try to find in left result
				if idx, ok := h.findColumnIndex(leftCol, true); ok {
					leftIdx = idx
				}
			}

			// Similar for right expression
			if rightIsQualified && leftIdx >= 0 {
				// Try right result first
				if idx, ok := h.findColumnIndex(rightCol, false); ok {
					rightIdx = idx
				}
			} else if rightIdx < 0 {
				// Try to find in right result
				if idx, ok := h.findColumnIndex(rightCol, false); ok {
					rightIdx = idx
				}
			}

			if leftIdx >= 0 && rightIdx >= 0 {
				h.joinColLeft = leftIdx
				h.joinColRight = rightIdx
				h.isHashJoin = true
				// Debug logging
				// fmt.Printf("Hash join: left col %d (%s), right col %d (%s)\n",
				//     leftIdx, h.leftColumns[leftIdx], rightIdx, h.rightColumns[rightIdx])
				return true
			}
		}
	}

	return false
}

// findColumnIndex finds the index of a column in either left or right result
func (h *HashJoinResult) findColumnIndex(colName string, isLeft bool) (int, bool) {
	if isLeft {
		// Check direct match
		if idx, ok := h.leftColIndex[colName]; ok {
			return idx, true
		}
		// Check unqualified match
		for col, idx := range h.leftColIndex {
			if getUnqualifiedName(col) == colName {
				return idx, true
			}
		}
	} else {
		// Check direct match
		if idx, ok := h.rightColIndex[colName]; ok {
			return idx, true
		}
		// Check unqualified match
		for col, idx := range h.rightColIndex {
			if getUnqualifiedName(col) == colName {
				return idx, true
			}
		}
	}
	return -1, false
}

// buildHashTable builds the hash table for hash join
func (h *HashJoinResult) buildHashTable() error {
	h.hashTable = make(map[interface{}][]storage.Row)

	// Build hash table from right side
	for h.rightResult.Next() {
		row := h.rightResult.Row()
		if row == nil || len(row) <= h.joinColRight {
			continue
		}

		// Make a copy of the row to avoid reuse issues
		rowCopy := make(storage.Row, len(row))
		copy(rowCopy, row)

		key := rowCopy[h.joinColRight].AsInterface()
		h.hashTable[key] = append(h.hashTable[key], rowCopy)
	}

	return nil
}

// Columns returns the column names
func (h *HashJoinResult) Columns() []string {
	return h.allColumns
}

// Next advances to the next row
func (h *HashJoinResult) Next() bool {
	if h.isHashJoin {
		return h.nextHashJoin()
	}
	return h.nextNestedLoop()
}

// nextHashJoin implements hash join iteration
func (h *HashJoinResult) nextHashJoin() bool {
	for {
		// If we have remaining right rows, process them
		if h.rightRows != nil && h.rightIndex < len(h.rightRows)-1 {
			h.rightIndex++
			if h.buildJoinedRow(h.currentRow[:len(h.leftColumns)], h.rightRows[h.rightIndex]) {
				return true
			}
		}

		// Get next left row
		if !h.leftResult.Next() {
			return false
		}

		leftRow := h.leftResult.Row()
		if leftRow == nil || len(leftRow) <= h.joinColLeft {
			continue
		}

		// Look up in hash table
		key := leftRow[h.joinColLeft].AsInterface()
		if rows, ok := h.hashTable[key]; ok && len(rows) > 0 {
			h.rightRows = rows
			h.rightIndex = 0
			if h.buildJoinedRow(leftRow, rows[0]) {
				return true
			}
		} else if h.joinType == parser.LeftJoin {
			// For LEFT JOIN, emit row with NULLs for right side
			h.buildLeftJoinRow(leftRow)
			return true
		}
	}
}

// nextNestedLoop implements nested loop join
func (h *HashJoinResult) nextNestedLoop() bool {
	// If we don't have a current left row, get the next one
	if h.currentRow == nil || h.rightIndex >= len(h.rightRows)-1 {
		// Check if we need to emit NULL row for LEFT JOIN before getting next left row
		if h.joinType == parser.LeftJoin && h.currentRow != nil && !h.leftRowMatched {
			h.buildLeftJoinRow(h.currentRow[:len(h.leftColumns)])
			// Reset for next left row
			h.currentRow = nil
			return true
		}

		// Get next left row
		if !h.leftResult.Next() {
			return false
		}

		// Store current left row
		leftRow := h.leftResult.Row()
		if leftRow == nil {
			return h.nextNestedLoop() // Try next left row
		}

		// For nested loop, we need to materialize all right rows once
		if !h.rightMaterialized {
			h.rightRows = make([]storage.Row, 0)

			// Materialize all right rows
			for h.rightResult.Next() {
				row := h.rightResult.Row()
				if row != nil {
					// Make a copy of the row to avoid reuse issues
					rowCopy := make(storage.Row, len(row))
					copy(rowCopy, row)
					h.rightRows = append(h.rightRows, rowCopy)
				}
			}
			h.rightMaterialized = true
		}

		// Start iterating through right rows
		h.rightIndex = -1
		h.leftRowMatched = false

		// Store the current left row for reuse
		h.currentRow = make(storage.Row, len(h.leftColumns))
		copy(h.currentRow, leftRow)
	}

	// Try next right row
	for h.rightIndex < len(h.rightRows)-1 {
		h.rightIndex++
		rightRow := h.rightRows[h.rightIndex]

		// Evaluate join condition if present
		if h.condition != nil {
			// Build the combined row for evaluation
			tempRow := make(storage.Row, len(h.allColumns))
			copy(tempRow, h.currentRow[:len(h.leftColumns)])
			copy(tempRow[len(h.leftColumns):], rightRow)

			// Set the row in evaluator
			h.evaluator.SetRowArray(tempRow, h.allColumns, h.allColIndex)

			// Evaluate condition
			match, err := h.evaluator.Evaluate(h.condition)
			if err != nil {
				continue
			}

			boolInterface := match.AsInterface()
			if b, ok := boolInterface.(bool); ok && b {
				// Match found, build joined row
				h.buildJoinedRow(h.currentRow[:len(h.leftColumns)], rightRow)
				h.leftRowMatched = true
				return true
			}
		} else {
			// No condition (CROSS JOIN), all rows match
			h.buildJoinedRow(h.currentRow[:len(h.leftColumns)], rightRow)
			h.leftRowMatched = true
			return true
		}
	}

	// No match found for LEFT JOIN, emit row with NULLs on right
	if h.joinType == parser.LeftJoin && !h.leftRowMatched {
		h.buildLeftJoinRow(h.currentRow[:len(h.leftColumns)])
		// Reset for next left row
		h.currentRow = nil
		return true
	}

	// Try next left row
	h.currentRow = nil
	return h.nextNestedLoop()
}

// buildJoinedRow builds the combined row
func (h *HashJoinResult) buildJoinedRow(leftRow, rightRow storage.Row) bool {
	h.currentRow = make(storage.Row, len(h.allColumns))

	// Copy left columns
	copy(h.currentRow, leftRow)

	// Copy right columns
	for i, val := range rightRow {
		h.currentRow[len(h.leftColumns)+i] = val
	}

	h.hasCurrentRow = true
	return true
}

// buildLeftJoinRow builds a row for LEFT JOIN with NULLs on right
func (h *HashJoinResult) buildLeftJoinRow(leftRow storage.Row) {
	h.currentRow = make(storage.Row, len(h.allColumns))

	// Copy left columns
	copy(h.currentRow, leftRow)

	// Fill right columns with NULL
	for i := len(h.leftColumns); i < len(h.allColumns); i++ {
		h.currentRow[i] = storage.NewDirectValueFromInterface(nil)
	}

	h.hasCurrentRow = true
}

// Row returns the current row
func (h *HashJoinResult) Row() storage.Row {
	if h.hasCurrentRow {
		return h.currentRow
	}
	return nil
}

// Scan copies column values to destinations
func (h *HashJoinResult) Scan(dest ...interface{}) error {
	if !h.hasCurrentRow {
		return fmt.Errorf("no current row")
	}

	if len(dest) != len(h.currentRow) {
		return fmt.Errorf("scan column count mismatch: %d != %d", len(dest), len(h.currentRow))
	}

	for i, val := range h.currentRow {
		if err := storage.ScanColumnValueToDestination(val, dest[i]); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the result
func (h *HashJoinResult) Close() error {
	if err := h.leftResult.Close(); err != nil {
		return err
	}
	if err := h.rightResult.Close(); err != nil {
		return err
	}
	return nil
}

// RowsAffected returns 0 for SELECT
func (h *HashJoinResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns 0 for SELECT
func (h *HashJoinResult) LastInsertID() int64 {
	return 0
}

// Context returns the context
func (h *HashJoinResult) Context() context.Context {
	return h.leftResult.Context()
}

// WithAliases returns self as joins don't support aliases
func (h *HashJoinResult) WithAliases(aliases map[string]string) storage.Result {
	return h
}

// getUnqualifiedName removes table prefix from column name
func getUnqualifiedName(colName string) string {
	if idx := lastIndex(colName, '.'); idx >= 0 {
		return colName[idx+1:]
	}
	return colName
}

// lastIndex finds the last occurrence of a character
func lastIndex(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}
