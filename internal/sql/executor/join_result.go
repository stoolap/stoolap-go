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
	"sync/atomic"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// Use common map pool for join operations

// JoinResult represents a result set from a JOIN operation
type JoinResult struct {
	leftResult  storage.Result
	rightResult storage.Result
	joinType    string // "INNER", "LEFT", "RIGHT", "FULL"
	joinCond    parser.Expression
	evaluator   *Evaluator

	// Current join state
	leftRows      []map[string]storage.ColumnValue
	rightRows     []map[string]storage.ColumnValue
	leftPrefix    string
	rightPrefix   string
	currentRow    map[string]storage.ColumnValue
	leftColumns   []string
	rightColumns  []string
	columns       []string
	currentRowIdx int
	joinedRows    []map[string]storage.ColumnValue
	closed        atomic.Bool
}

// NewJoinResult creates a new JoinResult
func NewJoinResult(leftResult, rightResult storage.Result, joinType string, joinCond parser.Expression,
	evaluator *Evaluator, leftPrefix, rightPrefix string) *JoinResult {

	// Get column names from both results
	leftCols := leftResult.Columns()
	rightCols := rightResult.Columns()

	// Combine column names with appropriate prefixes
	var columns []string
	for _, col := range leftCols {
		if leftPrefix != "" {
			columns = append(columns, leftPrefix+"."+col)
		} else {
			columns = append(columns, col)
		}
	}

	for _, col := range rightCols {
		if rightPrefix != "" {
			columns = append(columns, rightPrefix+"."+col)
		} else {
			columns = append(columns, col)
		}
	}

	// Get a map from the common pool for the current row
	currentRow := common.GetColumnValueMap(len(columns))

	return &JoinResult{
		leftResult:    leftResult,
		rightResult:   rightResult,
		joinType:      joinType,
		joinCond:      joinCond,
		evaluator:     evaluator,
		currentRow:    currentRow,
		leftPrefix:    leftPrefix,
		rightPrefix:   rightPrefix,
		leftColumns:   leftCols,
		rightColumns:  rightCols,
		columns:       columns,
		currentRowIdx: -1,
	}
}

// Columns returns the column names in the result
func (r *JoinResult) Columns() []string {
	return r.columns
}

// materializeResults loads all rows from a result into memory
func (r *JoinResult) materializeResults() error {
	if r.closed.Load() {
		return fmt.Errorf("result set is closed")
	}

	// Load all left rows
	r.leftRows = []map[string]storage.ColumnValue{}

	for r.leftResult.Next() {
		// Get a map from the pool instead of creating a new one
		row := common.GetColumnValueMap(len(r.leftColumns))

		// Clear the map if it's not empty (it might be a reused map)
		clear(row)

		leftRow := r.leftResult.Row()

		if row == nil {
			common.PutColumnValueMap(row, len(r.leftColumns)) // Return the map to the pool on error
			return fmt.Errorf("failed to get row from left result")
		}

		// Validate that we have enough values in leftRow
		if len(leftRow) < len(r.leftColumns) {
			common.PutColumnValueMap(row, len(r.leftColumns))
			// For debugging: log the column names
			return fmt.Errorf("row data mismatch for left table: expected %d columns (%v) but got %d values",
				len(r.leftColumns), r.leftColumns, len(leftRow))
		}

		for i, col := range r.leftColumns {
			row[col] = leftRow[i]

			// Also store with table prefix
			if r.leftPrefix != "" {
				row[r.leftPrefix+"."+col] = leftRow[i]
			}
		}

		r.leftRows = append(r.leftRows, row)
	}

	// Load all right rows
	r.rightRows = []map[string]storage.ColumnValue{}

	for r.rightResult.Next() {
		// Get a map from the pool instead of creating a new one
		row := common.GetColumnValueMap(len(r.leftColumns))

		// Clear the map if it's not empty (it might be a reused map)
		clear(row)

		rightRow := r.rightResult.Row()

		if rightRow == nil {
			common.PutColumnValueMap(row, len(r.leftColumns)) // Return the map to the pool on error
			return fmt.Errorf("failed to get row from right result")
		}

		// Validate that we have enough values in rightRow
		if len(rightRow) < len(r.rightColumns) {
			common.PutColumnValueMap(row, len(r.rightColumns))
			return fmt.Errorf("row data mismatch: expected %d columns but got %d values", len(r.rightColumns), len(rightRow))
		}

		for i, col := range r.rightColumns {
			row[col] = rightRow[i]

			// Also store with table prefix
			if r.rightPrefix != "" {
				row[r.rightPrefix+"."+col] = rightRow[i]
			}
		}

		r.rightRows = append(r.rightRows, row)
	}

	return nil
}

// performJoin creates the joined result set based on the join type
func (r *JoinResult) performJoin() error {
	// Check if already closed
	if r.closed.Load() {
		return fmt.Errorf("result set is closed")
	}

	// First materialize all results
	if err := r.materializeResults(); err != nil {
		return err
	}

	// Close and nil the original result sets to avoid double close
	if r.leftResult != nil {
		r.leftResult.Close()
		r.leftResult = nil
	}
	if r.rightResult != nil {
		r.rightResult.Close()
		r.rightResult = nil
	}

	// Perform the actual join based on join type
	switch r.joinType {
	case "INNER":
		return r.performInnerJoin()
	case "LEFT":
		return r.performLeftJoin()
	case "RIGHT":
		return r.performRightJoin()
	case "FULL":
		return r.performFullJoin()
	case "CROSS":
		return r.performCrossJoin()
	default:
		return fmt.Errorf("unsupported join type: %s", r.joinType)
	}

}

// performInnerJoin creates an inner join result
func (r *JoinResult) performInnerJoin() error {
	r.joinedRows = []map[string]storage.ColumnValue{}

	// For each left row
	for _, leftRow := range r.leftRows {
		// For each right row
		for _, rightRow := range r.rightRows {
			// Check if the join condition is met
			combined := r.combineRows(leftRow, rightRow)
			match, err := r.evaluateJoinCondition(combined)
			if err != nil {
				continue // Skip on evaluation errors
			}

			if match {
				r.joinedRows = append(r.joinedRows, combined)
			}
		}
	}

	return nil
}

// performLeftJoin creates a left join result
func (r *JoinResult) performLeftJoin() error {
	r.joinedRows = []map[string]storage.ColumnValue{}

	// For each left row
	for _, leftRow := range r.leftRows {
		matchFound := false

		// For each right row
		for _, rightRow := range r.rightRows {
			// Check if the join condition is met
			combined := r.combineRows(leftRow, rightRow)
			match, err := r.evaluateJoinCondition(combined)
			if err != nil {
				continue // Skip on evaluation errors
			}

			if match {
				r.joinedRows = append(r.joinedRows, combined)
				matchFound = true
			}
		}

		// If no match was found, add the left row with NULL values for right columns
		if !matchFound {
			combined := r.combineRows(leftRow, nil)
			r.joinedRows = append(r.joinedRows, combined)
		}
	}

	return nil
}

// performRightJoin creates a right join result
func (r *JoinResult) performRightJoin() error {
	r.joinedRows = []map[string]storage.ColumnValue{}

	// For each right row
	for _, rightRow := range r.rightRows {
		matchFound := false

		// For each left row
		for _, leftRow := range r.leftRows {
			// Check if the join condition is met
			combined := r.combineRows(leftRow, rightRow)
			match, err := r.evaluateJoinCondition(combined)
			if err != nil {
				continue // Skip on evaluation errors
			}

			if match {
				r.joinedRows = append(r.joinedRows, combined)
				matchFound = true
			}
		}

		// If no match was found, add the right row with NULL values for left columns
		if !matchFound {
			combined := r.combineRows(nil, rightRow)
			r.joinedRows = append(r.joinedRows, combined)
		}
	}

	return nil
}

// performFullJoin creates a full outer join result
func (r *JoinResult) performFullJoin() error {
	// First perform a left join
	if err := r.performLeftJoin(); err != nil {
		return err
	}

	// Then add unmatched right rows
	for _, rightRow := range r.rightRows {
		matchFound := false

		// Check if this right row matched with any left row
		for _, leftRow := range r.leftRows {
			combined := r.combineRows(leftRow, rightRow)
			match, err := r.evaluateJoinCondition(combined)
			if err == nil && match {
				matchFound = true
				break
			}
		}

		// If no match was found, add the right row with NULL values for left columns
		if !matchFound {
			combined := r.combineRows(nil, rightRow)
			r.joinedRows = append(r.joinedRows, combined)
		}
	}

	return nil
}

// performCrossJoin creates a cross join result
func (r *JoinResult) performCrossJoin() error {
	r.joinedRows = []map[string]storage.ColumnValue{}

	// For each left row
	for _, leftRow := range r.leftRows {
		// For each right row
		for _, rightRow := range r.rightRows {
			// Combine the rows
			combined := r.combineRows(leftRow, rightRow)
			r.joinedRows = append(r.joinedRows, combined)
		}
	}

	return nil
}

// combineRows combines a left and right row into a single row
func (r *JoinResult) combineRows(leftRow, rightRow map[string]storage.ColumnValue) map[string]storage.ColumnValue {
	// Get a map from the pool instead of creating a new one
	combined := common.GetColumnValueMap(len(r.columns))

	// Map should already be cleared by GetColumnValueMap

	// Add left columns
	if leftRow != nil {
		for k, v := range leftRow {
			combined[k] = v
		}
	} else {
		// Add NULL values for left columns
		for _, col := range r.leftColumns {
			combined[col] = storage.StaticNullUnknown
			if r.leftPrefix != "" {
				combined[r.leftPrefix+"."+col] = storage.StaticNullUnknown
			}
		}
	}

	// Add right columns
	if rightRow != nil {
		for k, v := range rightRow {
			combined[k] = v
		}
	} else {
		// Add NULL values for right columns
		for _, col := range r.rightColumns {
			combined[col] = storage.StaticNullUnknown
			if r.rightPrefix != "" {
				combined[r.rightPrefix+"."+col] = storage.StaticNullUnknown
			}
		}
	}

	return combined
}

// evaluateJoinCondition evaluates the join condition for the combined row
func (r *JoinResult) evaluateJoinCondition(combined map[string]storage.ColumnValue) (bool, error) {
	if r.joinCond == nil {
		// If no join condition is provided, perform a cross join (return true for all row combinations)
		return true, nil
	}

	// Evaluate the join condition
	return r.evaluator.EvaluateWhereClause(r.joinCond, combined)
}

// Next moves to the next row in the join result
func (r *JoinResult) Next() bool {
	if r.closed.Load() {
		return false
	}

	if r.currentRowIdx == -1 {
		// First initialize the join result
		if err := r.performJoin(); err != nil {
			return false
		}
	}

	// Move to the next row
	r.currentRowIdx++

	// Check if we've reached the end
	if r.currentRowIdx >= len(r.joinedRows) {
		return false
	}

	// Set the current row
	r.currentRow = r.joinedRows[r.currentRowIdx]

	return true
}

// Scan copies values from the current row into the provided variables
func (r *JoinResult) Scan(dest ...interface{}) error {
	if r.closed.Load() {
		return fmt.Errorf("result set is closed")
	}

	if r.currentRowIdx < 0 || r.currentRowIdx >= len(r.joinedRows) {
		return fmt.Errorf("no row to scan")
	}

	if len(dest) != len(r.columns) {
		return fmt.Errorf("expected %d destination arguments in Scan, got %d", len(r.columns), len(dest))
	}

	// Copy values to destination in the order of columns
	for i, col := range r.columns {
		val, ok := r.currentRow[col]
		if !ok {
			// Try to find the column without the prefix
			parts := strings.Split(col, ".")
			if len(parts) == 2 {
				val, ok = r.currentRow[parts[1]]
			}

			if !ok {
				val = nil // Column not found, use NULL
			}
		}

		// Set the value into the destination
		if err := r.copyValueToDestination(val, dest[i], col); err != nil {
			return err
		}
	}

	return nil
}

// copyValueToDestination copies a value to a destination pointer
func (r *JoinResult) copyValueToDestination(val interface{}, destPtr interface{}, colName string) error {
	if destPtr == nil {
		return fmt.Errorf("destination pointer is nil")
	}

	// If the value is already a ColumnValue, use it directly with the central utility
	if colVal, ok := val.(storage.ColumnValue); ok {
		if err := storage.ScanColumnValueToDestination(colVal, destPtr); err != nil {
			return fmt.Errorf("column %s: %w", colName, err)
		}
		return nil
	}

	// For nil values, use the central utility with nil ColumnValue
	if val == nil {
		if err := storage.ScanColumnValueToDestination(nil, destPtr); err != nil {
			return fmt.Errorf("column %s: %w", colName, err)
		}
		return nil
	}

	// For other types, convert to ColumnValue first using GetPooledColumnValue
	colVal := storage.GetPooledColumnValue(val)
	if err := storage.ScanColumnValueToDestination(colVal, destPtr); err != nil {
		return fmt.Errorf("column %s: %w", colName, err)
	}
	// Return the pooled value after use
	storage.PutPooledColumnValue(colVal)

	return nil
}

// Close closes the result set
func (r *JoinResult) Close() error {
	if r.closed.Swap(true) {
		return nil
	}

	// Close both result sets if they haven't been closed yet
	if r.leftResult != nil {
		if err := r.leftResult.Close(); err != nil {
			return err
		}
		r.leftResult = nil
	}

	if r.rightResult != nil {
		if err := r.rightResult.Close(); err != nil {
			return err
		}
		r.rightResult = nil
	}

	// Return all row maps to the pool
	if r.leftRows != nil {
		for _, row := range r.leftRows {
			// Map clearing is handled by PutColumnValueMap
			common.PutColumnValueMap(row, len(r.columns))
		}
		r.leftRows = nil
	}

	if r.rightRows != nil {
		for _, row := range r.rightRows {
			// Map clearing is handled by PutColumnValueMap
			common.PutColumnValueMap(row, len(r.columns))
		}
		r.rightRows = nil
	}

	// Return the current row to the pool if it exists
	if r.currentRow != nil {
		// Clear the map before returning it to the pool
		clear(r.currentRow)

		common.PutColumnValueMap(r.currentRow, len(r.columns))
		r.currentRow = nil
	}

	return nil
}

// RowsAffected returns the number of rows affected (not applicable for joins)
func (r *JoinResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns the last insert ID (not applicable for joins)
func (r *JoinResult) LastInsertID() int64 {
	return 0
}

// Context returns the result's context
func (r *JoinResult) Context() context.Context {
	// Use the left result's context if available
	if r.leftResult != nil {
		return r.leftResult.Context()
	}

	// Fallback to the right result's context
	if r.rightResult != nil {
		return r.rightResult.Context()
	}

	// Last resort
	return context.Background()
}

// WithAliases returns a new result with the given column aliases
func (r *JoinResult) WithAliases(aliases map[string]string) storage.Result {
	// Use the existing AliasedResult implementation in the package
	return NewAliasedResult(r, aliases)
}

// Row implements the storage.Result interface
// For a join result, this is a placeholder as we've materialized the results as maps rather than Rows
func (r *JoinResult) Row() storage.Row {
	if r.closed.Load() {
		return nil
	}

	// Convert the current row to a storage.Row
	columns := r.Columns()
	row := make(storage.Row, len(columns))

	for i, col := range columns {
		// Get value from the current row map
		val, exists := r.currentRow[col]
		if !exists {
			row[i] = storage.StaticNullUnknown
			continue
		}

		if val == nil {
			row[i] = storage.StaticNullUnknown
			continue
		}

		row[i] = val
	}

	return row
}
