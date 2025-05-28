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
	"log"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// A shared object pool for streaming join row maps to reduce memory allocations
// Reusing the same pool as in join_result.go would be ideal, but we create a separate one
// to avoid package-level dependencies in case the implementations change
var streamingJoinRowMapPool = &sync.Pool{
	New: func() interface{} {
		return make(map[string]storage.ColumnValue, 64) // Pre-allocate with reasonable capacity
	},
}

// StreamingJoinResult represents a result set from a JOIN operation with streaming execution
// It avoids materializing all rows from both tables by using a hash-join approach
type StreamingJoinResult struct {
	leftResult  storage.Result // The left side of the join
	rightResult storage.Result // The right side of the join
	joinType    string         // "INNER", "LEFT", "RIGHT", "FULL"
	joinCond    parser.Expression
	evaluator   *Evaluator

	// Join metadata
	leftPrefix   string
	rightPrefix  string
	leftColumns  []string
	rightColumns []string
	columns      []string
	closed       bool

	// Join state
	hashTable         map[uint64][]map[string]storage.ColumnValue // Hash table for the build phase
	buildComplete     bool                                        // Flag indicating if build phase is complete
	probeRowIdx       int                                         // Current index in the probe phase
	currentRightRow   map[string]storage.ColumnValue              // Current right row being probed
	rightRowsMatched  map[uint64]bool                             // Tracks if a right row matched (for RIGHT/FULL joins)
	matchBuffer       []map[string]storage.ColumnValue            // Buffer of matched rows
	bufferIdx         int                                         // Current index in the buffer
	rightRowsComplete bool                                        // Flag indicating if right rows have been processed
	currentRow        map[string]storage.ColumnValue              // Current row for scan
}

// NewStreamingJoinResult creates a new streaming join result
func NewStreamingJoinResult(
	leftResult, rightResult storage.Result,
	joinType string,
	joinCond parser.Expression,
	evaluator *Evaluator,
	leftPrefix, rightPrefix string) *StreamingJoinResult {

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

	// Get a map from the pool for the current row
	currentRow := streamingJoinRowMapPool.Get().(map[string]storage.ColumnValue)

	// Clear the map if it's not empty
	for k := range currentRow {
		delete(currentRow, k)
	}

	return &StreamingJoinResult{
		leftResult:        leftResult,
		rightResult:       rightResult,
		joinType:          joinType,
		joinCond:          joinCond,
		evaluator:         evaluator,
		leftPrefix:        leftPrefix,
		rightPrefix:       rightPrefix,
		leftColumns:       leftCols,
		rightColumns:      rightCols,
		columns:           columns,
		hashTable:         make(map[uint64][]map[string]storage.ColumnValue),
		probeRowIdx:       -1,
		bufferIdx:         -1,
		buildComplete:     false,
		rightRowsComplete: false,
		matchBuffer:       nil,
		rightRowsMatched:  make(map[uint64]bool),
		currentRow:        currentRow,
		closed:            false,
	}
}

// buildHashTable builds the hash table from the left table
func (r *StreamingJoinResult) buildHashTable() error {
	// Skip if already built
	if r.buildComplete {
		return nil
	}

	// Build a hash table from the left table (build phase)
	// Build a hash table from the left table (build phase)
	for r.leftResult.Next() {
		// Get a map from the pool instead of creating a new one
		leftRow := streamingJoinRowMapPool.Get().(map[string]storage.ColumnValue)

		// Clear the map if it's not empty
		for k := range leftRow {
			delete(leftRow, k)
		}

		// Try to get the row directly for best performance
		if rawRow := r.leftResult.Row(); rawRow != nil {
			// Populate the row map from the raw storage.Row
			for i, col := range r.leftColumns {
				if i < len(rawRow) {
					leftRow[col] = rawRow[i]

					// Also store with table prefix
					if r.leftPrefix != "" {
						leftRow[r.leftPrefix+"."+col] = rawRow[i]
					}
				}
			}
		}

		// Hash the row values for join condition matching
		hash := r.hashRow(leftRow)

		// Add to hash table
		r.hashTable[hash] = append(r.hashTable[hash], leftRow)
	}

	// Mark build phase as complete
	r.buildComplete = true

	r.leftResult.Close() // We can close the left result now

	return nil
}

// hashRow computes a hash value for the row for join matching
// In a real implementation, this would be based on join keys
// For simplicity, we'll use a basic hash of all column values
func (r *StreamingJoinResult) hashRow(row map[string]storage.ColumnValue) uint64 {
	_ = row // Avoid unused variable warning

	// Extract the join condition columns for a real hash implementation
	// For test simplicity, we'll:
	// 1. For rows with NULL in a join column, use a special hash bucket (0)
	// 2. For all other rows, use a constant hash (1)

	// Check if this row has any NULL join columns
	// This is a simplification - in a real implementation we would parse
	// the join condition to determine which columns to check

	// Regular hash for normal values - all grouped together for simplicity
	return 1 // Constant hash to ensure all rows are compared
}

// getNextRightRow gets the next right-side row for probing
func (r *StreamingJoinResult) getNextRightRow() (map[string]storage.ColumnValue, uint64, error) {
	// Check if we have a valid right result
	if r.rightResult == nil || !r.rightResult.Next() {
		r.rightRowsComplete = true
		return nil, 0, nil
	}

	// Get a map from the pool instead of creating a new one
	rightRow := streamingJoinRowMapPool.Get().(map[string]storage.ColumnValue)

	// Clear the map if it's not empty
	for k := range rightRow {
		delete(rightRow, k)
	}

	// Try to get the row directly for best performance
	if rawRow := r.rightResult.Row(); rawRow != nil {
		// Populate the row map from the raw storage.Row
		for i, col := range r.rightColumns {
			if i < len(rawRow) {
				rightRow[col] = rawRow[i]

				// Also store with table prefix
				if r.rightPrefix != "" {
					rightRow[r.rightPrefix+"."+col] = rawRow[i]
				}
			}
		}
	}

	// Hash the row
	hash := r.hashRow(rightRow)

	return rightRow, hash, nil
}

// matchAndBufferRows finds matches for the current right row and buffers them
func (r *StreamingJoinResult) matchAndBufferRows() error {
	// Clear the existing match buffer
	r.matchBuffer = nil
	r.bufferIdx = -1

	// Get the next right row to probe
	rightRow, rightHash, err := r.getNextRightRow()
	if err != nil {
		return err
	}

	// If no more right rows, we're done with probe phase
	if rightRow == nil {
		r.rightRowsComplete = true

		// For LEFT or FULL join, we need to process unmatched left rows
		if r.joinType == "LEFT" || r.joinType == "FULL" {
			r.processLeftJoin()
		}

		// For RIGHT/FULL join, we need to process non-matched right rows
		if r.joinType == "RIGHT" || r.joinType == "FULL" {
			return r.processRemainingRightRows()
		}

		return nil
	}

	// Save current right row for reference
	r.currentRightRow = rightRow

	// Lookup all matching rows in hash table
	matchedRows := r.hashTable[rightHash]
	matchFound := false

	// For each potential match from the hash table
	for _, leftRow := range matchedRows {
		// Combine the rows
		combined := r.combineRows(leftRow, rightRow)

		// Check if the join condition is satisfied
		match, err := r.evaluateJoinCondition(combined)
		if err != nil {
			// Skip on evaluation errors
			continue
		}

		if match {
			// Add matched row to buffer
			r.matchBuffer = append(r.matchBuffer, combined)
			matchFound = true
		}
	}

	// For RIGHT, FULL, and CROSS joins handle non-matched right rows
	if !matchFound {
		if r.joinType == "RIGHT" || r.joinType == "FULL" {
			// For RIGHT JOIN, add right row with NULL values for left columns
			combined := r.combineRows(nil, rightRow)
			r.matchBuffer = append(r.matchBuffer, combined)
		} else if r.joinType == "CROSS" {
			// For CROSS joins, we add all combinations
			for _, leftRow := range r.getAllLeftRows() {
				combined := r.combineRows(leftRow, rightRow)
				r.matchBuffer = append(r.matchBuffer, combined)
			}
		}
		// Mark this right row for later processing
		r.rightRowsMatched[rightHash] = true
	}

	// Reset buffer index
	r.bufferIdx = -1

	return nil
}

// getAllLeftRows returns all rows from the left table
// Used for CROSS joins
func (r *StreamingJoinResult) getAllLeftRows() []map[string]storage.ColumnValue {
	var allRows []map[string]storage.ColumnValue

	// Collect all rows from all hash buckets
	for _, rows := range r.hashTable {
		allRows = append(allRows, rows...)
	}

	return allRows
}

// processRemainingRightRows handles right rows that didn't match for RIGHT/FULL joins
func (r *StreamingJoinResult) processRemainingRightRows() error {
	// Nothing to do if not RIGHT or FULL join
	if r.joinType != "RIGHT" && r.joinType != "FULL" {
		return nil
	}

	// We've already handled unmatched right rows in the matchAndBufferRows function
	// by adding a combined row with NULL values for left columns.
	// The r.rightRowsMatched map tracks which right rows have been processed.

	// For LEFT or FULL joins, we also need to process unmatched left rows
	// (This is already called in matchAndBufferRows when r.rightRowsComplete = true)

	// The match buffer should already contain the necessary rows at this point

	// Reset buffer index to prepare for iteration
	r.bufferIdx = -1

	return nil
}

// processLeftJoin handles LEFT JOIN logic by adding unmatched left rows to the result
func (r *StreamingJoinResult) processLeftJoin() {
	// If not a LEFT or FULL join, nothing to do
	if r.joinType != "LEFT" && r.joinType != "FULL" {
		return
	}

	// Initialize tracking structures
	matchedLeftRowsKeys := make(map[string]bool)
	trackedLeftRows := make(map[string]map[string]storage.ColumnValue)

	// Create a new buffer for final results
	newBuffer := make([]map[string]storage.ColumnValue, 0)

	// If we have an existing buffer, keep all rows and track which left rows are matched
	if r.matchBuffer != nil {
		for _, combined := range r.matchBuffer {
			// Keep all existing rows
			newBuffer = append(newBuffer, combined)

			// Create a hash key for the left part of this row
			key := ""
			hasLeftValues := false
			for _, col := range r.leftColumns {
				// Try with column name directly
				val, valOk := combined[col]
				// Try with prefix
				if !valOk && r.leftPrefix != "" {
					val, valOk = combined[r.leftPrefix+"."+col]
				}

				if valOk && val != nil {
					hasLeftValues = true
					key += fmt.Sprintf("%v|", val)
				} else {
					key += "NULL|"
				}
			}

			// Only mark as matched if there are actual left values
			// (if all left values are NULL, this is a RIGHT JOIN row with no left match)
			if hasLeftValues {
				matchedLeftRowsKeys[key] = true
			}
		}
	}

	// Now collect all left rows for tracking
	for _, leftRows := range r.hashTable {
		for _, leftRow := range leftRows {
			// Create a hash key for this row
			key := ""
			for _, col := range r.leftColumns {
				if val, ok := leftRow[col]; ok && val != nil {
					key += fmt.Sprintf("%v|", val)
				} else if val, ok := leftRow[r.leftPrefix+"."+col]; ok && val != nil {
					key += fmt.Sprintf("%v|", val)
				} else {
					key += "NULL|"
				}
			}

			// Store the row for potential use
			trackedLeftRows[key] = leftRow
		}
	}

	// Add unmatched left rows
	for key, leftRow := range trackedLeftRows {
		if !matchedLeftRowsKeys[key] {
			combined := r.combineRows(leftRow, nil)
			newBuffer = append(newBuffer, combined)
		}
	}

	// Replace the match buffer with our new buffer
	r.matchBuffer = newBuffer
	r.bufferIdx = -1
}

// Columns returns the column names in the result
func (r *StreamingJoinResult) Columns() []string {
	return r.columns
}

// Next moves to the next row in the join result
func (r *StreamingJoinResult) Next() bool {
	if r.closed {
		return false
	}

	// First time initialization
	if !r.buildComplete {
		// Build the hash table from the left table
		if err := r.buildHashTable(); err != nil {
			// Log error (in a real implementation, propagate it)
			log.Printf("Error building hash table: %v\n", err)
			return false
		}
	}

	// If we have buffered matches, return the next one
	if r.matchBuffer != nil && r.bufferIdx < len(r.matchBuffer)-1 {
		r.bufferIdx++
		r.currentRow = r.matchBuffer[r.bufferIdx]
		return true
	}

	// If we've processed all right rows and already processed any final LEFT JOIN rows
	if r.rightRowsComplete {
		// One last check - for LEFT/FULL joins, ensure we've processed unmatched left rows
		if (r.joinType == "LEFT" || r.joinType == "FULL") &&
			(r.matchBuffer == nil || r.bufferIdx >= len(r.matchBuffer)-1) {
			r.processLeftJoin()

			// If we now have rows in the buffer, return one
			if len(r.matchBuffer) > 0 {
				r.bufferIdx = 0
				r.currentRow = r.matchBuffer[r.bufferIdx]
				return true
			}
		}
		return false
	}

	// Get the next batch of matches
	if err := r.matchAndBufferRows(); err != nil {
		// Log error (in a real implementation, propagate it)
		log.Printf("Error matching rows: %v\n", err)
		return false
	}

	// If no more matches, we're done
	if len(r.matchBuffer) == 0 {
		// Try again with the next right row
		return r.Next()
	}

	// Return the first match from the new buffer
	r.bufferIdx = 0
	r.currentRow = r.matchBuffer[r.bufferIdx]
	return true
}

// Scan copies values from the current row into the provided variables
func (r *StreamingJoinResult) Scan(dest ...interface{}) error {
	if r.closed {
		return fmt.Errorf("result set is closed")
	}

	if r.currentRow == nil {
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

		// Set the value into the destination using the helper function from join_result.go
		if err := copyValueToDestination(val, dest[i], col); err != nil {
			return err
		}
	}

	return nil
}

// copyValueToDestination copies a value to a destination pointer
// Uses the central utility function from storage package for consistent behavior
func copyValueToDestination(val interface{}, destPtr interface{}, colName string) error {
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

	// For other types, convert to ColumnValue first using pooled implementation
	colVal := storage.GetPooledColumnValue(val)
	if err := storage.ScanColumnValueToDestination(colVal, destPtr); err != nil {
		storage.PutPooledColumnValue(colVal) // Release the pooled value if scan fails
		return fmt.Errorf("column %s: %w", colName, err)
	}

	// Release the pooled value after successful scan
	storage.PutPooledColumnValue(colVal)
	return nil
}

// combineRows combines a left and right row into a single row
func (r *StreamingJoinResult) combineRows(leftRow, rightRow map[string]storage.ColumnValue) map[string]storage.ColumnValue {
	// Get a map from the pool instead of creating a new one
	combined := streamingJoinRowMapPool.Get().(map[string]storage.ColumnValue)

	// Clear the map by deleting all keys (faster than creating a new one)
	for k := range combined {
		delete(combined, k)
	}

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
func (r *StreamingJoinResult) evaluateJoinCondition(combined map[string]storage.ColumnValue) (bool, error) {
	if r.joinCond == nil {
		// If no join condition is provided, return true for all row combinations (CROSS JOIN)
		return true, nil
	}

	// Evaluate the join condition
	return r.evaluator.EvaluateWhereClause(r.joinCond, combined)
}

// Close closes the result set
func (r *StreamingJoinResult) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

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

	// Return all row maps in the hash table to the pool
	if r.hashTable != nil {
		for _, rows := range r.hashTable {
			for _, row := range rows {
				// Clear the map before returning to the pool
				for k := range row {
					delete(row, k)
				}
				streamingJoinRowMapPool.Put(row)
			}
		}
		r.hashTable = nil
	}

	// Return match buffer rows to the pool
	if r.matchBuffer != nil {
		for _, row := range r.matchBuffer {
			// Clear the map before returning to the pool
			for k := range row {
				delete(row, k)
			}
			streamingJoinRowMapPool.Put(row)
		}
		r.matchBuffer = nil
	}

	// Return current row to the pool if it exists
	if r.currentRow != nil {
		// Clear the map before returning to the pool
		for k := range r.currentRow {
			delete(r.currentRow, k)
		}
		streamingJoinRowMapPool.Put(r.currentRow)
		r.currentRow = nil
	}

	// Return current right row to the pool if it exists
	if r.currentRightRow != nil {
		// Clear the map before returning to the pool
		for k := range r.currentRightRow {
			delete(r.currentRightRow, k)
		}
		streamingJoinRowMapPool.Put(r.currentRightRow)
		r.currentRightRow = nil
	}

	// Clear other data structures
	r.rightRowsMatched = nil

	return nil
}

// RowsAffected returns the number of rows affected (not applicable for joins)
func (r *StreamingJoinResult) RowsAffected() int64 {
	return 0
}

// LastInsertID returns the last insert ID (not applicable for joins)
func (r *StreamingJoinResult) LastInsertID() int64 {
	return 0
}

// Context returns the result's context
func (r *StreamingJoinResult) Context() context.Context {
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
func (r *StreamingJoinResult) WithAliases(aliases map[string]string) storage.Result {
	// Use the existing AliasedResult implementation in the package
	return NewAliasedResult(r, aliases)
}

// Row implements the storage.Result interface
// Row implements the storage.Result interface
// For a streaming join result, this returns the current row as a storage.Row
func (r *StreamingJoinResult) Row() storage.Row {
	// We should check if we have a valid current row
	if r.currentRow == nil {
		return nil
	}

	// If possible, get the raw row from the underlying result
	// For joins, this would need to be constructed from the joined data

	// Create a Row with column values in the correct order
	row := make(storage.Row, len(r.columns))

	// Fill the row with references to values from currentRow
	for i, colName := range r.columns {
		if val, ok := r.currentRow[colName]; ok {
			row[i] = val // Direct reference, no need to copy
		} else {
			row[i] = storage.StaticNullUnknown
		}
	}

	return row
}
