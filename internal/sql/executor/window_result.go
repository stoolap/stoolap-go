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
	"sort"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// Object pool for partition maps
var windowPartitionMapPool = &sync.Pool{
	New: func() interface{} {
		return make(map[string][][]storage.ColumnValue, 32) // Pre-allocate with reasonable capacity
	},
}

// WindowFunctionInfo contains information about a window function in a query
type WindowFunctionInfo struct {
	Name        string                     // Function name
	Arguments   []parser.Expression        // Function arguments
	ColumnName  string                     // Output column name
	ColumnIndex int                        // Position in the result columns
	PartitionBy []parser.Expression        // PARTITION BY expressions
	OrderBy     []parser.OrderByExpression // ORDER BY expressions
}

// WindowResult represents a result set processed with window functions
type WindowResult struct {
	baseResult       storage.Result
	windowFunctions  []*WindowFunctionInfo
	partitionColumns []string
	orderByColumns   []string
	resultColumns    []string
	registry         contract.FunctionRegistry

	// Internal state
	allRows          [][]storage.ColumnValue   // All rows from the base result
	partitions       [][][]storage.ColumnValue // Rows grouped by partition
	partitionKeys    []string                  // Keys for each partition
	currentPartition int                       // Current partition index
	currentRow       int                       // Current row index within partition
	currentResults   []interface{}             // Results for the current row
	initialized      bool                      // Whether initialization is complete
}

// NewWindowResult creates a new window function result
func NewWindowResult(base storage.Result,
	functions []*WindowFunctionInfo,
	partitionColumns []string,
	orderByColumns []string) storage.Result {

	// Extract column names for the result
	resultColumns := make([]string, len(functions))
	for i, fn := range functions {
		resultColumns[i] = fn.ColumnName
	}

	// Create the result
	result := &WindowResult{
		baseResult:       base,
		windowFunctions:  functions,
		partitionColumns: partitionColumns,
		orderByColumns:   orderByColumns,
		resultColumns:    resultColumns,
		registry:         GetGlobalFunctionRegistry(),
		currentPartition: -1,
		currentRow:       -1,
		initialized:      false,
	}

	return result
}

// initialize loads all rows from the base result and groups them into partitions
func (r *WindowResult) initialize() error {
	if r.initialized {
		return nil
	}

	baseColumns := r.baseResult.Columns()
	r.allRows = [][]storage.ColumnValue{}

	// Collect all rows from the base result
	for r.baseResult.Next() {
		// Try to use Row() method first for better performance
		if row := r.baseResult.Row(); len(row) > 0 {
			r.allRows = append(r.allRows, row)
		}
	}

	// Group rows by partition
	if len(r.partitionColumns) > 0 {
		// Get a map from the pool for partition key -> rows
		partitionMap := windowPartitionMapPool.Get().(map[string][][]storage.ColumnValue)
		// Clear the map if it's not empty
		for k := range partitionMap {
			delete(partitionMap, k)
		}

		for _, row := range r.allRows {
			// Create a key for the partition
			var keyParts []string
			for _, partCol := range r.partitionColumns {
				// Find the column index
				colIndex := -1
				for i, col := range baseColumns {
					if col == partCol {
						colIndex = i
						break
					}
				}

				if colIndex >= 0 && colIndex < len(row) {
					// Add the column value to the key
					if row[colIndex] == nil {
						keyParts = append(keyParts, "NULL")
					} else {
						keyParts = append(keyParts, fmt.Sprintf("%v", row[colIndex]))
					}
				}
			}

			// Join the key parts
			key := strings.Join(keyParts, "||")

			// Add the row to the partition
			partitionMap[key] = append(partitionMap[key], row)
		}

		// Convert the map to a slice of partitions
		r.partitions = make([][][]storage.ColumnValue, 0, len(partitionMap))
		r.partitionKeys = make([]string, 0, len(partitionMap))

		for key, rows := range partitionMap {
			r.partitions = append(r.partitions, rows)
			r.partitionKeys = append(r.partitionKeys, key)
		}

		// Return the map to the pool
		windowPartitionMapPool.Put(partitionMap)
	} else {
		// If no PARTITION BY clause, all rows are in one partition
		r.partitions = [][][]storage.ColumnValue{r.allRows}
		r.partitionKeys = []string{""}
	}

	// Sort rows within each partition if ORDER BY is specified
	if len(r.orderByColumns) > 0 {
		for i, partition := range r.partitions {
			// Convert the executor's simple string-based orderByColumns to parser.OrderByExpression
			orderExprs := make([]parser.OrderByExpression, len(r.orderByColumns))
			for j, col := range r.orderByColumns {
				// Default to ascending order
				expr := &parser.Identifier{Value: col}
				orderExprs[j] = parser.OrderByExpression{
					Expression: expr,
					Ascending:  true, // Default to ascending
				}
			}

			// Sort the partition
			sortPartition(partition, baseColumns, orderExprs)
			r.partitions[i] = partition
		}
	}

	r.currentPartition = 0
	r.currentRow = -1
	r.initialized = true
	return nil
}

// sortPartition sorts rows within a partition based on ORDER BY clause
func sortPartition(rows [][]storage.ColumnValue, columns []string, orderBy []parser.OrderByExpression) {
	sort.Slice(rows, func(i, j int) bool {
		for _, expr := range orderBy {
			colIndex := -1

			// Find the column index
			if ident, ok := expr.Expression.(*parser.Identifier); ok {
				for idx, col := range columns {
					if col == ident.Value {
						colIndex = idx
						break
					}
				}
			}

			if colIndex < 0 {
				continue // Column not found
			}

			// Get values to compare
			aVal := rows[i][colIndex]
			bVal := rows[j][colIndex]

			// Handle NULL values
			aIsNull := aVal == nil || aVal.IsNull()
			bIsNull := bVal == nil || bVal.IsNull()

			if aIsNull && bIsNull {
				continue // Both NULL, move to next column
			}
			if aIsNull {
				return !expr.Ascending // NULL comes first if descending, last if ascending
			}
			if bIsNull {
				return expr.Ascending // NULL comes last if ascending, first if descending
			}

			// Compare non-NULL values using storage.ColumnValue.Compare
			result := 0
			cmpResult, err := aVal.Compare(bVal)
			if err == nil {
				result = cmpResult
			}

			// Apply sort direction
			if result != 0 {
				if expr.Ascending {
					return result < 0
				}
				return result > 0
			}
		}
		return false // Equal
	})
}

// Columns returns the column names for the window functions
func (r *WindowResult) Columns() []string {
	return r.resultColumns
}

// Next advances to the next row
func (r *WindowResult) Next() bool {
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return false
		}
	}

	// Check if we've processed all partitions
	if r.currentPartition >= len(r.partitions) {
		return false
	}

	// Get the current partition
	partition := r.partitions[r.currentPartition]

	// Move to the next row within the partition
	r.currentRow++

	// If we've processed all rows in this partition, move to the next one
	if r.currentRow >= len(partition) {
		r.currentPartition++
		r.currentRow = 0

		// Check if we've processed all partitions now
		if r.currentPartition >= len(r.partitions) {
			return false
		}

		// Get the new partition
		partition = r.partitions[r.currentPartition]
		//TODO: Handle partition key if needed
		_ = partition // Avoid unused variable error
	}

	// Process the window functions for this row
	if err := r.processWindowFunctions(); err != nil {
		// Log the error and return empty results
		r.currentResults = make([]interface{}, len(r.windowFunctions))
	}

	return true
}

// processWindowFunctions applies window functions to the current row
func (r *WindowResult) processWindowFunctions() error {
	if r.currentPartition < 0 || r.currentPartition >= len(r.partitions) {
		return fmt.Errorf("invalid partition index")
	}

	partition := r.partitions[r.currentPartition]
	if r.currentRow < 0 || r.currentRow >= len(partition) {
		return fmt.Errorf("invalid row index")
	}

	// Base columns for reference
	baseColumns := r.baseResult.Columns()

	// Current row data
	row := partition[r.currentRow]

	// Initialize results slice
	r.currentResults = make([]interface{}, len(r.windowFunctions))

	// For each window function
	for i, wfn := range r.windowFunctions {
		// Get the function implementation
		fn := r.registry.GetWindowFunction(wfn.Name)
		if fn == nil {
			// If window function not found, try scalar function as fallback
			r.currentResults[i] = nil
			continue
		}

		// For other functions, prepare arguments from the partition
		partitionValues := make([]interface{}, 0)
		orderByValues := make([]interface{}, 0)

		// For each argument, extract values from the current row
		for _, arg := range wfn.Arguments {
			switch expr := arg.(type) {
			case *parser.Identifier:
				// Find column index
				colIndex := -1
				for idx, col := range baseColumns {
					if col == expr.Value {
						colIndex = idx
						break
					}
				}

				if colIndex >= 0 && colIndex < len(row) {
					partitionValues = append(partitionValues, row[colIndex])
				} else {
					partitionValues = append(partitionValues, nil)
				}

			case *parser.StringLiteral:
				partitionValues = append(partitionValues, expr.Value)

			case *parser.IntegerLiteral:
				partitionValues = append(partitionValues, expr.Value)

			case *parser.FloatLiteral:
				partitionValues = append(partitionValues, expr.Value)

			default:
				// Use string representation as fallback
				partitionValues = append(partitionValues, expr.String())
			}
		}

		// Process the window function
		result, err := fn.Process(partitionValues, orderByValues)
		if err != nil {
			return fmt.Errorf("error processing window function: %v", err)
		}
		r.currentResults[i] = result
	}

	return nil
}

// Scan copies the current row values into the provided destination variables
func (r *WindowResult) Scan(dest ...interface{}) error {
	if r.currentResults == nil {
		return errors.New("no row available, call Next first")
	}

	if len(dest) != len(r.currentResults) {
		return fmt.Errorf("wrong number of scan destinations: got %d, want %d",
			len(dest), len(r.currentResults))
	}

	// Copy the window function results to the destinations
	for i, val := range r.currentResults {
		// Get the destination pointer
		dPtr := dest[i]

		// If value is already a ColumnValue, use it directly
		if colVal, ok := val.(storage.ColumnValue); ok {
			if err := storage.ScanColumnValueToDestination(colVal, dPtr); err != nil {
				return fmt.Errorf("window column %d: %w", i, err)
			}
			continue
		}

		// For nil values, use the central utility with nil ColumnValue
		if val == nil {
			if err := storage.ScanColumnValueToDestination(nil, dPtr); err != nil {
				return fmt.Errorf("window column %d: %w", i, err)
			}
			continue
		}

		// For other types, convert to ColumnValue first for consistent handling
		colVal := storage.GetPooledColumnValue(val)
		if err := storage.ScanColumnValueToDestination(colVal, dPtr); err != nil {
			storage.PutPooledColumnValue(colVal) // Return to pool on error
			return fmt.Errorf("window column %d: %w", i, err)
		}
		// Return to pool after successful use
		storage.PutPooledColumnValue(colVal)
	}

	return nil
}

// Close releases resources associated with the result
func (r *WindowResult) Close() error {
	return r.baseResult.Close()
}

// RowsAffected returns the number of rows affected
func (r *WindowResult) RowsAffected() int64 {
	return 0 // Not applicable for window results
}

// LastInsertID returns the last inserted ID
func (r *WindowResult) LastInsertID() int64 {
	return 0 // Not applicable for window results
}

// Context returns the context associated with the result
func (r *WindowResult) Context() context.Context {
	return r.baseResult.Context()
}

// WithAliases implements the storage.Result interface for column aliasing
func (r *WindowResult) WithAliases(aliases map[string]string) storage.Result {
	// Propagate aliases to the base result if possible
	if aliasable, ok := r.baseResult.(interface {
		WithAliases(map[string]string) storage.Result
	}); ok {
		base := aliasable.WithAliases(aliases)

		// Create a new result with the aliased base
		result := &WindowResult{
			baseResult:       base,
			windowFunctions:  r.windowFunctions,
			partitionColumns: r.partitionColumns,
			orderByColumns:   r.orderByColumns,
			resultColumns:    r.resultColumns,
			registry:         r.registry,
		}

		return result
	}

	// If base doesn't support aliases, wrap this result
	return NewAliasedResult(r, aliases)
}

// Row implements the storage.Result interface
func (r *WindowResult) Row() storage.Row {
	if r.currentResults == nil {
		return nil
	}

	// Convert the current results to a storage.Row
	row := make(storage.Row, len(r.currentResults))

	for i, val := range r.currentResults {
		if val == nil {
			row[i] = storage.StaticNullUnknown
			continue
		}

		// If value is already a ColumnValue, use it directly
		if cv, ok := val.(storage.ColumnValue); ok {
			row[i] = cv
			continue
		}

		// Convert interface{} values to appropriate ColumnValue types
		row[i] = storage.NewDirectValueFromInterface(val)
	}

	return row
}

// extractWindowFunction extracts window function information from a function call
func extractWindowFunction(expr *parser.FunctionCall, columnIndex int) *WindowFunctionInfo {
	// Get function name
	funcName := strings.ToUpper(expr.Function)

	// Check for PARTITION BY and ORDER BY clauses - these would be in a special
	// syntax or wrapped in a special node in a complete implementation

	// Default column name based on function
	columnName := fmt.Sprintf("%s_result", strings.ToLower(funcName))

	// Create the window function info
	return &WindowFunctionInfo{
		Name:        funcName,
		Arguments:   expr.Arguments,
		ColumnName:  columnName,
		ColumnIndex: columnIndex,
		PartitionBy: nil, // Default no partitioning
		OrderBy:     nil, // Default no ordering
	}
}

// containsWindowFunction checks if an expression contains a window function
func containsWindowFunction(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.FunctionCall:
		// Check if it's a registered window function
		registry := GetGlobalFunctionRegistry()
		return registry.IsWindowFunction(e.Function)
	case *parser.AliasedExpression:
		return containsWindowFunction(e.Expression)
	default:
		return false
	}
}
