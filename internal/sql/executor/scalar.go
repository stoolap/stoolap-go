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
	"strconv"
	"strings"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// scalarFunctionResult represents an improved version of the scalar function result
// with enhanced handling of NULL values and v3 storage types
type scalarFunctionResult struct {
	baseResult    storage.Result
	functionCalls []*parser.FunctionCall
	resultColumns []string
	registry      contract.FunctionRegistry
	currentRow    storage.Row
	rowMap        map[string]storage.ColumnValue // Reusable map to reduce allocations
}

// ScalarFunctionInfo contains information about a scalar function in a query
type ScalarFunctionInfo struct {
	Name        string              // Function name
	Arguments   []parser.Expression // Function arguments
	ColumnName  string              // Output column name
	ColumnIndex int                 // Position in the result columns
}

// NewScalarResult creates a new improved scalar function result
func NewScalarResult(base storage.Result, functions []*ScalarFunctionInfo, aliases map[string]string) storage.Result {
	// Extract FunctionCall objects from functions
	functionCalls := make([]*parser.FunctionCall, 0, len(functions))

	// We need to collect function calls in the correct order
	resultColumns := make([]string, len(functions))

	for i, fn := range functions {
		// Create a function call from function info
		funcCall := &parser.FunctionCall{
			Function:  fn.Name,
			Arguments: fn.Arguments,
		}

		// Add to the list
		functionCalls = append(functionCalls, funcCall)

		// Use column name for the result column
		resultColumns[i] = fn.ColumnName
	}

	// Create the result
	result := &scalarFunctionResult{
		baseResult:    base,
		functionCalls: functionCalls,
		resultColumns: resultColumns,
		registry:      GetGlobalFunctionRegistry(),
		currentRow:    nil,
	}

	return result
}

// Columns implements the storage.Result Columns method
func (r *scalarFunctionResult) Columns() []string {
	return r.resultColumns
}

// Next implements the storage.Result Next method
func (r *scalarFunctionResult) Next() bool {
	// First check if the base result has more rows
	hasNext := r.baseResult.Next()
	if !hasNext {
		return false
	}

	// Get the source columns
	sourceColumns := r.baseResult.Columns()

	// Get the row directly using Row() instead of Scan()
	row := r.baseResult.Row()
	if row == nil {
		return false
	}

	// Reuse the row map to reduce allocations
	// Initialize the map on first use, then clear and reuse it on subsequent calls
	if r.rowMap == nil {
		r.rowMap = make(map[string]storage.ColumnValue, len(sourceColumns)*2) // Pre-allocate capacity for both normal and lowercase versions
	} else {
		// Clear the map by deleting all keys
		for k := range r.rowMap {
			delete(r.rowMap, k)
		}
	}

	for i, col := range sourceColumns {
		if i < len(row) {
			// Store each ColumnValue from the raw row directly
			r.rowMap[col] = row[i]
			r.rowMap[strings.ToLower(col)] = row[i]
		}
	}

	// Evaluate each function and store the results
	r.currentRow = make(storage.Row, len(r.functionCalls))

	for i, fn := range r.functionCalls {
		// Process function arguments
		args := make([]interface{}, len(fn.Arguments))
		for j, arg := range fn.Arguments {
			switch expr := arg.(type) {
			case *parser.Identifier:
				// Column reference - get from the row map
				var val interface{}
				var ok bool

				colName := expr.Value
				if val, ok = r.rowMap[colName]; !ok {
					// Try case-insensitive match
					val, ok = r.rowMap[strings.ToLower(colName)]
				}

				if ok {
					if colVal, ok := val.(storage.ColumnValue); ok {
						args[j] = colVal.AsInterface()
					} else {
						args[j] = val
					}
				} else {
					// Column not found, but don't fail immediately
					// This might be another alias or a different column name
					// Set to nil which will be handled by the function
					args[j] = nil
				}

			case *parser.StringLiteral:
				args[j] = expr.Value

			case *parser.IntegerLiteral:
				args[j] = expr.Value

			case *parser.FloatLiteral:
				args[j] = expr.Value

			case *parser.BooleanLiteral:
				args[j] = expr.Value

			case *parser.NullLiteral:
				args[j] = nil

			case *parser.InfixExpression:
				// Evaluate infix expressions (like -10/3.0)
				evaluator := NewEvaluator(r.Context(), r.registry)
				evaluator.row = r.rowMap
				result, err := evaluator.Evaluate(expr)
				if err != nil {
					args[j] = nil
				} else {
					args[j] = result.AsInterface()
				}

			case *parser.PrefixExpression:
				// Evaluate prefix expressions (like -10)
				evaluator := NewEvaluator(r.Context(), r.registry)
				evaluator.row = r.rowMap
				result, err := evaluator.Evaluate(expr)
				if err != nil {
					args[j] = nil
				} else {
					args[j] = result.AsInterface()
				}

			case *parser.FunctionCall:
				// Nested function call - need to evaluate it first
				// For CAST functions, pass the target type as a string constant
				if strings.ToUpper(expr.Function) == "CAST" && len(expr.Arguments) >= 2 {
					// For CAST function with target type in second argument
					nestedFnImpl := r.registry.GetScalarFunction(expr.Function)
					if nestedFnImpl != nil {
						nestedArgs := make([]interface{}, len(expr.Arguments))

						// Process first argument (value to cast)
						if idExpr, ok := expr.Arguments[0].(*parser.Identifier); ok {
							if nestedVal, ok := r.rowMap[idExpr.Value]; ok {
								nestedArgs[0] = nestedVal
							} else if nestedVal, ok := r.rowMap[strings.ToLower(idExpr.Value)]; ok {
								nestedArgs[0] = nestedVal
							} else {
								nestedArgs[0] = nil
							}
						} else if litExpr, ok := expr.Arguments[0].(*parser.StringLiteral); ok {
							nestedArgs[0] = litExpr.Value
						} else if litExpr, ok := expr.Arguments[0].(*parser.IntegerLiteral); ok {
							nestedArgs[0] = litExpr.Value
						} else if litExpr, ok := expr.Arguments[0].(*parser.FloatLiteral); ok {
							nestedArgs[0] = litExpr.Value
						} else if litExpr, ok := expr.Arguments[0].(*parser.BooleanLiteral); ok {
							nestedArgs[0] = litExpr.Value
						} else if _, ok := expr.Arguments[0].(*parser.NullLiteral); ok {
							nestedArgs[0] = nil
						} else {
							nestedArgs[0] = expr.Arguments[0].String()
						}

						// Process second argument (target type)
						// The type name in a CAST is either a StringLiteral or an Identifier
						nestedArgs[1] = expr.Arguments[1].String()

						// Evaluate the nested function
						nestedResult, err := nestedFnImpl.Evaluate(nestedArgs...)
						if err != nil {
							args[j] = nil
						} else {
							args[j] = storage.NewDirectValueFromInterface(nestedResult)
						}
					} else {
						args[j] = nil
					}
				} else {
					// Use string representation for simplicity
					args[j] = expr.String()
				}

			default:
				// For other expressions, use string representation
				args[j] = arg.String()
			}
		}

		// Get the function implementation
		// Special handling for pass-through columns
		if fn.Function == "__PASSTHROUGH__" {
			// This is a simple column reference, just copy the value
			if len(fn.Arguments) > 0 {
				if ident, ok := fn.Arguments[0].(*parser.Identifier); ok {
					if val, ok := r.rowMap[ident.Value]; ok {
						r.currentRow[i] = val
					} else if val, ok := r.rowMap[strings.ToLower(ident.Value)]; ok {
						r.currentRow[i] = val
					} else {
						r.currentRow[i] = nil
					}
				} else {
					r.currentRow[i] = nil
				}
			} else {
				r.currentRow[i] = nil
			}
		} else if fn.Function == "ADD" || fn.Function == "SUBTRACT" || fn.Function == "MULTIPLY" || fn.Function == "DIVIDE" || fn.Function == "MODULO" {
			// Direct arithmetic evaluation
			// Create an evaluator
			evaluator := NewEvaluator(r.Context(), r.registry)

			// Use the row map directly to avoid unnecessary copying
			evaluator.WithRow(r.rowMap)

			// Get the left and right expressions
			if len(fn.Arguments) >= 2 {
				leftExpr := fn.Arguments[0]
				rightExpr := fn.Arguments[1]

				// Determine the operator based on function name
				var operator string
				switch fn.Function {
				case "ADD":
					operator = "+"
				case "SUBTRACT":
					operator = "-"
				case "MULTIPLY":
					operator = "*"
				case "DIVIDE":
					operator = "/"
				case "MODULO":
					operator = "%"
				default:
					operator = "+"
				}

				// Evaluate the arithmetic expression
				result, err := evaluator.EvaluateArithmeticExpression(leftExpr, rightExpr, operator)
				if err != nil {
					r.currentRow[i] = nil
				} else {
					// Store the ColumnValue directly - no conversion needed as it's already a ColumnValue
					r.currentRow[i] = result
				}
			} else {
				// Not enough arguments
				r.currentRow[i] = nil
			}
		} else {
			// For regular functions, use the registry
			fnImpl := r.registry.GetScalarFunction(fn.Function)
			if fnImpl != nil {
				// Call the function directly with the prepared arguments

				fnResult, err := fnImpl.Evaluate(args...)
				if err != nil {
					r.currentRow[i] = nil
				} else {
					// Ensure we have a storage.ColumnValue
					if colVal, ok := fnResult.(storage.ColumnValue); ok {
						r.currentRow[i] = colVal
					} else {
						r.currentRow[i] = storage.NewDirectValueFromInterface(fnResult)
					}
				}
			} else {
				// Function not found
				r.currentRow[i] = nil
			}
		}
	}

	return true
}

// Scan implements the storage.Result Scan method
func (r *scalarFunctionResult) Scan(dest ...interface{}) error {
	if r.currentRow == nil {
		return errors.New("no row available, call Next first")
	}

	// Check if we have the right number of destinations
	if len(dest) != len(r.functionCalls) {
		return fmt.Errorf("wrong number of scan destinations: got %d, want %d",
			len(dest), len(r.functionCalls))
	}

	// Get the current row
	row := r.Row()
	if row == nil {
		return errors.New("failed to get row data")
	}

	// Copy values to destinations using the central utility function
	for i, colVal := range row {
		if i >= len(dest) {
			break
		}

		// Use the central utility function for consistent scanning
		columnName := ""
		if i < len(r.resultColumns) {
			columnName = r.resultColumns[i]
		}

		if err := storage.ScanColumnValueToDestination(colVal, dest[i]); err != nil {
			if columnName != "" {
				return fmt.Errorf("column %s: %w", columnName, err)
			}
			return err
		}
	}

	return nil
}

// Close implements the storage.Result Close method
func (r *scalarFunctionResult) Close() error {
	return r.baseResult.Close()
}

// RowsAffected implements the storage.Result RowsAffected method
func (r *scalarFunctionResult) RowsAffected() int64 {
	return 0 // Not applicable for SELECT results
}

// LastInsertID implements the storage.Result LastInsertID method
func (r *scalarFunctionResult) LastInsertID() int64 {
	return 0 // Not applicable for SELECT results
}

// Context implements the storage.Result Context method
func (r *scalarFunctionResult) Context() context.Context {
	return r.baseResult.Context()
}

// WithAliases implements the storage.Result interface
func (r *scalarFunctionResult) WithAliases(aliases map[string]string) storage.Result {
	// Propagate aliases to the base result if possible
	if aliasable, ok := r.baseResult.(interface {
		WithAliases(map[string]string) storage.Result
	}); ok {
		base := aliasable.WithAliases(aliases)

		// Create a new result with the aliased base
		result := &scalarFunctionResult{
			baseResult:    base,
			functionCalls: r.functionCalls,
			resultColumns: r.resultColumns,
			registry:      r.registry,
		}

		return result
	}

	// If base doesn't support aliases, wrap this result
	return NewAliasedResult(r, aliases)
}

// Row implements the storage.Result interface
func (r *scalarFunctionResult) Row() storage.Row {
	if r.currentRow == nil {
		return nil
	}

	// Convert the current row to a storage.Row
	row := make(storage.Row, len(r.currentRow))

	for i, val := range r.currentRow {
		if val == nil {
			row[i] = storage.StaticNullUnknown
			continue
		}

		row[i] = val
		continue
	}

	return row
}

// executeSelectWithScalarFunctions executes a SELECT statement with scalar functions
func (e *Executor) executeSelectWithScalarFunctions(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
	// Extract table name from the table expression
	var tableName string

	// Handle different types of table expressions
	switch tableExpr := stmt.TableExpr.(type) {
	case *parser.Identifier:
		// Simple table name
		tableName = tableExpr.Value
	case *parser.SimpleTableSource:
		// Table with optional alias
		tableName = tableExpr.Name.Value
	default:
		// For other types of table expressions like JOINs, we need a different approach
		return nil, fmt.Errorf("complex table expressions not supported yet with scalar functions")
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Extract required columns and functions
	requiredColumns := make([]string, 0)
	columnAliases := make(map[string]string)

	// Track all SELECT items (both simple columns and expressions) in order
	allSelectItems := make([]*ScalarFunctionInfo, 0)

	// Analyze each column expression
	for colIndex, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *parser.Identifier:
			// Simple column reference
			if expr.Value != "*" {
				requiredColumns = append(requiredColumns, expr.Value)
				// Add simple column as a pass-through "function"
				allSelectItems = append(allSelectItems, &ScalarFunctionInfo{
					Name:        "__PASSTHROUGH__",
					Arguments:   []parser.Expression{expr},
					ColumnName:  expr.Value,
					ColumnIndex: colIndex,
				})
			} else {
				// For wildcard, we'll get all columns from the schema
				schema, err := e.engine.GetTableSchema(tableName)
				if err != nil {
					return nil, err
				}
				for _, col := range schema.Columns {
					requiredColumns = append(requiredColumns, col.Name)
					// Add each column as a pass-through "function"
					allSelectItems = append(allSelectItems, &ScalarFunctionInfo{
						Name:        "__PASSTHROUGH__",
						Arguments:   []parser.Expression{&parser.Identifier{Value: col.Name}},
						ColumnName:  col.Name,
						ColumnIndex: len(allSelectItems),
					})
				}
			}

		case *parser.CastExpression:
			// Handle CAST expressions directly
			// Extract required columns from the CAST expression
			castColumns := extractColumnsFromCastExpression(expr)
			requiredColumns = append(requiredColumns, castColumns...)

			// Convert the CastExpression to a FunctionCall for processing
			funcCall := expr.ToFunctionCall()

			// Add the CAST function to our list
			// Use strings.Builder instead of fmt.Sprintf for better performance
			var sb strings.Builder
			sb.WriteString("CAST_")
			sb.WriteString(strings.ToLower(expr.TypeName))
			castColName := sb.String()

			functionInfo := &ScalarFunctionInfo{
				Name:      "CAST",
				Arguments: funcCall.Arguments,
				// Use a descriptive column name
				ColumnName:  castColName,
				ColumnIndex: colIndex,
			}
			allSelectItems = append(allSelectItems, functionInfo)

		case *parser.FunctionCall:
			// Scalar function
			if !isScalarAggregateFunction(expr.Function) {
				// Process function arguments
				argColumns := extractColumnsFromArguments(expr.Arguments)
				requiredColumns = append(requiredColumns, argColumns...)

				// Use strings.Builder instead of fmt.Sprintf for better performance
				var sb strings.Builder
				sb.WriteString(strings.ToLower(expr.Function))
				sb.WriteString("_result")
				funcColName := sb.String()

				// Add the function to our list
				functionInfo := &ScalarFunctionInfo{
					Name:      expr.Function,
					Arguments: expr.Arguments,
					// Use a default column name based on function
					ColumnName:  funcColName,
					ColumnIndex: colIndex,
				}
				allSelectItems = append(allSelectItems, functionInfo)
			} else {
				// This is an aggregate function, should be handled by the aggregation executor
				return e.executeSelectWithAggregation(ctx, tx, stmt)
			}

		case *parser.AliasedExpression:
			// Aliased expression (can be a column or a function)
			switch innerExpr := expr.Expression.(type) {
			case *parser.Identifier:
				// Aliased column
				requiredColumns = append(requiredColumns, innerExpr.Value)
				columnAliases[expr.Alias.Value] = innerExpr.Value
				// Add aliased column as a pass-through "function"
				allSelectItems = append(allSelectItems, &ScalarFunctionInfo{
					Name:        "__PASSTHROUGH__",
					Arguments:   []parser.Expression{innerExpr},
					ColumnName:  expr.Alias.Value,
					ColumnIndex: colIndex,
				})

			case *parser.CastExpression:
				// Aliased CAST expression
				// Extract required columns from the CAST expression
				castColumns := extractColumnsFromCastExpression(innerExpr)
				requiredColumns = append(requiredColumns, castColumns...)

				// Convert the CastExpression to a FunctionCall for processing
				funcCall := innerExpr.ToFunctionCall()

				// Add the CAST function to our list using the alias as column name
				functionInfo := &ScalarFunctionInfo{
					Name:        "CAST",
					Arguments:   funcCall.Arguments,
					ColumnName:  expr.Alias.Value,
					ColumnIndex: colIndex,
				}
				allSelectItems = append(allSelectItems, functionInfo)

			case *parser.FunctionCall:
				// Aliased function
				if !isScalarAggregateFunction(innerExpr.Function) {
					// Process function arguments
					argColumns := extractColumnsFromArguments(innerExpr.Arguments)
					requiredColumns = append(requiredColumns, argColumns...)

					// Add the function to our list with the alias as column name
					functionInfo := &ScalarFunctionInfo{
						Name:        innerExpr.Function,
						Arguments:   innerExpr.Arguments,
						ColumnName:  expr.Alias.Value,
						ColumnIndex: colIndex,
					}
					allSelectItems = append(allSelectItems, functionInfo)
				} else {
					// This is an aggregate function, should be handled by the aggregation executor
					return e.executeSelectWithAggregation(ctx, tx, stmt)
				}

			case *parser.InfixExpression:
				// Aliased arithmetic expression (like id + 10)
				// Extract required columns from both sides of the expression
				if leftIdent, ok := innerExpr.Left.(*parser.Identifier); ok {
					requiredColumns = append(requiredColumns, leftIdent.Value)
				}

				if rightIdent, ok := innerExpr.Right.(*parser.Identifier); ok {
					requiredColumns = append(requiredColumns, rightIdent.Value)
				}

				// Create a synthetic function name based on operator
				var functionName string
				if innerExpr.Operator == "+" {
					functionName = "ADD"
				} else if innerExpr.Operator == "-" {
					functionName = "SUBTRACT"
				} else if innerExpr.Operator == "*" {
					functionName = "MULTIPLY"
				} else if innerExpr.Operator == "/" {
					functionName = "DIVIDE"
				} else if innerExpr.Operator == "%" {
					functionName = "MODULO"
				} else {
					functionName = "EXPRESSION"
				}

				// Add the expression as a special scalar function
				functionInfo := &ScalarFunctionInfo{
					Name: functionName,
					Arguments: []parser.Expression{
						innerExpr.Left,
						innerExpr.Right,
					},
					ColumnName:  expr.Alias.Value,
					ColumnIndex: colIndex,
				}
				allSelectItems = append(allSelectItems, functionInfo)

			default:
				return nil, fmt.Errorf("unsupported expression in aliased SELECT item: %T", expr.Expression)
			}

		case *parser.InfixExpression:
			infixExpr := colExpr.(*parser.InfixExpression)
			// Direct arithmetic expression (like int_value + 10)
			// Extract required columns from both sides of the expression
			if leftIdent, ok := infixExpr.Left.(*parser.Identifier); ok {
				requiredColumns = append(requiredColumns, leftIdent.Value)
			}

			if rightIdent, ok := infixExpr.Right.(*parser.Identifier); ok {
				requiredColumns = append(requiredColumns, rightIdent.Value)
			}

			// Create a synthetic function name based on operator
			var functionName string
			if infixExpr.Operator == "+" {
				functionName = "ADD"
			} else if infixExpr.Operator == "-" {
				functionName = "SUBTRACT"
			} else if infixExpr.Operator == "*" {
				functionName = "MULTIPLY"
			} else if infixExpr.Operator == "/" {
				functionName = "DIVIDE"
			} else if infixExpr.Operator == "%" {
				functionName = "MODULO"
			} else if infixExpr.Operator == "||" {
				functionName = "CONCAT"
			} else {
				functionName = "EXPRESSION"
			}

			// Add the expression as a special scalar function with a default column name
			// Use strings.Builder instead of fmt.Sprintf for better performance
			var sb strings.Builder
			sb.WriteString("column")
			sb.WriteString(strconv.Itoa(colIndex + 1))
			colName := sb.String()

			functionInfo := &ScalarFunctionInfo{
				Name: functionName,
				Arguments: []parser.Expression{
					infixExpr.Left,
					infixExpr.Right,
				},
				ColumnName:  colName,
				ColumnIndex: colIndex,
			}
			allSelectItems = append(allSelectItems, functionInfo)

		default:
			return nil, fmt.Errorf("unsupported expression in SELECT with scalar functions: %T", colExpr)
		}
	}

	// Remove duplicates from required columns and always include id column for WHERE clauses
	uniqueColumns := make([]string, 0, len(requiredColumns)+1)
	seen := make(map[string]bool)

	// Add id column first if we have a WHERE clause
	if stmt.Where != nil {
		// Use strings.Builder instead of fmt.Sprintf for better performance
		var sb strings.Builder
		sb.WriteString(stmt.Where.String())
		whereStr := sb.String()

		// Check if WHERE clause contains id reference
		if strings.Contains(whereStr, "id") {
			uniqueColumns = append(uniqueColumns, "id")
			seen["id"] = true
		}
	}

	// Add all other required columns
	for _, col := range requiredColumns {
		if !seen[col] {
			seen[col] = true
			uniqueColumns = append(uniqueColumns, col)
		}
	}

	// Execute the base query - unnecessary to check if there are rows
	// as subsequent processing handles empty results correctly
	baseResult, err := tx.Select(tableName, uniqueColumns, nil)
	if err != nil {
		return nil, err
	}

	// Filter by WHERE if needed
	if stmt.Where != nil {
		// Create an evaluator
		evaluator := NewEvaluator(ctx, e.functionRegistry)
		evaluator.WithColumnAliases(columnAliases)

		// For filter condition, we'll pass nil to get all rows and filter with our evaluator
		baseResult = &FilteredResult{
			result:    baseResult,
			whereExpr: stmt.Where,
			evaluator: evaluator,
		}
	}

	// Create a ScalarResult to apply scalar functions with improved handling of v3 storage types
	// This has been updated to handle v3 storage types better
	result := NewScalarResult(baseResult, allSelectItems, columnAliases)

	// Apply ORDER BY, LIMIT, OFFSET if specified
	if stmt.OrderBy != nil || stmt.Limit != nil || stmt.Offset != nil {
		result, err = applyOrderByLimitOffset(ctx, result, stmt)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// extractColumnsFromArguments extracts column names from function arguments
func extractColumnsFromArguments(args []parser.Expression) []string {
	columns := make([]string, 0)

	for _, arg := range args {
		switch expr := arg.(type) {
		case *parser.Identifier:
			columns = append(columns, expr.Value)
		case *parser.QualifiedIdentifier:
			columns = append(columns, expr.Name.Value)
		case *parser.AliasedExpression:
			if ident, ok := expr.Expression.(*parser.Identifier); ok {
				columns = append(columns, ident.Value)
			}
		case *parser.CastExpression:
			// Extract columns from CAST expression
			if innerCols := extractColumnsFromCastExpression(expr); len(innerCols) > 0 {
				columns = append(columns, innerCols...)
			}
		case *parser.FunctionCall:
			// Extract columns from nested function calls
			if innerCols := extractColumnsFromArguments(expr.Arguments); len(innerCols) > 0 {
				columns = append(columns, innerCols...)
			}
		}
		// Other argument types like literals don't need column access
	}

	return columns
}

// extractColumnsFromCastExpression extracts column names from a CAST expression
func extractColumnsFromCastExpression(expr *parser.CastExpression) []string {
	// Extract columns from the expression being cast
	var columns []string

	switch castExpr := expr.Expr.(type) {
	case *parser.Identifier:
		columns = append(columns, castExpr.Value)
	case *parser.QualifiedIdentifier:
		columns = append(columns, castExpr.Name.Value)
	case *parser.FunctionCall:
		// For nested function calls in CAST expressions
		columns = append(columns, extractColumnsFromArguments(castExpr.Arguments)...)
	}

	return columns
}

// Note: We're not implementing these functions here since they're defined elsewhere
// in the codebase. We were getting duplicate definitions.

// isScalarAggregateFunction determines if a function name is an aggregate function
// This is a local version of the function to avoid name conflicts
// isScalarAggregateFunction checks if a function name is an aggregate function
// Uses the global function registry to determine if a function is an aggregate function
func isScalarAggregateFunction(name string) bool {
	// Delegate to the common IsAggregateFunction which uses the registry
	return IsAggregateFunction(name)
}
