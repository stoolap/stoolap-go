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

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// executeSelectWithWindowFunctions executes a SELECT statement with window functions
func (e *Executor) executeSelectWithWindowFunctions(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
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
		return nil, fmt.Errorf("complex table expressions not supported yet with window functions")
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Extract required columns and window functions
	requiredColumns := make([]string, 0)
	windowFunctions := make([]*WindowFunctionInfo, 0)
	scalarFunctions := make([]*ScalarFunctionInfo, 0)
	columnAliases := make(map[string]string)

	// Extract partitioning and ordering columns
	// These would typically come from OVER clauses in a complete implementation
	partitionColumns := make([]string, 0)
	orderByColumns := make([]string, 0)

	// Analyze each column expression
	for i, colExpr := range stmt.Columns {
		switch expr := colExpr.(type) {
		case *parser.Identifier:
			// Simple column reference
			if expr.Value != "*" {
				requiredColumns = append(requiredColumns, expr.Value)
			} else {
				// For wildcard, we'll get all columns from the schema
				schema, err := e.engine.GetTableSchema(tableName)
				if err != nil {
					return nil, err
				}
				for _, col := range schema.Columns {
					requiredColumns = append(requiredColumns, col.Name)
				}
			}

		case *parser.FunctionCall:
			// Check if it's a window function
			if e.functionRegistry.IsWindowFunction(expr.Function) {
				// Process window function arguments
				argColumns := extractColumnsFromArguments(expr.Arguments)
				requiredColumns = append(requiredColumns, argColumns...)

				// Create window function info
				windowInfo := extractWindowFunction(expr, i)
				windowFunctions = append(windowFunctions, windowInfo)
			} else {
				// For scalar or aggregate functions, use a different execution path
				if e.functionRegistry.IsAggregateFunction(expr.Function) {
					return e.executeSelectWithAggregation(ctx, tx, stmt)
				} else {
					// Process function arguments
					argColumns := extractColumnsFromArguments(expr.Arguments)
					requiredColumns = append(requiredColumns, argColumns...)

					// Use strings.Builder instead of fmt.Sprintf for better performance
					var sb strings.Builder
					sb.WriteString(strings.ToLower(expr.Function))
					sb.WriteString("_result")
					funcColName := sb.String()

					// Add the function to our list for scalar execution
					scalarFunctions = append(scalarFunctions, &ScalarFunctionInfo{
						Name:        expr.Function,
						Arguments:   expr.Arguments,
						ColumnName:  funcColName,
						ColumnIndex: len(requiredColumns) + len(scalarFunctions) - 1,
					})
				}
			}

		case *parser.AliasedExpression:
			// Aliased expression (could be a window function with an alias)
			switch innerExpr := expr.Expression.(type) {
			case *parser.Identifier:
				// Aliased column
				requiredColumns = append(requiredColumns, innerExpr.Value)
				columnAliases[expr.Alias.Value] = innerExpr.Value

			case *parser.FunctionCall:
				// Check if it's a window function
				if e.functionRegistry.IsWindowFunction(innerExpr.Function) {
					// Process window function arguments
					argColumns := extractColumnsFromArguments(innerExpr.Arguments)
					requiredColumns = append(requiredColumns, argColumns...)

					// Create window function info with alias
					windowInfo := extractWindowFunction(innerExpr, i)
					windowInfo.ColumnName = expr.Alias.Value // Use the alias
					windowFunctions = append(windowFunctions, windowInfo)
				} else {
					// For other function types, use a different execution path
					if e.functionRegistry.IsAggregateFunction(innerExpr.Function) {
						return e.executeSelectWithAggregation(ctx, tx, stmt)
					} else {
						// Handle scalar function
						argColumns := extractColumnsFromArguments(innerExpr.Arguments)
						requiredColumns = append(requiredColumns, argColumns...)

						// Add the function to scalar functions list
						scalarFunctions = append(scalarFunctions, &ScalarFunctionInfo{
							Name:        innerExpr.Function,
							Arguments:   innerExpr.Arguments,
							ColumnName:  expr.Alias.Value,
							ColumnIndex: len(requiredColumns) + len(scalarFunctions) - 1,
						})
					}
				}
			}

		default:
			return nil, fmt.Errorf("unsupported expression in SELECT with window functions: %T", colExpr)
		}
	}

	// Extract PARTITION BY and ORDER BY from window definitions
	// In a complete implementation, these would come from OVER clauses

	// If there are any scalar functions but no window functions, use the scalar execution path
	if len(windowFunctions) == 0 && len(scalarFunctions) > 0 {
		return e.executeSelectWithScalarFunctions(ctx, tx, stmt)
	}

	// If no window functions were found, fall back to a basic query
	if len(windowFunctions) == 0 {
		// Extract table name and perform a basic query to avoid recursion
		var tableName string
		switch tableExpr := stmt.TableExpr.(type) {
		case *parser.Identifier:
			tableName = tableExpr.Value
		case *parser.SimpleTableSource:
			tableName = tableExpr.Name.Value
		default:
			return nil, fmt.Errorf("unsupported table expression type")
		}

		// Execute a simple SELECT query
		return tx.Select(tableName, []string{"*"}, nil)
	}

	// Remove duplicates from required columns
	uniqueColumns := make([]string, 0, len(requiredColumns))
	seen := make(map[string]bool)
	for _, col := range requiredColumns {
		if !seen[col] {
			seen[col] = true
			uniqueColumns = append(uniqueColumns, col)
		}
	}

	// Execute the base query
	baseResult, err := tx.Select(tableName, uniqueColumns, nil)
	if err != nil {
		return nil, err
	}

	// Filter by WHERE if needed
	if stmt.Where != nil {
		// Create an evaluator
		evaluator := NewEvaluator(ctx, e.functionRegistry)
		evaluator.WithColumnAliases(columnAliases)

		// Apply the WHERE filter
		baseResult = &FilteredResult{
			result:    baseResult,
			whereExpr: stmt.Where,
			evaluator: evaluator,
		}
	}

	// Create a WindowResult for window function processing
	result := NewWindowResult(baseResult, windowFunctions, partitionColumns, orderByColumns)

	// Apply ORDER BY, LIMIT, OFFSET if specified
	if stmt.OrderBy != nil || stmt.Limit != nil || stmt.Offset != nil {
		result, err = applyOrderByLimitOffset(ctx, result, stmt)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
