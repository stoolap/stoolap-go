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
	"reflect"
	"strings"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
)

// GetGlobalFunctionRegistry returns the global function registry
func GetGlobalFunctionRegistry() contract.FunctionRegistry {
	return registry.GetGlobal()
}

// executeCountStar is a special case handler for COUNT(*) queries
func (e *Executor) executeCountStar(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
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
		// For complex expressions like JOINs, use the regular path
		return e.executeSelectWithAggregation(ctx, tx, stmt)
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Get the schema to determine column count
	schema, err := e.engine.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	// FAST PATH: If there's no WHERE clause, we can directly ask the table for its row count
	// This avoids scanning all rows and is much faster
	if stmt.Where == nil {
		// Get the table from the transaction
		table, tableErr := tx.GetTable(tableName)
		if tableErr == nil {
			// Try to use the RowCount method if the table implements it
			if countable, ok := table.(interface{ RowCount() int }); ok {
				count := countable.RowCount()

				// The column name must be "COUNT(*)" because that's what's in the SQL query
				columnName := "COUNT(*)"

				// For aliased expressions like "COUNT(*) AS total", use the alias
				if len(stmt.Columns) == 1 {
					if aliased, ok := stmt.Columns[0].(*parser.AliasedExpression); ok {
						columnName = aliased.Alias.Value
					}
				}

				return &ExecResult{
					columns:      []string{columnName},
					rows:         [][]interface{}{{int64(count)}},
					isMemory:     true,
					rowsAffected: 0,
					lastInsertID: 0,
				}, nil
			}
		}
	}

	// SLOWER PATH: If there's a WHERE clause or the table doesn't support direct count

	// For COUNT(*), we need to select the minimum number of columns required for WHERE evaluation
	var columnsToSelect []string

	if stmt.Where == nil {
		// If there's no WHERE clause, we only need one column to count rows
		columnsToSelect = []string{schema.Columns[0].Name}
	} else {
		// If there's a WHERE clause, extract the columns it references
		whereColumns := getColumnsFromWhereClause(stmt.Where)

		// If we couldn't extract columns or the WHERE clause is complex, fall back to selecting all columns
		if len(whereColumns) == 0 {
			columnsToSelect = make([]string, len(schema.Columns))
			for i, col := range schema.Columns {
				columnsToSelect[i] = col.Name
			}
		} else {
			// Only select the columns needed for the WHERE clause
			columnsToSelect = whereColumns
		}
	}

	// Convert SQL WHERE to a storage condition if possible
	var whereExpr storage.Expression
	if stmt.Where != nil {
		whereExpr = createWhereExpression(ctx, stmt.Where, e.functionRegistry)
		if whereExpr == nil {
			// Handle simple column comparisons
			if infix, ok := stmt.Where.(*parser.InfixExpression); ok {
				if isSimpleComparison(ctx, infix) {
					// Regular comparison
					colName, op, val := extractComparisonComponents(ctx, infix)
					whereExpr = expression.NewSimpleExpression(colName, op, val)
				}
			}
		}

		if whereExpr != nil {
			if simpleExpr, ok := whereExpr.(*expression.SimpleExpression); ok {
				// If it's a simple expression, we need to prepare it for the schema
				simpleExpr.PrepareForSchema(schema)
			}
		}
	}

	// Execute the query with the condition pushed down to storage layer
	result, err := tx.SelectWithAliases(tableName, columnsToSelect, whereExpr, nil)
	if err != nil {
		return nil, err
	}

	// For all WHERE clauses, we need to ensure SQL-level filtering is applied
	// This is especially important for BETWEEN conditions, where we can only push down
	// part of the condition to the storage layer
	if stmt.Where != nil {
		// Apply SQL-level filtering to handle expressions that can't be pushed down to storage
		// The SQL-level filter will reevaluate the WHERE clause on each row
		evaluator := NewEvaluator(ctx, e.functionRegistry)
		result = &FilteredResult{
			result:       result,
			whereExpr:    stmt.Where,
			evaluator:    evaluator,
			currentRow:   nil,
			currentValid: false,
			closed:       false,
		}
	}

	// Count the rows
	var count int64
	for result.Next() {
		count++
	}

	// Don't forget to close the result
	result.Close()

	// The column name must be "COUNT(*)" because that's what's in the SQL query
	columnName := "COUNT(*)"

	// For aliased expressions like "COUNT(*) AS total", use the alias
	if len(stmt.Columns) == 1 {
		if aliased, ok := stmt.Columns[0].(*parser.AliasedExpression); ok {
			columnName = aliased.Alias.Value
		}
	}

	return &ExecResult{
		columns:      []string{columnName},
		rows:         [][]interface{}{{count}},
		isMemory:     true,
		rowsAffected: 0,
		lastInsertID: 0,
	}, nil
}

// executeSelectWithContext executes a SELECT statement
// This is the main entry point for SELECT query execution and contains
// the decision logic for when to use vectorized execution
func (e *Executor) executeSelectWithContext(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
	// Check if we should use vectorized execution for this query
	// This calls into the decision-making logic in executor.go
	useVectorized := e.shouldUseVectorizedExecution(stmt)

	// Special case for COUNT(*) queries
	// These have a specialized execution path for optimization
	if len(stmt.Columns) == 1 {
		if funcCall, ok := stmt.Columns[0].(*parser.FunctionCall); ok {
			if strings.ToUpper(funcCall.Function) == "COUNT" && len(funcCall.Arguments) == 1 {
				if ident, ok := funcCall.Arguments[0].(*parser.Identifier); ok && ident.Value == "*" {
					// COUNT(*) queries use a specialized execution path
					return e.executeCountStar(ctx, tx, stmt)
				}
			}
		}
		// Also handle aliased COUNT(*) expressions (COUNT(*) AS total)
		if aliased, ok := stmt.Columns[0].(*parser.AliasedExpression); ok {
			if funcCall, ok := aliased.Expression.(*parser.FunctionCall); ok {
				if strings.ToUpper(funcCall.Function) == "COUNT" && len(funcCall.Arguments) == 1 {
					if ident, ok := funcCall.Arguments[0].(*parser.Identifier); ok && ident.Value == "*" {
						return e.executeCountStar(ctx, tx, stmt)
					}
				}
			}
		}
	}

	// DECISION POINT 1: Use vectorized execution for suitable queries
	// If vectorized mode is enabled and the query is suitable for vectorized execution,
	// delegate to the vectorized engine
	if e.vectorizedMode && useVectorized {
		// This is the integration point with the vectorized engine in executor.go
		// The executeWithVectorizedEngine method will:
		// 1. Create a vectorized.Engine instance
		// 2. Delegate execution to the engine
		// 3. Return a vectorized.VectorizedResult that implements storage.Result
		return e.executeWithVectorizedEngine(ctx, tx, stmt)
	}

	// Handle special case for queries without a table (like "SELECT 1" or "SELECT 1 + 1")
	if stmt.TableExpr == nil {
		// Create a result with just the literal values
		columns := make([]string, len(stmt.Columns))
		values := make([][]interface{}, 1) // One row
		values[0] = make([]interface{}, len(stmt.Columns))

		// Create an evaluator for expressions
		evaluator := NewEvaluator(ctx, e.functionRegistry)

		for i, col := range stmt.Columns {
			// For aliased expressions, use the alias as column name
			if aliased, ok := col.(*parser.AliasedExpression); ok {
				columns[i] = aliased.Alias.Value
				// Evaluate the expression
				result, err := evaluator.Evaluate(aliased.Expression)
				defer storage.PutPooledColumnValue(result)
				if err != nil {
					return nil, fmt.Errorf("error evaluating expression: %v", err)
				}
				values[0][i] = result.AsInterface()
			} else {
				// For non-aliased expressions, generate more meaningful column names
				switch expr := col.(type) {
				case *parser.IntegerLiteral, *parser.FloatLiteral, *parser.StringLiteral,
					*parser.BooleanLiteral, *parser.NullLiteral:
					// For simple literals, use default column name
					columns[i] = fmt.Sprintf("column%d", i+1)
				case *parser.CastExpression:
					// For CAST expressions, use a more descriptive column name
					columns[i] = fmt.Sprintf("CAST_%s", strings.ToLower(expr.TypeName))
				case *parser.FunctionCall:
					// For function calls, use function name as column
					columns[i] = fmt.Sprintf("%s_result", strings.ToLower(expr.Function))
				default:
					// For other expressions, use default column name
					columns[i] = fmt.Sprintf("column%d", i+1)
				}

				// Extract the value based on the expression type
				switch expr := col.(type) {
				case *parser.IntegerLiteral:
					values[0][i] = expr.Value
				case *parser.FloatLiteral:
					values[0][i] = expr.Value
				case *parser.StringLiteral:
					values[0][i] = expr.Value
				case *parser.BooleanLiteral:
					values[0][i] = expr.Value
				case *parser.NullLiteral:
					values[0][i] = nil
				case *parser.CastExpression:
					// Handle CAST expressions directly using the evaluator
					result, err := evaluator.Evaluate(expr)
					defer storage.PutPooledColumnValue(result)
					if err == nil {
						values[0][i] = result.AsInterface()
					} else {
						// If there's an error, return a meaningful error message
						return nil, fmt.Errorf("error evaluating CAST expression: %v", err)
					}
				default:
					// Try to evaluate the expression
					result, err := evaluator.Evaluate(expr)
					if err != nil {
						return nil, fmt.Errorf("error evaluating expression %s: %v", expr.String(), err)
					}
					defer storage.PutPooledColumnValue(result)
					values[0][i] = result.AsInterface()
				}
			}
		}

		// Create a memory result with the values
		return &ExecResult{
			columns:  columns,
			rows:     values,
			isMemory: true,
		}, nil
	}

	// Check if the query has aggregations
	hasAggregation := false
	for _, col := range stmt.Columns {
		if containsAggregateFunction(col) {
			hasAggregation = true
			break
		}
	}

	// Check if the query has joins (currently not supported in the schema)
	hasJoins := false // Since we don't have a Joins field in SelectStatement

	// Check if the query has scalar functions
	hasScalarFunctions := false
	for _, col := range stmt.Columns {
		if containsScalarFunction(col) {
			hasScalarFunctions = true
			break
		}
	}

	// Check if the query has window functions
	hasWindowFunctions := false
	for _, col := range stmt.Columns {
		if containsWindowFunction(col) {
			hasWindowFunctions = true
			break
		}
	}

	// Special case for handling aggregations
	if hasAggregation {
		return e.executeSelectWithAggregation(ctx, tx, stmt)
	}

	// Special case for handling joins
	if hasJoins {
		return e.executeSelectWithJoins(ctx, tx, stmt)
	}

	// Special case for handling window functions
	if hasWindowFunctions {
		return e.executeSelectWithWindowFunctions(ctx, tx, stmt)
	}

	// DECISION POINT 2: Optimize scalar functions with vectorized execution
	// Special case for handling scalar functions - use vectorized if appropriate
	if hasScalarFunctions && useVectorized {
		// Scalar functions (ABS, ROUND, CEILING, etc.) benefit greatly from SIMD operations
		// The vectorized engine applies these functions using optimized implementations
		// from simd.go that use loop unrolling and CPU auto-vectorization
		return e.executeWithVectorizedEngine(ctx, tx, stmt)
	} else if hasScalarFunctions {
		// Fallback path for scalar functions when vectorized execution is not available
		return e.executeSelectWithScalarFunctions(ctx, tx, stmt)
	}

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
		// For other types of table expressions, we'll need to handle JOIN
		return e.executeSelectWithJoins(ctx, tx, stmt)
	}

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Extract column references
	var columns []string

	schema, err := e.engine.GetTableSchema(tableName)
	if err != nil {
		return nil, err
	}

	if len(stmt.Columns) == 1 && isAsterisk(stmt.Columns[0]) {
		// For SELECT *, get all columns from the schema
		columns = make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			columns[i] = col.Name
		}
	} else {
		// Otherwise, extract column names from the query
		columns = make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			// Handle column references
			if colRef, ok := col.(*parser.Identifier); ok {
				// Normalize column name to lowercase
				columns[i] = strings.ToLower(colRef.Value)
			} else if aliased, ok := col.(*parser.AliasedExpression); ok {
				// For aliased expressions, use the alias name
				columns[i] = aliased.Alias.Value
			} else {
				// For other expressions, generate a column name
				columns[i] = fmt.Sprintf("column%d", i+1)
			}
		}
	}

	// Execute the query based on the expression type
	var result storage.Result

	// Extract column aliases from the SELECT clause
	columnAliases := ExtractColumnAliases(stmt.Columns)

	// DECISION POINT 3: Optimize WHERE clause filtering with vectorized execution
	// Determine if we should use vectorized execution for WHERE processing
	if stmt.Where != nil && useVectorized {
		// WHERE clause filtering can be substantially faster with vectorized processing
		// The vectorized engine uses optimized filtering with:
		// 1. Column-oriented data layout for better cache locality
		// 2. Specialized processors that implement comparison operations
		// 3. Boolean selection vectors for efficient filtering
		// 4. Optimized SIMD implementations for comparison operations
		return e.executeWithVectorizedEngine(ctx, tx, stmt)
	} else if stmt.Where != nil {
		// Traditional row-based execution for WHERE clause
		columnsToFetch := make([]string, len(columns))
		copy(columnsToFetch, columns)

		// Get columns referenced in the WHERE clause
		whereColumns := getColumnsFromWhereClause(stmt.Where)

		// Add any missing columns needed by the WHERE clause
		for _, whereCol := range whereColumns {
			found := false
			for _, col := range columns {
				if strings.EqualFold(col, whereCol) {
					found = true
					break
				}
			}

			if !found {
				columnsToFetch = append(columnsToFetch, whereCol)
			}
		}

		// Determine if we need additional filtering based on storage capabilities
		needsFiltering := true
		var whereExpr storage.Expression

		if stmt.Where != nil {
			// Convert the WHERE clause to a storage expression
			whereExpr = createWhereExpression(ctx, stmt.Where, e.functionRegistry)

			// Check if the WHERE clause can be pushed down to storage
			if whereExpr != nil {
				whereExpr.PrepareForSchema(schema)
				result, err = tx.SelectWithAliases(tableName, columns, whereExpr, columnAliases)
				if err != nil {
					return nil, err
				}

				needsFiltering = false
			}
		}

		if needsFiltering {
			result, err = tx.SelectWithAliases(tableName, columnsToFetch, nil, columnAliases)
			if err != nil {
				return nil, err
			}

			// Filter the result with our expression
			evaluator := NewEvaluator(ctx, e.functionRegistry)
			// Pass column aliases to the evaluator
			evaluator.WithColumnAliases(columnAliases)

			result = &FilteredResult{
				result:       result,
				whereExpr:    stmt.Where,
				evaluator:    evaluator,
				currentRow:   nil,
				currentValid: false,
				closed:       false,
			}
		}
	} else {
		// No WHERE clause, just fetch the columns directly
		result, err = tx.SelectWithAliases(tableName, columns, nil, columnAliases)
		if err != nil {
			return nil, err
		}
	}

	// Apply DISTINCT if specified - must be after filtering but before ordering
	if stmt.Distinct {
		result = NewDistinctResult(result)
	}

	// Apply ORDER BY, LIMIT, OFFSET if specified
	if stmt.OrderBy != nil || stmt.Limit != nil || stmt.Offset != nil {
		result, err = applyOrderByLimitOffset(ctx, result, stmt)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// isAsterisk checks if an expression is the * wildcard
func isAsterisk(expr parser.Expression) bool {
	if ident, ok := expr.(*parser.Identifier); ok {
		return ident.Value == "*"
	}
	return false
}

// executeSelectWithJoins executes a SELECT query with JOINs
func (e *Executor) executeSelectWithJoins(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
	_ = tx // Unused for now, but could be used for transaction management

	// Create an evaluator for expression evaluation
	evaluator := NewEvaluator(ctx, e.functionRegistry)

	// Build a map of parameters (empty for now, could be populated from query params)
	params := make(map[string]interface{})

	// Execute the JoinQuery using the executor implementation
	return ExecuteJoinQuery(ctx, stmt, e.engine, evaluator, params)
}

// applyOrderByLimitOffset applies ORDER BY, LIMIT, and OFFSET clauses to a result set
func applyOrderByLimitOffset(ctx context.Context, result storage.Result, stmt *parser.SelectStatement) (storage.Result, error) {
	// Apply ORDER BY if specified
	if len(stmt.OrderBy) > 0 {
		result = &OrderedResult{
			baseResult: result,
			orderBy:    stmt.OrderBy,
		}
	}

	var ps *parameter
	if p, ok := ctx.Value(psContextKey).(*parameter); ok {
		ps = p
	}

	// Apply LIMIT and OFFSET if specified
	if stmt.Limit != nil || stmt.Offset != nil {
		var limit, offset int64 = -1, 0
		var err error

		if stmt.Limit != nil {
			if pp, ok := stmt.Limit.(*parser.Parameter); ok {
				// If it's a parameter, we need to resolve it
				if ps != nil {
					nm := ps.GetValue(pp)
					intVal := reflect.ValueOf(nm.Value).Int()
					limit = intVal
				}
			} else if limitLit, ok := stmt.Limit.(*parser.IntegerLiteral); ok {
				limit = limitLit.Value
			} else {
				// For other expressions, we'll convert to string and parse
				limitStr := stmt.Limit.String()
				if strings.HasPrefix(limitStr, "'") && strings.HasSuffix(limitStr, "'") {
					// Remove quotes if present
					limitStr = limitStr[1 : len(limitStr)-1]
				}
				limit, err = parseLimit(limitStr)
				if err != nil {
					return nil, fmt.Errorf("invalid LIMIT value: %s", err)
				}
			}
		}

		if stmt.Offset != nil {
			if pp, ok := stmt.Offset.(*parser.Parameter); ok {
				// If it's a parameter, we need to resolve it
				if ps != nil {
					nm := ps.GetValue(pp)
					intVal := reflect.ValueOf(nm.Value).Int()
					offset = intVal
				}
			} else if offsetLit, ok := stmt.Offset.(*parser.IntegerLiteral); ok {
				offset = offsetLit.Value
			} else {
				// For other expressions, we'll convert to string and parse
				offsetStr := stmt.Offset.String()
				if strings.HasPrefix(offsetStr, "'") && strings.HasSuffix(offsetStr, "'") {
					// Remove quotes if present
					offsetStr = offsetStr[1 : len(offsetStr)-1]
				}
				offset, err = parseLimit(offsetStr)
				if err != nil {
					return nil, fmt.Errorf("invalid OFFSET value: %s", err)
				}
			}
		}

		result = &LimitedResult{
			baseResult: result,
			limit:      limit,
			offset:     offset,
		}
	}

	return result, nil
}

// containsAggregateFunction checks if an expression contains an aggregate function
func containsAggregateFunction(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *parser.FunctionCall:
		// Check if the function name is an aggregate function
		fname := strings.ToUpper(e.Function)
		isAgg := IsAggregateFunction(fname)
		return isAgg

	case *parser.InfixExpression:
		// Check both sides
		return containsAggregateFunction(e.Left) || containsAggregateFunction(e.Right)

	case *parser.PrefixExpression:
		// Check the operand
		return containsAggregateFunction(e.Right)

	case *parser.AliasedExpression:
		// Check the expression
		return containsAggregateFunction(e.Expression)

	default:
		// Other expressions don't contain aggregate functions
		return false
	}
}

// IsAggregateFunction checks if a function name is an aggregate function
// Uses the global function registry to determine if a function is an aggregate function
func IsAggregateFunction(name string) bool {
	registry := GetGlobalFunctionRegistry()

	return registry.IsAggregateFunction(name)
}

// containsScalarFunction checks if an expression contains a scalar function
func containsScalarFunction(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *parser.FunctionCall:
		// Check if the function name is NOT an aggregate function
		fname := strings.ToUpper(e.Function)
		isScalar := !IsAggregateFunction(fname)
		return isScalar

	case *parser.CastExpression:
		// CAST is a scalar function
		return true

	case *parser.InfixExpression:
		// Arithmetic operations are scalar functions
		if e.Operator == "+" || e.Operator == "-" || e.Operator == "*" || e.Operator == "/" || e.Operator == "%" || e.Operator == "||" {
			return true
		}

		// Check both sides
		leftContains := containsScalarFunction(e.Left)
		rightContains := containsScalarFunction(e.Right)
		return leftContains || rightContains

	case *parser.PrefixExpression:
		// Check the operand
		return containsScalarFunction(e.Right)

	case *parser.AliasedExpression:
		// Check the expression
		isScalar := containsScalarFunction(e.Expression)
		return isScalar

	default:
		// Other expressions don't contain scalar functions
		return false
	}
}

// parseLimit parses a limit/offset value from a string
func parseLimit(limitStr string) (int64, error) {
	// Try to parse as an integer
	var limit int64
	var err error
	limit, err = parseInt(limitStr)
	if err != nil {
		return 0, err
	}
	if limit < 0 {
		return 0, fmt.Errorf("limit/offset value must be non-negative")
	}
	return limit, nil
}

// parseInt parses an integer value from a string
func parseInt(str string) (int64, error) {
	// Try to parse as an integer
	var val int64
	var err error
	val, err = parseInt64(str)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// parseInt64 parses an int64 value from a string
func parseInt64(str string) (int64, error) {
	// Remove any surrounding quotes
	str = strings.Trim(str, "'\"")

	// Try to parse as an integer
	var val int64
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid integer value: %s", str)
		}
		val = val*10 + int64(c-'0')
	}

	return val, nil
}

// isSimpleComparison checks if an expression is a simple binary comparison
// that can be directly converted to a storage.Expression
func isSimpleComparison(ctx context.Context, expr *parser.InfixExpression) bool {
	// Handle operators we can convert
	switch expr.Operator {
	case "=", "==", "!=", "<>", ">", ">=", "<", "<=":
		return isColumnAndLiteral(ctx, expr)
	default:
		return false
	}
}

// extractComparisonComponents extracts the column name, operator, and value from a comparison
func extractComparisonComponents(ctx context.Context, expr *parser.InfixExpression) (string, storage.Operator, interface{}) {
	// Get column name and value using existing helpers
	colName, val := extractColumnAndValue(ctx, expr)

	// Convert operator to storage.Operator
	var op storage.Operator
	switch expr.Operator {
	case "=", "==":
		op = storage.EQ
	case "!=", "<>":
		op = storage.NE
	case ">":
		op = storage.GT
	case ">=":
		op = storage.GTE
	case "<":
		op = storage.LT
	case "<=":
		op = storage.LTE
	}

	return colName, op, val
}

// getColumnsFromWhereClause extracts column names referenced in a WHERE clause
func getColumnsFromWhereClause(whereExpr parser.Expression) []string {
	if whereExpr == nil {
		return nil
	}

	columns := make(map[string]bool)

	switch expr := whereExpr.(type) {
	case *parser.Identifier:
		// Simple column reference
		columns[expr.Value] = true

	case *parser.QualifiedIdentifier:
		// Table.column reference
		columns[expr.Name.Value] = true

	case *parser.CastExpression:
		// Extract columns from CAST expression
		if ident, ok := expr.Expr.(*parser.Identifier); ok {
			columns[ident.Value] = true
		} else {
			// For more complex expressions inside CAST, recursively extract columns
			for _, col := range getColumnsFromWhereClause(expr.Expr) {
				columns[col] = true
			}
		}

	case *parser.BetweenExpression:
		// Extract columns from BETWEEN expressions
		// The column is the expression being evaluated
		if expr.Expr != nil {
			for _, col := range getColumnsFromWhereClause(expr.Expr) {
				columns[col] = true
			}
		}
		// Also check the range bounds
		if expr.Lower != nil {
			for _, col := range getColumnsFromWhereClause(expr.Lower) {
				columns[col] = true
			}
		}
		if expr.Upper != nil {
			for _, col := range getColumnsFromWhereClause(expr.Upper) {
				columns[col] = true
			}
		}

	case *parser.InfixExpression:
		// Check both sides of the expression
		if expr.Left != nil {
			for _, col := range getColumnsFromWhereClause(expr.Left) {
				columns[col] = true
			}
		}
		if expr.Right != nil {
			for _, col := range getColumnsFromWhereClause(expr.Right) {
				columns[col] = true
			}
		}

	case *parser.PrefixExpression:
		// Check the operand
		if expr.Right != nil {
			for _, col := range getColumnsFromWhereClause(expr.Right) {
				columns[col] = true
			}
		}

	case *parser.FunctionCall:
		// Check function arguments
		for _, arg := range expr.Arguments {
			for _, col := range getColumnsFromWhereClause(arg) {
				columns[col] = true
			}
		}
	}

	// Convert the map to a slice
	result := make([]string, 0, len(columns))
	for col := range columns {
		result = append(result, col)
	}

	return result
}
