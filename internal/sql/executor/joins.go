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
	"database/sql"
	"fmt"
	"strings"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// ExecuteJoin executes a JOIN operation between two table sources
func (e *Executor) ExecuteJoin(ctx context.Context, joinSource *parser.JoinTableSource,
	evaluator *Evaluator, params map[string]interface{}) (storage.Result, error) {
	return e.ExecuteJoinWithRegistry(ctx, joinSource, evaluator, params)
}

// ExecuteJoinWithRegistry executes a JOIN operation with CTE registry
func (e *Executor) ExecuteJoinWithRegistry(ctx context.Context, joinSource *parser.JoinTableSource,
	evaluator *Evaluator, params map[string]interface{}) (storage.Result, error) {

	// Execute the left side of the join
	leftSource := joinSource.Left
	if leftSource == nil {
		return nil, fmt.Errorf("left side of JOIN is not a valid table source")
	}
	leftResult, _, err := e.executeTableSourceWithRegistry(ctx, leftSource, evaluator, params)
	if err != nil {
		return nil, fmt.Errorf("error executing left side of JOIN: %w", err)
	}

	// Execute the right side of the join
	rightSource := joinSource.Right
	if rightSource == nil {
		return nil, fmt.Errorf("right side of JOIN is not a valid table source")
	}
	rightResult, _, err := e.executeTableSourceWithRegistry(ctx, rightSource, evaluator, params)
	if err != nil {
		leftResult.Close()
		return nil, fmt.Errorf("error executing right side of JOIN: %w", err)
	}

	// Determine the join type
	joinType := strings.ToUpper(joinSource.JoinType)
	if joinType == "" {
		joinType = "INNER" // Default to INNER JOIN
	}

	// Validate join type
	switch joinType {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS":
		// Valid join types
	default:
		leftResult.Close()
		rightResult.Close()
		return nil, fmt.Errorf("unsupported join type: %s", joinType)
	}

	// For CROSS JOIN, use nil condition to match all rows
	var joinCond parser.Expression
	if joinType != "CROSS" {
		joinCond = joinSource.Condition

		// Ensure there is a join condition for non-CROSS joins
		if joinCond == nil {
			leftResult.Close()
			rightResult.Close()
			return nil, fmt.Errorf("%s JOIN requires a join condition", joinType)
		}
	}

	// Convert joinType string to parser.JoinType
	var parserJoinType parser.JoinType
	switch joinType {
	case "INNER":
		parserJoinType = parser.InnerJoin
	case "LEFT":
		parserJoinType = parser.LeftJoin
	case "RIGHT":
		parserJoinType = parser.RightJoin
	case "FULL":
		parserJoinType = parser.FullJoin
	case "CROSS":
		parserJoinType = parser.CrossJoin
	default:
		leftResult.Close()
		rightResult.Close()
		return nil, fmt.Errorf("unsupported join type: %s", joinType)
	}

	// Use efficient hash join
	hashJoinResult, err := NewHashJoinResult(leftResult, rightResult, parserJoinType, joinCond, evaluator)
	if err != nil {
		return nil, fmt.Errorf("failed to create join: %w", err)
	}

	return hashJoinResult, nil
}

// executeTableSourceWithRegistry executes a table source with CTE registry
func (e *Executor) executeTableSourceWithRegistry(ctx context.Context, tableSource parser.TableSource,
	evaluator *Evaluator, params map[string]interface{}) (storage.Result, string, error) {

	switch source := tableSource.(type) {
	case *parser.SimpleTableSource:
		// Simple table source (table name)
		tableName := source.Name.Value

		// Check if this is a CTE reference
		if cteResult, isCTE := e.resolveCTETable(ctx, nil, tableName); isCTE {
			// Use the table alias if provided, otherwise use the CTE name
			tableAlias := tableName
			if source.Alias != nil {
				tableAlias = source.Alias.Value
			}

			return cteResult, tableAlias, nil
		}

		// Not a CTE, proceed with regular table
		// Start a transaction to get the table
		tx, err := e.engine.BeginTx(ctx, sql.LevelReadCommitted)
		if err != nil {
			return nil, "", fmt.Errorf("error beginning transaction: %w", err)
		}
		defer tx.Rollback() // Rollback if not committed

		table, err := tx.GetTable(tableName)
		if err != nil {
			return nil, "", fmt.Errorf("error getting table %s: %w", tableName, err)
		}

		// Get columns to scan - we need to qualify them with table alias for joins
		columns := make([]string, 0)
		for _, col := range table.Schema().Columns {
			columns = append(columns, col.Name)
		}

		// Create a result from the table
		result, err := tx.Select(tableName, columns, nil)
		if err != nil {
			return nil, "", fmt.Errorf("error creating scanner for table %s: %w", tableName, err)
		}

		// Use the table alias if provided, otherwise use the table name
		tableAlias := tableName
		if source.Alias != nil {
			tableAlias = source.Alias.Value
		}

		// Wrap the result to add table qualification to column names
		qualifiedResult := NewQualifiedResult(result, tableAlias)

		return qualifiedResult, tableAlias, nil

	case *parser.SubqueryTableSource:
		// Subquery table source
		// We need to handle subqueries differently since we don't have a full ExecuteSelect implementation
		// For now, just return an error indicating this is not implemented yet
		return nil, "", fmt.Errorf("subqueries in JOIN are not fully implemented yet")

	case *parser.JoinTableSource:
		// Nested JOIN
		joinResult, err := e.ExecuteJoinWithRegistry(ctx, source, evaluator, params)
		if err != nil {
			return nil, "", fmt.Errorf("error executing nested JOIN: %w", err)
		}

		// No obvious alias for a nested join, use empty string
		return joinResult, "", nil

	case *parser.CTEReference:
		// Common Table Expression reference
		// This would need to be implemented to support WITH clauses
		return nil, "", fmt.Errorf("CTE references in JOINs are not yet supported")

	default:
		return nil, "", fmt.Errorf("unsupported table source type: %T", tableSource)
	}
}

// ExecuteJoinQuery executes a SELECT query that contains JOINs
func (e *Executor) ExecuteJoinQuery(ctx context.Context, stmt *parser.SelectStatement,
	evaluator *Evaluator, params map[string]interface{}) (storage.Result, error) {
	return e.ExecuteJoinQueryWithRegistry(ctx, stmt, evaluator, params)
}

// ExecuteJoinQueryWithRegistry executes a SELECT query with JOINs and CTE registry
func (e *Executor) ExecuteJoinQueryWithRegistry(ctx context.Context, stmt *parser.SelectStatement,
	evaluator *Evaluator, params map[string]interface{}) (storage.Result, error) {

	// Get the joined result
	joinSource, ok := stmt.TableExpr.(*parser.JoinTableSource)
	if !ok {
		return nil, fmt.Errorf("FROM clause is not a JOIN")
	}

	joinResult, err := e.ExecuteJoinWithRegistry(ctx, joinSource, evaluator, params)
	if err != nil {
		return nil, err
	}

	// Apply WHERE clause if present
	if stmt.Where != nil {
		joinResult = &FilteredResult{
			result:    joinResult,
			whereExpr: stmt.Where,
			evaluator: evaluator,
		}
	}

	// Apply column projection based on SELECT clause
	if len(stmt.Columns) > 0 && !isAllColumns(stmt.Columns) {
		joinResult, err = applyColumnProjection(joinResult, stmt.Columns)
		if err != nil {
			joinResult.Close()
			return nil, err
		}
	}

	// Apply ORDER BY, LIMIT, OFFSET if specified
	if stmt.OrderBy != nil || stmt.Limit != nil || stmt.Offset != nil {
		// Extract values needed for ORDER BY, LIMIT, OFFSET
		var limit, offset int64 = -1, 0

		// Extract LIMIT value
		if stmt.Limit != nil {
			if limitLit, ok := stmt.Limit.(*parser.IntegerLiteral); ok {
				limit = limitLit.Value
			} else {
				// For more complex expressions, default to -1 (no limit)
				limit = -1
			}
		}

		// Extract OFFSET value
		if stmt.Offset != nil {
			if offsetLit, ok := stmt.Offset.(*parser.IntegerLiteral); ok {
				offset = offsetLit.Value
			} else {
				// For more complex expressions, default to 0 (no offset)
				offset = 0
			}
		}

		// Apply ORDER BY if specified
		if len(stmt.OrderBy) > 0 {
			joinResult = &OrderedResult{
				baseResult: joinResult,
				orderBy:    stmt.OrderBy,
			}
		}

		// Apply LIMIT and OFFSET if specified
		if limit >= 0 || offset > 0 {
			joinResult = &LimitedResult{
				baseResult: joinResult,
				limit:      limit,
				offset:     offset,
			}
		}
	}

	return joinResult, nil
}

// isAllColumns checks if the columns clause contains just "*"
func isAllColumns(columns []parser.Expression) bool {
	if len(columns) != 1 {
		return false
	}

	if ident, ok := columns[0].(*parser.Identifier); ok && ident.Value == "*" {
		return true
	}

	return false
}

// applyColumnProjection applies a column projection to a result
func applyColumnProjection(result storage.Result, columns []parser.Expression) (storage.Result, error) {
	// Build the list of projected column names
	projectedColumns := make([]string, len(columns))

	for i, col := range columns {
		switch expr := col.(type) {
		case *parser.Identifier:
			projectedColumns[i] = expr.Value
		case *parser.QualifiedIdentifier:
			// Use just the column name without the table qualifier for the result
			projectedColumns[i] = expr.Name.Value
		case *parser.AliasedExpression:
			projectedColumns[i] = expr.Alias.Value
		default:
			// For other expressions, generate a column name
			projectedColumns[i] = fmt.Sprintf("column%d", i+1)
		}
	}

	// Use the optimized ArrayProjectedResult
	return NewArrayProjectedResult(result, projectedColumns, columns, GetGlobalFunctionRegistry()), nil
}
