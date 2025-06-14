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

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// executeSubquery executes a subquery and returns its results as a slice of values
// This is used for IN (subquery) expressions
func (e *Executor) executeSubquery(ctx context.Context, tx storage.Transaction, subquery *parser.ScalarSubquery) ([]interface{}, error) {
	// Execute the subquery's SELECT statement
	result, err := e.executeSelectWithContext(ctx, tx, subquery.Subquery)
	if err != nil {
		return nil, fmt.Errorf("error executing subquery: %w", err)
	}
	defer result.Close()

	// Collect all values from the first column of the result
	values := make([]interface{}, 0)
	for result.Next() {
		row := result.Row()
		if len(row) > 0 {
			values = append(values, row[0].AsInterface())
		}
	}

	return values, nil
}

// executeScalarSubquery executes a subquery that should return a single value
// This is used for scalar subqueries in WHERE clauses like WHERE col = (SELECT ...)
func (e *Executor) executeScalarSubquery(ctx context.Context, tx storage.Transaction, subquery *parser.ScalarSubquery) (interface{}, error) {
	// Execute the subquery's SELECT statement
	result, err := e.executeSelectWithContext(ctx, tx, subquery.Subquery)
	if err != nil {
		return nil, fmt.Errorf("error executing scalar subquery: %w", err)
	}
	defer result.Close()

	// Get the first value from the first row
	if !result.Next() {
		// No rows returned, return NULL
		return nil, nil
	}

	row := result.Row()
	if len(row) == 0 {
		return nil, nil
	}

	// Check if there are more rows (scalar subquery should return only one row)
	if result.Next() {
		return nil, fmt.Errorf("scalar subquery returned more than one row")
	}

	return row[0].AsInterface(), nil
}

// executeExistsSubquery executes an EXISTS subquery and returns true if any rows exist
func (e *Executor) executeExistsSubquery(ctx context.Context, tx storage.Transaction, subquery *parser.ExistsExpression) (bool, error) {
	// Execute the subquery
	result, err := e.executeSelectWithContext(ctx, tx, subquery.Subquery)
	if err != nil {
		return false, fmt.Errorf("error executing EXISTS subquery: %w", err)
	}
	defer result.Close()

	// Check if there's at least one row
	exists := result.Next()

	return exists, nil
}

// processSelectSubqueries processes subqueries in SELECT expressions, replacing them with their values
func (e *Executor) processSelectSubqueries(ctx context.Context, tx storage.Transaction, expressions []parser.Expression) ([]parser.Expression, error) {
	result := make([]parser.Expression, len(expressions))
	for i, expr := range expressions {
		processed, err := e.processExpressionSubqueries(ctx, tx, expr)
		if err != nil {
			return nil, err
		}
		result[i] = processed
	}
	return result, nil
}

// processExpressionSubqueries processes subqueries in any expression, replacing them with their values
func (e *Executor) processExpressionSubqueries(ctx context.Context, tx storage.Transaction, expr parser.Expression) (parser.Expression, error) {
	switch exp := expr.(type) {
	case *parser.ScalarSubquery:
		// Execute the scalar subquery and replace with its value
		value, err := e.executeScalarSubquery(ctx, tx, exp)
		if err != nil {
			return nil, err
		}

		// Convert the value to a parser expression
		switch v := value.(type) {
		case int64:
			return &parser.IntegerLiteral{Value: v}, nil
		case float64:
			return &parser.FloatLiteral{Value: v}, nil
		case string:
			return &parser.StringLiteral{Value: v}, nil
		case bool:
			return &parser.BooleanLiteral{Value: v}, nil
		case nil:
			return &parser.NullLiteral{}, nil
		default:
			// Convert other types to string
			return &parser.StringLiteral{Value: fmt.Sprintf("%v", v)}, nil
		}

	case *parser.AliasedExpression:
		// Process the inner expression
		processed, err := e.processExpressionSubqueries(ctx, tx, exp.Expression)
		if err != nil {
			return nil, err
		}
		return &parser.AliasedExpression{
			Expression: processed,
			Alias:      exp.Alias,
		}, nil

	case *parser.InfixExpression:
		// Process both sides
		left, err := e.processExpressionSubqueries(ctx, tx, exp.Left)
		if err != nil {
			return nil, err
		}
		right, err := e.processExpressionSubqueries(ctx, tx, exp.Right)
		if err != nil {
			return nil, err
		}
		return &parser.InfixExpression{
			Left:     left,
			Operator: exp.Operator,
			Right:    right,
		}, nil

	default:
		// Return as-is for other expression types
		return expr, nil
	}
}

// processWhereSubqueries processes subqueries in WHERE clauses, replacing them with their values
func (e *Executor) processWhereSubqueries(ctx context.Context, tx storage.Transaction, expr parser.Expression) (parser.Expression, error) {
	switch exp := expr.(type) {
	case *parser.InExpression:
		// Check if the right side is a subquery
		if subquery, ok := exp.Right.(*parser.ScalarSubquery); ok {
			// Execute the subquery and get its results
			values, err := e.executeSubquery(ctx, tx, subquery)
			if err != nil {
				return nil, err
			}

			// Convert the values to parser expressions
			valueExprs := make([]parser.Expression, len(values))
			for i, val := range values {
				switch v := val.(type) {
				case int64:
					valueExprs[i] = &parser.IntegerLiteral{Value: v}
				case float64:
					valueExprs[i] = &parser.FloatLiteral{Value: v}
				case string:
					valueExprs[i] = &parser.StringLiteral{Value: v}
				case bool:
					valueExprs[i] = &parser.BooleanLiteral{Value: v}
				case nil:
					valueExprs[i] = &parser.NullLiteral{}
				default:
					// Convert other types to string
					valueExprs[i] = &parser.StringLiteral{Value: fmt.Sprintf("%v", v)}
				}
			}

			// Create an expression list with the values
			exprList := &parser.ExpressionList{
				Expressions: valueExprs,
			}

			// Replace the subquery with the list of values
			return &parser.InExpression{
				Left:  exp.Left,
				Not:   exp.Not,
				Right: exprList,
			}, nil
		}
		// If right side is not a subquery, return as is
		return exp, nil

	case *parser.InfixExpression:
		// Process left side
		processedLeft, err := e.processWhereSubqueries(ctx, tx, exp.Left)
		if err != nil {
			return nil, err
		}
		exp.Left = processedLeft

		// Process right side for scalar subqueries
		if subquery, ok := exp.Right.(*parser.ScalarSubquery); ok {
			value, err := e.executeScalarSubquery(ctx, tx, subquery)
			if err != nil {
				return nil, err
			}

			// Convert the value to a parser expression
			switch v := value.(type) {
			case int64:
				exp.Right = &parser.IntegerLiteral{Value: v}
			case float64:
				exp.Right = &parser.FloatLiteral{Value: v}
			case string:
				exp.Right = &parser.StringLiteral{Value: v}
			case bool:
				exp.Right = &parser.BooleanLiteral{Value: v}
			case nil:
				exp.Right = &parser.NullLiteral{}
			default:
				// Convert other types to string
				exp.Right = &parser.StringLiteral{Value: fmt.Sprintf("%v", v)}
			}
		} else {
			// Process right side recursively
			processedRight, err := e.processWhereSubqueries(ctx, tx, exp.Right)
			if err != nil {
				return nil, err
			}
			exp.Right = processedRight
		}

		// Handle EXISTS expressions in logical operators
		if exp.Operator == "AND" || exp.Operator == "OR" {
			// Check if left side is EXISTS
			if existsExpr, ok := exp.Left.(*parser.ExistsExpression); ok {
				exists, err := e.executeExistsSubquery(ctx, tx, existsExpr)
				if err != nil {
					return nil, err
				}
				exp.Left = &parser.BooleanLiteral{Value: exists}
			}

			// Check if right side is EXISTS
			if existsExpr, ok := exp.Right.(*parser.ExistsExpression); ok {
				exists, err := e.executeExistsSubquery(ctx, tx, existsExpr)
				if err != nil {
					return nil, err
				}
				exp.Right = &parser.BooleanLiteral{Value: exists}
			}
		}

		return exp, nil

	case *parser.BetweenExpression:
		processedExpr, err := e.processWhereSubqueries(ctx, tx, exp.Expr)
		if err != nil {
			return nil, err
		}
		exp.Expr = processedExpr
		processedLower, err := e.processWhereSubqueries(ctx, tx, exp.Lower)
		if err != nil {
			return nil, err
		}
		exp.Lower = processedLower
		processedUpper, err := e.processWhereSubqueries(ctx, tx, exp.Upper)
		if err != nil {
			return nil, err
		}
		exp.Upper = processedUpper
		return exp, nil

	case *parser.ExistsExpression:
		// Execute the EXISTS subquery
		exists, err := e.executeExistsSubquery(ctx, tx, exp)
		if err != nil {
			return nil, err
		}
		// Replace with a boolean literal
		return &parser.BooleanLiteral{Value: exists}, nil

	case *parser.PrefixExpression:
		// Handle NOT EXISTS
		if exp.Operator == "NOT" {
			if existsExpr, ok := exp.Right.(*parser.ExistsExpression); ok {
				// Execute the EXISTS subquery
				exists, err := e.executeExistsSubquery(ctx, tx, existsExpr)
				if err != nil {
					return nil, err
				}
				// Return NOT of the result
				return &parser.BooleanLiteral{Value: !exists}, nil
			}
		}
		// For other prefix expressions, process the right side recursively
		processedRight, err := e.processWhereSubqueries(ctx, tx, exp.Right)
		if err != nil {
			return nil, err
		}
		return &parser.PrefixExpression{
			Token:    exp.Token,
			Operator: exp.Operator,
			Right:    processedRight,
		}, nil

	default:
		// For other expression types, return as is
		return expr, nil
	}
}
