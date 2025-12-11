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
package expression

import (
	"github.com/stoolap/stoolap-go/internal/storage"
)

// NotExpression represents a logical NOT of an expression
type NotExpression struct {
	Expr storage.Expression

	// Schema optimization
	isOptimized bool // Indicates if this expression has already been prepared for a schema
}

// NewNotExpression creates a new NOT expression
func NewNotExpression(expr storage.Expression) *NotExpression {
	return &NotExpression{
		Expr: expr,
	}
}

// Evaluate implements the Expression interface for NOT expressions
func (e *NotExpression) Evaluate(row storage.Row) (bool, error) {
	result, err := e.Expr.Evaluate(row)
	if err != nil {
		return false, err
	}
	return !result, nil
}

// WithAliases implements the Expression interface
func (e *NotExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Apply aliases to the inner expression
	if aliasable, ok := e.Expr.(interface {
		WithAliases(map[string]string) storage.Expression
	}); ok {
		// Create a new expression with the aliased inner expression
		return &NotExpression{
			Expr: aliasable.WithAliases(aliases),
		}
	}

	// If inner expression doesn't support aliases, return a copy of this expression
	return &NotExpression{
		Expr: e.Expr,
	}
}

// PrepareForSchema optimizes the expression for a specific schema
func (e *NotExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already optimized, don't redo the work
	if e.isOptimized {
		return e
	}

	// Optimize the inner expression
	e.Expr = e.Expr.PrepareForSchema(schema)
	e.isOptimized = true

	return e
}

// EvaluateFast implements the Expression interface for fast evaluation
func (e *NotExpression) EvaluateFast(row storage.Row) bool {
	// Simply negate the result of the inner expression's fast evaluation
	return !e.Expr.EvaluateFast(row)
}
