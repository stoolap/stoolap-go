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

// AndExpression represents a logical AND of multiple expressions
type AndExpression struct {
	Expressions []storage.Expression
	aliases     map[string]string // Column aliases
	isOptimized bool              // Whether this expression has been optimized for schema
}

// NewAndExpression creates a new AND expression
func NewAndExpression(expressions ...storage.Expression) *AndExpression {
	return &AndExpression{
		Expressions: expressions,
	}
}

// Evaluate implements the Expression interface for AND expressions
func (e *AndExpression) Evaluate(row storage.Row) (bool, error) {
	for _, expr := range e.Expressions {
		result, err := expr.Evaluate(row)
		if err != nil {
			return false, err
		}
		if !result {
			return false, nil // Short-circuit on first false
		}
	}
	return true, nil // All expressions were true
}

func (e *AndExpression) EvaluateFast(row storage.Row) bool {
	// For more expressions, use existing logic
	for _, expr := range e.Expressions {
		if !expr.EvaluateFast(row) {
			return false
		}
	}
	return true
}

// PrepareForSchema prepares the expression for a given schema
func (e *AndExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	if e.isOptimized {
		return e
	}

	// Optimize all child expressions
	for i, expr := range e.Expressions {
		e.Expressions[i] = expr.PrepareForSchema(schema)
	}

	e.isOptimized = true
	return e
}

// WithAliases implements the Expression interface
func (e *AndExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same structure
	result := &AndExpression{
		Expressions: make([]storage.Expression, len(e.Expressions)),
		aliases:     aliases,
	}

	// Apply aliases to all child expressions
	for i, expr := range e.Expressions {
		if aliasable, ok := expr.(interface {
			WithAliases(map[string]string) storage.Expression
		}); ok {
			result.Expressions[i] = aliasable.WithAliases(aliases)
		} else {
			result.Expressions[i] = expr
		}
	}

	return result
}

// OrExpression represents a logical OR of multiple expressions
type OrExpression struct {
	Expressions []storage.Expression
	aliases     map[string]string // Column aliases
	isOptimized bool              // Whether this expression has been optimized for schema
}

// NewOrExpression creates a new OR expression
func NewOrExpression(expressions ...storage.Expression) *OrExpression {
	return &OrExpression{
		Expressions: expressions,
	}
}

// Evaluate implements the Expression interface for OR expressions
func (e *OrExpression) Evaluate(row storage.Row) (bool, error) {
	for _, expr := range e.Expressions {
		result, err := expr.Evaluate(row)
		if err != nil {
			continue // Skip errors in OR expressions
		}
		if result {
			return true, nil // Short-circuit on first true
		}
	}
	return false, nil // No expression was true
}

// EvaluateFast implements the Expression interface for fast evaluation
func (e *OrExpression) EvaluateFast(row storage.Row) bool {
	for _, expr := range e.Expressions {
		if expr.EvaluateFast(row) {
			return true // Short-circuit on first true
		}
	}
	return false // No expression was true
}

// PrepareForSchema prepares the expression for a given schema
func (e *OrExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	if e.isOptimized {
		return e
	}

	// Optimize all child expressions
	for i, expr := range e.Expressions {
		e.Expressions[i] = expr.PrepareForSchema(schema)
	}

	e.isOptimized = true
	return e
}

// WithAliases implements the Expression interface
func (e *OrExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same structure
	result := &OrExpression{
		Expressions: make([]storage.Expression, len(e.Expressions)),
		aliases:     aliases,
	}

	// Apply aliases to all child expressions
	for i, expr := range e.Expressions {
		if aliasable, ok := expr.(interface {
			WithAliases(map[string]string) storage.Expression
		}); ok {
			result.Expressions[i] = aliasable.WithAliases(aliases)
		} else {
			result.Expressions[i] = expr
		}
	}

	return result
}
