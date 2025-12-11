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

// NOTE: The EvalExpression is currently only used for testing purposes

// EvalExpression represents an expression implemented as a Go function
// Deprecated: This is a generic expression that evaluates a function
type EvalExpression struct {
	evalFn func(row storage.Row) (bool, error)

	// EvalExpression doesn't directly support aliases since it's
	// a black box function. However, the function inside could potentially
	// handle aliases if it's designed to do so.
}

// NewEvalExpression creates a new function expression using a function
func NewEvalExpression(evalFn func(row storage.Row) (bool, error)) *EvalExpression {
	return &EvalExpression{
		evalFn: evalFn,
	}
}

// Evaluate implements the storage.Expression interface
func (e *EvalExpression) Evaluate(row storage.Row) (bool, error) {
	// If evalFn is nil, always return false
	if e.evalFn == nil {
		return false, nil
	}

	return e.evalFn(row)
}

func (e *EvalExpression) EvaluateFast(row storage.Row) bool {
	if e.evalFn == nil {
		return false
	}

	result, err := e.evalFn(row)
	if err != nil {
		// If there's an error, we can't determine the result
		return false
	}

	return result
}

// PrepareForSchema returns the expression itself since EvalExpression
func (e *EvalExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	return e
}

// GetColumnName returns an empty string since we don't know which column is used
func (e *EvalExpression) GetColumnName() string {
	return ""
}

// GetValue returns nil since we don't know what value is being compared
func (e *EvalExpression) GetValue() interface{} {
	return nil
}

// GetOperator returns an unknown operator
func (e *EvalExpression) GetOperator() storage.Operator {
	return storage.EQ // Arbitrary default
}

// CanUseIndex returns false since we can't optimize function expressions directly
func (e *EvalExpression) CanUseIndex() bool {
	return false
}

// WithAliases implements the Expression interface
// For EvalExpression, we can't modify the internal function, so we return a wrapper
// that applies alias handling around the original function
func (e *EvalExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a function wrapper that will attempt to convert alias names to real column names
	aliasedFn := func(row storage.Row) (bool, error) {
		// Just pass through to the original function for now
		// A more sophisticated implementation could try to intercept column lookups
		// but that would require more complex function composition
		return e.evalFn(row)
	}

	return &EvalExpression{
		evalFn: aliasedFn,
	}
}
