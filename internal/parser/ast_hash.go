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
package parser

// InExpressionHash is an optimized IN expression that uses a hash set for O(1) lookups
// This is created by the query optimizer from regular InExpression when processing subqueries
type InExpressionHash struct {
	Left     Expression
	Not      bool
	ValueSet map[interface{}]bool
	HasNull  bool
}

// expressionNode marks this as an expression
func (e *InExpressionHash) expressionNode() {}

// TokenLiteral returns the token literal
func (e *InExpressionHash) TokenLiteral() string {
	if e.Not {
		return "NOT IN"
	}
	return "IN"
}

// String returns the string representation
func (e *InExpressionHash) String() string {
	result := e.Left.String()
	if e.Not {
		result += " NOT IN"
	} else {
		result += " IN"
	}
	result += " (<optimized hash set>)"
	return result
}

// Position returns the position of the node in the source code
func (e *InExpressionHash) Position() Position {
	// This is a synthetic node created during optimization, so return zero position
	return Position{Line: 0, Column: 0}
}

// Accept implements the Visitor pattern
// For now, this is a placeholder as InExpressionHash is an internal optimization
func (e *InExpressionHash) Accept(v interface{}) {
	// No-op for now
}
