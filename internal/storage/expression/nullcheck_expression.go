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

// NullCheckExpression is a concrete implementation of IndexableExpression
// that supports IS NULL and IS NOT NULL operators
type NullCheckExpression struct {
	Column         string
	isNull         bool              // true for IS NULL, false for IS NOT NULL
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias

	// Schema optimization fields
	ColIndex     int  // Column index for fast lookup
	IndexPrepped bool // Whether we've prepared column index mapping
}

// NewIsNullExpression creates an expression for IS NULL
func NewIsNullExpression(columnName string) storage.Expression {
	return &NullCheckExpression{
		Column: columnName,
		isNull: true,
	}
}

// NewIsNotNullExpression creates an expression for IS NOT NULL
func NewIsNotNullExpression(columnName string) storage.Expression {
	return &NullCheckExpression{
		Column: columnName,
		isNull: false,
	}
}

// Evaluate evaluates the NULL check expression against a row
func (e *NullCheckExpression) Evaluate(row storage.Row) (bool, error) {
	// If we have a prepared index from schema optimization, use it
	if e.IndexPrepped && e.ColIndex >= 0 && e.ColIndex < len(row) {
		colValue := row[e.ColIndex]
		isNull := colValue == nil || colValue.IsNull()
		return e.isNull == isNull, nil
	}

	// Return true for IS NULL check when IsNull() is true
	// Return false for IS NOT NULL check when IsNull() is true
	if e.isNull {
		return true, nil // IS NULL always returns true when index not prepped
	}
	return false, nil // IS NOT NULL always returns false when index not prepped
}

// IsNull returns true if this expression checks for NULL
func (e *NullCheckExpression) IsNull() bool {
	return e.isNull
}

// SetIsNull sets the NULL check status for this expression
func (e *NullCheckExpression) SetIsNull(isNull bool) {
	e.isNull = isNull
}

// GetColumnName returns the column name this expression operates on
func (e *NullCheckExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the value this expression compares against
func (e *NullCheckExpression) GetValue() interface{} {
	return nil
}

// GetOperator returns the operator this expression uses
func (e *NullCheckExpression) GetOperator() storage.Operator {
	if e.isNull {
		return storage.ISNULL
	}
	return storage.ISNOTNULL
}

// CanUseIndex returns true if this expression can use an index
func (e *NullCheckExpression) CanUseIndex() bool {
	// NULL checks can use indexes if the index supports it
	return true
}

// WithAliases implements the storage.Expression interface
func (e *NullCheckExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &NullCheckExpression{
		Column:  e.Column,
		isNull:  e.isNull,
		aliases: aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}

// PrepareForSchema optimizes the expression for a specific schema
func (e *NullCheckExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already prepped with valid index, don't redo the work
	if e.IndexPrepped && e.ColIndex >= 0 {
		return e
	}

	// Find the column index for fast lookup
	colName := e.Column
	if e.originalColumn != "" {
		colName = e.originalColumn
	}

	// Try to find the column in the schema
	for i, col := range schema.Columns {
		if col.Name == colName || col.Name == e.Column {
			e.ColIndex = i
			break
		}
	}

	e.IndexPrepped = true
	return e
}

// EvaluateFast implements the Expression interface for fast evaluation
func (e *NullCheckExpression) EvaluateFast(row storage.Row) bool {
	// If we've prepped with the schema, use the column index for direct access
	if e.IndexPrepped && e.ColIndex >= 0 && e.ColIndex < len(row) {
		colValue := row[e.ColIndex]

		// Check if the value is NULL
		isNull := colValue == nil || colValue.IsNull()

		// For IS NULL, return true if isNull is true
		// For IS NOT NULL, return true if isNull is false
		return e.isNull == isNull
	}

	// When schema is not available, we have to return deterministic behavior
	// that matches the Evaluate method and SchemaAwareExpression
	if e.isNull {
		return true // IS NULL always returns true when index not prepped
	}
	return false // IS NOT NULL always returns false when index not prepped
}
