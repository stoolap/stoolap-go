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
	"fmt"
	"strconv"
	"strings"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// InListExpression represents an expression that checks if a column value is in a list of values
type InListExpression struct {
	Column string
	Values []interface{}
	Not    bool // If true, this is a NOT IN expression

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias

	// Schema optimization fields
	ColIndex     int  // Column index for fast lookup
	IndexPrepped bool // Whether we've prepared column index mapping
}

// Evaluate implements the Expression interface
func (e *InListExpression) Evaluate(row storage.Row) (bool, error) {
	// Only use prepared column indices, don't try to find columns by name
	if !e.IndexPrepped || e.ColIndex < 0 || e.ColIndex >= len(row) {
		return false, nil
	}

	// Get the column value
	colValue := row[e.ColIndex]
	if colValue == nil {
		// NULL IN (...) is always false
		// For NOT IN this also remains false (NULL NOT IN ... is also false)
		// This is standard SQL behavior for NULL in comparisons
		return false, nil
	}

	// Flag to track if we found a match
	found := false

	// Try to use the appropriate type comparison based on column type
	switch colValue.Type() {
	case storage.TypeInteger:
		// For integers, convert and compare numerically
		colInt, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to int64 for comparison
			var valueInt int64
			switch v := value.(type) {
			case int:
				valueInt = int64(v)
			case int64:
				valueInt = v
			case float64:
				valueInt = int64(v)
			case string:
				// Try to parse as int
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
					valueInt = parsed
				} else {
					// Skip this value if not parseable
					continue
				}
			default:
				// Skip non-numeric values
				continue
			}

			// Compare
			if colInt == valueInt {
				found = true
				break // Found a match, no need to check further
			}
		}

	case storage.TypeFloat:
		// For floats, convert and compare numerically
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to float64 for comparison
			var valueFloat float64
			switch v := value.(type) {
			case int:
				valueFloat = float64(v)
			case int64:
				valueFloat = float64(v)
			case float64:
				valueFloat = v
			case string:
				// Try to parse as float
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					valueFloat = parsed
				} else {
					// Skip this value if not parseable
					continue
				}
			default:
				// Skip non-numeric values
				continue
			}

			// Compare
			if colFloat == valueFloat {
				found = true
				break // Found a match, no need to check further
			}
		}

	default:
		// For other types, fall back to string comparison
		colStrVal, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to string for comparison
			var valueStr string
			switch v := value.(type) {
			case string:
				valueStr = v
			default:
				valueStr = fmt.Sprintf("%v", v)
			}

			// Compare
			if colStrVal == valueStr {
				found = true
				break // Found a match, no need to check further
			}
		}
	}

	// For NOT IN, invert the result
	if e.Not {
		return !found, nil
	}

	return found, nil
}

// Implement the IndexableExpression interface

// GetColumnName returns the column name
func (e *InListExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the first value in the list (not very useful)
func (e *InListExpression) GetValue() interface{} {
	if len(e.Values) > 0 {
		return e.Values[0]
	}
	return nil
}

// GetOperator returns EQ (equals)
func (e *InListExpression) GetOperator() storage.Operator {
	return storage.EQ
}

// CanUseIndex returns true, as IN can potentially use an index
func (e *InListExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the Expression interface for InListExpression
func (e *InListExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &InListExpression{
		Column:  e.Column,
		Values:  e.Values,
		Not:     e.Not,
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
func (e *InListExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
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
		if col.Name == colName || strings.EqualFold(col.Name, colName) {
			e.ColIndex = i
			break
		}
	}

	e.IndexPrepped = true
	return e
}

// EvaluateFast implements fast evaluation for the Expression interface
func (e *InListExpression) EvaluateFast(row storage.Row) bool {
	// Only use prepared column indices, don't try to find columns by name
	if !e.IndexPrepped || e.ColIndex < 0 || e.ColIndex >= len(row) {
		return false
	}

	colValue := row[e.ColIndex]
	if colValue == nil {
		// NULL IN (...) is always false (same for NOT IN)
		return false
	}

	// Flag to track if we found a match
	found := false

	// Fast path evaluations based on type
	switch colValue.Type() {
	case storage.TypeInteger:
		colInt, ok := colValue.AsInt64()
		if !ok {
			return e.Not // Not found if conversion failed
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to int64 for comparison (only handle simple numeric types in fast path)
			switch v := value.(type) {
			case int:
				if colInt == int64(v) {
					found = true
					break
				}
			case int64:
				if colInt == v {
					found = true
					break
				}
			case float64:
				if colInt == int64(v) {
					found = true
					break
				}
			}
		}

	case storage.TypeFloat:
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return e.Not // Not found if conversion failed
		}

		// Check if the value is in the list
		for _, value := range e.Values {
			// Convert value to float64 for comparison (only handle simple numeric types in fast path)
			switch v := value.(type) {
			case int:
				if colFloat == float64(v) {
					found = true
					break
				}
			case int64:
				if colFloat == float64(v) {
					found = true
					break
				}
			case float64:
				if colFloat == v {
					found = true
					break
				}
			}
		}

	case storage.TypeString:
		colStr, ok := colValue.AsString()
		if !ok {
			return e.Not // Not found if conversion failed
		}

		// Check if the value is in the list (only handle string types in fast path)
		for _, value := range e.Values {
			if strValue, ok := value.(string); ok {
				if colStr == strValue {
					found = true
					break
				}
			}
		}

	default:
		// For other types, simply return the default value
		if e.Not {
			return true // NOT IN for unknown type
		}
		return false // IN for unknown type
	}

	// For NOT IN, invert the result
	if e.Not {
		return !found
	}
	return found
}
