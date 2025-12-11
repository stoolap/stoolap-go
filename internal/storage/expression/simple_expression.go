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
	"strings"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// SimpleExpression is a basic implementation of a boolean expression
type SimpleExpression struct {
	Column   string           // Name of the column
	Operator storage.Operator // Comparison operator
	Value    interface{}      // Value to compare against (for backward compatibility)

	// Optimized typed value storage to avoid interface{} overhead
	Int64Value   int64
	Float64Value float64
	StringValue  string
	BoolValue    bool
	TimeValue    time.Time

	// Type information and optimization flags
	ValueType    storage.DataType // Type of the value for fast paths
	ValueIsNil   bool             // Whether the value is nil
	ColIndex     int              // Column index for fast lookup
	IndexPrepped bool             // Whether we've prepared column index mapping

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
}

// NewSimpleExpression creates a new SimpleExpression
func NewSimpleExpression(column string, operator storage.Operator, value interface{}) *SimpleExpression {
	expr := &SimpleExpression{
		Column:       column,
		Operator:     operator,
		Value:        value,
		IndexPrepped: false,
		ColIndex:     -1,
		ValueIsNil:   value == nil,
		aliases:      make(map[string]string),
	}

	// Pre-process the value to avoid repeated type assertions during evaluation
	if !expr.ValueIsNil {
		switch v := value.(type) {
		case int:
			expr.ValueType = storage.TypeInteger
			expr.Int64Value = int64(v)
		case int64:
			expr.ValueType = storage.TypeInteger
			expr.Int64Value = v
		case float64:
			expr.ValueType = storage.TypeFloat
			expr.Float64Value = v
		case string:
			expr.ValueType = storage.TypeString
			expr.StringValue = v
		case bool:
			expr.ValueType = storage.TypeBoolean
			expr.BoolValue = v
		case time.Time:
			expr.ValueType = storage.TypeTimestamp
			expr.TimeValue = v
		default:
			// Fall back to string for other types
			expr.ValueType = storage.TypeString
			expr.StringValue = fmt.Sprintf("%v", value)
		}
	}

	return expr
}

// Evaluate implements the Expression interface
func (e *SimpleExpression) Evaluate(row storage.Row) (bool, error) {
	// Fast paths are critical - we need to handle the most common cases first
	// without function calls
	if !e.IndexPrepped {
		return false, nil
	}

	// Inlined column fetch and bounds check
	colIdx := e.ColIndex
	if colIdx < 0 || colIdx >= len(row) {
		return false, nil
	}

	// Get column value
	colVal := row[colIdx]

	// NULL handling
	if colVal == nil {
		return e.Operator == storage.ISNULL, nil
	}

	// Special NULL check operators
	if e.Operator == storage.ISNULL {
		return colVal.IsNull(), nil
	}
	if e.Operator == storage.ISNOTNULL {
		return !colVal.IsNull(), nil
	}

	// All other operators return false for NULL values
	if colVal.IsNull() {
		return false, nil
	}

	// At this point, we know the column value is not NULL
	// Now optimize for the most common operation: equality checks
	if e.Operator == storage.EQ {
		// Integer equality (most common case)
		if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
			v, ok := colVal.AsInt64()
			if ok {
				return v == e.Int64Value, nil
			}
			return false, nil
		}

		// String equality (second most common case)
		if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
			v, ok := colVal.AsString()
			if ok {
				return v == e.StringValue, nil
			}
			return false, nil
		}

		// Boolean equality (much simpler than other types)
		if e.ValueType == storage.TypeBoolean && colVal.Type() == storage.TypeBoolean {
			v, ok := colVal.AsBoolean()
			if ok {
				return v == e.BoolValue, nil
			}
			return false, nil
		}

		// Float equality - these are less common but still worth inlining
		if e.ValueType == storage.TypeFloat && colVal.Type() == storage.TypeFloat {
			v, ok := colVal.AsFloat64()
			if ok {
				return v == e.Float64Value, nil
			}
			return false, nil
		}
	}

	// For inequality checks (second most common operation)
	if e.Operator == storage.NE {
		// Integer inequality
		if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
			v, ok := colVal.AsInt64()
			if ok {
				return v != e.Int64Value, nil
			}
			return false, nil
		}

		// String inequality
		if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
			v, ok := colVal.AsString()
			if ok {
				return v != e.StringValue, nil
			}
			return false, nil
		}
	}

	// Range comparisons for integers (common in filtering)
	if colVal.Type() == storage.TypeInteger && e.ValueType == storage.TypeInteger {
		v, ok := colVal.AsInt64()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.GT:
			return v > e.Int64Value, nil
		case storage.GTE:
			return v >= e.Int64Value, nil
		case storage.LT:
			return v < e.Int64Value, nil
		case storage.LTE:
			return v <= e.Int64Value, nil
		}
	}

	// Delegate to type-specific comparisons for less common cases
	return e.evaluateComparison(colVal)
}

// IsEquality returns true if this is an equality expression
func (e *SimpleExpression) IsEquality() bool {
	return e.Operator == storage.EQ
}

// GetColumnName returns the column name for this expression
func (e *SimpleExpression) GetColumnName() string {
	return e.Column
}

// GetValue returns the value for this expression
func (e *SimpleExpression) GetValue() interface{} {
	return e.Value
}

// GetOperator returns the operator for this expression
func (e *SimpleExpression) GetOperator() storage.Operator {
	return e.Operator
}

// CanUseIndex returns whether this expression can use an index
func (e *SimpleExpression) CanUseIndex() bool {
	// Allow index usage for comparison operators as well
	return e.Operator == storage.EQ ||
		e.Operator == storage.GT ||
		e.Operator == storage.GTE ||
		e.Operator == storage.LT ||
		e.Operator == storage.LTE
}

// WithAliases implements the Expression interface for SimpleExpression
func (e *SimpleExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &SimpleExpression{
		Column:       e.Column,
		Operator:     e.Operator,
		Value:        e.Value,
		Int64Value:   e.Int64Value,
		Float64Value: e.Float64Value,
		StringValue:  e.StringValue,
		BoolValue:    e.BoolValue,
		TimeValue:    e.TimeValue,
		ValueType:    e.ValueType,
		ValueIsNil:   e.ValueIsNil,
		IndexPrepped: false, // Reset index preparation as column names might change
		ColIndex:     -1,    // Reset column index
		aliases:      aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}

// SetOriginalColumn sets the original column name for this expression
func (e *SimpleExpression) SetOriginalColumn(col string) {
	e.originalColumn = col
}

// evaluateComparison evaluates a comparison between a column value and expression value
func (e *SimpleExpression) evaluateComparison(colValue storage.ColumnValue) (bool, error) {
	// Optimization: First check for matching types (most common case)
	if e.ValueType == colValue.Type() {
		return e.evaluateTypedComparison(colValue)
	}

	// Optimization: Handle numeric types (INTEGER vs FLOAT) - common in analytics
	if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
		(colValue.Type() == storage.TypeInteger || colValue.Type() == storage.TypeFloat) {
		return e.evaluateNumericComparison(colValue)
	}

	// Fall back to type-specific implementations for more complex cases
	switch colValue.Type() {
	case storage.TypeInteger:
		v, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value to avoid interface{} overhead
		var compareValue int64

		// Fast path for common expression value types
		if e.ValueType == storage.TypeInteger {
			compareValue = e.Int64Value
		} else if e.ValueType == storage.TypeFloat {
			// Common case: float compared with integer
			compareValue = int64(e.Float64Value)
		} else {
			// Rare case: handle other types through interface{}
			switch val := e.Value.(type) {
			case int:
				compareValue = int64(val)
			case int64:
				compareValue = val
			case float64:
				compareValue = int64(val)
			default:
				// Special handling for custom types with AsInt64 method
				if intVal, ok := e.Value.(interface{ AsInt64() (int64, bool) }); ok {
					if i64, ok := intVal.AsInt64(); ok {
						compareValue = i64
					} else {
						return false, fmt.Errorf("failed to convert %T to int64", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare integer with %T", e.Value)
				}
			}
		}

		// Apply the operator with inline comparison for better branch prediction
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", e.Operator)
		}

	case storage.TypeFloat:
		v, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Use pre-calculated value for better performance
		var compareValue float64

		// Handle common cases directly
		if e.ValueType == storage.TypeFloat {
			compareValue = e.Float64Value
		} else if e.ValueType == storage.TypeInteger {
			// Common case: integer compared with float
			compareValue = float64(e.Int64Value)
		} else {
			// Rare cases handled through interface{}
			switch val := e.Value.(type) {
			case int:
				compareValue = float64(val)
			case int64:
				compareValue = float64(val)
			case float64:
				compareValue = val
			default:
				// Special handling for custom types with AsFloat64 method
				if floatVal, ok := e.Value.(interface{ AsFloat64() (float64, bool) }); ok {
					if f64, ok := floatVal.AsFloat64(); ok {
						compareValue = f64
					} else {
						return false, fmt.Errorf("failed to convert %T to float64", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare float with %T", e.Value)
				}
			}
		}

		// Apply the operator with each case inlined
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for float: %v", e.Operator)
		}

	case storage.TypeString:
		v, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Use pre-calculated string value where possible
		var compareValue string
		if e.ValueType == storage.TypeString {
			compareValue = e.StringValue
		} else {
			// For non-string expression types, convert to string
			switch val := e.Value.(type) {
			case string:
				compareValue = val
			default:
				// Try to use AsString method if available
				if strVal, ok := e.Value.(interface{ AsString() (string, bool) }); ok {
					if str, ok := strVal.AsString(); ok {
						compareValue = str
					} else {
						compareValue = fmt.Sprintf("%v", e.Value)
					}
				} else {
					// Last resort: generic string conversion
					compareValue = fmt.Sprintf("%v", val)
				}
			}
		}

		// Handle string operations
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		case storage.GT:
			return v > compareValue, nil
		case storage.GTE:
			return v >= compareValue, nil
		case storage.LT:
			return v < compareValue, nil
		case storage.LTE:
			return v <= compareValue, nil
		case storage.LIKE:
			// LIKE operator not yet fully implemented
			return false, fmt.Errorf("LIKE operator not implemented yet")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", e.Operator)
		}

	case storage.TypeBoolean:
		v, ok := colValue.AsBoolean()
		if !ok {
			return false, nil
		}

		// Get boolean comparison value
		var compareValue bool
		if e.ValueType == storage.TypeBoolean {
			compareValue = e.BoolValue
		} else {
			// Convert from other types to boolean
			switch val := e.Value.(type) {
			case bool:
				compareValue = val
			case string:
				// Common string representations of boolean
				compareValue = val == "true" || val == "1" || val == "t" || val == "yes" || val == "y"
			case int:
				compareValue = val != 0
			case int64:
				compareValue = val != 0
			case float64:
				compareValue = val != 0
			default:
				// Try to use AsBoolean method if available
				if boolVal, ok := e.Value.(interface{ AsBoolean() (bool, bool) }); ok {
					if b, ok := boolVal.AsBoolean(); ok {
						compareValue = b
					} else {
						return false, fmt.Errorf("failed to convert %T to boolean", e.Value)
					}
				} else {
					return false, fmt.Errorf("cannot compare boolean with %T", e.Value)
				}
			}
		}

		// Boolean only supports equality and inequality
		switch e.Operator {
		case storage.EQ:
			return v == compareValue, nil
		case storage.NE:
			return v != compareValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", e.Operator)
		}

	case storage.TypeTimestamp:
		timestamp, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Get timestamp comparison value
		var compareTime time.Time
		if e.ValueType == storage.TypeTimestamp {
			compareTime = e.TimeValue
		} else {
			// Convert from other types to timestamp
			switch val := e.Value.(type) {
			case time.Time:
				compareTime = val
			case string:
				// Try to parse string as timestamp
				var err error
				compareTime, err = storage.ParseTimestamp(val)
				if err != nil {
					return false, fmt.Errorf("could not parse timestamp string: %v", err)
				}
			default:
				return false, fmt.Errorf("cannot compare timestamp with %T", e.Value)
			}
		}

		// Apply timestamp-specific comparison operators
		switch e.Operator {
		case storage.EQ:
			return timestamp.Equal(compareTime), nil
		case storage.NE:
			return !timestamp.Equal(compareTime), nil
		case storage.GT:
			return timestamp.After(compareTime), nil
		case storage.GTE:
			return timestamp.After(compareTime) || timestamp.Equal(compareTime), nil
		case storage.LT:
			return timestamp.Before(compareTime), nil
		case storage.LTE:
			return timestamp.Before(compareTime) || timestamp.Equal(compareTime), nil
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", e.Operator)
		}

	case storage.TypeJSON:
		// JSON type needs custom handling
		jsonVal, ok := colValue.AsJSON()
		if !ok {
			return false, nil
		}

		// Basic equality for JSON objects
		if e.Operator == storage.EQ && e.ValueType == storage.TypeJSON {
			// We can only do very basic equality checks on JSON
			jsonStr1 := fmt.Sprintf("%v", jsonVal)
			jsonStr2 := fmt.Sprintf("%v", e.Value)
			return jsonStr1 == jsonStr2, nil
		}

		return false, fmt.Errorf("only equality comparisons supported for JSON type")
	}

	// Default case - type not supported for comparison
	return false, fmt.Errorf("unsupported column type for comparison: %v", colValue.Type())
}

// evaluateTypedComparison provides a fast path for comparing values of the same type
// This function is only used by the non-fast evaluation path, as EvaluateFast inlines this logic
func (e *SimpleExpression) evaluateTypedComparison(colValue storage.ColumnValue) (bool, error) {
	switch e.ValueType {
	case storage.TypeInteger:
		v, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.Int64Value, nil
		case storage.NE:
			return v != e.Int64Value, nil
		case storage.GT:
			return v > e.Int64Value, nil
		case storage.GTE:
			return v >= e.Int64Value, nil
		case storage.LT:
			return v < e.Int64Value, nil
		case storage.LTE:
			return v <= e.Int64Value, nil
		default:
			return false, fmt.Errorf("unsupported operator for integer: %v", e.Operator)
		}

	case storage.TypeFloat:
		v, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.Float64Value, nil
		case storage.NE:
			return v != e.Float64Value, nil
		case storage.GT:
			return v > e.Float64Value, nil
		case storage.GTE:
			return v >= e.Float64Value, nil
		case storage.LT:
			return v < e.Float64Value, nil
		case storage.LTE:
			return v <= e.Float64Value, nil
		default:
			return false, fmt.Errorf("unsupported operator for float: %v", e.Operator)
		}

	case storage.TypeString:
		v, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.StringValue, nil
		case storage.NE:
			return v != e.StringValue, nil
		case storage.GT:
			return v > e.StringValue, nil
		case storage.GTE:
			return v >= e.StringValue, nil
		case storage.LT:
			return v < e.StringValue, nil
		case storage.LTE:
			return v <= e.StringValue, nil
		case storage.LIKE:
			// LIKE pattern matching would need more implementation
			return false, fmt.Errorf("LIKE operator not implemented yet")
		default:
			return false, fmt.Errorf("unsupported operator for string: %v", e.Operator)
		}

	case storage.TypeBoolean:
		v, ok := colValue.AsBoolean()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v == e.BoolValue, nil
		case storage.NE:
			return v != e.BoolValue, nil
		default:
			return false, fmt.Errorf("unsupported operator for boolean: %v", e.Operator)
		}

	case storage.TypeTimestamp:
		v, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		switch e.Operator {
		case storage.EQ:
			return v.Equal(e.TimeValue), nil
		case storage.NE:
			return !v.Equal(e.TimeValue), nil
		case storage.GT:
			return v.After(e.TimeValue), nil
		case storage.GTE:
			return v.After(e.TimeValue) || v.Equal(e.TimeValue), nil
		case storage.LT:
			return v.Before(e.TimeValue), nil
		case storage.LTE:
			return v.Before(e.TimeValue) || v.Equal(e.TimeValue), nil
		default:
			return false, fmt.Errorf("unsupported operator for timestamp: %v", e.Operator)
		}
	}

	return false, fmt.Errorf("unsupported value type: %v", e.ValueType)
}

// evaluateNumericComparison provides a fast path for numeric comparisons between integer and float types
// This function is only used by the non-fast evaluation path, as EvaluateFast inlines this logic
func (e *SimpleExpression) evaluateNumericComparison(colValue storage.ColumnValue) (bool, error) {
	// Convert both values to float64 for comparison
	var colFloat float64

	// Get column value as float
	if colValue.Type() == storage.TypeInteger {
		intVal, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}
		colFloat = float64(intVal)
	} else {
		var ok bool
		colFloat, ok = colValue.AsFloat64()
		if !ok {
			return false, nil
		}
	}

	// Get expression value as float
	var exprFloat float64
	if e.ValueType == storage.TypeInteger {
		exprFloat = float64(e.Int64Value)
	} else {
		exprFloat = e.Float64Value
	}

	// Perform the comparison
	switch e.Operator {
	case storage.EQ:
		return colFloat == exprFloat, nil
	case storage.NE:
		return colFloat != exprFloat, nil
	case storage.GT:
		return colFloat > exprFloat, nil
	case storage.GTE:
		return colFloat >= exprFloat, nil
	case storage.LT:
		return colFloat < exprFloat, nil
	case storage.LTE:
		return colFloat <= exprFloat, nil
	default:
		return false, fmt.Errorf("unsupported operator for numeric comparison: %v", e.Operator)
	}
}

// PrepareForSchema optimizes the expression for a specific schema by calculating
// column indices in advance for fast evaluation
func (e *SimpleExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already prepped with valid index, don't redo the work
	if e.IndexPrepped && e.ColIndex >= 0 {
		return e
	}

	// Find the column index to optimize lookup - avoid function calls for better performance
	schemaColumns := schema.Columns
	colName := e.Column
	numColumns := len(schemaColumns)

	// Fast path: direct case-sensitive match first (most common case)
	for i := 0; i < numColumns; i++ {
		if schemaColumns[i].Name == colName {
			e.ColIndex = i
			e.IndexPrepped = true

			// Verify column type for potential additional optimizations
			if schemaColumns[i].Type == e.ValueType {
				// Cache information that types match for even faster evaluation
				// This could be used for future optimization
			}

			return e
		}
	}

	// Slower fallback path: case-insensitive match as a last resort
	colNameLower := strings.ToLower(colName)
	for i := 0; i < numColumns; i++ {
		if strings.ToLower(schemaColumns[i].Name) == colNameLower {
			e.ColIndex = i
			e.IndexPrepped = true
			return e
		}
	}

	// Mark as prepped even if not found - don't wastefully repeat the search
	e.IndexPrepped = true
	e.ColIndex = -1 // Explicitly mark as not found
	return e
}

// EvaluateFast is an ultra-optimized version of Evaluate that avoids interface method calls
// and type assertions where possible, for the critical path in query processing
func (e *SimpleExpression) EvaluateFast(row storage.Row) bool {
	// Safety check: invalid index preparation or out-of-bounds
	// Inlined validation for speed - avoid function calls
	colIdx := e.ColIndex
	if !e.IndexPrepped || colIdx < 0 || colIdx >= len(row) {
		return false
	}

	// Get the column value directly
	colVal := row[colIdx]

	// Handle NULL column value cases first (very common in real-world databases)
	// This also simplifies the rest of the function since we can assume non-null after this
	if colVal == nil {
		return e.Operator == storage.ISNULL
	}

	// Special case: NULL check operators
	// We handle these immediately for better branch prediction and clarity
	switch e.Operator {
	case storage.ISNULL:
		return colVal.IsNull()
	case storage.ISNOTNULL:
		return !colVal.IsNull()
	}

	// For all other operations, NULL values always yield false in SQL semantics
	if colVal.IsNull() {
		return false
	}

	// At this point, we know:
	// 1. We have a valid column value (not nil)
	// 2. The column value is not NULL
	// 3. We're not doing a NULL check operation

	// Fast paths organized by operator frequency in typical workloads
	// Equality is by far the most common operator in database queries
	if e.Operator == storage.EQ {
		// Integer equality (most common case) - directly unrolled
		if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
			v, ok := colVal.AsInt64()
			if ok {
				return v == e.Int64Value
			}
			return false
		}

		// String equality (second most common case) - directly unrolled
		if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
			v, ok := colVal.AsString()
			if ok {
				return v == e.StringValue
			}
			return false
		}

		// Boolean equality (third most common case) - fast path
		if e.ValueType == storage.TypeBoolean && colVal.Type() == storage.TypeBoolean {
			v, ok := colVal.AsBoolean()
			if ok {
				return v == e.BoolValue
			}
			return false
		}

		// Float equality - fast path
		if e.ValueType == storage.TypeFloat && colVal.Type() == storage.TypeFloat {
			v, ok := colVal.AsFloat64()
			if ok {
				return v == e.Float64Value
			}
			return false
		}

		// Timestamp equality - requires special handling for time.Time comparison
		if e.ValueType == storage.TypeTimestamp && colVal.Type() == storage.TypeTimestamp {
			v, ok := colVal.AsTimestamp()
			if ok {
				return v.Equal(e.TimeValue)
			}
			return false
		}

		// Handle mixed numeric comparisons (common in analytics)
		if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
			(colVal.Type() == storage.TypeInteger || colVal.Type() == storage.TypeFloat) {
			// Convert both to float64 for comparison
			var colFloat float64

			// Extract column value as float
			if colVal.Type() == storage.TypeInteger {
				if v, ok := colVal.AsInt64(); ok {
					colFloat = float64(v)
				} else {
					return false
				}
			} else {
				if v, ok := colVal.AsFloat64(); ok {
					colFloat = v
				} else {
					return false
				}
			}

			// Extract expression value as float
			var exprFloat float64
			if e.ValueType == storage.TypeInteger {
				exprFloat = float64(e.Int64Value)
			} else {
				exprFloat = e.Float64Value
			}

			return colFloat == exprFloat
		}
	}

	// Range comparisons (GT, GTE, LT, LTE) are the next most common in analytics
	// These are highly optimized for numeric types which are most common in ranges
	if e.Operator == storage.GT || e.Operator == storage.GTE ||
		e.Operator == storage.LT || e.Operator == storage.LTE {

		// Integer comparisons - most common for range operations
		if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
			v, ok := colVal.AsInt64()
			if !ok {
				return false
			}

			switch e.Operator {
			case storage.GT:
				return v > e.Int64Value
			case storage.GTE:
				return v >= e.Int64Value
			case storage.LT:
				return v < e.Int64Value
			case storage.LTE:
				return v <= e.Int64Value
			}
		}

		// Float comparisons - also common for range operations
		if e.ValueType == storage.TypeFloat && colVal.Type() == storage.TypeFloat {
			v, ok := colVal.AsFloat64()
			if !ok {
				return false
			}

			switch e.Operator {
			case storage.GT:
				return v > e.Float64Value
			case storage.GTE:
				return v >= e.Float64Value
			case storage.LT:
				return v < e.Float64Value
			case storage.LTE:
				return v <= e.Float64Value
			}
		}

		// String comparisons - less common but still important
		if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
			v, ok := colVal.AsString()
			if !ok {
				return false
			}

			switch e.Operator {
			case storage.GT:
				return v > e.StringValue
			case storage.GTE:
				return v >= e.StringValue
			case storage.LT:
				return v < e.StringValue
			case storage.LTE:
				return v <= e.StringValue
			}
		}

		// Timestamp comparisons
		if e.ValueType == storage.TypeTimestamp && colVal.Type() == storage.TypeTimestamp {
			v, ok := colVal.AsTimestamp()
			if !ok {
				return false
			}

			switch e.Operator {
			case storage.GT:
				return v.After(e.TimeValue)
			case storage.GTE:
				return v.After(e.TimeValue) || v.Equal(e.TimeValue)
			case storage.LT:
				return v.Before(e.TimeValue)
			case storage.LTE:
				return v.Before(e.TimeValue) || v.Equal(e.TimeValue)
			}
		}

		// Mixed numeric comparisons (int vs float)
		if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
			(colVal.Type() == storage.TypeInteger || colVal.Type() == storage.TypeFloat) {

			// Convert both to float64 for comparison
			var colFloat float64

			// Extract column value as float
			if colVal.Type() == storage.TypeInteger {
				if v, ok := colVal.AsInt64(); ok {
					colFloat = float64(v)
				} else {
					return false
				}
			} else {
				if v, ok := colVal.AsFloat64(); ok {
					colFloat = v
				} else {
					return false
				}
			}

			// Extract expression value as float
			var exprFloat float64
			if e.ValueType == storage.TypeInteger {
				exprFloat = float64(e.Int64Value)
			} else {
				exprFloat = e.Float64Value
			}

			// Perform comparison
			switch e.Operator {
			case storage.GT:
				return colFloat > exprFloat
			case storage.GTE:
				return colFloat >= exprFloat
			case storage.LT:
				return colFloat < exprFloat
			case storage.LTE:
				return colFloat <= exprFloat
			}
		}
	}

	// Handle inequality operator (!=)
	if e.Operator == storage.NE {
		// Integer inequality - directly unrolled
		if e.ValueType == storage.TypeInteger && colVal.Type() == storage.TypeInteger {
			v, ok := colVal.AsInt64()
			if ok {
				return v != e.Int64Value
			}
			return false
		}

		// String inequality - directly unrolled
		if e.ValueType == storage.TypeString && colVal.Type() == storage.TypeString {
			v, ok := colVal.AsString()
			if ok {
				return v != e.StringValue
			}
			return false
		}

		// Boolean inequality
		if e.ValueType == storage.TypeBoolean && colVal.Type() == storage.TypeBoolean {
			v, ok := colVal.AsBoolean()
			if ok {
				return v != e.BoolValue
			}
			return false
		}

		// Float inequality
		if e.ValueType == storage.TypeFloat && colVal.Type() == storage.TypeFloat {
			v, ok := colVal.AsFloat64()
			if ok {
				return v != e.Float64Value
			}
			return false
		}

		// Timestamp inequality
		if e.ValueType == storage.TypeTimestamp && colVal.Type() == storage.TypeTimestamp {
			v, ok := colVal.AsTimestamp()
			if ok {
				return !v.Equal(e.TimeValue)
			}
			return false
		}

		// Mixed numeric comparisons
		if (e.ValueType == storage.TypeInteger || e.ValueType == storage.TypeFloat) &&
			(colVal.Type() == storage.TypeInteger || colVal.Type() == storage.TypeFloat) {

			var colFloat float64

			if colVal.Type() == storage.TypeInteger {
				if v, ok := colVal.AsInt64(); ok {
					colFloat = float64(v)
				} else {
					return false
				}
			} else {
				if v, ok := colVal.AsFloat64(); ok {
					colFloat = v
				} else {
					return false
				}
			}

			var exprFloat float64
			if e.ValueType == storage.TypeInteger {
				exprFloat = float64(e.Int64Value)
			} else {
				exprFloat = e.Float64Value
			}

			return colFloat != exprFloat
		}
	}

	// At this point, we weren't able to use any of our optimized paths,
	// so fall back to the standard implementation for other cases
	result, _ := e.evaluateComparison(colVal)
	return result
}

// PrimaryKeyDetector is a helper function to check if an expression is a primary key operation
func PrimaryKeyDetector(expr storage.Expression, schema storage.Schema) (*SimpleExpression, bool) {
	// Fast exit if no expression
	if expr == nil {
		return nil, false
	}

	// Find the primary key column
	var pkColName string
	var pkColIndex int = -1

	for i, col := range schema.Columns {
		if col.PrimaryKey {
			pkColName = col.Name
			pkColIndex = i
			break
		}
	}

	// If no primary key found, can't optimize
	if pkColName == "" {
		return nil, false
	}

	// Check if it's a SimpleExpression
	if simpleExpr, ok := expr.(*SimpleExpression); ok {
		// Only for primary key column
		if simpleExpr.Column != pkColName {
			return nil, false
		}

		// For supported operators
		if simpleExpr.Operator != storage.EQ &&
			simpleExpr.Operator != storage.NE &&
			simpleExpr.Operator != storage.GT &&
			simpleExpr.Operator != storage.GTE &&
			simpleExpr.Operator != storage.LT &&
			simpleExpr.Operator != storage.LTE {
			return nil, false
		}

		simpleExpr.ColIndex = pkColIndex
		simpleExpr.IndexPrepped = true

		return simpleExpr, true
	}

	return nil, false
}
