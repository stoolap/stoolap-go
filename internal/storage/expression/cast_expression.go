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
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// CastExpression represents a CAST(column AS type) expression
type CastExpression struct {
	Column     string            // The column name to cast
	TargetType storage.DataType  // The target data type to cast to
	aliases    map[string]string // Column aliases (alias -> original name)

	// Schema optimization fields
	ColIndex     int  // Column index for fast lookup
	IndexPrepped bool // Whether we've prepared column index mapping
}

// NewCastExpression creates a new cast expression
func NewCastExpression(column string, targetType storage.DataType) *CastExpression {
	return &CastExpression{
		Column:     column,
		TargetType: targetType,
		aliases:    make(map[string]string),
	}
}

// Evaluate implements the Expression interface for CastExpression
func (ce *CastExpression) Evaluate(row storage.Row) (bool, error) {
	// Check if we've been optimized by schema preparation
	if ce.IndexPrepped && ce.ColIndex >= 0 && ce.ColIndex < len(row) {
		// Column found by index, get the value
		colValue := row[ce.ColIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL values remain NULL after CAST
			return false, nil
		}

		// Perform the cast operation
		_, err := ce.PerformCast(colValue)
		if err != nil {
			return false, err
		}

		// CAST expression by itself should not filter rows
		// When used in a comparison, the parent expression will handle the comparison
		return true, nil
	}

	// If column index not prepared, simply return false
	// We don't want to do expensive column name lookups or interface calls
	return false, nil
}

// WithAliases implements the storage.Expression interface
func (ce *CastExpression) WithAliases(aliases map[string]string) storage.Expression {
	result := &CastExpression{
		Column:     ce.Column,
		TargetType: ce.TargetType,
		aliases:    make(map[string]string),
	}

	// Copy aliases
	for k, v := range aliases {
		result.aliases[k] = v
	}

	// Apply aliases to column name
	if origCol, ok := aliases[ce.Column]; ok {
		result.Column = origCol
	}

	return result
}

// PrepareForSchema optimizes the expression for a specific schema
func (ce *CastExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already prepped with valid index, don't redo the work
	if ce.IndexPrepped && ce.ColIndex >= 0 {
		return ce
	}

	// Try to find the column in the schema
	for i, col := range schema.Columns {
		if col.Name == ce.Column || strings.EqualFold(col.Name, ce.Column) {
			ce.ColIndex = i
			break
		}
	}

	ce.IndexPrepped = true
	return ce
}

// EvaluateFast implements the Expression interface for fast evaluation
func (ce *CastExpression) EvaluateFast(row storage.Row) bool {
	// Check if we've been optimized by schema preparation
	if ce.IndexPrepped && ce.ColIndex >= 0 && ce.ColIndex < len(row) {
		// Column found by index, get the value
		colValue := row[ce.ColIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL values remain NULL after CAST
			return false
		}

		// Perform the cast operation
		_, err := ce.PerformCast(colValue)
		if err != nil {
			return false
		}

		// CAST expression by itself should not filter rows
		// When used in a comparison, the parent expression will handle the comparison
		return true
	}

	// If column index not prepared, simply return false
	// We don't want to do expensive column name lookups or interface calls
	return false
}

// GetColumnName returns the column name this expression operates on
func (ce *CastExpression) GetColumnName() string {
	return ce.Column
}

// GetTargetType returns the target data type for the cast
func (ce *CastExpression) GetTargetType() storage.DataType {
	return ce.TargetType
}

// PerformCast performs the actual cast operation on a column value
func (ce *CastExpression) PerformCast(value storage.ColumnValue) (storage.ColumnValue, error) {
	if value == nil || value.IsNull() {
		return storage.NewNullValue(ce.TargetType), nil
	}

	switch ce.TargetType {
	case storage.INTEGER:
		return castToInteger(value)
	case storage.FLOAT:
		return castToFloat(value)
	case storage.TEXT:
		return castToString(value)
	case storage.BOOLEAN:
		return castToBoolean(value)
	case storage.TIMESTAMP:
		return castToTimestamp(value)
	case storage.JSON:
		return castToJSON(value)
	default:
		return nil, fmt.Errorf("unsupported cast target type: %v", ce.TargetType)
	}
}

// CompoundExpression represents a CAST(column AS type) operator value expression
// This is used to handle WHERE clauses with CAST like: WHERE CAST(column AS INTEGER) > 100
type CompoundExpression struct {
	CastExpr *CastExpression   // The CAST expression
	Operator storage.Operator  // The comparison operator
	Value    interface{}       // The value to compare against
	aliases  map[string]string // Column aliases (alias -> original)

	// Schema optimization
	isOptimized bool // Indicates if this expression has already been prepared for a schema
}

// Evaluate implements the Expression interface for CompoundExpression
func (ce *CompoundExpression) Evaluate(row storage.Row) (bool, error) {
	// Fast path is only possible if the CastExpr is optimized
	if ce.CastExpr.IndexPrepped && ce.CastExpr.ColIndex >= 0 && ce.CastExpr.ColIndex < len(row) {
		colValue := row[ce.CastExpr.ColIndex]

		if colValue == nil || colValue.IsNull() {
			// NULL values always return false for comparisons
			return false, nil
		}

		// Cast the column value to the target type
		castedValue, err := ce.CastExpr.PerformCast(colValue)
		if err != nil {
			return false, err
		}

		// Now compare the casted value with the comparison value
		// Convert literal value to column value for comparison
		var comparisonValue storage.ColumnValue

		switch ce.CastExpr.TargetType {
		case storage.INTEGER:
			// Convert comparison value to integer
			var intVal int64
			switch v := ce.Value.(type) {
			case int:
				intVal = int64(v)
			case int64:
				intVal = v
			case float64:
				intVal = int64(v)
			case string:
				if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
					intVal = parsed
				} else {
					return false, fmt.Errorf("cannot convert comparison value %v to INTEGER", ce.Value)
				}
			default:
				return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
			}
			comparisonValue = storage.NewIntegerValue(intVal)

		case storage.FLOAT:
			// Convert comparison value to float
			var floatVal float64
			switch v := ce.Value.(type) {
			case int:
				floatVal = float64(v)
			case int64:
				floatVal = float64(v)
			case float64:
				floatVal = v
			case string:
				if parsed, err := strconv.ParseFloat(v, 64); err == nil {
					floatVal = parsed
				} else {
					return false, fmt.Errorf("cannot convert comparison value %v to FLOAT", ce.Value)
				}
			default:
				return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
			}
			comparisonValue = storage.NewFloatValue(floatVal)

		case storage.TEXT:
			// Convert comparison value to string
			var strVal string
			switch v := ce.Value.(type) {
			case string:
				strVal = v
			default:
				strVal = fmt.Sprintf("%v", ce.Value)
			}
			comparisonValue = storage.NewStringValue(strVal)

		case storage.BOOLEAN:
			// Convert comparison value to boolean
			var boolVal bool
			switch v := ce.Value.(type) {
			case bool:
				boolVal = v
			case int:
				boolVal = v != 0
			case int64:
				boolVal = v != 0
			case float64:
				boolVal = v != 0
			case string:
				s := strings.ToLower(v)
				boolVal = s == "true" || s == "1" || s == "t" || s == "yes" || s == "y"
			default:
				return false, fmt.Errorf("unsupported comparison value type: %T", ce.Value)
			}
			comparisonValue = storage.NewBooleanValue(boolVal)

		default:
			// For other types, just use string conversion as fallback
			comparisonValue = storage.NewStringValue(fmt.Sprintf("%v", ce.Value))
		}

		// Compare the values based on the operator
		var result bool
		switch ce.Operator {
		case storage.EQ:
			result = compareColumnValues(castedValue, comparisonValue) == 0
		case storage.NE:
			result = compareColumnValues(castedValue, comparisonValue) != 0
		case storage.GT:
			result = compareColumnValues(castedValue, comparisonValue) > 0
		case storage.GTE:
			result = compareColumnValues(castedValue, comparisonValue) >= 0
		case storage.LT:
			result = compareColumnValues(castedValue, comparisonValue) < 0
		case storage.LTE:
			result = compareColumnValues(castedValue, comparisonValue) <= 0
		default:
			return false, fmt.Errorf("unsupported operator: %v", ce.Operator)
		}

		return result, nil
	}

	// If column index not prepared, simply return false
	return false, nil
}

// WithAliases implements the storage.Expression interface
func (ce *CompoundExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a new expression with the same properties
	result := &CompoundExpression{
		CastExpr: ce.CastExpr,
		Operator: ce.Operator,
		Value:    ce.Value,
		aliases:  make(map[string]string),
	}

	// Apply aliases to the cast expression
	if castWithAliases, ok := ce.CastExpr.WithAliases(aliases).(*CastExpression); ok {
		result.CastExpr = castWithAliases
	}

	// Copy aliases
	for k, v := range aliases {
		result.aliases[k] = v
	}

	return result
}

// PrepareForSchema optimizes the expression for a specific schema
func (ce *CompoundExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
	// If already optimized, don't redo the work
	if ce.isOptimized {
		return ce
	}

	// Optimize the cast expression
	if preparedCast, ok := ce.CastExpr.PrepareForSchema(schema).(*CastExpression); ok {
		ce.CastExpr = preparedCast
	}

	ce.isOptimized = true
	return ce
}

// EvaluateFast implements the Expression interface for fast evaluation
func (ce *CompoundExpression) EvaluateFast(row storage.Row) bool {
	// Fast path is only possible if the CastExpr is optimized
	if ce.CastExpr.IndexPrepped && ce.CastExpr.ColIndex >= 0 && ce.CastExpr.ColIndex < len(row) {
		colValue := row[ce.CastExpr.ColIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL comparisons are always false in SQL
			return false
		}

		// Compare based on the target type and operator
		switch ce.CastExpr.TargetType {
		case storage.INTEGER:
			// Cast column value to integer and compare
			var colInt int64
			switch colValue.Type() {
			case storage.INTEGER:
				var ok bool
				colInt, ok = colValue.AsInt64()
				if !ok {
					return false
				}
			case storage.FLOAT:
				var f float64
				var ok bool
				f, ok = colValue.AsFloat64()
				if !ok {
					return false
				}
				colInt = int64(f)
			case storage.BOOLEAN:
				var b bool
				var ok bool
				b, ok = colValue.AsBoolean()
				if !ok {
					return false
				}
				if b {
					colInt = 1
				} else {
					colInt = 0
				}
			default:
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Get the comparison value
			var compInt int64
			switch v := ce.Value.(type) {
			case int:
				compInt = int64(v)
			case int64:
				compInt = v
			case float64:
				compInt = int64(v)
			default:
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Compare with the operator
			switch ce.Operator {
			case storage.EQ:
				return colInt == compInt
			case storage.NE:
				return colInt != compInt
			case storage.GT:
				return colInt > compInt
			case storage.GTE:
				return colInt >= compInt
			case storage.LT:
				return colInt < compInt
			case storage.LTE:
				return colInt <= compInt
			default:
				// Not a supported operator, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

		case storage.FLOAT:
			// Cast column value to float and compare
			var colFloat float64
			switch colValue.Type() {
			case storage.INTEGER:
				var i int64
				var ok bool
				i, ok = colValue.AsInt64()
				if !ok {
					return false
				}
				colFloat = float64(i)
			case storage.FLOAT:
				var ok bool
				colFloat, ok = colValue.AsFloat64()
				if !ok {
					return false
				}
			case storage.BOOLEAN:
				var b bool
				var ok bool
				b, ok = colValue.AsBoolean()
				if !ok {
					return false
				}
				if b {
					colFloat = 1.0
				} else {
					colFloat = 0.0
				}
			default:
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Get the comparison value
			var compFloat float64
			switch v := ce.Value.(type) {
			case int:
				compFloat = float64(v)
			case int64:
				compFloat = float64(v)
			case float64:
				compFloat = v
			default:
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Compare with the operator
			switch ce.Operator {
			case storage.EQ:
				return colFloat == compFloat
			case storage.NE:
				return colFloat != compFloat
			case storage.GT:
				return colFloat > compFloat
			case storage.GTE:
				return colFloat >= compFloat
			case storage.LT:
				return colFloat < compFloat
			case storage.LTE:
				return colFloat <= compFloat
			default:
				// Not a supported operator, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

		case storage.TEXT:
			// Cast column value to string and compare
			var colStr string
			var ok bool
			colStr, ok = colValue.AsString()
			if !ok {
				return false
			}

			// Get the comparison value
			compStr, ok := ce.Value.(string)
			if !ok {
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Compare with the operator
			switch ce.Operator {
			case storage.EQ:
				return colStr == compStr
			case storage.NE:
				return colStr != compStr
			case storage.GT:
				return colStr > compStr
			case storage.GTE:
				return colStr >= compStr
			case storage.LT:
				return colStr < compStr
			case storage.LTE:
				return colStr <= compStr
			default:
				// Not a supported operator, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

		case storage.BOOLEAN:
			// Cast column value to boolean and compare
			var colBool bool
			switch colValue.Type() {
			case storage.INTEGER:
				var i int64
				var ok bool
				i, ok = colValue.AsInt64()
				if !ok {
					return false
				}
				colBool = i != 0
			case storage.FLOAT:
				var f float64
				var ok bool
				f, ok = colValue.AsFloat64()
				if !ok {
					return false
				}
				colBool = f != 0
			case storage.BOOLEAN:
				var ok bool
				colBool, ok = colValue.AsBoolean()
				if !ok {
					return false
				}
			default:
				// Not one of our fast-path types, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

			// Get the comparison value
			compBool, ok := ce.Value.(bool)
			if !ok {
				// Try to convert int/float to bool
				switch v := ce.Value.(type) {
				case int:
					compBool = v != 0
				case int64:
					compBool = v != 0
				case float64:
					compBool = v != 0
				default:
					// Not one of our fast-path types, return false
					// We don't want to do expensive fallbacks to slow path
					return false
				}
			}

			// For booleans, only EQ and NE make sense
			switch ce.Operator {
			case storage.EQ:
				return colBool == compBool
			case storage.NE:
				return colBool != compBool
			default:
				// Not a supported operator, return false
				// We don't want to do expensive fallbacks to slow path
				return false
			}

		default:
			// Not one of our fast-path types, return false
			// We don't want to do expensive fallbacks to slow path
			return false
		}
	}

	// If not schema-prepared or column index not found, simply return false
	// We don't want to do expensive fallbacks to the slow path Evaluate method
	return false
}

// compareColumnValues compares two column values
// Returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
func compareColumnValues(a, b storage.ColumnValue) int {
	// Handle NULL values
	if a == nil || a.IsNull() || b == nil || b.IsNull() {
		// NULL is considered equal only to NULL
		if (a == nil || a.IsNull()) && (b == nil || b.IsNull()) {
			return 0
		}
		// NULL is less than everything else
		if a == nil || a.IsNull() {
			return -1
		}
		return 1
	}

	// Compare based on types
	aType := a.Type()
	bType := b.Type()

	// If types are the same, compare directly
	if aType == bType {
		switch aType {
		case storage.INTEGER:
			aVal, _ := a.AsInt64()
			bVal, _ := b.AsInt64()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.FLOAT:
			aVal, _ := a.AsFloat64()
			bVal, _ := b.AsFloat64()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.TEXT:
			aVal, _ := a.AsString()
			bVal, _ := b.AsString()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0

		case storage.BOOLEAN:
			aVal, _ := a.AsBoolean()
			bVal, _ := b.AsBoolean()
			if !aVal && bVal {
				return -1
			} else if aVal && !bVal {
				return 1
			}
			return 0

		case storage.TIMESTAMP:
			aVal, _ := a.AsTimestamp()
			bVal, _ := b.AsTimestamp()
			if aVal.Before(bVal) {
				return -1
			} else if aVal.After(bVal) {
				return 1
			}
			return 0

		default:
			// Fallback to string comparison
			aVal, _ := a.AsString()
			bVal, _ := b.AsString()
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	}

	// Types differ, try numeric comparison for numeric types
	if (aType == storage.INTEGER || aType == storage.FLOAT) &&
		(bType == storage.INTEGER || bType == storage.FLOAT) {
		// Convert both to float for comparison
		aVal, _ := a.AsFloat64()
		bVal, _ := b.AsFloat64()
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}

	// Fallback to string comparison
	aStr, _ := a.AsString()
	bStr, _ := b.AsString()
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// Cast helpers

func castToInteger(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Already an integer, return as is
		i, _ := value.AsInt64()
		return storage.NewIntegerValue(i), nil
	case storage.FLOAT:
		// Convert float to integer
		f, _ := value.AsFloat64()
		return storage.NewIntegerValue(int64(f)), nil
	case storage.TEXT:
		// Parse string to integer
		s, _ := value.AsString()
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// If string can't be parsed as int, try to parse as float first
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return storage.NewIntegerValue(int64(f)), nil
			}
			// Return 0 for invalid string to integer conversions
			return storage.NewIntegerValue(0), nil
		}
		return storage.NewIntegerValue(i), nil
	case storage.BOOLEAN:
		// Convert boolean to integer (true=1, false=0)
		b, _ := value.AsBoolean()
		if b {
			return storage.NewIntegerValue(1), nil
		}
		return storage.NewIntegerValue(0), nil
	case storage.TIMESTAMP:
		// Convert timestamp to Unix timestamp (seconds since epoch)
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewIntegerValue(0), nil
		}
		return storage.NewIntegerValue(t.Unix()), nil
	default:
		// Default to 0 for unsupported types
		return storage.NewIntegerValue(0), nil
	}
}

func castToFloat(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Convert integer to float
		i, _ := value.AsInt64()
		return storage.NewFloatValue(float64(i)), nil
	case storage.FLOAT:
		// Already a float, return as is
		f, _ := value.AsFloat64()
		return storage.NewFloatValue(f), nil
	case storage.TEXT:
		// Parse string to float
		s, _ := value.AsString()
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			// Return 0.0 for invalid string to float conversions
			return storage.NewFloatValue(0.0), nil
		}
		return storage.NewFloatValue(f), nil
	case storage.BOOLEAN:
		// Convert boolean to float (true=1.0, false=0.0)
		b, _ := value.AsBoolean()
		if b {
			return storage.NewFloatValue(1.0), nil
		}
		return storage.NewFloatValue(0.0), nil
	case storage.TIMESTAMP:
		// Convert timestamp to Unix timestamp as float
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewFloatValue(0.0), nil
		}
		return storage.NewFloatValue(float64(t.Unix())), nil
	default:
		// Default to 0.0 for unsupported types
		return storage.NewFloatValue(0.0), nil
	}
}

func castToString(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Convert integer to string
		i, _ := value.AsInt64()
		return storage.NewStringValue(strconv.FormatInt(i, 10)), nil
	case storage.FLOAT:
		// Convert float to string
		f, _ := value.AsFloat64()
		return storage.NewStringValue(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case storage.TEXT:
		// Already a string, return as is
		s, _ := value.AsString()
		return storage.NewStringValue(s), nil
	case storage.BOOLEAN:
		// Convert boolean to string
		b, _ := value.AsBoolean()
		if b {
			return storage.NewStringValue("true"), nil
		}
		return storage.NewStringValue("false"), nil
	case storage.TIMESTAMP:
		// Format timestamp as string (ISO format)
		t, ok := value.AsTimestamp()
		if !ok {
			return storage.NewStringValue(""), nil
		}
		return storage.NewStringValue(t.Format(time.RFC3339)), nil
	case storage.JSON:
		// JSON as string (the raw JSON string)
		s, _ := value.AsString()
		return storage.NewStringValue(s), nil
	default:
		// Default to empty string for unsupported types
		return storage.NewStringValue(""), nil
	}
}

func castToBoolean(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// 0 is false, anything else is true
		i, _ := value.AsInt64()
		return storage.NewBooleanValue(i != 0), nil
	case storage.FLOAT:
		// 0.0 is false, anything else is true
		f, _ := value.AsFloat64()
		return storage.NewBooleanValue(f != 0.0), nil
	case storage.TEXT:
		// Parse string to boolean
		s, _ := value.AsString()
		s = strings.ToLower(s)
		return storage.NewBooleanValue(s == "true" || s == "1" || s == "t" || s == "yes" || s == "y"), nil
	case storage.BOOLEAN:
		// Already a boolean, return as is
		b, _ := value.AsBoolean()
		return storage.NewBooleanValue(b), nil
	default:
		// Default to false for unsupported types
		return storage.NewBooleanValue(false), nil
	}
}

func castToTimestamp(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.INTEGER:
		// Interpret integer as Unix timestamp
		i, _ := value.AsInt64()
		return storage.NewTimestampValue(time.Unix(i, 0)), nil
	case storage.FLOAT:
		// Interpret float as Unix timestamp
		f, _ := value.AsFloat64()
		return storage.NewTimestampValue(time.Unix(int64(f), 0)), nil
	case storage.TIMESTAMP:
		// Already a timestamp, return as is
		t, _ := value.AsTimestamp()
		return storage.NewTimestampValue(t), nil
	case storage.TEXT:
		// Parse string to timestamp
		s, _ := value.AsString()
		if t, err := storage.ParseTimestamp(s); err == nil {
			return storage.NewTimestampValue(t), nil
		}
		// Default to current timestamp if parsing fails
		return storage.NewTimestampValue(time.Now()), nil
	default:
		// Default to current timestamp for unsupported types
		return storage.NewTimestampValue(time.Now()), nil
	}
}

func castToJSON(value storage.ColumnValue) (storage.ColumnValue, error) {
	switch value.Type() {
	case storage.JSON:
		// Already JSON, return as is
		j, _ := value.AsString()
		return storage.NewJSONValue(j), nil
	case storage.TEXT:
		// String to JSON (assuming the string is valid JSON)
		s, _ := value.AsString()
		return storage.NewJSONValue(s), nil
	default:
		// For other types, convert to string and wrap as JSON string
		s := ""
		switch value.Type() {
		case storage.INTEGER:
			i, _ := value.AsInt64()
			s = strconv.FormatInt(i, 10)
		case storage.FLOAT:
			f, _ := value.AsFloat64()
			s = strconv.FormatFloat(f, 'f', -1, 64)
		case storage.BOOLEAN:
			b, _ := value.AsBoolean()
			if b {
				s = "true"
			} else {
				s = "false"
			}
		default:
			s = "null"
		}
		return storage.NewJSONValue(s), nil
	}
}
