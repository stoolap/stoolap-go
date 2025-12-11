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

// RangeExpression represents an expression that checks if a column value is within a range
// with custom inclusivity flags. This is used for optimizing patterns like:
// "column > min AND column <= max" into a single expression
type RangeExpression struct {
	Column     string
	MinValue   interface{} // Minimum (lower) bound
	MaxValue   interface{} // Maximum (upper) bound
	IncludeMin bool        // Whether to include the minimum bound (>= vs >)
	IncludeMax bool        // Whether to include the maximum bound (<= vs <)

	// Pre-computed type-specific bounds for faster evaluation
	// Integer bounds
	Int64Min int64
	Int64Max int64

	// Float bounds
	Float64Min float64
	Float64Max float64

	// String bounds
	StringMin string
	StringMax string

	// Time bounds
	TimeMin time.Time
	TimeMax time.Time

	// Value type for optimization
	DataType storage.DataType

	// Schema and alias handling
	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias
	ColIndex       int               // Column index for fast lookup
	IndexPrepped   bool              // Whether we've prepared column index mapping
}

// NewRangeExpression creates a new range expression with custom inclusivity flags
func NewRangeExpression(column string, minValue, maxValue interface{}, includeMin, includeMax bool) *RangeExpression {
	expr := &RangeExpression{
		Column:     column,
		MinValue:   minValue,
		MaxValue:   maxValue,
		IncludeMin: includeMin,
		IncludeMax: includeMax,
		ColIndex:   -1, // Not prepared yet
	}

	// Pre-compute type-specific values
	expr.computeTypedBounds()

	return expr
}

// computeTypedBounds pre-computes type-specific bounds for faster evaluation
func (e *RangeExpression) computeTypedBounds() {
	// Process minimum bound
	if e.MinValue != nil {
		switch v := e.MinValue.(type) {
		case int:
			e.Int64Min = int64(v)
			e.Float64Min = float64(v)
			e.DataType = storage.TypeInteger

		case int64:
			e.Int64Min = v
			e.Float64Min = float64(v)
			e.DataType = storage.TypeInteger

		case float64:
			e.Float64Min = v
			e.Int64Min = int64(v)
			e.DataType = storage.TypeFloat

		case string:
			e.StringMin = v
			e.DataType = storage.TypeString

			// Try to convert to number if it looks like one
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				e.Int64Min = intVal
				e.Float64Min = float64(intVal)
			} else if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				e.Float64Min = floatVal
				e.Int64Min = int64(floatVal)
			}

			// Try to convert to time if it looks like a timestamp
			if timeVal, err := storage.ParseTimestamp(v); err == nil {
				e.TimeMin = timeVal
				e.DataType = storage.TypeTimestamp
			}

		case time.Time:
			e.TimeMin = v
			e.DataType = storage.TypeTimestamp
		}
	}

	// Process maximum bound
	if e.MaxValue != nil {
		switch v := e.MaxValue.(type) {
		case int:
			e.Int64Max = int64(v)
			e.Float64Max = float64(v)
			if e.DataType == storage.NULL {
				e.DataType = storage.TypeInteger
			}

		case int64:
			e.Int64Max = v
			e.Float64Max = float64(v)
			if e.DataType == storage.NULL {
				e.DataType = storage.TypeInteger
			}

		case float64:
			e.Float64Max = v
			e.Int64Max = int64(v)
			if e.DataType == storage.NULL {
				e.DataType = storage.TypeFloat
			}

		case string:
			e.StringMax = v
			if e.DataType == storage.NULL {
				e.DataType = storage.TypeString
			}

			// Try to convert to number if it looks like one
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				e.Int64Max = intVal
				e.Float64Max = float64(intVal)
			} else if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				e.Float64Max = floatVal
				e.Int64Max = int64(floatVal)
			}

			// Try to convert to time if it looks like a timestamp
			if timeVal, err := storage.ParseTimestamp(v); err == nil {
				e.TimeMax = timeVal
				if e.DataType == storage.NULL {
					e.DataType = storage.TypeTimestamp
				}
			}

		case time.Time:
			e.TimeMax = v
			if e.DataType == storage.NULL {
				e.DataType = storage.TypeTimestamp
			}
		}
	}
}

// Evaluate implements the Expression interface for RangeExpression
func (e *RangeExpression) Evaluate(row storage.Row) (bool, error) {
	// Only use prepared column indices, don't try to find columns by name
	if !e.IndexPrepped || e.ColIndex < 0 || e.ColIndex >= len(row) {
		return false, nil
	}

	// Get the column value
	colValue := row[e.ColIndex]
	if colValue == nil || colValue.IsNull() {
		// NULL in range check is always false
		return false, nil
	}

	// Fast path using pre-computed values based on column type
	switch colValue.Type() {
	case storage.TypeInteger:
		// For integers, use pre-computed int bounds
		colInt, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Check minimum bound if available
		if e.IncludeMin {
			if colInt < e.Int64Min {
				return false, nil
			}
		} else {
			if colInt <= e.Int64Min {
				return false, nil
			}
		}

		// Check maximum bound if available
		if e.IncludeMax {
			if colInt > e.Int64Max {
				return false, nil
			}
		} else {
			if colInt >= e.Int64Max {
				return false, nil
			}
		}

		return true, nil

	case storage.TypeFloat:
		// For floats, use pre-computed float bounds
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Check minimum bound if available
		if e.IncludeMin {
			if colFloat < e.Float64Min {
				return false, nil
			}
		} else {
			if colFloat <= e.Float64Min {
				return false, nil
			}
		}

		// Check maximum bound if available
		if e.IncludeMax {
			if colFloat > e.Float64Max {
				return false, nil
			}
		} else {
			if colFloat >= e.Float64Max {
				return false, nil
			}
		}

		return true, nil

	case storage.TypeString:
		// For strings, use pre-computed string bounds
		colStr, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Check minimum bound if available
		if e.IncludeMin {
			if colStr < e.StringMin {
				return false, nil
			}
		} else {
			if colStr <= e.StringMin {
				return false, nil
			}
		}

		// Check maximum bound if available
		if e.IncludeMax {
			if colStr > e.StringMax {
				return false, nil
			}
		} else {
			if colStr >= e.StringMax {
				return false, nil
			}
		}

		return true, nil

	case storage.TypeTimestamp:
		// For timestamp values, use pre-computed time bounds
		colTime, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Check minimum bound if available
		if e.IncludeMin {
			// >= minimum (equal or after)
			if !(colTime.Equal(e.TimeMin) || colTime.After(e.TimeMin)) {
				return false, nil
			}
		} else {
			// > minimum (strictly after)
			if !colTime.After(e.TimeMin) {
				return false, nil
			}
		}

		// Check maximum bound if available
		if e.IncludeMax {
			// <= maximum (equal or before)
			if !(colTime.Equal(e.TimeMax) || colTime.Before(e.TimeMax)) {
				return false, nil
			}
		} else {
			// < maximum (strictly before)
			if !colTime.Before(e.TimeMax) {
				return false, nil
			}
		}

		return true, nil

	default:
		// For other types, just return false
		return false, fmt.Errorf("unsupported column type for range expression: %v", colValue.Type())
	}
}

// GetColumnName implements IndexableExpression interface
func (e *RangeExpression) GetColumnName() string {
	return e.Column
}

// GetValue implements IndexableExpression interface
// For range expressions, we return the minimum bound since that's typically used for index scanning
func (e *RangeExpression) GetValue() interface{} {
	return e.MinValue
}

// GetOperator implements IndexableExpression interface
// For range expressions, we return GTE or GT as the operator
// (since we'll start scanning from the minimum bound)
func (e *RangeExpression) GetOperator() storage.Operator {
	if e.IncludeMin {
		return storage.GTE
	}
	return storage.GT
}

// CanUseIndex implements IndexableExpression interface
func (e *RangeExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the Expression interface for RangeExpression
func (e *RangeExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &RangeExpression{
		// Basic properties
		Column:     e.Column,
		MinValue:   e.MinValue,
		MaxValue:   e.MaxValue,
		IncludeMin: e.IncludeMin,
		IncludeMax: e.IncludeMax,

		// Copy all pre-computed fields for performance
		DataType: e.DataType,

		// Integer bounds
		Int64Min: e.Int64Min,
		Int64Max: e.Int64Max,

		// Float bounds
		Float64Min: e.Float64Min,
		Float64Max: e.Float64Max,

		// String bounds
		StringMin: e.StringMin,
		StringMax: e.StringMax,

		// Time bounds
		TimeMin: e.TimeMin,
		TimeMax: e.TimeMax,

		// Alias handling
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
func (e *RangeExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
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

// EvaluateFast implements the Expression interface for fast evaluation
func (e *RangeExpression) EvaluateFast(row storage.Row) bool {
	// If we've prepped with the schema, use the column index for direct access
	if e.IndexPrepped && e.ColIndex >= 0 && e.ColIndex < len(row) {
		colValue := row[e.ColIndex]
		if colValue == nil || colValue.IsNull() {
			// NULL in range check is always false
			return false
		}

		// Fast path evaluations based on type
		switch colValue.Type() {
		case storage.TypeInteger:
			colInt, ok := colValue.AsInt64()
			if !ok {
				return false
			}

			// Check minimum bound if available
			if e.IncludeMin {
				if colInt < e.Int64Min {
					return false
				}
			} else {
				if colInt <= e.Int64Min {
					return false
				}
			}

			// Check maximum bound if available
			if e.IncludeMax {
				if colInt > e.Int64Max {
					return false
				}
			} else {
				if colInt >= e.Int64Max {
					return false
				}
			}

			return true

		case storage.TypeFloat:
			colFloat, ok := colValue.AsFloat64()
			if !ok {
				return false
			}

			// Check minimum bound if available
			if e.IncludeMin {
				if colFloat < e.Float64Min {
					return false
				}
			} else {
				if colFloat <= e.Float64Min {
					return false
				}
			}

			// Check maximum bound if available
			if e.IncludeMax {
				if colFloat > e.Float64Max {
					return false
				}
			} else {
				if colFloat >= e.Float64Max {
					return false
				}
			}

			return true

		case storage.TypeString:
			colStr, ok := colValue.AsString()
			if !ok {
				return false
			}

			// Check minimum bound if available
			if e.IncludeMin {
				if colStr < e.StringMin {
					return false
				}
			} else {
				if colStr <= e.StringMin {
					return false
				}
			}

			// Check maximum bound if available
			if e.IncludeMax {
				if colStr > e.StringMax {
					return false
				}
			} else {
				if colStr >= e.StringMax {
					return false
				}
			}

			return true

		case storage.TypeTimestamp:
			colTime, ok := colValue.AsTimestamp()
			if !ok {
				return false
			}

			// Check minimum bound if available
			if e.IncludeMin {
				// >= minimum (equal or after)
				if !(colTime.Equal(e.TimeMin) || colTime.After(e.TimeMin)) {
					return false
				}
			} else {
				// > minimum (strictly after)
				if !colTime.After(e.TimeMin) {
					return false
				}
			}

			// Check maximum bound if available
			if e.IncludeMax {
				// <= maximum (equal or before)
				if !(colTime.Equal(e.TimeMax) || colTime.Before(e.TimeMax)) {
					return false
				}
			} else {
				// < maximum (strictly before)
				if !colTime.Before(e.TimeMax) {
					return false
				}
			}

			return true
		}
	}

	return false
}
