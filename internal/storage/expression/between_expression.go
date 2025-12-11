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

// BetweenExpression represents an expression that checks if a column value is between a lower and upper bound
type BetweenExpression struct {
	Column     string
	LowerBound interface{}
	UpperBound interface{}
	Inclusive  bool

	aliases        map[string]string // Column aliases (alias -> original)
	originalColumn string            // Original column name if Column is an alias

	// Schema optimization fields
	ColIndex     int  // Column index for fast lookup
	IndexPrepped bool // Whether we've prepared column index mapping
}

func NewBetweenExpression(column string, lowerBound, upperBound interface{}, inclusive bool) *BetweenExpression {
	return &BetweenExpression{
		Column:     column,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		Inclusive:  inclusive,
	}
}

// Evaluate implements the Expression interface for BetweenExpression
func (e *BetweenExpression) Evaluate(row storage.Row) (bool, error) {
	// Only use prepared column indices, don't try to find columns by name
	if !e.IndexPrepped || e.ColIndex < 0 || e.ColIndex >= len(row) {
		return false, nil
	}

	// Get the column value
	colValue := row[e.ColIndex]
	if colValue == nil {
		// NULL BETWEEN ... is always false
		return false, nil
	}

	// Try to use the appropriate type comparison based on column type
	switch colValue.Type() {
	case storage.TypeInteger:
		// For integers, convert and compare numerically
		colInt, ok := colValue.AsInt64()
		if !ok {
			return false, nil
		}

		// Convert lower bound to int64
		var lowerInt int64
		switch v := e.LowerBound.(type) {
		case int:
			lowerInt = int64(v)
		case int64:
			lowerInt = v
		case float64:
			lowerInt = int64(v)
		case string:
			// Try to parse as int
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				lowerInt = parsed
			} else {
				return false, fmt.Errorf("cannot convert lower bound to int64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to int64
		var upperInt int64
		switch v := e.UpperBound.(type) {
		case int:
			upperInt = int64(v)
		case int64:
			upperInt = v
		case float64:
			upperInt = int64(v)
		case string:
			// Try to parse as int
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				upperInt = parsed
			} else {
				return false, fmt.Errorf("cannot convert upper bound to int64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colInt >= lowerInt && colInt <= upperInt, nil
		}
		return colInt > lowerInt && colInt < upperInt, nil

	case storage.TypeFloat:
		// For floats, convert and compare numerically
		colFloat, ok := colValue.AsFloat64()
		if !ok {
			return false, nil
		}

		// Convert lower bound to float64
		var lowerFloat float64
		switch v := e.LowerBound.(type) {
		case int:
			lowerFloat = float64(v)
		case int64:
			lowerFloat = float64(v)
		case float64:
			lowerFloat = v
		case string:
			// Try to parse as float
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				lowerFloat = parsed
			} else {
				return false, fmt.Errorf("cannot convert lower bound to float64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to float64
		var upperFloat float64
		switch v := e.UpperBound.(type) {
		case int:
			upperFloat = float64(v)
		case int64:
			upperFloat = float64(v)
		case float64:
			upperFloat = v
		case string:
			// Try to parse as float
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				upperFloat = parsed
			} else {
				return false, fmt.Errorf("cannot convert upper bound to float64: %s", v)
			}
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colFloat >= lowerFloat && colFloat <= upperFloat, nil
		}
		return colFloat > lowerFloat && colFloat < upperFloat, nil

	case storage.TypeString:
		// For strings, compare lexicographically
		colStr, ok := colValue.AsString()
		if !ok {
			return false, nil
		}

		// Convert lower bound to string
		var lowerStr string
		switch v := e.LowerBound.(type) {
		case string:
			lowerStr = v
		default:
			lowerStr = fmt.Sprintf("%v", e.LowerBound)
		}

		// Convert upper bound to string
		var upperStr string
		switch v := e.UpperBound.(type) {
		case string:
			upperStr = v
		default:
			upperStr = fmt.Sprintf("%v", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return colStr >= lowerStr && colStr <= upperStr, nil
		}
		return colStr > lowerStr && colStr < upperStr, nil

	case storage.TypeTimestamp:
		// For timestamp values, convert and compare
		colTime, ok := colValue.AsTimestamp()
		if !ok {
			return false, nil
		}

		// Convert lower bound to time.Time
		var lowerTime time.Time
		switch v := e.LowerBound.(type) {
		case time.Time:
			lowerTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTimestamp(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse lower bound as time: %s", v)
			}
			lowerTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for lower bound: %T", e.LowerBound)
		}

		// Convert upper bound to time.Time
		var upperTime time.Time
		switch v := e.UpperBound.(type) {
		case time.Time:
			upperTime = v
		case string:
			// Try different time formats
			parsed, err := storage.ParseTimestamp(v)
			if err != nil {
				return false, fmt.Errorf("cannot parse upper bound as time: %s", v)
			}
			upperTime = parsed
		default:
			return false, fmt.Errorf("unsupported type for upper bound: %T", e.UpperBound)
		}

		// Compare with bounds
		if e.Inclusive {
			return (colTime.Equal(lowerTime) || colTime.After(lowerTime)) &&
				(colTime.Equal(upperTime) || colTime.Before(upperTime)), nil
		}
		return colTime.After(lowerTime) && colTime.Before(upperTime), nil

	default:
		// For other types, just return false
		return false, fmt.Errorf("unsupported column type for BETWEEN: %v", colValue.Type())
	}
}

// GetColumnName implements IndexableExpression interface
func (e *BetweenExpression) GetColumnName() string {
	return e.Column
}

// GetValue implements IndexableExpression interface
// For BETWEEN expressions, we return the lower bound since that's typically used for index scanning
func (e *BetweenExpression) GetValue() interface{} {
	return e.LowerBound
}

// GetOperator implements IndexableExpression interface
// For BETWEEN expressions, we return GTE as the operator
// (since we'll start scanning from the lower bound)
func (e *BetweenExpression) GetOperator() storage.Operator {
	return storage.GTE
}

// CanUseIndex implements IndexableExpression interface
func (e *BetweenExpression) CanUseIndex() bool {
	return true
}

// WithAliases implements the Expression interface for BetweenExpression
func (e *BetweenExpression) WithAliases(aliases map[string]string) storage.Expression {
	// Create a copy of the expression with aliases
	expr := &BetweenExpression{
		Column:     e.Column,
		LowerBound: e.LowerBound,
		UpperBound: e.UpperBound,
		Inclusive:  e.Inclusive,
		aliases:    aliases,
	}

	// If the column name is an alias, resolve it to the original column name
	if originalCol, isAlias := aliases[e.Column]; isAlias {
		expr.originalColumn = e.Column // Keep track of the original alias name
		expr.Column = originalCol      // Replace with the actual column name
	}

	return expr
}

// PrepareForSchema optimizes the expression for a specific schema
func (e *BetweenExpression) PrepareForSchema(schema storage.Schema) storage.Expression {
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
func (e *BetweenExpression) EvaluateFast(row storage.Row) bool {
	// If we've prepped with the schema, use the column index for direct access
	if e.IndexPrepped && e.ColIndex >= 0 && e.ColIndex < len(row) {
		colValue := row[e.ColIndex]
		if colValue == nil {
			// NULL BETWEEN ... is always false
			return false
		}

		// Fast path evaluations based on type
		switch colValue.Type() {
		case storage.TypeInteger:
			colInt, ok := colValue.AsInt64()
			if !ok {
				return false
			}

			// Convert bounds to int64 (assuming the bounds are compatible)
			var lowerInt, upperInt int64

			switch v := e.LowerBound.(type) {
			case int:
				lowerInt = int64(v)
			case int64:
				lowerInt = v
			case float64:
				lowerInt = int64(v)
			default:
				return false // Not comparable in fast path
			}

			switch v := e.UpperBound.(type) {
			case int:
				upperInt = int64(v)
			case int64:
				upperInt = v
			case float64:
				upperInt = int64(v)
			default:
				return false // Not comparable in fast path
			}

			// Compare with bounds
			if e.Inclusive {
				return colInt >= lowerInt && colInt <= upperInt
			}
			return colInt > lowerInt && colInt < upperInt

		case storage.TypeFloat:
			colFloat, ok := colValue.AsFloat64()
			if !ok {
				return false
			}

			// Convert bounds to float64 (assuming the bounds are compatible)
			var lowerFloat, upperFloat float64

			switch v := e.LowerBound.(type) {
			case int:
				lowerFloat = float64(v)
			case int64:
				lowerFloat = float64(v)
			case float64:
				lowerFloat = v
			default:
				return false // Not comparable in fast path
			}

			switch v := e.UpperBound.(type) {
			case int:
				upperFloat = float64(v)
			case int64:
				upperFloat = float64(v)
			case float64:
				upperFloat = v
			default:
				return false // Not comparable in fast path
			}

			// Compare with bounds
			if e.Inclusive {
				return colFloat >= lowerFloat && colFloat <= upperFloat
			}
			return colFloat > lowerFloat && colFloat < upperFloat

		case storage.TypeString:
			colStr, ok := colValue.AsString()
			if !ok {
				return false
			}

			// For strings, we only handle string bounds in fast path
			lowerStr, lowerOk := e.LowerBound.(string)
			upperStr, upperOk := e.UpperBound.(string)

			if !lowerOk || !upperOk {
				return false // Not string bounds, can't use fast path
			}

			// Compare with bounds
			if e.Inclusive {
				return colStr >= lowerStr && colStr <= upperStr
			}
			return colStr > lowerStr && colStr < upperStr

		case storage.TypeTimestamp:
			colTime, ok := colValue.AsTimestamp()
			if !ok {
				return false
			}

			// For timestamps, we only handle time.Time bounds in fast path
			lowerTime, lowerOk := e.LowerBound.(time.Time)
			upperTime, upperOk := e.UpperBound.(time.Time)

			if !lowerOk || !upperOk {
				return false // Not time.Time bounds, can't use fast path
			}

			// Compare with bounds
			if e.Inclusive {
				return (colTime.Equal(lowerTime) || colTime.After(lowerTime)) &&
					(colTime.Equal(upperTime) || colTime.Before(upperTime))
			}
			return colTime.After(lowerTime) && colTime.Before(upperTime)
		}
	}

	// Fallback: If we can't use the fast path, evaluate "slowly" but without error handling
	res, _ := e.Evaluate(row)
	return res
}
