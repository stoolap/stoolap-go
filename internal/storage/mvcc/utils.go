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
package mvcc

import (
	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// GetPKOperationInfo optimizes and analyzes a filter expression for primary key operations
// Returns information that can be used for fast path execution
// It's especially useful for range narrowing in expressions like "id > 10 AND id < 20"
func GetPKOperationInfo(expr storage.Expression, schema storage.Schema) []PKOperationInfo {
	// Default result with invalid state
	result := PKOperationInfo{
		ID:          0,
		Expr:        nil,
		Operator:    storage.EQ,
		Valid:       false,
		EmptyResult: false,
	}

	// Quick exit for nil expressions
	if expr == nil {
		return []PKOperationInfo{result}
	}

	// Handle BetweenExpression directly
	if betweenExpr, ok := expr.(*expression.BetweenExpression); ok {
		// Check if this is operating on a primary key column
		pkInfo := getOrCreatePKInfo(schema)
		isPKColumn := false
		for _, col := range schema.Columns {
			if col.Name == betweenExpr.Column && col.PrimaryKey {
				isPKColumn = true
				break
			}
		}

		if isPKColumn && pkInfo.pkType == storage.TypeInteger {
			// Extract bounds for integer PK
			var lowerBound, upperBound int64

			// Handle lower bound conversion based on type
			switch v := betweenExpr.LowerBound.(type) {
			case int:
				lowerBound = int64(v)
			case int64:
				lowerBound = v
			case float64:
				lowerBound = int64(v)
			default:
				// If we can't convert to int64, we can't optimize
				return []PKOperationInfo{result}
			}

			// Handle upper bound conversion based on type
			switch v := betweenExpr.UpperBound.(type) {
			case int:
				upperBound = int64(v)
			case int64:
				upperBound = v
			case float64:
				upperBound = int64(v)
			default:
				// If we can't convert to int64, we can't optimize
				return []PKOperationInfo{result}
			}

			// Create simple expressions for lower and upper bounds
			// with appropriate operators based on inclusivity
			minOp := storage.GT
			if betweenExpr.Inclusive {
				minOp = storage.GTE
			}

			maxOp := storage.LT
			if betweenExpr.Inclusive {
				maxOp = storage.LTE
			}

			// Create lower bound expression
			minSimpleExpr := expression.NewSimpleExpression(
				betweenExpr.Column,
				minOp,
				lowerBound,
			)
			minSimpleExpr.ColIndex = pkInfo.singlePKIndex
			minSimpleExpr.IndexPrepped = true

			// Create upper bound expression
			maxSimpleExpr := expression.NewSimpleExpression(
				betweenExpr.Column,
				maxOp,
				upperBound,
			)
			maxSimpleExpr.ColIndex = pkInfo.singlePKIndex
			maxSimpleExpr.IndexPrepped = true

			// Create PKOperationInfo for both bounds
			minResult := PKOperationInfo{
				ID:       lowerBound,
				Expr:     minSimpleExpr,
				Operator: minOp,
				Valid:    true,
			}

			maxResult := PKOperationInfo{
				ID:       upperBound,
				Expr:     maxSimpleExpr,
				Operator: maxOp,
				Valid:    true,
			}

			// Get the actual bounds, adjusting for inclusive/exclusive
			lowerVal := lowerBound
			if minOp == storage.GT {
				lowerVal++ // For exclusive bounds (id > X), add 1 to get the minimum possible value
			}

			upperVal := upperBound
			if maxOp == storage.LT {
				upperVal-- // For exclusive bounds (id < X), subtract 1 to get the maximum possible value
			}

			// Check if bounds are contradictory (min > max)
			if lowerVal > upperVal {
				result.Valid = true
				result.EmptyResult = true
				return []PKOperationInfo{result}
			}

			// If the range narrows to an exact value (id >= 5 AND id <= 5 or BETWEEN 5 AND 5)
			if lowerVal == upperVal {
				// Create an exact equality expression
				simpleExpr := expression.NewSimpleExpression(
					betweenExpr.Column,
					storage.EQ,
					lowerVal,
				)
				simpleExpr.ColIndex = pkInfo.singlePKIndex
				simpleExpr.IndexPrepped = true

				result.Expr = simpleExpr
				result.Operator = storage.EQ
				result.Valid = true
				result.ID = lowerVal
				return []PKOperationInfo{result}
			}

			// Return both bounds for range optimization
			return []PKOperationInfo{minResult, maxResult}
		}
	}

	// Handle RangeExpression directly
	if rangeExpr, ok := expr.(*expression.RangeExpression); ok {
		// Check if this is operating on a primary key column
		pkInfo := getOrCreatePKInfo(schema)
		isPKColumn := false
		for _, col := range schema.Columns {
			if col.Name == rangeExpr.Column && col.PrimaryKey {
				isPKColumn = true
				break
			}
		}

		if isPKColumn {
			// Since primary keys are always Int64, we can directly use the pre-computed Int64 values
			if pkInfo.pkType == storage.TypeInteger {
				// Extract min value (lower bound)
				var minResult, maxResult PKOperationInfo

				// Handle min value if available
				if rangeExpr.MinValue != nil {
					// Use pre-computed Int64Min value directly
					minValue := rangeExpr.Int64Min

					// Create operator based on inclusivity
					minOp := storage.GT
					if rangeExpr.IncludeMin {
						minOp = storage.GTE
					}

					// Create simple expression for lower bound
					minSimpleExpr := expression.NewSimpleExpression(
						rangeExpr.Column,
						minOp,
						minValue,
					)
					minSimpleExpr.ColIndex = pkInfo.singlePKIndex
					minSimpleExpr.IndexPrepped = true

					minResult = PKOperationInfo{
						ID:       minValue,
						Expr:     minSimpleExpr,
						Operator: minOp,
						Valid:    true,
					}
				}

				// Handle max value if available
				if rangeExpr.MaxValue != nil {
					// Use pre-computed Int64Max value directly
					maxValue := rangeExpr.Int64Max

					// Create operator based on inclusivity
					maxOp := storage.LT
					if rangeExpr.IncludeMax {
						maxOp = storage.LTE
					}

					// Create simple expression for upper bound
					maxSimpleExpr := expression.NewSimpleExpression(
						rangeExpr.Column,
						maxOp,
						maxValue,
					)
					maxSimpleExpr.ColIndex = pkInfo.singlePKIndex
					maxSimpleExpr.IndexPrepped = true

					maxResult = PKOperationInfo{
						ID:       maxValue,
						Expr:     maxSimpleExpr,
						Operator: maxOp,
						Valid:    true,
					}
				}

				// Check if we have both bounds
				if minResult.Valid && maxResult.Valid {
					// Get the actual bounds, adjusting for inclusive/exclusive
					lowerVal := minResult.ID
					if minResult.Operator == storage.GT {
						lowerVal++ // For exclusive bounds (id > X), we add 1 to get the minimum possible value
					}

					upperVal := maxResult.ID
					if maxResult.Operator == storage.LT {
						upperVal-- // For exclusive bounds (id < X), we subtract 1 to get the maximum possible value
					}

					// Check if bounds are contradictory (min > max)
					if lowerVal > upperVal {
						result.Valid = true
						result.EmptyResult = true
						return []PKOperationInfo{result}
					}

					// If the range narrows to an exact value (id >= 5 AND id <= 5)
					if lowerVal == upperVal {
						// Create an exact equality expression
						simpleExpr := expression.NewSimpleExpression(
							rangeExpr.Column,
							storage.EQ,
							lowerVal,
						)
						simpleExpr.ColIndex = pkInfo.singlePKIndex
						simpleExpr.IndexPrepped = true

						result.Expr = simpleExpr
						result.Operator = storage.EQ
						result.Valid = true
						result.ID = lowerVal
						return []PKOperationInfo{result}
					}

					// Return both bounds for range optimization
					return []PKOperationInfo{minResult, maxResult}
				} else if minResult.Valid {
					// Only have lower bound
					return []PKOperationInfo{minResult}
				} else if maxResult.Valid {
					// Only have upper bound
					return []PKOperationInfo{maxResult}
				}
			}
		}
	}

	// Try to use our fast PK detector/optimizer
	simpleExpr, ok := expression.PrimaryKeyDetector(expr, schema)
	if ok {
		// We have an optimized expression
		result.Expr = simpleExpr
		result.Operator = simpleExpr.Operator
		result.Valid = true

		// For integer PKs, also extract the ID for legacy code paths
		if simpleExpr.ValueType == storage.TypeInteger {
			result.ID = simpleExpr.Int64Value
		}

		return []PKOperationInfo{result}
	}

	// For complex AND expressions, check for special cases
	if andExpr, ok := expr.(*expression.AndExpression); ok && len(andExpr.Expressions) == 2 {
		// Try to extract operations on the same PK column
		pkInfoMap := make(map[string][]PKOperationInfo)

		// Extract PK info for all subexpressions
		for _, subExpr := range andExpr.Expressions {
			pkInfos := GetPKOperationInfo(subExpr, schema)
			if len(pkInfos) == 1 {
				if pkInfos[0].Valid && pkInfos[0].Expr != nil {
					pkInfoMap[pkInfos[0].Expr.Column] = append(pkInfoMap[pkInfos[0].Expr.Column], pkInfos[0])
				}
			}
		}

		// Look for contradictions or range expressions on the same PK
		for _, infoList := range pkInfoMap {
			if len(infoList) >= 2 {
				// Look for EQ operations on the same column
				eqValues := make(map[int64]bool)
				for _, info := range infoList {
					if info.Operator == storage.EQ && info.ID != 0 {
						eqValues[info.ID] = true
					}
				}

				if len(eqValues) > 1 {
					// Contradiction: x = 1 AND x = 2
					result.Valid = true
					result.EmptyResult = true
					return []PKOperationInfo{result}
				} else if len(eqValues) == 1 {
					// Multiple conditions but only one equality value
					// Return it as the canonical expression
					for _, info := range infoList {
						if info.Operator == storage.EQ {
							return []PKOperationInfo{info}
						}
					}
				}

				// Range narrowing for GT/LT combinations on integer PKs
				// This optimizes expressions like "id > 10 AND id < 20"
				var minBound, maxBound PKOperationInfo

				// First, extract any lower and upper bounds on integers
				for _, info := range infoList {
					// Only process integer expressions
					if info.Expr != nil && info.Expr.ValueType == storage.TypeInteger {
						if info.Operator == storage.GT || info.Operator == storage.GTE {
							// Lower bound (id > X or id >= X)
							if minBound.Expr == nil || info.Expr.Int64Value > minBound.Expr.Int64Value {
								minBound = info // Keep highest minimum value
							}
						} else if info.Operator == storage.LT || info.Operator == storage.LTE {
							// Upper bound (id < X or id <= X)
							if maxBound.Expr == nil || info.Expr.Int64Value < maxBound.Expr.Int64Value {
								maxBound = info // Keep lowest maximum value
							}
						}
					}
				}

				// Handle optimization for both upper and lower bounds
				// First, if we only have an upper bound, use that
				if minBound.Expr == nil && maxBound.Expr != nil {
					// Just return the upper bound as it's all we have
					return []PKOperationInfo{maxBound}
				}

				// If we only have a lower bound, use that
				if minBound.Expr != nil && maxBound.Expr == nil {
					// Just return the lower bound as it's all we have
					return []PKOperationInfo{minBound}
				}

				// If we have both bounds, check if we can optimize further
				if minBound.Expr != nil && maxBound.Expr != nil {
					// Get the actual bounds, adjusting for inclusive/exclusive
					lowerVal := minBound.Expr.Int64Value
					if minBound.Operator == storage.GT {
						lowerVal++ // For exclusive bounds (id > X), we add 1 to get the minimum possible value
					}

					upperVal := maxBound.Expr.Int64Value
					if maxBound.Operator == storage.LT {
						upperVal-- // For exclusive bounds (id < X), we subtract 1 to get the maximum possible value
					}

					// Check if bounds are contradictory (min > max)
					if lowerVal > upperVal {
						result.Valid = true
						result.EmptyResult = true
						return []PKOperationInfo{result}
					}

					// If the range narrows to an exact value (id >= 5 AND id <= 5)
					if lowerVal == upperVal {
						// Create an exact equality expression
						simpleExpr := expression.NewSimpleExpression(
							minBound.Expr.Column,
							storage.EQ,
							lowerVal,
						)
						simpleExpr.ColIndex = minBound.Expr.ColIndex
						simpleExpr.IndexPrepped = true

						result.Expr = simpleExpr
						result.Operator = storage.EQ
						result.Valid = true
						result.ID = lowerVal
						return []PKOperationInfo{result}
					}

					return []PKOperationInfo{minBound, maxBound}
				}
			}
		}
	}

	// Return default "not optimizable" result
	return []PKOperationInfo{result}
}

// Helper function to check if an expression has an equality condition on a column
func hasEqualityCondition(expr storage.Expression, columnName string) bool {
	// Check simple expression
	if simpleExpr, ok := expr.(*expression.SimpleExpression); ok {
		return simpleExpr.Column == columnName && simpleExpr.Operator == storage.EQ
	}

	// Check AND expression
	if andExpr, ok := expr.(*expression.AndExpression); ok {
		for _, subExpr := range andExpr.Expressions {
			if hasEqualityCondition(subExpr, columnName) {
				return true
			}
		}
	}

	// Check OR expression
	if orExpr, ok := expr.(*expression.OrExpression); ok {
		for _, subExpr := range orExpr.Expressions {
			if hasEqualityCondition(subExpr, columnName) {
				return true
			}
		}
	}

	return false
}

func extractColumnReferences(expr storage.Expression) []string {
	columnSet := make(map[string]bool)

	// Helper function to process a single expression
	var processExpression func(e storage.Expression)
	processExpression = func(e storage.Expression) {
		if e == nil {
			return
		}

		// Try different expression types to extract column references
		switch typedExpr := e.(type) {
		case *expression.SimpleExpression:
			// Simple expressions directly reference columns
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.AndExpression:
			// Process each child expression of AND
			for _, child := range typedExpr.Expressions {
				processExpression(child)
			}

		case *expression.OrExpression:
			// Process each child expression of OR
			for _, child := range typedExpr.Expressions {
				processExpression(child)
			}

		case *expression.BetweenExpression:
			// BETWEEN expressions reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.RangeExpression:
			// RangeExpression references a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.InListExpression:
			// IN expressions reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.CastExpression:
			// Cast expressions may reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}

		case *expression.NullCheckExpression:
			// Null check expressions may reference a column
			if typedExpr.Column != "" {
				columnSet[typedExpr.Column] = true
			}
		}
	}

	// Process the top-level expression
	if expr != nil {
		processExpression(expr)
	}

	// Convert set to slice
	var result []string

	for col := range columnSet {
		result = append(result, col)
	}

	return result
}
