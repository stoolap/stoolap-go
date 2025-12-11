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
package test

import (
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// BenchmarkSimpleExpressionEvaluate benchmarks the standard Evaluate method
func BenchmarkSimpleExpressionEvaluate(b *testing.B) {
	// Create a row with some test data
	row := storage.Row{
		storage.NewIntegerValue(100),
		storage.NewStringValue("test"),
		storage.NewFloatValue(123.45),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(time.Now()),
	}

	// Create test expressions for different data types and operators
	expressions := []storage.Expression{
		// Integer equality (most common case)
		createExprWithIndex(0, storage.EQ, int64(100)),
		createExprWithIndex(0, storage.EQ, int64(200)), // No match

		// String comparison
		createExprWithIndex(1, storage.EQ, "test"),
		createExprWithIndex(1, storage.GT, "abc"),

		// Float comparison
		createExprWithIndex(2, storage.GT, 100.0),

		// Boolean comparison
		createExprWithIndex(3, storage.EQ, true),

		// Mixed type comparison (int vs float)
		createExprWithIndex(0, storage.LT, 150.0),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, expr := range expressions {
			result, _ := expr.Evaluate(row)
			// Use the result to prevent optimizer from removing the call
			_ = result
		}
	}
}

// BenchmarkSimpleExpressionEvaluateFast benchmarks the optimized EvaluateFast method
func BenchmarkSimpleExpressionEvaluateFast(b *testing.B) {
	// Create a row with some test data
	row := storage.Row{
		storage.NewIntegerValue(100),
		storage.NewStringValue("test"),
		storage.NewFloatValue(123.45),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(time.Now()),
	}

	// Create test expressions for different data types and operators
	expressions := []expression.SimpleExpression{
		// Integer equality (most common case)
		*createExprWithIndex(0, storage.EQ, int64(100)),
		*createExprWithIndex(0, storage.EQ, int64(200)), // No match

		// String comparison
		*createExprWithIndex(1, storage.EQ, "test"),
		*createExprWithIndex(1, storage.GT, "abc"),

		// Float comparison
		*createExprWithIndex(2, storage.GT, 100.0),

		// Boolean comparison
		*createExprWithIndex(3, storage.EQ, true),

		// Mixed type comparison (int vs float)
		*createExprWithIndex(0, storage.LT, 150.0),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, expr := range expressions {
			result := expr.EvaluateFast(row)
			// Use the result to prevent optimizer from removing the call
			_ = result
		}
	}
}

// BenchmarkSimpleExpressionInteger specifically benchmarks integer equality comparison
func BenchmarkSimpleExpressionInteger(b *testing.B) {
	// Create a row with some test data
	row := storage.Row{
		storage.NewIntegerValue(100),
	}

	// Create test expression
	expr := createExprWithIndex(0, storage.EQ, int64(100))
	exprFast := *expr

	b.Run("Evaluate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result, _ := expr.Evaluate(row)
			_ = result
		}
	})

	b.Run("EvaluateFast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := exprFast.EvaluateFast(row)
			_ = result
		}
	})
}

// BenchmarkSimpleExpressionString specifically benchmarks string equality comparison
func BenchmarkSimpleExpressionString(b *testing.B) {
	// Create a row with some test data
	row := storage.Row{
		storage.NewStringValue("test"),
	}

	// Create test expression
	expr := createExprWithIndex(0, storage.EQ, "test")
	exprFast := *expr

	b.Run("Evaluate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result, _ := expr.Evaluate(row)
			_ = result
		}
	})

	b.Run("EvaluateFast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := exprFast.EvaluateFast(row)
			_ = result
		}
	})
}

// BenchmarkSimpleExpressionMixedNumeric specifically benchmarks mixed numeric types comparison
func BenchmarkSimpleExpressionMixedNumeric(b *testing.B) {
	// Create a row with some test data
	row := storage.Row{
		storage.NewIntegerValue(100),
	}

	// Create test expression - float comparison with integer column
	expr := createExprWithIndex(0, storage.LT, 150.0)
	exprFast := *expr

	b.Run("Evaluate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result, _ := expr.Evaluate(row)
			_ = result
		}
	})

	b.Run("EvaluateFast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result := exprFast.EvaluateFast(row)
			_ = result
		}
	})
}

// Helper function to create a SimpleExpression with index properly set
func createExprWithIndex(colIndex int, operator storage.Operator, value interface{}) *expression.SimpleExpression {
	expr := expression.NewSimpleExpression("col"+string(rune('0'+colIndex)), operator, value)
	expr.ColIndex = colIndex
	expr.IndexPrepped = true
	return expr
}
