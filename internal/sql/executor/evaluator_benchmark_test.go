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
package executor

import (
	"context"
	"testing"

	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// BenchmarkValueCompare tests the performance of value comparison
func BenchmarkValueCompare(b *testing.B) {
	testCases := []struct {
		name  string
		left  storage.ColumnValue
		right storage.ColumnValue
	}{
		{
			name:  "IntVsInt",
			left:  storage.NewIntegerValue(100),
			right: storage.NewIntegerValue(200),
		},
		{
			name:  "IntVsFloat",
			left:  storage.NewIntegerValue(100),
			right: storage.NewFloatValue(200.5),
		},
		{
			name:  "StringVsString",
			left:  storage.NewStringValue("apple"),
			right: storage.NewStringValue("banana"),
		},
		{
			name:  "StringVsInt", // Common case requiring conversion
			left:  storage.NewStringValue("100"),
			right: storage.NewIntegerValue(100),
		},
		{
			name:  "StringVsFloat", // Common case requiring conversion
			left:  storage.NewStringValue("100.5"),
			right: storage.NewFloatValue(100.5),
		},
		{
			name:  "BoolVsBool",
			left:  storage.NewBooleanValue(true),
			right: storage.NewBooleanValue(false),
		},
		{
			name:  "StringVsBoolean", // Common case requiring string conversion
			left:  storage.NewStringValue("true"),
			right: storage.NewBooleanValue(true),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = tc.left.Compare(tc.right)
			}
		})
	}
}

// BenchmarkAsString tests the performance of conversion to string
func BenchmarkAsString(b *testing.B) {
	testCases := []struct {
		name  string
		value storage.ColumnValue
	}{
		{
			name:  "IntToString_Small",
			value: storage.NewIntegerValue(42),
		},
		{
			name:  "IntToString_Large",
			value: storage.NewIntegerValue(1234567890),
		},
		{
			name:  "FloatToString_IntLike",
			value: storage.NewFloatValue(42.0),
		},
		{
			name:  "FloatToString_WithDecimals",
			value: storage.NewFloatValue(42.75),
		},
		{
			name:  "BoolToString",
			value: storage.NewBooleanValue(true),
		},
		{
			name:  "StringToString", // No conversion
			value: storage.NewStringValue("hello"),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = tc.value.AsString()
			}
		})
	}
}

// BenchmarkValueConversion tests complex expression evaluation
func BenchmarkValueConversion(b *testing.B) {
	// Use the global function registry
	functionRegistry := registry.GetGlobal()
	evaluator := NewEvaluator(context.Background(), functionRegistry)

	// A WHERE clause with multiple comparisons
	whereClause := &parser.InfixExpression{
		Left:     &parser.Identifier{Value: "price"},
		Operator: ">",
		Right:    &parser.FloatLiteral{Value: 100.0},
	}

	// Use a row map with a mix of types
	row := map[string]storage.ColumnValue{
		"price":      storage.NewFloatValue(150.0),
		"name":       storage.NewStringValue("Product1"),
		"in_stock":   storage.NewBooleanValue(true),
		"quantity":   storage.NewIntegerValue(35),
		"sale_price": storage.NewStringValue("125.50"), // String that needs numeric conversion
	}

	evaluator = evaluator.WithRow(row)

	b.Run("WhereClauseEvaluation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = evaluator.EvaluateWhereClause(whereClause, row)
		}
	})

	// Test more complex WHERE clause with multiple conditions
	complexWhere := &parser.InfixExpression{
		Left: &parser.InfixExpression{
			Left:     &parser.Identifier{Value: "price"},
			Operator: ">",
			Right:    &parser.FloatLiteral{Value: 100.0},
		},
		Operator: "AND",
		Right: &parser.InfixExpression{
			Left:     &parser.Identifier{Value: "quantity"},
			Operator: "<",
			Right:    &parser.IntegerLiteral{Value: 50},
		},
	}

	b.Run("ComplexWhereClause", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = evaluator.EvaluateWhereClause(complexWhere, row)
		}
	})

	// Test with a WHERE clause that requires string-to-number conversion
	stringConversionWhere := &parser.InfixExpression{
		Left:     &parser.Identifier{Value: "sale_price"}, // This is stored as string "125.50"
		Operator: ">",
		Right:    &parser.FloatLiteral{Value: 120.0},
	}

	b.Run("StringToNumberConversion", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = evaluator.EvaluateWhereClause(stringConversionWhere, row)
		}
	})
}
