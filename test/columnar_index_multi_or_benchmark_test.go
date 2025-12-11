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

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// BenchmarkMultiColumnarIndexWithORClause specifically benchmarks the query pattern
// "value > ? AND value <= ? AND (active = true OR active = false)"
// that appears in the main benchmark
func BenchmarkMultiColumnarIndexWithORClause(b *testing.B) {
	// Create a version store for testing
	vs := mvcc.NewVersionStore("benchmark_table", mvcc.NewMVCCEngine(&storage.Config{}))

	// Create a multi-columnar index
	idx := mvcc.NewMultiColumnarIndex(
		"test_multi_index",
		"benchmark_table",
		[]string{"value", "active"},
		[]int{0, 1},
		[]storage.DataType{storage.FLOAT, storage.BOOLEAN},
		vs,
		false,
	)

	// Add 1000 rows to the index for testing
	for i := 0; i < 1000; i++ {
		values := []storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5), // value column
			storage.NewBooleanValue(i%2 == 0),       // active column
		}
		idx.Add(values, int64(i+1), 0)
	}

	// Create the expression for "value > ? AND value <= ? AND (active = true OR active = false)"
	b.Run("Original", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create the range expression for "value > 0 AND value <= 100"
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    float64(0),
			}

			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    float64(100),
			}

			// Create the OR expression for "active = true OR active = false"
			expr3 := &expression.SimpleExpression{
				Column:   "active",
				Operator: storage.EQ,
				Value:    true,
			}

			expr4 := &expression.SimpleExpression{
				Column:   "active",
				Operator: storage.EQ,
				Value:    false,
			}

			orExpr := &expression.OrExpression{
				Expressions: []storage.Expression{expr3, expr4},
			}

			// Combine with AND
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2, orExpr},
			}

			// Execute query
			result := idx.GetFilteredRowIDs(andExpr)

			// Validate result
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})

	// Create a version store for testing
	vs2 := mvcc.NewVersionStore("benchmark_table2", mvcc.NewMVCCEngine(&storage.Config{}))

	// Create a columnar index (single column)
	valueIdx := mvcc.NewColumnarIndex(
		"test_value_index",
		"benchmark_table2",
		"value",
		0,
		storage.FLOAT,
		vs2,
		false,
	)

	// Add 1000 rows to the index for testing
	for i := 0; i < 1000; i++ {
		values := []storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5),
		}
		valueIdx.Add(values, int64(i+1), 0)
	}

	b.Run("SingleColumnRange", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create the range expression for "value > 0 AND value <= 100"
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    float64(0),
			}

			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    float64(100),
			}

			// Combine with AND
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Execute query
			result := valueIdx.GetFilteredRowIDs(andExpr)

			// Validate result
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})
}
