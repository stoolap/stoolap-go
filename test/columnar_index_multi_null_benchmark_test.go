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

// BenchmarkMultiColumnarIndexWithNullCheck specifically benchmarks the query pattern
// "value > ? AND value <= ? AND active IS NOT NULL"
// which should be optimized in the multi-columnar index.
// This is a simplified test to focus only on the index functionality.
func BenchmarkMultiColumnarIndexWithNullCheck(b *testing.B) {
	// Create a multi-columnar index
	idx := mvcc.NewMultiColumnarIndex(
		"test_multi_index",
		"benchmark_table",
		[]string{"value", "active"},
		[]int{0, 1},
		[]storage.DataType{storage.FLOAT, storage.BOOLEAN},
		nil, // No version store needed for this test
		false,
	)

	// First build and manually populate the individual column indices
	// for the tests to work correctly since we're not using version store
	for i := 0; i < 1000; i++ {
		// For rows divisible by 100, set active to NULL to test NULL handling
		var activeValue storage.ColumnValue
		if i%100 == 0 {
			activeValue = nil // NULL value
		} else {
			activeValue = storage.NewBooleanValue(i%2 == 0)
		}

		// Add to index
		values := []storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5), // value column
			activeValue,                             // active column
		}
		idx.Add(values, int64(i+1), 0)
	}

	// Benchmark the multi-column index's handling of
	// "value > ? AND value <= ? AND active IS NOT NULL" pattern
	b.Run("MultiColumn_ValueRange_NotNull", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create expressions for "value > 0 AND value <= 100 AND active IS NOT NULL"
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

			// IS NOT NULL expression
			nullCheckExpr := expression.NewIsNotNullExpression("active")

			// Combine with AND
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2, nullCheckExpr},
			}

			// Execute query
			result := idx.GetFilteredRowIDs(andExpr)

			// Expected: 99 results (rows 1-100, excluding row 100 which has NULL)
			if len(result) != 99 {
				b.Fatalf("Expected 99 results (excluding NULL rows), got %d", len(result))
			}
		}
	})

	// Create a columnar index for a single column (value) as control
	valueIdx := mvcc.NewColumnarIndex(
		"test_value_index",
		"benchmark_table2",
		"value",
		0,
		storage.FLOAT,
		nil, // No version store needed for this test
		false,
	)

	// Add 1000 rows to the index for testing
	for i := 0; i < 1000; i++ {
		values := []storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5),
		}
		valueIdx.Add(values, int64(i+1), 0)
	}

	// Benchmark just the range part as control
	b.Run("SingleColumn_ValueRange", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create expressions for "value > 0 AND value <= 100"
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

			// Should get all 100 rows (1-100)
			if len(result) != 100 {
				b.Fatalf("Expected 100 results, got %d", len(result))
			}
		}
	})

	// Create a boolean columnar index as control for IS NOT NULL test
	activeIdx := mvcc.NewColumnarIndex(
		"test_active_index",
		"benchmark_table3",
		"active",
		0, // Column ID is 0 in this table
		storage.BOOLEAN,
		nil, // No version store needed for this test
		false,
	)

	// Add 1000 rows to the index, with some NULL values
	for i := 0; i < 1000; i++ {
		var activeValue storage.ColumnValue
		if i%100 == 0 {
			activeValue = nil // NULL
		} else {
			activeValue = storage.NewBooleanValue(i%2 == 0)
		}

		activeIdx.Add([]storage.ColumnValue{activeValue}, int64(i+1), 0)
	}

	// Benchmark just the IS NOT NULL part as control
	b.Run("SingleColumn_NotNull", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// IS NOT NULL expression
			nullCheckExpr := expression.NewIsNotNullExpression("active")

			// Execute query
			result := activeIdx.GetFilteredRowIDs(nullCheckExpr)

			// Should get 990 rows (excluding the 10 NULL values)
			if len(result) != 990 {
				b.Fatalf("Expected 990 results, got %d", len(result))
			}
		}
	})
}
