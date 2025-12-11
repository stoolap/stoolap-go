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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// This file contains benchmarks to compare performance between
// the original MultiColumnarIndex implementation and the enhanced
// version that uses individual ColumnarIndex instances.

// BenchmarkComparisonMultiColumnarIndex runs performance comparisons between
// old and new MultiColumnarIndex implementations for various operations
func BenchmarkComparisonMultiColumnarIndex(b *testing.B) {
	// Initialize data and parameters for all benchmarks
	rand.Seed(time.Now().UnixNano())
	numRows := 10000

	// Benchmark data insertion
	b.Run("DataInsertion", func(b *testing.B) {
		vs := createVersionStore()

		// Create test data
		testData := make([][]storage.ColumnValue, numRows)
		rowIDs := make([]int64, numRows)

		for i := 0; i < numRows; i++ {
			testData[i] = []storage.ColumnValue{
				storage.NewIntegerValue(int64(i)),
				storage.NewStringValue(fmt.Sprintf("User%d", i)),
				storage.NewIntegerValue(int64(20 + (i % 50))),
			}
			rowIDs[i] = int64(i + 1)
		}

		// Benchmark index population
		b.Run("PopulateIndex_10K_Rows", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create a fresh index for each iteration
				idx := mvcc.NewMultiColumnarIndex(
					"idx_benchmark",
					"benchmark_table",
					[]string{"id", "name", "age"},
					[]int{0, 1, 2},
					[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
					vs,
					false,
				)

				// Add all rows
				for j := 0; j < numRows; j++ {
					_ = idx.Add(testData[j], rowIDs[j], 0)
				}
			}
		})
	})

	// Benchmark exact match queries
	b.Run("ExactMatchQueries", func(b *testing.B) {
		vs := createVersionStore()

		// Create and populate the index
		idx := mvcc.NewMultiColumnarIndex(
			"idx_benchmark",
			"benchmark_table",
			[]string{"id", "name", "age"},
			[]int{0, 1, 2},
			[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
			vs,
			false,
		)

		for i := 0; i < numRows; i++ {
			values := []storage.ColumnValue{
				storage.NewIntegerValue(int64(i)),
				storage.NewStringValue(fmt.Sprintf("User%d", i)),
				storage.NewIntegerValue(int64(20 + (i % 50))),
			}
			_ = idx.Add(values, int64(i+1), 0)
		}

		// Benchmark single column match
		b.Run("SingleColumnMatch", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lookupID := rand.Intn(numRows)
				values := []storage.ColumnValue{
					storage.NewIntegerValue(int64(lookupID)),
				}
				_ = idx.GetRowIDsEqual(values)
			}
		})

		// Benchmark two column match
		b.Run("TwoColumnMatch", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lookupID := rand.Intn(numRows)
				values := []storage.ColumnValue{
					storage.NewIntegerValue(int64(lookupID)),
					storage.NewStringValue(fmt.Sprintf("User%d", lookupID)),
				}
				_ = idx.GetRowIDsEqual(values)
			}
		})

		// Benchmark three column match
		b.Run("ThreeColumnMatch", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				lookupID := rand.Intn(numRows)
				values := []storage.ColumnValue{
					storage.NewIntegerValue(int64(lookupID)),
					storage.NewStringValue(fmt.Sprintf("User%d", lookupID)),
					storage.NewIntegerValue(int64(20 + (lookupID % 50))),
				}
				_ = idx.GetRowIDsEqual(values)
			}
		})
	})

	// Benchmark range queries
	b.Run("RangeQueries", func(b *testing.B) {
		vs := createVersionStore()

		// Create and populate the index
		idx := mvcc.NewMultiColumnarIndex(
			"idx_benchmark",
			"benchmark_table",
			[]string{"id", "name", "age"},
			[]int{0, 1, 2},
			[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
			vs,
			false,
		)

		for i := 0; i < numRows; i++ {
			values := []storage.ColumnValue{
				storage.NewIntegerValue(int64(i)),
				storage.NewStringValue(fmt.Sprintf("User%d", i)),
				storage.NewIntegerValue(int64(20 + (i % 50))),
			}
			_ = idx.Add(values, int64(i+1), 0)
		}

		// Benchmark small range (around 100 rows)
		b.Run("SmallRange_100_Rows", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				startID := rand.Intn(numRows - 100)
				endID := startID + 100

				minValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(startID)),
				}
				maxValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(endID)),
				}

				_ = idx.GetRowIDsInRange(minValues, maxValues, true, true)
			}
		})

		// Benchmark medium range (around 1000 rows)
		b.Run("MediumRange_1000_Rows", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				startID := rand.Intn(numRows - 1000)
				endID := startID + 1000

				minValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(startID)),
				}
				maxValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(endID)),
				}

				_ = idx.GetRowIDsInRange(minValues, maxValues, true, true)
			}
		})

		// Benchmark multi-column range
		b.Run("MultiColumnRange", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				startID := rand.Intn(numRows - 1000)
				endID := startID + 1000

				minValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(startID)),
					storage.NewStringValue("User"),
				}
				maxValues := []storage.ColumnValue{
					storage.NewIntegerValue(int64(endID)),
					storage.NewStringValue("UserZ"),
				}

				_ = idx.GetRowIDsInRange(minValues, maxValues, true, true)
			}
		})
	})

	// Benchmark expression queries
	b.Run("ExpressionQueries", func(b *testing.B) {
		vs := createVersionStore()

		// Create and populate the index
		idx := mvcc.NewMultiColumnarIndex(
			"idx_benchmark",
			"benchmark_table",
			[]string{"id", "name", "age"},
			[]int{0, 1, 2},
			[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
			vs,
			false,
		)

		for i := 0; i < numRows; i++ {
			values := []storage.ColumnValue{
				storage.NewIntegerValue(int64(i)),
				storage.NewStringValue(fmt.Sprintf("User%d", i)),
				storage.NewIntegerValue(int64(20 + (i % 50))),
			}
			_ = idx.Add(values, int64(i+1), 0)
		}

		// Benchmark simple expression
		b.Run("SimpleExpression", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				targetAge := int64(20 + (rand.Intn(50)))
				expr := &expression.SimpleExpression{
					Column:   "age",
					Operator: storage.EQ,
					Value:    targetAge,
				}

				_ = idx.GetFilteredRowIDs(expr)
			}
		})

		// Benchmark AND expression
		b.Run("ANDExpression", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				startID := rand.Intn(numRows - 1000)
				targetAge := int64(20 + (rand.Intn(50)))

				expr1 := &expression.SimpleExpression{
					Column:   "id",
					Operator: storage.GT,
					Value:    int64(startID),
				}
				expr2 := &expression.SimpleExpression{
					Column:   "age",
					Operator: storage.EQ,
					Value:    targetAge,
				}
				andExpr := &expression.AndExpression{
					Expressions: []storage.Expression{expr1, expr2},
				}

				_ = idx.GetFilteredRowIDs(andExpr)
			}
		})
	})

	// Benchmark batch operations
	b.Run("BatchOperations", func(b *testing.B) {
		vs := createVersionStore()

		// Create test batch data
		batchSize := 1000
		numBatches := numRows / batchSize

		// Prepare batch data
		batches := make([]map[int64][]storage.ColumnValue, numBatches)
		for i := 0; i < numBatches; i++ {
			batches[i] = make(map[int64][]storage.ColumnValue)

			for j := 0; j < batchSize; j++ {
				id := i*batchSize + j
				batches[i][int64(id+1)] = []storage.ColumnValue{
					storage.NewIntegerValue(int64(id)),
					storage.NewStringValue(fmt.Sprintf("User%d", id)),
					storage.NewIntegerValue(int64(20 + (id % 50))),
				}
			}
		}

		// Benchmark AddBatch operation
		b.Run("AddBatch", func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create a fresh index for each iteration
				idx := mvcc.NewMultiColumnarIndex(
					"idx_benchmark",
					"benchmark_table",
					[]string{"id", "name", "age"},
					[]int{0, 1, 2},
					[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
					vs,
					false,
				)

				// Add all batches
				for j := 0; j < numBatches; j++ {
					_ = idx.AddBatch(batches[j])
				}
			}
		})

		// Benchmark RemoveBatch operation
		b.Run("RemoveBatch", func(b *testing.B) {
			// Only run this a limited number of times as it's more expensive
			if b.N > 100 {
				b.N = 100
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create and populate a fresh index for each iteration
				idx := mvcc.NewMultiColumnarIndex(
					"idx_benchmark",
					"benchmark_table",
					[]string{"id", "name", "age"},
					[]int{0, 1, 2},
					[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
					vs,
					false,
				)

				// Add all data
				for j := 0; j < numBatches; j++ {
					_ = idx.AddBatch(batches[j])
				}

				// Measure time to remove one batch
				batchToRemove := batches[rand.Intn(numBatches)]
				_ = idx.RemoveBatch(batchToRemove)
			}
		})
	})
}

// Helper function to create a version store for testing
func createVersionStore() *mvcc.VersionStore {
	return mvcc.NewVersionStore("benchmark_table", mvcc.NewMVCCEngine(&storage.Config{}))
}
