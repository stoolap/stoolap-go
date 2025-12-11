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
	"math/rand"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// createTestIndex creates a test columnar index with random data
func createTestIndex(size int, valuesPerRow int) *ColumnarIndex {
	index := NewColumnarIndex(
		"test_index",
		"test_table",
		"test_column",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add test data
	for rowID := int64(0); rowID < int64(size); rowID++ {
		// Add rowID % valuesPerRow as the value
		// This creates valuesPerRow different values distributed evenly
		value := storage.NewIntegerValue(rowID % int64(valuesPerRow))
		index.Add([]storage.ColumnValue{value}, rowID, 0)
	}

	return index
}

// createTestTimestampIndex creates a test columnar index with timestamp data
func createTestTimestampIndex(size int) *ColumnarIndex {
	index := NewColumnarIndex(
		"test_time_index",
		"test_table",
		"timestamp",
		0,
		storage.TIMESTAMP,
		nil,
		false,
	)

	// Add timestamp values with 1 day interval
	startDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := int64(0); i < int64(size); i++ {
		date := startDate.AddDate(0, 0, int(i))
		index.Add([]storage.ColumnValue{storage.NewTimestampValue(date)}, i, 0)
	}

	return index
}

// BenchmarkColumnarIndexAllocFree compares original and allocation-free methods
func BenchmarkColumnarIndexAllocFree(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	valuesPerRow := 100 // Number of unique values

	for _, size := range sizes {
		b.Run("Size_"+string(rune(size)), func(b *testing.B) {
			index := createTestIndex(size, valuesPerRow)
			testValue := storage.NewIntegerValue(42) // Search for the value 42

			// Benchmark GetRowIDsEqual
			b.Run("GetRowIDsEqual", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetRowIDsEqual([]storage.ColumnValue{testValue})
					// Consume the result to make sure work is done
					_ = len(rowIDs)
				}
			})

			// Use a range query to test range methods
			minValue := storage.NewIntegerValue(20)
			maxValue := storage.NewIntegerValue(60)

			// Benchmark GetRowIDsInRange
			b.Run("GetRowIDsInRange", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetRowIDsInRange([]storage.ColumnValue{minValue}, []storage.ColumnValue{maxValue}, true, true)
					// Consume the result to make sure work is done
					_ = len(rowIDs)
				}
			})

			// Benchmark operator methods
			b.Run("FindWithOperator", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					entries, _ := index.FindWithOperator(storage.GT, []storage.ColumnValue{minValue})
					_ = len(entries)
				}
			})

			// Create a simple test expression for equality
			simpleExpr := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.EQ,
				Value:    int64(42),
			}

			// Create sample schema
			schema := storage.Schema{
				TableName: "test_table",
				Columns: []storage.SchemaColumn{
					{
						Name: "test_column",
						Type: storage.INTEGER,
					},
				},
			}

			simpleExpr.PrepareForSchema(schema)

			// Benchmark filtered methods
			b.Run("GetFilteredRowIDs", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(simpleExpr)
					_ = len(rowIDs)
				}
			})

			// Create complex expressions for AND/OR benchmarks
			simpleExpr1 := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.GT,
				Value:    int64(30),
			}

			simpleExpr2 := &expression.SimpleExpression{
				Column:   "test_column",
				Operator: storage.LT,
				Value:    int64(50),
			}

			// AND expression benchmark
			andExpr := expression.NewAndExpression(simpleExpr1, simpleExpr2)
			andExpr.PrepareForSchema(schema)

			b.Run("GetFilteredRowIDs_And", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(andExpr)
					_ = len(rowIDs)
				}
			})

			// OR expression benchmark
			orExpr := expression.NewOrExpression(simpleExpr1, simpleExpr2)
			orExpr.PrepareForSchema(schema)

			b.Run("GetFilteredRowIDs_Or", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rowIDs := index.GetFilteredRowIDs(orExpr)
					_ = len(rowIDs)
				}
			})
		})
	}
}

// BenchmarkColumnarIndexTimestamp tests timestamp operations
func BenchmarkColumnarIndexTimestamp(b *testing.B) {
	index := createTestTimestampIndex(365 * 3) // ~3 years of daily data

	// Test timestamp equality
	b.Run("TimestampEquality", func(b *testing.B) {
		// Search for a specific date
		targetDate := time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC)
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.Find([]storage.ColumnValue{storage.NewTimestampValue(targetDate)})
			_ = len(entries)
		}
	})

	// Test timestamp range
	b.Run("TimestampRange", func(b *testing.B) {
		// Search for a 30-day range
		minDate := time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC)
		maxDate := time.Date(2021, 5, 31, 0, 0, 0, 0, time.UTC)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.FindRange(
				[]storage.ColumnValue{storage.NewTimestampValue(minDate)},
				[]storage.ColumnValue{storage.NewTimestampValue(maxDate)},
				true, true)
			_ = len(entries)
		}
	})

	// Test timestamp with operator
	b.Run("TimestampOperator", func(b *testing.B) {
		pivotDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			entries, _ := index.FindWithOperator(
				storage.GT, []storage.ColumnValue{storage.NewTimestampValue(pivotDate)})
			_ = len(entries)
		}
	})

	// Create schema for expression tests
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "timestamp", Type: storage.TIMESTAMP},
		},
	}

	// Test timestamp with GetFilteredRowIDs
	b.Run("TimestampFilteredRowIDs", func(b *testing.B) {
		targetDate := time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC)
		simpleExpr := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.EQ,
			Value:    targetDate,
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rowIDs := index.GetFilteredRowIDs(simpleExpr)
			_ = len(rowIDs)
		}
	})

	// Test timestamp with complex expressions
	b.Run("TimestampComplexFilter", func(b *testing.B) {
		// Range from 2021-01-01 to 2021-12-31
		startDate := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)

		expr1 := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.GTE,
			Value:    startDate,
		}
		expr2 := &expression.SimpleExpression{
			Column:   "timestamp",
			Operator: storage.LTE,
			Value:    endDate,
		}

		andExpr := expression.NewAndExpression(expr1, expr2)
		andExpr.PrepareForSchema(schema)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rowIDs := index.GetFilteredRowIDs(andExpr)
			_ = len(rowIDs)
		}
	})
}

// BenchmarkColumnarIndexSingleIntAdd benchmarks adding integers to a single-column ColumnarIndex
func BenchmarkColumnarIndexSingleIntAdd(b *testing.B) {
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"num",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		num := int64(i % 10000)
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(num)}, int64(i), 0)
	}
}

// BenchmarkColumnarIndexSingleTimestampAdd benchmarks adding timestamps to a single-column ColumnarIndex
func BenchmarkColumnarIndexSingleTimestampAdd(b *testing.B) {
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"ts",
		0,
		storage.TIMESTAMP,
		nil,
		false,
	)

	// Base timestamp
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add time with 1-minute intervals
		ts := baseTime.Add(time.Duration(i%10000) * time.Minute)
		idx.Add([]storage.ColumnValue{storage.NewTimestampValue(ts)}, int64(i), 0)
	}
}

// BenchmarkColumnarIndexSingleIntRange benchmarks integer range queries on a single-column ColumnarIndex
func BenchmarkColumnarIndexSingleIntRange(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"num",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add 10000 rows with sequential integer data
	for i := 0; i < 10000; i++ {
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
	}

	// Generate random range bounds
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	minVals := make([]int64, b.N)
	maxVals := make([]int64, b.N)

	for i := 0; i < b.N; i++ {
		minVals[i] = int64(r.Intn(8000))
		maxVals[i] = minVals[i] + int64(r.Intn(2000)) + 1 // Ensure range is at least 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.GetRowIDsInRange(
			[]storage.ColumnValue{storage.NewIntegerValue(minVals[i])},
			[]storage.ColumnValue{storage.NewIntegerValue(maxVals[i])},
			true, true,
		)
	}
}

// BenchmarkColumnarIndexSingleTimestampRange benchmarks timestamp range queries on a single-column ColumnarIndex
func BenchmarkColumnarIndexSingleTimestampRange(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"ts",
		0,
		storage.TIMESTAMP,
		nil,
		false,
	)

	// Base timestamp
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 10000 rows with sequential timestamp data, 1 hour intervals
	for i := 0; i < 10000; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		idx.Add([]storage.ColumnValue{storage.NewTimestampValue(ts)}, int64(i), 0)
	}

	// Generate random time range bounds
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	startHours := make([]int, b.N)
	rangeLengths := make([]int, b.N)

	for i := 0; i < b.N; i++ {
		startHours[i] = r.Intn(8000)
		rangeLengths[i] = 100 + r.Intn(1900) // Range spans 100-2000 hours
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		minTime := baseTime.Add(time.Duration(startHours[i]) * time.Hour)
		maxTime := baseTime.Add(time.Duration(startHours[i]+rangeLengths[i]) * time.Hour)

		idx.GetRowIDsInRange(
			[]storage.ColumnValue{storage.NewTimestampValue(minTime)},
			[]storage.ColumnValue{storage.NewTimestampValue(maxTime)},
			true, true,
		)
	}
}

// BenchmarkColumnarIndexSingleFloatRange benchmarks float range queries on a single-column ColumnarIndex
func BenchmarkColumnarIndexSingleFloatRange(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"float_val",
		0,
		storage.FLOAT,
		nil,
		false,
	)

	// Add 10000 rows with sequential float data
	for i := 0; i < 10000; i++ {
		floatVal := float64(i) + float64(i)/10.0
		idx.Add([]storage.ColumnValue{storage.NewFloatValue(floatVal)}, int64(i), 0)
	}

	// Generate random float range bounds
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	minVals := make([]float64, b.N)
	maxVals := make([]float64, b.N)

	for i := 0; i < b.N; i++ {
		minVals[i] = float64(r.Intn(8000)) + r.Float64()
		maxVals[i] = minVals[i] + float64(r.Intn(2000)) + 1.0 // Ensure range is at least 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.GetRowIDsInRange(
			[]storage.ColumnValue{storage.NewFloatValue(minVals[i])},
			[]storage.ColumnValue{storage.NewFloatValue(maxVals[i])},
			true, true,
		)
	}
}

// BenchmarkColumnarIndexSingleIntFilter benchmarks filtering on a single-column integer ColumnarIndex
func BenchmarkColumnarIndexSingleIntFilter(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"num",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add 10000 rows with sequential integer data
	for i := 0; i < 10000; i++ {
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
	}

	// Create schema for expression tests
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "num", Type: storage.INTEGER},
		},
	}

	// Generate random filter values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	filterValues := make([]int64, b.N)
	operators := []storage.Operator{storage.GT, storage.LT, storage.EQ, storage.GTE, storage.LTE}

	for i := 0; i < b.N; i++ {
		filterValues[i] = int64(r.Intn(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create expression: num <op> value
		op := operators[i%len(operators)]
		expr := &expression.SimpleExpression{
			Column:   "num",
			Operator: op,
			Value:    filterValues[i],
		}

		// Create schema-aware expression
		expr.PrepareForSchema(schema)

		idx.GetFilteredRowIDs(expr)
	}
}

// BenchmarkColumnarIndexSingleTimestampFilter benchmarks filtering on a single-column timestamp ColumnarIndex
func BenchmarkColumnarIndexSingleTimestampFilter(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewColumnarIndex(
		"bench_idx",
		"bench_table",
		"ts",
		0,
		storage.TIMESTAMP,
		nil,
		false,
	)

	// Base timestamp
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 10000 rows with sequential timestamp data, 1 hour intervals
	for i := 0; i < 10000; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		idx.Add([]storage.ColumnValue{storage.NewTimestampValue(ts)}, int64(i), 0)
	}

	// Create schema for expression tests
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "ts", Type: storage.TIMESTAMP},
		},
	}

	// Generate random filter values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	filterHours := make([]int, b.N)
	operators := []storage.Operator{storage.GT, storage.LT, storage.EQ, storage.GTE, storage.LTE}

	for i := 0; i < b.N; i++ {
		filterHours[i] = r.Intn(10000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create expression: ts <op> value
		op := operators[i%len(operators)]
		filterTime := baseTime.Add(time.Duration(filterHours[i]) * time.Hour)
		expr := &expression.SimpleExpression{
			Column:   "ts",
			Operator: op,
			Value:    filterTime,
		}

		expr.PrepareForSchema(schema)

		idx.GetFilteredRowIDs(expr)
	}
}

// BenchmarkColumnarIndexSparseData tests performance with sparse data
func BenchmarkColumnarIndexSparseData(b *testing.B) {
	// Create a columnar index with very sparse data (many unique values)
	index := NewColumnarIndex(
		"sparse_index",
		"test_table",
		"sparse_column",
		0,
		storage.INTEGER,
		nil,
		false,
	)

	// Add sparse values - each row has a unique value
	for i := int64(0); i < 10000; i++ {
		index.Add([]storage.ColumnValue{storage.NewIntegerValue(i)}, i, 0)
	}

	// Test with single lookups (worst case for bitmap operations)
	b.Run("SparseEquality", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			value := int64(i % 10000)
			entries, _ := index.Find([]storage.ColumnValue{storage.NewIntegerValue(value)})
			_ = len(entries)
		}
	})

	// Test with range queries (common case)
	b.Run("SparseRange", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			start := int64(i % 9000)
			end := start + 1000
			entries, _ := index.FindRange(
				[]storage.ColumnValue{storage.NewIntegerValue(start)},
				[]storage.ColumnValue{storage.NewIntegerValue(end)},
				true, true)
			_ = len(entries)
		}
	})

	// Test with GetFilteredRowIDs
	b.Run("SparseFilteredRowIDs", func(b *testing.B) {
		// Create schema
		schema := storage.Schema{
			TableName: "test_table",
			Columns: []storage.SchemaColumn{
				{Name: "sparse_column", Type: storage.INTEGER},
			},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			value := int64(i % 10000)
			expr := &expression.SimpleExpression{
				Column:   "sparse_column",
				Operator: storage.EQ,
				Value:    value,
			}
			expr.PrepareForSchema(schema)

			rowIDs := index.GetFilteredRowIDs(expr)
			_ = len(rowIDs)
		}
	})
}
