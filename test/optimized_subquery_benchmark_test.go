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
)

// BenchmarkINSubqueryOptimization demonstrates the performance difference between
// slice-based and hash-based IN subquery implementations
func BenchmarkINSubqueryOptimization(b *testing.B) {
	testCases := []struct {
		name          string
		subquerySize  int
		mainTableSize int
	}{
		{"Small_10_100", 10, 100},
		{"Medium_100_1000", 100, 1000},
		{"Large_1000_10000", 1000, 10000},
		{"VeryLarge_10000_100000", 10000, 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Simulate slice-based lookup (current implementation)
			b.Run("Slice_O(n*m)", func(b *testing.B) {
				// Create test data
				subqueryResults := make([]interface{}, tc.subquerySize)
				for i := 0; i < tc.subquerySize; i++ {
					subqueryResults[i] = i * 2 // Even numbers
				}

				mainTableValues := make([]interface{}, tc.mainTableSize)
				for i := 0; i < tc.mainTableSize; i++ {
					mainTableValues[i] = i
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					matches := 0
					// O(n*m) complexity
					for _, mainVal := range mainTableValues {
						for _, subVal := range subqueryResults {
							if mainVal == subVal {
								matches++
								break
							}
						}
					}
					_ = matches
				}
			})

			// Simulate hash-based lookup (optimized implementation)
			b.Run("Hash_O(n+m)", func(b *testing.B) {
				// Create test data
				subqueryResults := make(map[interface{}]bool, tc.subquerySize)
				for i := 0; i < tc.subquerySize; i++ {
					subqueryResults[i*2] = true // Even numbers
				}

				mainTableValues := make([]interface{}, tc.mainTableSize)
				for i := 0; i < tc.mainTableSize; i++ {
					mainTableValues[i] = i
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					matches := 0
					// O(n+m) complexity
					for _, mainVal := range mainTableValues {
						if subqueryResults[mainVal] {
							matches++
						}
					}
					_ = matches
				}
			})
		})
	}
}

// BenchmarkCTEMaterializationStrategies compares different CTE materialization approaches
func BenchmarkCTEMaterializationStrategies(b *testing.B) {
	rows := 10000
	cols := 10

	// Generate test data
	type Row struct {
		values []interface{}
	}

	testData := make([]Row, rows)
	for i := 0; i < rows; i++ {
		row := Row{values: make([]interface{}, cols)}
		for j := 0; j < cols; j++ {
			row.values[j] = float64(i * j)
		}
		testData[i] = row
	}

	b.Run("MapBased_Current", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Current implementation using maps
			materializedRows := make([]map[string]interface{}, 0, rows)
			columns := make([]string, cols)
			for j := 0; j < cols; j++ {
				columns[j] = string(rune('A' + j))
			}

			for _, row := range testData {
				rowMap := make(map[string]interface{}, cols)
				for j, col := range columns {
					rowMap[col] = row.values[j]
				}
				materializedRows = append(materializedRows, rowMap)
			}

			// Simulate reading back
			sum := 0.0
			for _, row := range materializedRows {
				if val, ok := row["A"]; ok {
					if v, ok := val.(float64); ok {
						sum += v
					}
				}
			}
			_ = sum
		}
	})

	b.Run("Columnar_Optimized", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Optimized columnar storage
			columns := make([][]interface{}, cols)
			for j := 0; j < cols; j++ {
				columns[j] = make([]interface{}, 0, rows)
			}

			// Store data column-wise
			for _, row := range testData {
				for j, val := range row.values {
					columns[j] = append(columns[j], val)
				}
			}

			// Simulate reading back
			sum := 0.0
			firstCol := columns[0] // Direct access to first column
			for _, val := range firstCol {
				if v, ok := val.(float64); ok {
					sum += v
				}
			}
			_ = sum
		}
	})

	b.Run("RowBased_Optimized", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Optimized row-based storage (no maps)
			materializedRows := make([][]interface{}, 0, rows)

			for _, row := range testData {
				rowCopy := make([]interface{}, cols)
				copy(rowCopy, row.values)
				materializedRows = append(materializedRows, rowCopy)
			}

			// Simulate reading back
			sum := 0.0
			for _, row := range materializedRows {
				if len(row) > 0 {
					if v, ok := row[0].(float64); ok {
						sum += v
					}
				}
			}
			_ = sum
		}
	})
}

// BenchmarkSubqueryResultCaching tests the impact of caching repeated subqueries
func BenchmarkSubqueryResultCaching(b *testing.B) {
	// Simulate a query with repeated scalar subqueries
	b.Run("Without_Cache", func(b *testing.B) {
		b.ReportAllocs()

		computeAverage := func() float64 {
			// Simulate expensive computation
			sum := 0.0
			for i := 0; i < 10000; i++ {
				sum += float64(i)
			}
			return sum / 10000
		}

		for i := 0; i < b.N; i++ {
			// Simulate query with 5 references to same subquery
			results := make([]float64, 5)
			for j := 0; j < 5; j++ {
				results[j] = computeAverage() // Recomputed each time
			}
			_ = results
		}
	})

	b.Run("With_Cache", func(b *testing.B) {
		b.ReportAllocs()

		computeAverage := func() float64 {
			// Simulate expensive computation
			sum := 0.0
			for i := 0; i < 10000; i++ {
				sum += float64(i)
			}
			return sum / 10000
		}

		for i := 0; i < b.N; i++ {
			// Simulate query with cache
			cache := make(map[string]float64)
			results := make([]float64, 5)

			for j := 0; j < 5; j++ {
				cacheKey := "avg_query"
				if val, ok := cache[cacheKey]; ok {
					results[j] = val // Use cached value
				} else {
					val := computeAverage()
					cache[cacheKey] = val
					results[j] = val
				}
			}
			_ = results
		}
	})
}
