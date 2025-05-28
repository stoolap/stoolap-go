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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stoolap/stoolap/internal/sql/executor/vectorized"
)

// BenchmarkSIMDOperations tests the performance of our SIMD operations
func BenchmarkSIMDOperations(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("MulFloat64SIMD_Size=%d", size), func(b *testing.B) {
			// Prepare test data
			a := make([]float64, size)
			b2 := make([]float64, size) // named b2 to avoid shadowing b
			dst := make([]float64, size)

			// Fill with test data
			for i := 0; i < size; i++ {
				a[i] = float64(i%100) * 0.01
				b2[i] = float64((i+1)%100) * 0.1
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				vectorized.MulFloat64SIMD(dst, a, b2, size)

				// Use result to prevent compiler optimization
				_ = dst[0]
			}
		})

		b.Run(fmt.Sprintf("AddFloat64SIMD_Size=%d", size), func(b *testing.B) {
			// Prepare test data
			a := make([]float64, size)
			b2 := make([]float64, size)
			dst := make([]float64, size)

			// Fill with test data
			for i := 0; i < size; i++ {
				a[i] = float64(i%100) * 0.01
				b2[i] = float64((i+1)%100) * 0.1
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				vectorized.AddFloat64SIMD(dst, a, b2, size)

				// Use result to prevent compiler optimization
				_ = dst[0]
			}
		})

		// Add a non-SIMD baseline for comparison
		b.Run(fmt.Sprintf("MulFloat64Loop_Size=%d", size), func(b *testing.B) {
			// Prepare test data
			a := make([]float64, size)
			b2 := make([]float64, size)
			dst := make([]float64, size)

			// Fill with test data
			for i := 0; i < size; i++ {
				a[i] = float64(i%100) * 0.01
				b2[i] = float64((i+1)%100) * 0.1
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Simple loop multiplication
				for j := 0; j < size; j++ {
					dst[j] = a[j] * b2[j]
				}

				// Use result to prevent compiler optimization
				_ = dst[0]
			}
		})
	}
}

// setupVectorizedBatchData creates a test batch with random data
func setupVectorizedBatchData(size int) *vectorized.Batch {
	// Create a vectorized batch with the given size
	batch := vectorized.NewBatch(size) // Use the proper size

	// Add columns of different types
	batch.AddIntColumn("id")
	batch.AddIntColumn("quantity")
	batch.AddFloatColumn("price")
	batch.AddFloatColumn("discount")
	batch.AddStringColumn("name")
	batch.AddStringColumn("category")
	batch.AddBoolColumn("in_stock")
	batch.AddBoolColumn("is_featured")

	// Generate random data
	r := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility

	// Categories for string data
	categories := []string{"Electronics", "Clothing", "Furniture", "Books", "Food"}

	// Fill the batch with data
	for i := 0; i < size; i++ {
		// Integer columns
		batch.IntColumns["id"][i] = int64(i + 1)
		batch.IntColumns["quantity"][i] = int64(r.Intn(100))
		batch.NullBitmaps["id"][i] = false
		batch.NullBitmaps["quantity"][i] = r.Float32() < 0.05 // 5% NULL values

		// Float columns
		batch.FloatColumns["price"][i] = r.Float64() * 1000
		batch.FloatColumns["discount"][i] = r.Float64() * 0.5
		batch.NullBitmaps["price"][i] = false
		batch.NullBitmaps["discount"][i] = r.Float32() < 0.1 // 10% NULL values

		// String columns
		batch.StringColumns["name"][i] = fmt.Sprintf("Product%c", 'A'+(i%26))
		batch.StringColumns["category"][i] = categories[i%len(categories)]
		batch.NullBitmaps["name"][i] = false
		batch.NullBitmaps["category"][i] = r.Float32() < 0.01 // 1% NULL values

		// Boolean columns
		batch.BoolColumns["in_stock"][i] = r.Float32() < 0.8    // 80% in stock
		batch.BoolColumns["is_featured"][i] = r.Float32() < 0.2 // 20% featured
		batch.NullBitmaps["in_stock"][i] = false
		batch.NullBitmaps["is_featured"][i] = r.Float32() < 0.05 // 5% NULL values
	}

	return batch
}

// setupRowBasedData creates row-based data for traditional execution
func setupRowBasedData(size int) ([]map[string]interface{}, []string) {
	// The rows to process
	rows := make([]map[string]interface{}, size)

	// Column names
	columns := []string{"id", "quantity", "price", "discount", "name", "category", "in_stock", "is_featured"}

	// Generate the same data as in the vectorized version
	r := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility
	categories := []string{"Electronics", "Clothing", "Furniture", "Books", "Food"}

	for i := 0; i < size; i++ {
		row := make(map[string]interface{})

		// Integer values
		row["id"] = int64(i + 1)
		if r.Float32() < 0.05 {
			row["quantity"] = nil // 5% NULL
		} else {
			row["quantity"] = int64(r.Intn(100))
		}

		// Float values
		row["price"] = r.Float64() * 1000
		if r.Float32() < 0.1 {
			row["discount"] = nil // 10% NULL
		} else {
			row["discount"] = r.Float64() * 0.5
		}

		// String values
		row["name"] = fmt.Sprintf("Product%c", 'A'+(i%26))
		if r.Float32() < 0.01 {
			row["category"] = nil // 1% NULL
		} else {
			row["category"] = categories[i%len(categories)]
		}

		// Boolean values
		row["in_stock"] = r.Float32() < 0.8 // 80% in stock
		if r.Float32() < 0.05 {
			row["is_featured"] = nil // 5% NULL
		} else {
			row["is_featured"] = r.Float32() < 0.2 // 20% featured
		}

		rows[i] = row
	}

	return rows, columns
}

// BenchmarkSimpleComparison compares performance of vectorized vs. non-vectorized operations
func BenchmarkSimpleComparison(b *testing.B) {
	// Data sizes to test
	dataSizes := []int{1000, 10000}

	for _, size := range dataSizes {
		// Setup vectorized data
		batch := setupVectorizedBatchData(size)

		// Setup row-based data
		rows, _ := setupRowBasedData(size)

		// Benchmark a simple int comparison
		b.Run(fmt.Sprintf("VectorizedIntComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a compare processor for "id > 500"
				processor := vectorized.NewCompareConstantProcessor("id", ">", int64(500))

				// Process the batch
				result, _ := processor.Process(batch)

				// Use the result to prevent optimization
				_ = result.Size

				// Return the result batch to the pool
				vectorized.ReleaseBatch(result)
			}
		})

		// Benchmark the same comparison using traditional row-based approach
		b.Run(fmt.Sprintf("RowBasedIntComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Manual comparison
				matchCount := 0
				for _, row := range rows {
					idVal, ok := row["id"].(int64)
					if ok && idVal > 500 {
						matchCount++
					}
				}

				// Use the result to prevent optimization
				_ = matchCount
			}
		})
	}
}

// BenchmarkStringOperations compares performance for string operations
func BenchmarkStringOperations(b *testing.B) {
	// Data sizes to test
	dataSizes := []int{1000, 10000}

	for _, size := range dataSizes {
		// Setup vectorized data
		batch := setupVectorizedBatchData(size)

		// Setup row-based data
		rows, _ := setupRowBasedData(size)

		// Benchmark string comparison in vectorized approach
		b.Run(fmt.Sprintf("VectorizedStringComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a compare processor for "category = 'Electronics'"
				processor := vectorized.NewCompareConstantProcessor("category", "=", "Electronics")

				// Process the batch
				result, _ := processor.Process(batch)

				// Use the result to prevent optimization
				_ = result.Size

				// Return the result batch to the pool
				vectorized.ReleaseBatch(result)
			}
		})

		// Benchmark the same string comparison using traditional row-based approach
		b.Run(fmt.Sprintf("RowBasedStringComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Manual string comparison
				matchCount := 0
				for _, row := range rows {
					categoryVal, ok := row["category"].(string)
					if ok && categoryVal == "Electronics" {
						matchCount++
					}
				}

				// Use the result to prevent optimization
				_ = matchCount
			}
		})
	}
}

// setupBatchWithDoubleColumn creates a batch with an id_double column for comparison tests
func setupBatchWithDoubleColumn(size int) *vectorized.Batch {
	batch := vectorized.NewBatch(size)

	// Add basic columns
	batch.AddIntColumn("id")
	batch.AddIntColumn("id_double")

	// Fill the columns
	for i := 0; i < size; i++ {
		batch.IntColumns["id"][i] = int64(i + 1)
		// id_double is id * 2 for about half the rows, otherwise different
		if i%2 == 0 {
			batch.IntColumns["id_double"][i] = int64(i+1) * 2
		} else {
			batch.IntColumns["id_double"][i] = int64(i+1)*2 + 1
		}
		batch.NullBitmaps["id"][i] = false
		batch.NullBitmaps["id_double"][i] = false
	}

	return batch
}

// setupRowsWithDoubleColumn creates a slice of rows with an id_double column for comparison tests
func setupRowsWithDoubleColumn(size int) []map[string]interface{} {
	rows := make([]map[string]interface{}, size)

	for i := 0; i < size; i++ {
		row := make(map[string]interface{})
		row["id"] = int64(i + 1)

		// id_double is id * 2 for about half the rows, otherwise different
		if i%2 == 0 {
			row["id_double"] = int64(i+1) * 2
		} else {
			row["id_double"] = int64(i+1)*2 + 1
		}

		rows[i] = row
	}

	return rows
}

// BenchmarkCompareColumns compares the performance of column comparison
func BenchmarkCompareColumns(b *testing.B) {
	// Data sizes to test - stay below MaxBatchSize (32K)
	dataSizes := []int{1000, 10000, 30000}

	for _, size := range dataSizes {
		// Skip larger sizes in short mode
		if testing.Short() && size > 10000 {
			continue
		}

		// Create fresh batch and rows for each test size
		batch := setupBatchWithDoubleColumn(size)
		rows := setupRowsWithDoubleColumn(size)

		// Benchmark column comparison in vectorized approach
		b.Run(fmt.Sprintf("VectorizedColumnComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Create a compare processor for "id_double = id * 2"
				processor := vectorized.NewCompareColumnsProcessor("id_double", "=", "id")

				// Process the batch
				result, _ := processor.Process(batch)

				// Use the result to prevent optimization
				_ = result.Size

				// Return the result batch to the pool
				vectorized.ReleaseBatch(result)
			}
		})

		// Benchmark the same comparison using traditional row-based approach
		b.Run(fmt.Sprintf("RowBasedColumnComparison_Size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Manual column comparison
				matchCount := 0
				for _, row := range rows {
					idVal, ok1 := row["id"].(int64)
					idDoubleVal, ok2 := row["id_double"].(int64)

					if ok1 && ok2 && idDoubleVal == idVal*2 {
						matchCount++
					}
				}

				// Use the result to prevent optimization
				_ = matchCount
			}
		})
	}
}
