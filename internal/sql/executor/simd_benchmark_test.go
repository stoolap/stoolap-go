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
	"math"
	"math/rand"
	"testing"

	"github.com/stoolap/stoolap/internal/sql/executor/vectorized"
)

// Benchmark sizes
var benchSizes = []int{1000, 10000, 100000}

// BenchmarkBasicOperations benchmarks the basic arithmetic operations
func BenchmarkBasicOperations(b *testing.B) {
	for _, size := range benchSizes {
		// Prepare test data
		a := make([]float64, size)
		b2 := make([]float64, size) // named b2 to avoid shadowing b
		dst := make([]float64, size)

		for i := 0; i < size; i++ {
			a[i] = float64(i%100) * 0.01
			b2[i] = float64((i+1)%100) * 0.1
		}

		b.Run("AddFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size)) // 8 bytes per float64 * size elements
			for i := 0; i < b.N; i++ {
				vectorized.AddFloat64SIMD(dst, a, b2, size)
			}
		})

		b.Run("MulFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.MulFloat64SIMD(dst, a, b2, size)
			}
		})

		b.Run("SubFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.SubFloat64SIMD(dst, a, b2, size)
			}
		})

		b.Run("DivFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.DivFloat64SIMD(dst, a, b2, size)
			}
		})

		// Baseline (non-vectorized) implementation for comparison
		b.Run("AddBaseline_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					dst[j] = a[j] + b2[j]
				}
			}
		})
	}
}

// BenchmarkScalarOperations benchmarks operations with scalar values
func BenchmarkScalarOperations(b *testing.B) {
	for _, size := range benchSizes {
		// Prepare test data
		a := make([]float64, size)
		dst := make([]float64, size)
		scalar := 3.14159

		for i := 0; i < size; i++ {
			a[i] = float64(i%100) * 0.01
		}

		b.Run("MulScalarFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.MulScalarFloat64SIMD(dst, a, scalar, size)
			}
		})

		b.Run("AddScalarFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.AddScalarFloat64SIMD(dst, a, scalar, size)
			}
		})

		// Baseline implementations
		b.Run("MulScalarBaseline_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					dst[j] = a[j] * scalar
				}
			}
		})
	}
}

// BenchmarkMathOperations benchmarks mathematical functions
func BenchmarkMathOperations(b *testing.B) {
	for _, size := range benchSizes {
		// Prepare test data - values between 0 and 10 for reasonable math operations
		a := make([]float64, size)
		dst := make([]float64, size)

		for i := 0; i < size; i++ {
			a[i] = float64(i%1000) * 0.01 // 0 to 9.99
		}

		b.Run("SqrtFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.SqrtFloat64SIMD(dst, a, size)
			}
		})

		b.Run("ExpFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.ExpFloat64SIMD(dst, a, size)
			}
		})

		b.Run("LogFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			// Ensure no zero or negative values for log
			for i := 0; i < size; i++ {
				a[i] = float64(i%1000)*0.01 + 0.1 // 0.1 to 10.09
			}

			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.LogFloat64SIMD(dst, a, size)
			}
		})

		// Baseline implementation for comparison
		b.Run("SqrtBaseline_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					dst[j] = math.Sqrt(a[j])
				}
			}
		})
	}
}

// BenchmarkStatisticalOperations benchmarks statistical functions
func BenchmarkStatisticalOperations(b *testing.B) {
	for _, size := range benchSizes {
		// Prepare data
		a := make([]float64, size)

		for i := 0; i < size; i++ {
			a[i] = float64(i%1000) * 0.01
		}

		b.Run("SumFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			var result float64
			for i := 0; i < b.N; i++ {
				result = vectorized.SumFloat64SIMD(a, size)
			}
			// Use result to prevent optimization
			_ = result
		})

		b.Run("MeanFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			var result float64
			for i := 0; i < b.N; i++ {
				result = vectorized.MeanFloat64SIMD(a, size)
			}
			// Use result to prevent optimization
			_ = result
		})

		b.Run("MinMaxFloat64SIMD_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			var min, max float64
			for i := 0; i < b.N; i++ {
				min, max = vectorized.MinMaxFloat64SIMD(a, size)
			}
			// Use results to prevent optimization
			_, _ = min, max
		})

		// Baseline implementations
		b.Run("SumBaseline_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				var sum float64
				for j := 0; j < size; j++ {
					sum += a[j]
				}
				// Use result to prevent optimization
				_ = sum
			}
		})
	}
}

// BenchmarkComplexOperations benchmarks more complex operations
func BenchmarkComplexOperations(b *testing.B) {
	for _, size := range benchSizes {
		// Prepare data for trig functions
		result := make([]float64, size)
		values := make([]float64, size)
		scales := make([]float64, size)
		offsets := make([]float64, size)
		angles := make([]float64, size)

		for i := 0; i < size; i++ {
			values[i] = 100.0 + float64(i%10)
			angles[i] = float64(i % 360)
			scales[i] = 1.5 + float64(i%5)*0.1
			offsets[i] = float64(i % 20)
		}

		b.Run("VectorizedTrigBatch_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			for i := 0; i < b.N; i++ {
				vectorized.VectorizedTrigBatch(result, values, scales, offsets, angles, size)
			}
		})

		// Simple boolean operations
		a := make([]float64, size)
		b2 := make([]float64, size)

		for i := 0; i < size; i++ {
			a[i] = float64(i)
			if rand.Float64() < 0.5 {
				b2[i] = float64(i)
			} else {
				b2[i] = float64(i + 1)
			}
		}

		b.Run("CompareEqual_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			var result []bool
			for i := 0; i < b.N; i++ {
				result = vectorized.CompareEqual(a, b2, size)
			}
			// Use result to prevent optimization
			_ = result
		})

		// Generate mask for FilterByMask
		mask := make([]bool, size)
		for i := 0; i < size; i++ {
			mask[i] = i%3 == 0 // 1/3 of elements are true
		}

		dst := make([]float64, size)

		b.Run("FilterByMask_"+sizeStr(size), func(b *testing.B) {
			b.ResetTimer()
			b.SetBytes(int64(8 * size))
			var count int
			for i := 0; i < b.N; i++ {
				count = vectorized.FilterByMask(dst, a, mask, size)
			}
			// Use result to prevent optimization
			_ = count
		})
	}
}

// Helper function to format the size as a string for benchmark names
func sizeStr(size int) string {
	if size == 1000 {
		return "Size_1K"
	} else if size == 10000 {
		return "Size_10K"
	} else if size == 100000 {
		return "Size_100K"
	}
	return "Size_" + fmt.Sprintf("%d", size)
}
