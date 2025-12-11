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

	"github.com/stoolap/stoolap-go/internal/sql/executor/vectorized"
)

// TestSIMDFunctions tests that our SIMD optimized functions work correctly
func TestSIMDFunctions(t *testing.T) {
	// Test vector addition
	t.Run("AddFloat64SIMD", func(t *testing.T) {
		a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		b := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
		dst := make([]float64, len(a))
		expected := []float64{11, 22, 33, 44, 55, 66, 77, 88, 99, 110}

		vectorized.AddFloat64SIMD(dst, a, b, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("AddFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test vector multiplication
	t.Run("MulFloat64SIMD", func(t *testing.T) {
		a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		b := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
		dst := make([]float64, len(a))
		expected := []float64{10, 40, 90, 160, 250, 360, 490, 640, 810, 1000}

		vectorized.MulFloat64SIMD(dst, a, b, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("MulFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test scalar addition
	t.Run("AddScalarFloat64SIMD", func(t *testing.T) {
		a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		scalar := 10.0
		dst := make([]float64, len(a))
		expected := []float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

		vectorized.AddScalarFloat64SIMD(dst, a, scalar, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("AddScalarFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test scalar multiplication
	t.Run("MulScalarFloat64SIMD", func(t *testing.T) {
		a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		scalar := 10.0
		dst := make([]float64, len(a))
		expected := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

		vectorized.MulScalarFloat64SIMD(dst, a, scalar, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("MulScalarFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test vector subtraction
	t.Run("SubFloat64SIMD", func(t *testing.T) {
		a := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
		b := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		dst := make([]float64, len(a))
		expected := []float64{9, 18, 27, 36, 45, 54, 63, 72, 81, 90}

		vectorized.SubFloat64SIMD(dst, a, b, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("SubFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test vector division
	t.Run("DivFloat64SIMD", func(t *testing.T) {
		a := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
		b := []float64{2, 4, 5, 8, 10, 12, 14, 16, 18, 20}
		dst := make([]float64, len(a))
		expected := []float64{5, 5, 6, 5, 5, 5, 5, 5, 5, 5}

		vectorized.DivFloat64SIMD(dst, a, b, len(a))

		for i, v := range expected {
			if dst[i] != v {
				t.Errorf("DivFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], v)
			}
		}
	})

	// Test larger arrays to ensure block processing works correctly
	t.Run("LargeArrays", func(t *testing.T) {
		// Create arrays that are larger than the unrolling factor
		size := 1000
		a := make([]float64, size)
		b := make([]float64, size)
		dst := make([]float64, size)
		expected := make([]float64, size)

		// Initialize with test values
		for i := 0; i < size; i++ {
			a[i] = float64(i)
			b[i] = float64(i * 2)
			expected[i] = float64(i) + float64(i*2) // a[i] + b[i]
		}

		// Test addition on large arrays
		vectorized.AddFloat64SIMD(dst, a, b, size)

		// Check results at key positions
		checkPoints := []int{0, 1, 7, 8, 15, 16, 63, 64, 100, size - 1}
		for _, i := range checkPoints {
			if dst[i] != expected[i] {
				t.Errorf("Large AddFloat64SIMD: dst[%d] = %f, expected %f", i, dst[i], expected[i])
			}
		}
	})
}
