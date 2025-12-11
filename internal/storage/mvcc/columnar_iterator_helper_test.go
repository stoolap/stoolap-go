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
	"reflect"
	"sort"
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// TestIntersectSortedIDs tests the intersectSortedIDs helper function
func TestIntersectSortedIDs(t *testing.T) {
	// Test cases for intersectSortedIDs
	testCases := []struct {
		name     string
		a        []int64
		b        []int64
		expected []int64
	}{
		{
			name:     "Empty slices",
			a:        []int64{},
			b:        []int64{},
			expected: nil,
		},
		{
			name:     "One empty slice",
			a:        []int64{1, 2, 3},
			b:        []int64{},
			expected: nil,
		},
		{
			name:     "Disjoint sets",
			a:        []int64{1, 3, 5},
			b:        []int64{2, 4, 6},
			expected: []int64{},
		},
		{
			name:     "Identical sets",
			a:        []int64{1, 2, 3},
			b:        []int64{1, 2, 3},
			expected: []int64{1, 2, 3},
		},
		{
			name:     "Subset",
			a:        []int64{1, 2, 3, 4, 5},
			b:        []int64{2, 4},
			expected: []int64{2, 4},
		},
		{
			name:     "Partial overlap",
			a:        []int64{1, 2, 3, 4},
			b:        []int64{3, 4, 5, 6},
			expected: []int64{3, 4},
		},
		{
			name:     "Large sets",
			a:        generateSequence(1, 1000),
			b:        generateSequence(500, 1500),
			expected: generateSequence(500, 1000),
		},
		{
			name:     "Unsorted input",
			a:        []int64{5, 3, 1, 4, 2},
			b:        []int64{6, 4, 2},
			expected: []int64{2, 4},
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make copies to ensure originals aren't modified
			a := make([]int64, len(tc.a))
			b := make([]int64, len(tc.b))
			copy(a, tc.a)
			copy(b, tc.b)

			// Run intersection
			result := intersectSortedIDs(a, b)

			// Sort result and expected for comparison
			sort.Slice(result, func(i, j int) bool {
				return result[i] < result[j]
			})
			sort.Slice(tc.expected, func(i, j int) bool {
				return tc.expected[i] < tc.expected[j]
			})

			// Compare result with expected
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestIsSorted tests the isSorted helper function
func TestIsSorted(t *testing.T) {
	testCases := []struct {
		name     string
		ids      []int64
		expected bool
	}{
		{
			name:     "Empty slice",
			ids:      []int64{},
			expected: true,
		},
		{
			name:     "Single element",
			ids:      []int64{5},
			expected: true,
		},
		{
			name:     "Sorted ascending",
			ids:      []int64{1, 2, 3, 4, 5},
			expected: true,
		},
		{
			name:     "Unsorted",
			ids:      []int64{5, 2, 3, 1, 4},
			expected: false,
		},
		{
			name:     "Equal elements",
			ids:      []int64{1, 1, 1, 1},
			expected: true,
		},
		{
			name:     "Descending",
			ids:      []int64{5, 4, 3, 2, 1},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isSorted(tc.ids)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestValueToColumnValue tests the storage.ValueToColumnValue function
func TestValueToColumnValue(t *testing.T) {
	testCases := []struct {
		name          string
		value         interface{}
		expectedType  string
		shouldBeValid bool
	}{
		{
			name:          "Integer",
			value:         42,
			expectedType:  "Integer",
			shouldBeValid: true,
		},
		{
			name:          "Int64",
			value:         int64(42),
			expectedType:  "Integer",
			shouldBeValid: true,
		},
		{
			name:          "Float64",
			value:         42.5,
			expectedType:  "Float",
			shouldBeValid: true,
		},
		{
			name:          "String",
			value:         "test",
			expectedType:  "String",
			shouldBeValid: true,
		},
		{
			name:          "Boolean",
			value:         true,
			expectedType:  "Boolean",
			shouldBeValid: true,
		},
		{
			name:          "Nil",
			value:         nil,
			expectedType:  "Nil",
			shouldBeValid: false,
		},
		{
			name:          "Unsupported type",
			value:         []int{1, 2, 3},
			expectedType:  "Unknown",
			shouldBeValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := storage.ValueToColumnValue(tc.value, storage.NULL)

			if tc.shouldBeValid && result == nil {
				t.Errorf("Expected valid value, got nil")
				return
			}

			if !tc.shouldBeValid && result != nil && !result.IsNull() {
				t.Errorf("Expected nil or NULL value, got %v", result)
				return
			}

			// Skip type checking for nil/invalid cases
			if !tc.shouldBeValid {
				return
			}

			// Check type
			var typeMatches bool
			switch tc.expectedType {
			case "Integer":
				_, typeMatches = result.AsInt64()
			case "Float":
				_, typeMatches = result.AsFloat64()
			case "String":
				_, typeMatches = result.AsString()
			case "Boolean":
				_, typeMatches = result.AsBoolean()
			}

			if !typeMatches {
				t.Errorf("Expected %s type, but conversion failed", tc.expectedType)
			}
		})
	}
}

// Helper function to generate a sequence of integers
func generateSequence(start, end int64) []int64 {
	result := make([]int64, 0, end-start+1)
	for i := start; i <= end; i++ {
		result = append(result, i)
	}
	return result
}
