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
	"github.com/stoolap/stoolap-go/internal/storage/bitmap"
)

func TestBitmapLikePatternMatching(t *testing.T) {
	// Debug flag to help identify test failures
	debugMode := false
	// Create a bitmap index
	bitmapIndex := bitmap.NewSingleColumnIndex("test_column", storage.TEXT, 0)

	// Add some test data - various fruit names at different positions
	testData := []struct {
		value    string
		position int64
	}{
		{"apple", 1},
		{"banana", 2},
		{"pineapple", 3},
		{"grape", 4},
		{"grapefruit", 5},
		{"orange", 6},
		{"strawberry", 7},
		{"blueberry", 8},
		{"blackberry", 9},
		{"cranberry", 10},
	}

	// Add all test data to the index
	for _, data := range testData {
		err := bitmapIndex.AddValue(data.value, data.position)
		if err != nil {
			t.Fatalf("Failed to add value %s to index: %v", data.value, err)
		}
	}

	// Test cases with different LIKE patterns
	tests := []struct {
		name        string
		pattern     string
		expectedPos []int64
	}{
		{
			name:        "Exact Match",
			pattern:     "apple",
			expectedPos: []int64{1}, // Only match for apple
		},
		{
			name:        "Contains Match",
			pattern:     "%berry%",
			expectedPos: []int64{7, 8, 9, 10}, // All berries
		},
		{
			name:        "Starts With",
			pattern:     "grape%",
			expectedPos: []int64{4, 5}, // grape and grapefruit
		},
		{
			name:        "Ends With",
			pattern:     "%apple",
			expectedPos: []int64{1, 3}, // apple and pineapple
		},
		{
			name:        "Contains with Wildcards",
			pattern:     "%a%",
			expectedPos: []int64{1, 2, 3, 4, 5, 6, 7, 9, 10}, // Everything with 'a' - including cranberry
		},
		{
			name:        "Single Character Wildcard",
			pattern:     "_range",
			expectedPos: []int64{6}, // orange
		},
		{
			name:        "Pattern with Wildcards",
			pattern:     "%a%e%",
			expectedPos: []int64{1, 3, 4, 5, 6, 7, 9, 10}, // Has 'a' followed by 'e'
		},
		{
			name:        "No Match",
			pattern:     "mango",
			expectedPos: []int64{}, // No mangos in our data
		},
		{
			name:        "Match Any",
			pattern:     "%",
			expectedPos: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Everything
		},
		{
			name:        "Start and End Pattern",
			pattern:     "b%y",
			expectedPos: []int64{8, 9}, // blueberry, blackberry
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Query using LIKE operator
			resultBitmap, err := bitmapIndex.GetMatchingPositions(storage.LIKE, tt.pattern)
			if err != nil {
				t.Fatalf("Failed to execute LIKE query: %v", err)
			}

			// Check cardinality
			if resultBitmap.Cardinality() != int64(len(tt.expectedPos)) {
				t.Errorf("Expected %d results, got %d", len(tt.expectedPos), resultBitmap.Cardinality())
			}

			// Collect actual positions
			actualPos := make([]int64, 0)
			resultBitmap.Iterate(func(pos int64) bool {
				actualPos = append(actualPos, pos)
				return true
			})

			if debugMode {
				// Print debug information to help update test expectations
				t.Logf("Pattern: %s, Actual positions: %v", tt.pattern, actualPos)

				// Print the value at each position for better understanding
				for _, pos := range actualPos {
					for _, data := range testData {
						if data.position == pos {
							t.Logf("Position %d: %s", pos, data.value)
							break
						}
					}
				}
			}

			// Verify all expected positions are found
			foundMap := make(map[int64]bool)
			for _, pos := range actualPos {
				foundMap[pos] = true
			}

			for _, expectedPos := range tt.expectedPos {
				if !foundMap[expectedPos] {
					t.Errorf("Expected position %d not found in results", expectedPos)
				}
			}
		})
	}
}

// Test for matchSQLPattern function (internal function)
func TestMatchSQLPattern(t *testing.T) {
	tests := []struct {
		str     string
		pattern string
		want    bool
	}{
		// Basic tests
		{"hello", "hello", true},
		{"hello", "world", false},

		// Wildcard % (matches any sequence)
		{"hello", "%", true},
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "h%o", true},
		{"hello", "%e%", true},
		{"hello", "h%l%o", true},
		{"hello", "%hello%", true},
		{"hello", "world%", false},

		// Wildcard _ (matches single character)
		{"hello", "h_llo", true},
		{"hello", "_ello", true},
		{"hello", "hell_", true},
		{"hello", "_____", true},
		{"hello", "h__l_", true},
		{"hello", "___", false},

		// Combined wildcards
		{"hello", "h_%", true},
		{"hello", "%l_o", true},
		{"hello", "_e%o", true},
		{"hello", "%_l%", true},

		// Edge cases
		{"", "", true},
		{"", "%", true},
		{"hello", "", false},
		{"a", "_", true},
		{"a", "%", true},
		{"a", "_%", true},
		{"a", "%_", true},
	}

	for _, tt := range tests {
		t.Run(tt.str+"-"+tt.pattern, func(t *testing.T) {
			// Call the exported version
			if got := bitmap.MatchSQLPattern(tt.str, tt.pattern); got != tt.want {
				t.Errorf("MatchSQLPattern(%q, %q) = %v, want %v", tt.str, tt.pattern, got, tt.want)
			}
		})
	}
}
