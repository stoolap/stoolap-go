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

func TestBitmapIndexRangeOperations(t *testing.T) {
	// Create a bitmap index
	bitmapIndex := bitmap.NewSingleColumnIndex("test_column", storage.INTEGER, 0)

	// Add some test data
	// We'll use integer values from 10 to 100 in steps of 10
	for i := int64(10); i <= 100; i += 10 {
		err := bitmapIndex.AddValue(i, i/10) // Use i/10 as position
		if err != nil {
			t.Fatalf("Failed to add value %d to index: %v", i, err)
		}
	}

	// Test range query: 30 <= value <= 70
	minVal := int64(30)
	maxVal := int64(70)

	resultBitmap, err := bitmapIndex.GetMatchingRange(minVal, maxVal, true, true)
	if err != nil {
		t.Fatalf("Failed to execute range query: %v", err)
	}

	// We should have 5 bits set (positions 3, 4, 5, 6, 7 corresponding to values 30, 40, 50, 60, 70)
	if resultBitmap.Cardinality() != 5 {
		t.Errorf("Expected 5 bits set, got %d", resultBitmap.Cardinality())
	}

	// Verify the expected positions
	expected := map[int64]bool{3: true, 4: true, 5: true, 6: true, 7: true}
	resultBitmap.Iterate(func(pos int64) bool {
		if !expected[pos] {
			t.Errorf("Unexpected position: %d", pos)
		}
		delete(expected, pos) // Remove found positions
		return true
	})

	// Make sure all expected positions were found
	if len(expected) > 0 {
		t.Errorf("Some expected positions were not found: %v", expected)
	}

	// Test exclusive range query: 30 < value < 70
	resultBitmap, err = bitmapIndex.GetMatchingRange(minVal, maxVal, false, false)
	if err != nil {
		t.Fatalf("Failed to execute exclusive range query: %v", err)
	}

	// We should have 3 bits set (positions 4, 5, 6 corresponding to values 40, 50, 60)
	if resultBitmap.Cardinality() != 3 {
		t.Errorf("Expected 3 bits set, got %d", resultBitmap.Cardinality())
	}

	// Verify the expected positions
	expected = map[int64]bool{4: true, 5: true, 6: true}
	resultBitmap.Iterate(func(pos int64) bool {
		if !expected[pos] {
			t.Errorf("Unexpected position: %d", pos)
		}
		delete(expected, pos) // Remove found positions
		return true
	})

	// Make sure all expected positions were found
	if len(expected) > 0 {
		t.Errorf("Some expected positions were not found: %v", expected)
	}

	// Test open-ended range query: value >= 80
	resultBitmap, err = bitmapIndex.GetMatchingRange(int64(80), nil, true, false)
	if err != nil {
		t.Fatalf("Failed to execute open-ended range query: %v", err)
	}

	// We should have 3 bits set (positions 8, 9, 10 corresponding to values 80, 90, 100)
	if resultBitmap.Cardinality() != 3 {
		t.Errorf("Expected 3 bits set, got %d", resultBitmap.Cardinality())
	}

	// Verify the expected positions
	expected = map[int64]bool{8: true, 9: true, 10: true}
	resultBitmap.Iterate(func(pos int64) bool {
		if !expected[pos] {
			t.Errorf("Unexpected position: %d", pos)
		}
		delete(expected, pos) // Remove found positions
		return true
	})
}

func TestBitmapIndexRangeMultipleTypes(t *testing.T) {
	// Test float range
	t.Run("FloatRange", func(t *testing.T) {
		floatIndex := bitmap.NewSingleColumnIndex("float_column", storage.FLOAT, 0)

		// Add float values from 1.5 to 10.5 with increments of 1.0
		for i := int64(1); i <= 10; i++ {
			val := float64(i) + 0.5
			err := floatIndex.AddValue(val, i)
			if err != nil {
				t.Fatalf("Failed to add float value %f to index: %v", val, err)
			}
		}

		// Test range query: 3.5 <= value <= 7.5
		resultBitmap, err := floatIndex.GetMatchingRange(3.5, 7.5, true, true)
		if err != nil {
			t.Fatalf("Failed to execute float range query: %v", err)
		}

		// We should have 5 bits set (positions 3, 4, 5, 6, 7 corresponding to values 3.5, 4.5, 5.5, 6.5, 7.5)
		if resultBitmap.Cardinality() != 5 {
			t.Errorf("Expected 5 bits set, got %d", resultBitmap.Cardinality())
		}

		// Verify exact positions
		expected := map[int64]bool{3: true, 4: true, 5: true, 6: true, 7: true}
		resultBitmap.Iterate(func(pos int64) bool {
			if !expected[pos] {
				t.Errorf("Unexpected position: %d", pos)
			}
			delete(expected, pos)
			return true
		})

		if len(expected) > 0 {
			t.Errorf("Some expected positions were not found: %v", expected)
		}
	})

	// Test string range
	t.Run("StringRange", func(t *testing.T) {
		stringIndex := bitmap.NewSingleColumnIndex("string_column", storage.TEXT, 0)

		// Add string values (fruit names in alphabetical order)
		fruits := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew"}

		for i, fruit := range fruits {
			err := stringIndex.AddValue(fruit, int64(i))
			if err != nil {
				t.Fatalf("Failed to add string value %s to index: %v", fruit, err)
			}
		}

		// Test range query: "cherry" <= value <= "grape"
		resultBitmap, err := stringIndex.GetMatchingRange("cherry", "grape", true, true)
		if err != nil {
			t.Fatalf("Failed to execute string range query: %v", err)
		}

		// We should have 5 bits set (positions 2, 3, 4, 5, 6 corresponding to values "cherry", "date", "elderberry", "fig", "grape")
		if resultBitmap.Cardinality() != 5 {
			t.Errorf("Expected 5 bits set, got %d", resultBitmap.Cardinality())
		}

		// Verify exact positions
		expected := map[int64]bool{2: true, 3: true, 4: true, 5: true, 6: true}
		resultBitmap.Iterate(func(pos int64) bool {
			if !expected[pos] {
				t.Errorf("Unexpected position: %d", pos)
			}
			delete(expected, pos)
			return true
		})

		if len(expected) > 0 {
			t.Errorf("Some expected positions were not found: %v", expected)
		}
	})

	// Test boolean values
	t.Run("BooleanRange", func(t *testing.T) {
		boolIndex := bitmap.NewSingleColumnIndex("bool_column", storage.BOOLEAN, 0)

		// Add boolean values: false at position 0, true at position 1
		boolIndex.AddValue(false, 0)
		boolIndex.AddValue(true, 1)

		// For boolean values, using range operations is unusual but should still work logically
		resultBitmap, err := boolIndex.GetMatchingRange(false, true, true, true)
		if err != nil {
			t.Fatalf("Failed to execute boolean range query: %v", err)
		}

		// We should have 2 bits set (positions 0 and 1)
		if resultBitmap.Cardinality() != 2 {
			t.Errorf("Expected 2 bits set, got %d", resultBitmap.Cardinality())
		}

		// Verify exact positions
		expected := map[int64]bool{0: true, 1: true}
		resultBitmap.Iterate(func(pos int64) bool {
			if !expected[pos] {
				t.Errorf("Unexpected position in boolean range: %d", pos)
			}
			delete(expected, pos)
			return true
		})
	})
}
