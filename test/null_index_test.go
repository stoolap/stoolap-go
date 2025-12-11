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
	"github.com/stoolap/stoolap-go/internal/storage/btree"
)

// TestNullIndexLookup directly tests the NULL handling in both index types
func TestNullIndexLookup(t *testing.T) {
	t.Run("Bitmap Index NULL Handling", func(t *testing.T) {
		// Create a bitmap index
		idx := bitmap.NewSingleColumnIndex("col", storage.INTEGER, 10)

		// Add test data: positions 0 and 2 are NULL, positions 1, 3, and 4 have values
		idx.AddValue(nil, 0)        // NULL
		idx.AddValue(int64(42), 1)  // Non-NULL
		idx.AddValue(nil, 2)        // NULL
		idx.AddValue(int64(84), 3)  // Non-NULL
		idx.AddValue(int64(100), 4) // Non-NULL

		// IS NULL test
		nullBitmap, err := idx.GetMatchingPositions(storage.ISNULL, nil)
		if err != nil {
			t.Fatalf("Error in IS NULL lookup: %v", err)
		}

		// Collect all positions from bitmap
		nullPositions := []int64{}
		nullBitmap.Iterate(func(pos int64) bool {
			nullPositions = append(nullPositions, pos)
			return true
		})

		// Verify correct positions (0 and 2)
		if len(nullPositions) != 2 {
			t.Errorf("Expected 2 NULL positions, got %d", len(nullPositions))
		}
		hasPos0 := false
		hasPos2 := false
		for _, pos := range nullPositions {
			if pos == 0 {
				hasPos0 = true
			} else if pos == 2 {
				hasPos2 = true
			}
		}
		if !hasPos0 || !hasPos2 {
			t.Errorf("Expected positions 0 and 2 to be NULL, got: %v", nullPositions)
		}

		// IS NOT NULL test
		notNullBitmap, err := idx.GetMatchingPositions(storage.ISNOTNULL, nil)
		if err != nil {
			t.Fatalf("Error in IS NOT NULL lookup: %v", err)
		}

		// Collect all positions from bitmap
		notNullPositions := []int64{}
		notNullBitmap.Iterate(func(pos int64) bool {
			notNullPositions = append(notNullPositions, pos)
			return true
		})

		// Verify correct positions (1, 3, and 4)
		if len(notNullPositions) != 3 {
			t.Errorf("Expected 3 NOT NULL positions, got %d", len(notNullPositions))
		}
		hasPos1 := false
		hasPos3 := false
		hasPos4 := false
		for _, pos := range notNullPositions {
			if pos == 1 {
				hasPos1 = true
			} else if pos == 3 {
				hasPos3 = true
			} else if pos == 4 {
				hasPos4 = true
			}
		}
		if !hasPos1 || !hasPos3 || !hasPos4 {
			t.Errorf("Expected positions 1, 3, and 4 to be NOT NULL, got: %v", notNullPositions)
		}
	})

	t.Run("BTree Index NULL Handling", func(t *testing.T) {
		// Create a btree index
		idx := btree.NewSingleColumnIndex("col", storage.INTEGER, 10)

		// Add test data: positions 0 and 2 are NULL, positions 1, 3, and 4 have values
		idx.AddValue(nil, 0)        // NULL
		idx.AddValue(int64(42), 1)  // Non-NULL
		idx.AddValue(nil, 2)        // NULL
		idx.AddValue(int64(84), 3)  // Non-NULL
		idx.AddValue(int64(100), 4) // Non-NULL

		// IS NULL test
		nullPositions, err := idx.GetMatchingPositions(storage.ISNULL, nil)
		if err != nil {
			t.Fatalf("Error in BTree IS NULL lookup: %v", err)
		}

		// Verify correct positions (0 and 2)
		if len(nullPositions) != 2 {
			t.Errorf("Expected 2 NULL positions from BTree, got %d", len(nullPositions))
		}
		hasPos0 := false
		hasPos2 := false
		for _, pos := range nullPositions {
			if pos == 0 {
				hasPos0 = true
			} else if pos == 2 {
				hasPos2 = true
			}
		}
		if !hasPos0 || !hasPos2 {
			t.Errorf("Expected positions 0 and 2 to be NULL in BTree, got: %v", nullPositions)
		}

		// IS NOT NULL test
		notNullPositions, err := idx.GetMatchingPositions(storage.ISNOTNULL, nil)
		if err != nil {
			t.Fatalf("Error in BTree IS NOT NULL lookup: %v", err)
		}

		// Verify correct positions (1, 3, and 4)
		if len(notNullPositions) != 3 {
			t.Errorf("Expected 3 NOT NULL positions from BTree, got %d", len(notNullPositions))
		}
		hasPos1 := false
		hasPos3 := false
		hasPos4 := false
		for _, pos := range notNullPositions {
			if pos == 1 {
				hasPos1 = true
			} else if pos == 3 {
				hasPos3 = true
			} else if pos == 4 {
				hasPos4 = true
			}
		}
		if !hasPos1 || !hasPos3 || !hasPos4 {
			t.Errorf("Expected positions 1, 3, and 4 to be NOT NULL in BTree, got: %v", notNullPositions)
		}
	})
}
