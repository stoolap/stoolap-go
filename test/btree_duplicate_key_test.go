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
	"testing"

	"github.com/stoolap/stoolap-go/internal/btree"
)

// TestBTreeDuplicateKeyAfterSplit tests the bug where inserting a key that equals
// a promoted median after a split creates a duplicate instead of updating the promoted key
func TestBTreeDuplicateKeyAfterSplit(t *testing.T) {
	// Create a B-tree with string values
	tree := btree.NewInt64BTree[string]()

	// Constants from the btree package
	const bTreeMaxKeys = 31 // This matches the constant in int64_btree.go

	// Step 1: make the tree two-level by inserting 32 sequential keys (0 to 31)
	for k := int64(0); k <= bTreeMaxKeys; k++ {
		tree.Insert(k, fmt.Sprintf("orig-%d", k))
	}
	// At this point: root.keys = [15], with children containing 0-14 and 16-31

	// Step 2: fill the left child until it is full (31 keys total)
	for k := int64(-17); k <= -2; k++ { // 16 extra keys: -17, -16, ..., -2
		tree.Insert(k, fmt.Sprintf("extra-%d", k))
	}
	// Left child now has keys: [-17, -16, ..., -2, 0, 1, ..., 14] (32 keys total)
	// The median of the full left child should be -2

	// Step 3: Insert the median key (-2) again with a new value
	// This should trigger a split where -2 is promoted to the parent
	// But then we immediately try to insert -2 again
	tree.Insert(-2, "new-value-for-median")

	// Step 4: Check how many times -2 appears in the tree
	count := 0
	var values []string
	tree.ForEach(func(k int64, v string) bool {
		if k == -2 {
			values = append(values, v)
			count++
		}
		return true
	})

	// There should only be ONE occurrence of -2
	if count != 1 {
		t.Errorf("Expected exactly 1 occurrence of key -2, but found %d", count)
		t.Logf("Values found for key -2: %v", values)
	}

	// Step 5: Verify that Search returns the latest value
	value, found := tree.Search(-2)
	if !found {
		t.Error("Key -2 should be found in the tree")
	}
	if value != "new-value-for-median" {
		t.Errorf("Expected value 'new-value-for-median' for key -2, got '%s'", value)
	}

	// Step 6: Verify that Delete works correctly
	deleted := tree.Delete(-2)
	if !deleted {
		t.Error("Should be able to delete key -2")
	}

	// After deletion, the key should not be found
	_, found = tree.Search(-2)
	if found {
		t.Error("Key -2 should not be found after deletion")
	}

	// And ForEach should not find it either
	count = 0
	tree.ForEach(func(k int64, v string) bool {
		if k == -2 {
			count++
		}
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 occurrences of key -2 after deletion, but found %d", count)
	}
}

// TestBTreeDuplicateKeySimple tests a simpler case of the same issue
func TestBTreeDuplicateKeySimple(t *testing.T) {
	tree := btree.NewInt64BTree[string]()

	// Create a scenario where we force a split and then insert the promoted key again
	const bTreeMaxKeys = 31

	// Fill root to capacity
	for k := int64(0); k <= bTreeMaxKeys; k++ {
		tree.Insert(k, fmt.Sprintf("value-%d", k))
	}

	// This should cause a split, promoting key 15 to a new root
	// Now insert key 15 again with a different value
	tree.Insert(15, "updated-value-15")

	// Verify only one occurrence of key 15
	count := 0
	var foundValue string
	tree.ForEach(func(k int64, v string) bool {
		if k == 15 {
			count++
			foundValue = v
		}
		return true
	})

	if count != 1 {
		t.Errorf("Expected exactly 1 occurrence of key 15, but found %d", count)
	}

	if foundValue != "updated-value-15" {
		t.Errorf("Expected value 'updated-value-15' for key 15, got '%s'", foundValue)
	}
}

// TestBTreeInsertionCorrectness tests general B-tree insertion correctness
func TestBTreeInsertionCorrectness(t *testing.T) {
	tree := btree.NewInt64BTree[int]()

	// Insert a bunch of keys in random order
	keys := []int64{50, 25, 75, 10, 30, 60, 80, 5, 15, 27, 35, 55, 65, 77, 85}
	values := []int{50, 25, 75, 10, 30, 60, 80, 5, 15, 27, 35, 55, 65, 77, 85}

	for i, key := range keys {
		tree.Insert(key, values[i])
	}

	// Verify all keys can be found with correct values
	for i, key := range keys {
		value, found := tree.Search(key)
		if !found {
			t.Errorf("Key %d should be found", key)
		}
		if value != values[i] {
			t.Errorf("Expected value %d for key %d, got %d", values[i], key, value)
		}
	}

	// Update some keys
	for i, key := range keys[:5] {
		newValue := values[i] * 2
		tree.Insert(key, newValue)
		value, found := tree.Search(key)
		if !found {
			t.Errorf("Updated key %d should be found", key)
		}
		if value != newValue {
			t.Errorf("Expected updated value %d for key %d, got %d", newValue, key, value)
		}
	}

	// Verify tree size is correct (should be same as number of unique keys)
	expectedSize := len(keys)
	if tree.Size() != expectedSize {
		t.Errorf("Expected tree size %d, got %d", expectedSize, tree.Size())
	}
}
