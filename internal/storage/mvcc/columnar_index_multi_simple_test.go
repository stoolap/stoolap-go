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
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
)

func TestEnhancedMultiColumnarIndex_Basic(t *testing.T) {
	// Create the original, simple version of MultiColumnarIndex
	originalIdx := NewMultiColumnarIndex(
		"original_idx",
		"test_table",
		[]string{"id", "name"},
		[]int{0, 1},
		[]storage.DataType{storage.INTEGER, storage.TEXT},
		nil,
		true, // With uniqueness constraint
	)

	// Basic test - add some values
	values1 := []storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
	}
	if err := originalIdx.Add(values1, 1, 0); err != nil {
		t.Fatalf("Failed to add values to original index: %v", err)
	}

	values2 := []storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
	}
	if err := originalIdx.Add(values2, 2, 0); err != nil {
		t.Fatalf("Failed to add values to original index: %v", err)
	}

	// Verify columnIndices was initialized correctly
	if originalIdx.columnIndices == nil {
		t.Fatalf("columnIndices is nil in the enhanced implementation")
	}

	if len(originalIdx.columnIndices) != 2 {
		t.Fatalf("Expected 2 column indices, got %d", len(originalIdx.columnIndices))
	}

	// Check that individual column indices were created properly
	idxId := originalIdx.columnIndices[0]
	if idxId == nil {
		t.Fatalf("Index for ID column is nil")
	}
	if idxId.columnName != "id" {
		t.Errorf("Expected column name 'id', got '%s'", idxId.columnName)
	}

	idxName := originalIdx.columnIndices[1]
	if idxName == nil {
		t.Fatalf("Index for name column is nil")
	}
	if idxName.columnName != "name" {
		t.Errorf("Expected column name 'name', got '%s'", idxName.columnName)
	}

	// Test GetRowIDsEqual with single column
	idRows := originalIdx.GetRowIDsEqual([]storage.ColumnValue{storage.NewIntegerValue(1)})
	if len(idRows) != 1 || idRows[0] != 1 {
		t.Errorf("Expected to get row ID 1 for ID 1, got %v", idRows)
	}

	nameRows := originalIdx.GetRowIDsEqual([]storage.ColumnValue{nil, storage.NewStringValue("Bob")})
	if len(nameRows) != 1 || nameRows[0] != 2 {
		t.Errorf("Expected to get row ID 2 for name 'Bob', got %v", nameRows)
	}

	// Test GetRowIDsEqual with both columns
	bothRows := originalIdx.GetRowIDsEqual([]storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
	})
	if len(bothRows) != 1 || bothRows[0] != 2 {
		t.Errorf("Expected to get row ID 2 for ID 2 and name 'Bob', got %v", bothRows)
	}
}
