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

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// TestColumnarIndexIterator tests the actual ColumnarIndexIterator implementation
func TestColumnarIndexIterator(t *testing.T) {
	engine := NewMVCCEngine(&storage.Config{})

	// Create a mock version store with more data to test batching
	vs := newTestVersionStore(engine, "test_table", 200)

	// Create a schema for testing
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER},
			{Name: "value", Type: storage.TEXT},
		},
	}

	// Test basic iterator functionality with batching
	t.Run("BasicIteration", func(t *testing.T) {
		// Create a list of all row IDs
		var rowIDs []int64
		for i := int64(1); i <= 200; i++ {
			rowIDs = append(rowIDs, i)
		}

		// Shuffle IDs to test sorting
		shuffleIDs(rowIDs)

		// Create iterator with a small batch size to test batching
		iterator := &ColumnarIndexIterator{
			versionStore:  vs,
			txnID:         100, // Any valid txnID
			schema:        schema,
			columnIndices: nil, // Get all columns
			rowIDs:        rowIDs,
			idIndex:       -1,
			batchSize:     25, // Small batch size to test multiple batches
			prefetchIndex: 0,
			prefetchMap:   fastmap.NewInt64Map[storage.Row](25),
		}

		// Count rows and verify all 200 rows are returned
		count := 0
		ids := make(map[int64]bool)

		for iterator.Next() {
			row := iterator.Row()
			count++

			// Check row has two columns
			if len(row) != 2 {
				t.Errorf("Row should have 2 columns, got %d", len(row))
				continue
			}

			// Verify ID
			id, ok := row[0].AsInt64()
			if !ok {
				t.Errorf("Expected integer ID")
				continue
			}

			// Check ID is in expected range and not duplicated
			if id < 1 || id > 200 {
				t.Errorf("ID out of range: %d", id)
			}
			if ids[id] {
				t.Errorf("Duplicate ID returned: %d", id)
			}
			ids[id] = true

			// Verify value
			value, ok := row[1].AsString()
			if !ok {
				t.Errorf("Expected string value")
				continue
			}
			expectedValue := "value-" + string(rune('0'+id%10))
			if value != expectedValue {
				t.Errorf("Expected value %s, got %s", expectedValue, value)
			}
		}

		// Check iterator error
		if err := iterator.Err(); err != nil {
			t.Errorf("Iterator error: %v", err)
		}

		// Clean up
		iterator.Close()

		// Verify row count
		if count != 200 {
			t.Errorf("Expected 200 rows, got %d", count)
		}
	})

	// Test column projection
	t.Run("ColumnProjection", func(t *testing.T) {
		// Create a list of row IDs
		var rowIDs []int64
		for i := int64(1); i <= 100; i++ {
			rowIDs = append(rowIDs, i)
		}

		// Create iterator with column projection
		iterator := &ColumnarIndexIterator{
			versionStore:  vs,
			txnID:         100,
			schema:        schema,
			columnIndices: []int{1}, // Only the value column
			rowIDs:        rowIDs,
			idIndex:       -1,
			batchSize:     30,
			prefetchIndex: 0,
			prefetchMap:   fastmap.NewInt64Map[storage.Row](30),
			projectedRow:  make(storage.Row, 1),
		}

		// Count rows and verify projected column
		count := 0
		for iterator.Next() {
			row := iterator.Row()
			count++

			// Check row has only one column
			if len(row) != 1 {
				t.Errorf("Projected row should have 1 column, got %d", len(row))
				continue
			}

			// Verify value is of the right type
			_, ok := row[0].AsString()
			if !ok {
				t.Errorf("Expected string value in projection")
			}
		}

		// Clean up
		iterator.Close()

		// Verify row count
		if count != 100 {
			t.Errorf("Expected 100 rows, got %d", count)
		}
	})

	// Test empty result set
	t.Run("EmptyResultSet", func(t *testing.T) {
		// Create empty row IDs list
		rowIDs := []int64{}

		// Create iterator
		iterator := &ColumnarIndexIterator{
			versionStore:  vs,
			txnID:         100,
			schema:        schema,
			columnIndices: nil,
			rowIDs:        rowIDs,
			idIndex:       -1,
			batchSize:     30,
			prefetchIndex: 0,
			prefetchMap:   fastmap.NewInt64Map[storage.Row](30),
		}

		// Should return no rows
		if iterator.Next() {
			t.Errorf("Expected no rows, but iterator.Next() returned true")
		}

		// Clean up
		iterator.Close()
	})

	// Test with rows that don't exist in the version store
	t.Run("NonExistentRows", func(t *testing.T) {
		// Create row IDs that don't exist
		rowIDs := []int64{1001, 1002, 1003}

		// Create iterator
		iterator := &ColumnarIndexIterator{
			versionStore:  vs,
			txnID:         100,
			schema:        schema,
			columnIndices: nil,
			rowIDs:        rowIDs,
			idIndex:       -1,
			batchSize:     30,
			prefetchIndex: 0,
			prefetchMap:   fastmap.NewInt64Map[storage.Row](30),
		}

		// Should return no rows
		if iterator.Next() {
			t.Errorf("Expected no rows for non-existent IDs, but iterator.Next() returned true")
		}

		// Clean up
		iterator.Close()
	})
}

// newTestVersionStore creates a mock version store with test data
func newTestVersionStore(engine *MVCCEngine, tableName string, rowCount int) *VersionStore {
	store := NewVersionStore(tableName, engine)

	// Add test data to both the store and our local map
	for i := int64(1); i <= int64(rowCount); i++ {
		rowData := storage.Row{
			storage.NewIntegerValue(i),
			storage.NewStringValue("value-" + string(rune('0'+i%10))),
		}

		rv := RowVersion{
			TxnID:          100,
			DeletedAtTxnID: 0,
			Data:           rowData,
			RowID:          i,
		}

		store.AddVersion(i, rv)
	}

	return store
}

// Helper function to shuffle a slice of int64 for testing
func shuffleIDs(ids []int64) {
	// Simple Fisher-Yates shuffle
	for i := len(ids) - 1; i > 0; i-- {
		j := int64(i) % int64(i+1)
		ids[i], ids[j] = ids[j], ids[i]
	}
}
