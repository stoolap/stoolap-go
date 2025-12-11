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
	"strconv"
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	mvcc "github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestPrimaryKeyFastPath tests the fast path optimization for primary key operations
func TestPrimaryKeyFastPath(t *testing.T) {
	// Create a schema with a primary key
	schema := storage.Schema{
		TableName: "test_fast_path",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TEXT, Nullable: true, PrimaryKey: false},
			{ID: 2, Name: "value", Type: storage.FLOAT, Nullable: true, PrimaryKey: false},
		},
	}

	// Create an MVCC engine and table
	config := &storage.Config{}
	engine := mvcc.NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Begin a transaction
	txn, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	mvccTxn := txn.(*mvcc.MVCCTransaction)

	// Create a table
	table, err := mvccTxn.CreateTable("test_fast_path", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	mvccTable := table.(*mvcc.MVCCTable)

	// Insert test data
	for i := int64(1); i <= 100; i++ {
		row := storage.Row{
			storage.NewIntegerValue(i),                                // id
			storage.NewStringValue("name" + strconv.FormatInt(i, 10)), // name
			storage.NewFloatValue(float64(i) * 1.5),                   // value
		}
		if err := mvccTable.Insert(row); err != nil {
			t.Fatalf("Failed to insert test row: %v", err)
		}
	}

	// Create a simple expression for id = 50
	expr := &expression.SimpleExpression{
		Column:   "id",
		Operator: storage.EQ,
		Value:    int64(50),
	}

	// TEST 1: Scan with fast path
	scanner, err := mvccTable.Scan([]int{0, 1, 2}, expr)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	// Verify we get exactly one row with id = 50
	rowCount := 0
	for scanner.Next() {
		row := scanner.Row()
		rowCount++

		// Check id value
		idVal, ok := row[0].AsInt64()
		if !ok || idVal != 50 {
			t.Errorf("Expected id = 50, got %v", idVal)
		}
	}
	scanner.Close()

	if rowCount != 1 {
		t.Errorf("Expected 1 row, got %d", rowCount)
	}

	// TEST 2: Update with fast path
	updater := func(row storage.Row) (storage.Row, bool) {
		newRow := make(storage.Row, len(row))
		copy(newRow, row)
		newRow[1] = storage.NewStringValue("updated")
		return newRow, true
	}

	count, err := mvccTable.Update(expr, updater)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected to update 1 row, got %d", count)
	}

	// Verify the update worked
	scanner, err = mvccTable.Scan([]int{0, 1, 2}, expr)
	if err != nil {
		t.Fatalf("Failed to scan after update: %v", err)
	}

	updated := false
	for scanner.Next() {
		row := scanner.Row()
		nameVal, ok := row[1].AsString()
		if !ok || nameVal != "updated" {
			t.Errorf("Update failed, expected name = 'updated', got %v", nameVal)
		}
		updated = true
	}
	scanner.Close()

	if !updated {
		t.Errorf("Failed to find updated row")
	}

	// TEST 3: Delete with fast path
	count, err = mvccTable.Delete(expr)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected to delete 1 row, got %d", count)
	}

	// Verify the delete worked
	scanner, err = mvccTable.Scan([]int{0, 1, 2}, expr)
	if err != nil {
		t.Fatalf("Failed to scan after delete: %v", err)
	}

	rowCount = 0
	for scanner.Next() {
		rowCount++
	}
	scanner.Close()

	if rowCount != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", rowCount)
	}
}
