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
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestBasicMVCCIsolation tests the most basic form of MVCC isolation:
// One transaction cannot see another transaction's uncommitted changes
//
// Note: This test is currently skipped as the actual isolation behavior of the
// system differs from what's expected in this test. We need to adapt this test
// to the actual behavior of the current MVCC implementation.
func TestBasicMVCCIsolation(t *testing.T) {
	// t.Skip("Skipping test due to mismatch with actual MVCC implementation")

	// Create the engine
	engine := mvcc.NewMVCCEngine(&storage.Config{
		Path: "memory://",
	})

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Set the isolation level to Snapshot Isolation for this test
	if err := engine.SetIsolationLevel(storage.SnapshotIsolation); err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Start transaction 1
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Create a test table with a simple schema
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TEXT, Nullable: false},
		},
	}

	_, err = tx1.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit the table creation
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Start transaction 2
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Insert a row in transaction 2
	table2, err := tx2.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 2: %v", err)
	}

	// Insert a row with ID=1
	row1 := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Value from Transaction 2"),
	}

	if err := table2.Insert(row1); err != nil {
		t.Fatalf("Failed to insert row in transaction 2: %v", err)
	}

	// Start transaction 3 (without committing transaction 2)
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Check that transaction 3 cannot see transaction 2's uncommitted changes
	table3, err := tx3.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 3: %v", err)
	}

	scanner, err := table3.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 3: %v", err)
	}

	// Transaction 3 should not see any rows
	rowCount := 0
	for scanner.Next() {
		rowCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 0 {
		t.Errorf("Expected transaction 3 to see 0 rows (tx2's changes are uncommitted), but saw %d rows", rowCount)
	}

	// Now commit transaction 2
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Transaction 3 still should not see the changes because it started before tx2 committed
	scanner, err = table3.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 3 after tx2 commit: %v", err)
	}

	rowCount = 0
	for scanner.Next() {
		rowCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 0 {
		t.Errorf("Expected transaction 3 to still see 0 rows (snapshot isolation), but saw %d rows", rowCount)
	}

	// Start transaction 4 after tx2 is committed
	tx4, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	// Transaction 4 should see transaction 2's committed changes
	table4, err := tx4.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 4: %v", err)
	}

	scanner, err = table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 4: %v", err)
	}

	rowCount = 0
	for scanner.Next() {
		row := scanner.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()

		if id != 1 || value != "Value from Transaction 2" {
			t.Errorf("Expected row with id=1, value='Value from Transaction 2', got id=%d, value='%s'", id, value)
		}

		rowCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 1 {
		t.Errorf("Expected transaction 4 to see 1 row (tx2's committed change), but saw %d rows", rowCount)
	}

	// Clean up
	tx3.Rollback()
	tx4.Rollback()
}

// TestMVCCDataOverwrite verifies that transactions don't overwrite each other's physical data
//
// Note: This test is currently skipped as the actual isolation behavior of the
// system differs from what's expected in this test. We need to adapt this test
// to the actual behavior of the current MVCC implementation.
func TestMVCCDataOverwrite(t *testing.T) {
	// t.Skip("Skipping test due to mismatch with actual MVCC implementation")

	// Create the engine
	engine := mvcc.NewMVCCEngine(&storage.Config{
		Path: "memory://",
	})

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Set the isolation level to Snapshot Isolation for this test
	if err := engine.SetIsolationLevel(storage.SnapshotIsolation); err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Start transaction 1
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Create a test table with a simple schema
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TEXT, Nullable: false},
		},
	}

	_, err = tx1.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit the table creation
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Start transaction 2
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Start transaction 3
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Get the table in both transactions
	table2, err := tx2.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 2: %v", err)
	}

	table3, err := tx3.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 3: %v", err)
	}

	// Insert a row with ID=1 in transaction 2
	row2 := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Value from Transaction 2"),
	}

	if err := table2.Insert(row2); err != nil {
		t.Fatalf("Failed to insert row in transaction 2: %v", err)
	}

	// Commit transaction 2
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Start transaction 4 (which should see tx2's committed row)
	tx4, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	table4, err := tx4.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 4: %v", err)
	}

	// Update the row in transaction 4
	expr := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		id, _ := row[0].AsInt64()
		return id == 1, nil
	})

	updateCount, err := table4.Update(expr, func(row storage.Row) (storage.Row, bool) {
		// Change value
		row[1] = storage.NewStringValue("Updated by Transaction 4")
		return row, true
	})

	if err != nil {
		t.Fatalf("Failed to update row in transaction 4: %v", err)
	}

	if updateCount != 1 {
		t.Errorf("Expected 1 row updated, got %d", updateCount)
	}

	// Insert a row with ID=2 in transaction 3
	// This transaction started before tx2 committed, so it shouldn't see tx2's changes
	row3 := storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Value from Transaction 3"),
	}

	if err := table3.Insert(row3); err != nil {
		t.Fatalf("Failed to insert row in transaction 3: %v", err)
	}

	// Transaction 3 should not see transaction 2's or 4's changes
	scanner, err := table3.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 3: %v", err)
	}

	rowCount := 0
	var rowID int64
	var rowValue string

	for scanner.Next() {
		rowCount++
		row := scanner.Row()
		rowID, _ = row[0].AsInt64()
		rowValue, _ = row[1].AsString()
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 1 {
		t.Errorf("Expected transaction 3 to see 1 row (its own insert), but saw %d rows", rowCount)
	}

	if rowID != 2 || rowValue != "Value from Transaction 3" {
		t.Errorf("Transaction 3 saw unexpected data: id=%d, value='%s'", rowID, rowValue)
	}

	// Transaction 4 should see its updated value for id=1
	scanner, err = table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 4: %v", err)
	}

	rowCount = 0
	for scanner.Next() {
		row := scanner.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()

		if id != 1 || value != "Updated by Transaction 4" {
			t.Errorf("Transaction 4 should see its own update: id=%d, value='%s'", id, value)
		}

		rowCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 1 {
		t.Errorf("Expected transaction 4 to see 1 row, but saw %d rows", rowCount)
	}

	// Commit all transactions
	tx3.Commit()
	tx4.Commit()

	// Start transaction 5 to see the final state
	tx5, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 5: %v", err)
	}

	table5, err := tx5.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 5: %v", err)
	}

	// Transaction 5 should see both rows with their final values
	scanner, err = table5.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 5: %v", err)
	}

	rowCount = 0
	foundIDs := make(map[int64]string)

	for scanner.Next() {
		row := scanner.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()
		foundIDs[id] = value
		rowCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Error scanning: %v", err)
	}
	scanner.Close()

	if rowCount != 2 {
		t.Errorf("Expected transaction 5 to see 2 rows, but saw %d rows", rowCount)
	}

	if value, ok := foundIDs[1]; !ok || value != "Updated by Transaction 4" {
		t.Errorf("Expected row with id=1 to have value='Updated by Transaction 4', got value='%s'", value)
	}

	if value, ok := foundIDs[2]; !ok || value != "Value from Transaction 3" {
		t.Errorf("Expected row with id=2 to have value='Value from Transaction 3', got value='%s'", value)
	}

	// Clean up
	tx5.Rollback()
}
