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
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

func TestMVCCTable(t *testing.T) {
	// Create a memory table
	schema := storage.Schema{
		TableName: "test_mvcc_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString, Nullable: false},
			{ID: 2, Name: "age", Type: storage.TypeInteger, Nullable: true},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create an MVCC engine with its own registry
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
	txnID1 := mvccTxn.ID()
	t.Logf("Starting transaction with ID: %d", txnID1)

	// Create a table
	table, err := mvccTxn.CreateTable("test_mvcc_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	mvccTable := table.(*mvcc.MVCCTable)

	// Test 1: Basic insertion
	row1 := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
		storage.NewIntegerValue(30),
	}

	err = mvccTable.Insert(row1)
	if err != nil {
		t.Errorf("Failed to insert row1: %v", err)
	}

	// Test 2: Duplicate key rejection
	err = mvccTable.Insert(row1)
	if err == nil {
		t.Errorf("Expected error for duplicate key, got nil")
	}

	// Test 3: Insert another row
	row2 := storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
		storage.NewNullIntegerValue(),
	}

	err = mvccTable.Insert(row2)
	if err != nil {
		t.Errorf("Failed to insert row2: %v", err)
	}

	// Test 4: Count rows
	if count := mvccTable.RowCount(); count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	// Test 5: Scan rows
	scanner, err := mvccTable.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner: %v", err)
	}

	rowCount := 0
	for scanner.Next() {
		row := scanner.Row()
		if len(row) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(row))
		}

		// Check ID column
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d", rowCount)
		}

		if id == 1 {
			// Check Alice's data
			name, ok := row[1].AsString()
			if !ok || name != "Alice" {
				t.Errorf("Expected name 'Alice', got '%s'", name)
			}

			age, ok := row[2].AsInt64()
			if !ok || age != 30 {
				t.Errorf("Expected age 30, got %d", age)
			}
		} else if id == 2 {
			// Check Bob's data
			name, ok := row[1].AsString()
			if !ok || name != "Bob" {
				t.Errorf("Expected name 'Bob', got '%s'", name)
			}

			if !row[2].IsNull() {
				t.Errorf("Expected NULL age for Bob")
			}
		} else {
			t.Errorf("Unexpected ID: %d", id)
		}

		rowCount++
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}

	if rowCount != 2 {
		t.Errorf("Expected to scan 2 rows, got %d", rowCount)
	}

	// Test 6: Update row
	updater := func(row storage.Row) (storage.Row, bool) {
		// Update Alice's age to 31
		if id, ok := row[0].AsInt64(); ok && id == 1 {
			updated := make(storage.Row, len(row))
			copy(updated, row)
			updated[2] = storage.NewIntegerValue(31)
			return updated, true
		}
		return row, true
	}

	updateCount, err := mvccTable.Update(nil, updater)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	if updateCount != 2 { // Both rows should be updated (though only Alice's age changed)
		t.Errorf("Expected 2 rows affected by update, got %d", updateCount)
	}

	// Verify the update worked
	scanner, err = mvccTable.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner after update: %v", err)
	}

	aliceFound := false
	for scanner.Next() {
		row := scanner.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to read ID after update")
			continue
		}

		if id == 1 {
			// Check Alice's updated age
			age, ok := row[2].AsInt64()
			if !ok || age != 31 {
				t.Errorf("Failed to update Alice's age to 31, got %d", age)
			}
			aliceFound = true
		}
	}

	if !aliceFound {
		t.Errorf("Failed to find Alice after update")
	}

	// Test 7: Delete a row
	filter := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		// Delete Alice
		id, ok := row[0].AsInt64()
		return ok && id == 1, nil
	})

	deleteCount, err := mvccTable.Delete(filter)
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	if deleteCount != 1 {
		t.Errorf("Expected 1 row affected by delete, got %d", deleteCount)
	}

	// Verify the delete worked
	scanner, err = mvccTable.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner after delete: %v", err)
	}

	rowCount = 0
	for scanner.Next() {
		row := scanner.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to read ID after delete")
			continue
		}

		if id == 1 {
			t.Errorf("Found Alice after deletion")
		}

		rowCount++
	}

	if rowCount != 1 {
		t.Errorf("Expected 1 row after delete, got %d", rowCount)
	}

	// Test 8: Commit changes
	// In the new design, committing the transaction automatically commits all tables
	// No need to commit the tables separately

	// Commit the transaction
	err = mvccTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	t.Logf("Committed transaction %d", txnID1)

	// Before starting the second transaction, let's sleep a tiny bit
	// to ensure the first transaction's commit is fully processed
	time.Sleep(10 * time.Millisecond)

	// Create another transaction view AFTER committing the first one
	// This follows proper snapshot isolation semantics where txn2 should see
	// all changes committed before it started
	txn2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}
	mvccTxn2 := txn2.(*mvcc.MVCCTransaction)
	txnID2 := mvccTxn2.ID()

	// Get the table in the second transaction
	table2, err := mvccTxn2.GetTable("test_mvcc_table")
	if err != nil {
		t.Fatalf("Failed to get table in second transaction: %v", err)
	}
	mvccTable2 := table2.(*mvcc.MVCCTable)

	// Log the transaction details
	registry := engine.GetRegistry()
	commitTS1, committed := registry.GetCommitSequence(txnID1)
	t.Logf("Txn1 (ID %d) committed: %v, commit timestamp: %d", txnID1, committed, commitTS1)

	// We can't directly access begin timestamps anymore since they're internal
	// Instead, just log what we can access
	t.Logf("Txn2 (ID %d) is active", txnID2)

	// We know that by design, the second transaction started after the first one committed,
	// so we can assume the timestamps are correct without explicitly checking

	// Verify changes are visible to the new transaction
	scanner, err = mvccTable2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner for second transaction: %v", err)
	}

	// Debug info
	t.Logf("Transaction 1 ID: %d, Transaction 2 ID: %d", txnID1, txnID2)

	rowCount = 0
	for scanner.Next() {
		row := scanner.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to read ID in second transaction")
			continue
		}
		if id == 1 {
			t.Errorf("Found Alice in second transaction after deletion")
		}
		rowCount++
	}

	if rowCount != 1 {
		t.Errorf("Expected 1 row in second transaction, got %d", rowCount)
	}
}
