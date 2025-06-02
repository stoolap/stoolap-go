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
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

// TestTransactionIDsVisibilityAfterRecovery verifies that transaction IDs are properly
// preserved during recovery and transaction visibility rules are maintained.
func TestTransactionIDsVisibilityAfterRecovery(t *testing.T) {
	// Create a temporary directory for the database files
	dbDir := t.TempDir()

	// Configure the storage engine
	config := &storage.Config{
		Path: dbDir,
		Persistence: storage.PersistenceConfig{
			Enabled:  true,
			SyncMode: 1, // Normal
		},
	}

	// Create and open the first engine instance
	engine := mvcc.NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Create a table
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TypeString},
			{ID: 2, Name: "txn_info", Type: storage.TypeString},
		},
	}

	_, err = tx.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit schema transaction: %v", err)
	}

	// Insert rows with transaction 1 - this will be a committed transaction
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	table, err := tx1.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 1: %v", err)
	}

	// Insert rows for transaction 1
	for i := 1; i <= 5; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewStringValue("tx1_value_" + string(rune(i))),
			storage.NewStringValue("from_committed_transaction"),
		}

		err = table.Insert(row)
		if err != nil {
			t.Fatalf("Failed to insert row in transaction 1: %v", err)
		}
	}

	// Commit transaction 1
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Insert rows with transaction 2 - this will be an uncommitted transaction
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	table, err = tx2.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 2: %v", err)
	}

	// Insert rows for transaction 2
	for i := 6; i <= 10; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewStringValue("tx2_value_" + string(rune(i))),
			storage.NewStringValue("from_uncommitted_transaction"),
		}

		err = table.Insert(row)
		if err != nil {
			t.Fatalf("Failed to insert row in transaction 2: %v", err)
		}
	}

	// DO NOT commit transaction 2 - we want to verify uncommitted transactions aren't visible after recovery

	// Force disk flush but do not close transaction 2 cleanly
	// We expect an error here because tx2 is still active - this simulates a crash
	err = engine.Close()
	if err != nil {
		// This is expected because transaction 2 wasn't committed
		t.Logf("Expected error on engine close (simulating crash): %v", err)
	}

	// Reopen with a new engine instance to simulate recovery after crash
	newEngine := mvcc.NewMVCCEngine(config)
	err = newEngine.Open()
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}

	// Wait for recovery to complete
	time.Sleep(1 * time.Second)

	// Start a new transaction to verify the data
	newTx, err := newEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction after recovery: %v", err)
	}

	// Query all rows from the table
	result, err := newTx.SelectWithAliases(
		"test_table",
		[]string{"id", "value", "txn_info"},
		nil, // No filter
		nil, // No aliases
	)
	if err != nil {
		t.Fatalf("Failed to select data after recovery: %v", err)
	}

	// Count rows and check transaction info
	rowCount := 0
	uncommittedCount := 0
	committedCount := 0

	for result.Next() {
		row := result.Row()
		id, _ := row[0].AsInt64()
		txnInfo, _ := row[2].AsString()

		t.Logf("Row after recovery: id=%d, txn_info=%s", id, txnInfo)

		if txnInfo == "from_uncommitted_transaction" {
			uncommittedCount++
		} else if txnInfo == "from_committed_transaction" {
			committedCount++
		}

		rowCount++
	}
	result.Close()

	// We should have only 5 rows (from transaction 1 which was committed)
	// Transaction 2 wasn't committed, so its rows should not be visible
	if rowCount != 5 {
		t.Errorf("Wrong number of visible rows after recovery: got %d, expected 5", rowCount)
	}

	if committedCount != 5 {
		t.Errorf("Wrong number of committed transaction rows: got %d, expected 5", committedCount)
	}

	if uncommittedCount != 0 {
		t.Errorf("Found %d uncommitted transaction rows, expected 0", uncommittedCount)
	}

	// Clean up
	newTx.Commit()
	newEngine.Close()
}
