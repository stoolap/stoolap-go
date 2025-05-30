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
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestUncommittedWritesTracking tests the issue where committed rows still appear
// to be "being modified by another transaction"
func TestUncommittedWritesTracking(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a row in a transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	_, err = tx1.Exec("INSERT INTO test_table (id, value) VALUES (1, 'initial value')")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Commit the transaction
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	t.Log("Transaction 1 committed successfully with row id=1")

	// Now start a new transaction and try to update the same row
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	_, err = tx2.Exec("UPDATE test_table SET value = 'updated value' WHERE id = 1")
	if err != nil {
		t.Errorf("Failed to update row in transaction 2: %v", err)
		if err.Error() == "row is being modified by another transaction" {
			t.Error("BUG: The row should not be claimed by any transaction since tx1 was committed!")
		}
	} else {
		t.Log("Successfully updated row in transaction 2")
	}

	// Try to commit tx2
	err = tx2.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction 2: %v", err)
	}

	// Try a third transaction to see if the issue persists
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	_, err = tx3.Exec("UPDATE test_table SET value = 'third update' WHERE id = 1")
	if err != nil {
		t.Errorf("Failed to update row in transaction 3: %v", err)
		if err.Error() == "row is being modified by another transaction" {
			t.Error("BUG: The uncommittedWrites map is not being cleaned up properly!")
		}
	}

	tx3.Rollback()

	// Verify the final value
	var value string
	err = db.QueryRow("SELECT value FROM test_table WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query final value: %v", err)
	}
	t.Logf("Final value in database: %s", value)
}

// TestUncommittedWritesCleanupOnCommit verifies that uncommitted writes are properly cleaned up after commit
func TestUncommittedWritesCleanupOnCommit(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE cleanup_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO cleanup_test (id, value) VALUES (?, ?)", i, fmt.Sprintf("value %d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Now update multiple rows in a transaction
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Update rows 1, 3, and 5
	for _, id := range []int{1, 3, 5} {
		_, err = tx2.Exec("UPDATE cleanup_test SET value = ? WHERE id = ?", fmt.Sprintf("updated value %d", id), id)
		if err != nil {
			t.Fatalf("Failed to update row %d: %v", id, err)
		}
	}

	// Commit transaction 2
	err = tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Now try to update the same rows in a new transaction
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Try to update the same rows that were updated in tx2
	for _, id := range []int{1, 3, 5} {
		_, err = tx3.Exec("UPDATE cleanup_test SET value = ? WHERE id = ?", fmt.Sprintf("re-updated value %d", id), id)
		if err != nil {
			t.Errorf("Failed to update row %d in tx3: %v", id, err)
			if err.Error() == "row is being modified by another transaction" {
				t.Errorf("BUG: Row %d should not be locked after tx2 committed!", id)
			}
		}
	}

	tx3.Rollback()
}

// TestUncommittedWritesCleanupOnRollback verifies that uncommitted writes are properly cleaned up after rollback
func TestUncommittedWritesCleanupOnRollback(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with data
	_, err = db.Exec("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a row
	_, err = db.Exec("INSERT INTO rollback_test (id, value) VALUES (1, 'initial value')")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Start a transaction that will be rolled back
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Update the row
	_, err = tx2.Exec("UPDATE rollback_test SET value = 'updated in tx2' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update row in tx2: %v", err)
	}

	// Rollback instead of commit
	err = tx2.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction 2: %v", err)
	}

	// Now try to update the same row in a new transaction
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// This should succeed since tx2 was rolled back
	_, err = tx3.Exec("UPDATE rollback_test SET value = 'updated in tx3' WHERE id = 1")
	if err != nil {
		t.Errorf("Failed to update row in tx3 after tx2 rollback: %v", err)
		if err.Error() == "row is being modified by another transaction" {
			t.Error("BUG: Row should not be locked after tx2 was rolled back!")
		}
	}

	tx3.Commit()
}
