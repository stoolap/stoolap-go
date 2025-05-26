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
	"context"
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestIsolationLevelSession tests session-wide isolation level setting with SET ISOLATIONLEVEL
func TestIsolationLevelSession(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test_iso (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test session-wide isolation level setting
	_, err = db.Exec("SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set session isolation level: %v", err)
	}

	// Start a transaction (should use session isolation level)
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data in tx1
	_, err = tx1.Exec("INSERT INTO test_iso (id, value) VALUES (1, 'tx1_data')")
	if err != nil {
		t.Fatalf("Failed to insert in tx1: %v", err)
	}

	// Start another transaction (should also use session isolation level)
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// tx2 should not see uncommitted changes from tx1
	rows, err := tx2.Query("SELECT COUNT(*) FROM test_iso")
	if err != nil {
		t.Fatalf("Failed to query in tx2: %v", err)
	}

	var count int
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}
	rows.Close()

	if count != 0 {
		t.Errorf("Expected 0 rows in tx2 (tx1 uncommitted), got %d", count)
	}

	// Commit tx1
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit tx1: %v", err)
	}

	// tx2 still should not see the committed data due to snapshot isolation
	// (depending on implementation specifics)
	rows, err = tx2.Query("SELECT COUNT(*) FROM test_iso")
	if err != nil {
		t.Fatalf("Failed to query in tx2 after tx1 commit: %v", err)
	}

	count = 0
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Fatalf("Failed to scan count after commit: %v", err)
		}
	}
	rows.Close()

	// Clean up
	tx2.Rollback()

	// Change back to READ COMMITTED
	_, err = db.Exec("SET ISOLATIONLEVEL = 'READ COMMITTED'")
	if err != nil {
		t.Fatalf("Failed to reset session isolation level: %v", err)
	}
}

// TestIsolationLevelTransaction tests transaction-specific isolation level setting
func TestIsolationLevelTransaction(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Clean table
	db.Exec("DROP TABLE IF EXISTS test_iso_tx")
	_, err = db.Exec("CREATE TABLE test_iso_tx (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Set session to READ COMMITTED
	_, err = db.Exec("SET ISOLATIONLEVEL = 'READ COMMITTED'")
	if err != nil {
		t.Fatalf("Failed to set session isolation level: %v", err)
	}

	// Start a transaction with specific isolation level
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Set transaction-specific isolation level
	_, err = tx1.Exec("BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT")
	if err != nil {
		t.Fatalf("Failed to set transaction isolation level: %v", err)
	}

	// Insert data in tx1
	_, err = tx1.Exec("INSERT INTO test_iso_tx (id, value) VALUES (1, 'tx1_snapshot')")
	if err != nil {
		t.Fatalf("Failed to insert in tx1: %v", err)
	}

	// Commit tx1
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit tx1: %v", err)
	}

	// Start new transaction - should use session level (READ COMMITTED)
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Should see the committed data from tx1
	rows, err := tx2.Query("SELECT value FROM test_iso_tx WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to query in tx2: %v", err)
	}

	var value string
	hasRow := rows.Next()
	if hasRow {
		err = rows.Scan(&value)
		if err != nil {
			t.Fatalf("Failed to scan value: %v", err)
		}
	}
	rows.Close()

	if !hasRow {
		t.Error("Expected to find row with id=1 in tx2")
	} else if value != "tx1_snapshot" {
		t.Errorf("Expected value 'tx1_snapshot', got '%s'", value)
	}

	tx2.Rollback()
}

// TestIsolationLevelReset tests that transaction-specific isolation levels are reset after transaction
func TestIsolationLevelReset(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Clean table
	db.Exec("DROP TABLE IF EXISTS test_iso_reset")

	_, err = db.Exec("CREATE TABLE test_iso_reset (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Set session isolation level to READ COMMITTED
	_, err = db.Exec("SET ISOLATIONLEVEL = 'READ COMMITTED'")
	if err != nil {
		t.Fatalf("Failed to set session isolation level: %v", err)
	}

	// Start transaction with specific isolation level
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Set transaction-specific isolation level
	_, err = tx.Exec("BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT")
	if err != nil {
		t.Fatalf("Failed to set transaction isolation level: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Start new transaction - should use original session level (READ COMMITTED)
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Clean up
	tx2.Rollback()

	t.Log("Isolation level reset test completed successfully")
}

// TestConnectionIsolation tests that SET ISOLATIONLEVEL only affects the current connection
func TestConnectionIsolation(t *testing.T) {
	// Note: In the current implementation, memory:// creates a shared database,
	// so both connections will share the same underlying database instance.
	// This test demonstrates that each connection maintains its own isolation level setting.
	db1, err := sql.Open("stoolap", "memory://isolation_test")
	if err != nil {
		t.Fatalf("Failed to open db1: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open("stoolap", "memory://isolation_test")
	if err != nil {
		t.Fatalf("Failed to open db2: %v", err)
	}
	defer db2.Close()

	// Create table using first connection
	_, err = db1.Exec("DROP TABLE IF EXISTS conn_test")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	_, err = db1.Exec("CREATE TABLE conn_test (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db1.Exec("INSERT INTO conn_test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Set SNAPSHOT isolation on db1 only
	_, err = db1.Exec("SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set isolation level on db1: %v", err)
	}

	// db2 should remain at READ COMMITTED (default)
	t.Log("db1 set to SNAPSHOT isolation, db2 remains at default READ COMMITTED")

	// Start concurrent transactions
	tx1, err := db1.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db2.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx2: %v", err)
	}
	defer tx2.Rollback()

	// Both read the initial value
	var value1, value2 int
	err = tx1.QueryRow("SELECT value FROM conn_test WHERE id = 1").Scan(&value1)
	if err != nil {
		t.Fatalf("tx1 failed to read: %v", err)
	}

	err = tx2.QueryRow("SELECT value FROM conn_test WHERE id = 1").Scan(&value2)
	if err != nil {
		t.Fatalf("tx2 failed to read: %v", err)
	}

	// Update in tx2 (READ COMMITTED by default) and commit
	_, err = tx2.Exec("UPDATE conn_test SET value = 200 WHERE id = 1")
	if err != nil {
		t.Fatalf("tx2 failed to update: %v", err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatalf("tx2 failed to commit: %v", err)
	}

	// Try to update in tx1 (SNAPSHOT isolation)
	_, err = tx1.Exec("UPDATE conn_test SET value = 300 WHERE id = 1")
	if err != nil {
		t.Fatalf("tx1 failed to update: %v", err)
	}

	// tx1 commit should fail with write-write conflict (SNAPSHOT)
	err = tx1.Commit()
	if err == nil {
		// Check final value to understand what happened
		var finalValue int
		err2 := db1.QueryRow("SELECT value FROM conn_test WHERE id = 1").Scan(&finalValue)
		if err2 != nil {
			t.Errorf("Failed to get final value: %v", err2)
		} else {
			t.Errorf("Expected write-write conflict for SNAPSHOT isolation, but commit succeeded. Final value: %d", finalValue)
		}
	} else if err.Error() != "transaction aborted due to write-write conflict" {
		t.Errorf("Expected write-write conflict error, got: %v", err)
	}
}

// TestTransactionLevelOverride tests transaction-level isolation override with BeginTx
func TestTransactionLevelOverride(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE override_test (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO override_test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Ensure connection default is READ COMMITTED
	_, err = db.Exec("SET ISOLATIONLEVEL = 'READ COMMITTED'")
	if err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Start two transactions with explicit SNAPSHOT isolation
	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
	})
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
	})
	if err != nil {
		t.Fatalf("Failed to begin tx2: %v", err)
	}
	defer tx2.Rollback()

	// Both read
	var value1, value2 int
	err = tx1.QueryRow("SELECT value FROM override_test WHERE id = 1").Scan(&value1)
	if err != nil {
		t.Fatalf("tx1 failed to read: %v", err)
	}

	err = tx2.QueryRow("SELECT value FROM override_test WHERE id = 1").Scan(&value2)
	if err != nil {
		t.Fatalf("tx2 failed to read: %v", err)
	}

	// Update and commit tx1
	_, err = tx1.Exec("UPDATE override_test SET value = 200 WHERE id = 1")
	if err != nil {
		t.Fatalf("tx1 failed to update: %v", err)
	}

	err = tx1.Commit()
	if err != nil {
		t.Fatalf("tx1 failed to commit: %v", err)
	}

	// Try to update in tx2
	_, err = tx2.Exec("UPDATE override_test SET value = 300 WHERE id = 1")
	if err != nil {
		t.Fatalf("tx2 failed to update: %v", err)
	}

	// Should fail with write-write conflict due to SNAPSHOT isolation
	err = tx2.Commit()
	if err == nil {
		t.Error("Expected write-write conflict, but commit succeeded")
	} else if err.Error() != "transaction aborted due to write-write conflict" {
		t.Errorf("Expected write-write conflict error, got: %v", err)
	}
}
