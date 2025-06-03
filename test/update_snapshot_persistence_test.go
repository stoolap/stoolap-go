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

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/common"
	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestUpdatePersistenceWithSnapshot verifies that UPDATE operations are persisted
// correctly across database restarts when snapshots exist.
func TestUpdatePersistenceWithSnapshot(t *testing.T) {
	// Create a temporary directory for the database
	tempDir := common.TempDir(t)
	dbPath := fmt.Sprintf("file://%s/test.db", tempDir)

	// First connection - create table and insert data
	db1, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create table
	_, err = db1.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT,
		created_at TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db1.Exec(`INSERT INTO users (id, name, email, created_at) 
		VALUES (1, 'John Doe', 'john@example.com', NOW())`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify initial data
	var name string
	err = db1.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query initial data: %v", err)
	}
	if name != "John Doe" {
		t.Errorf("Expected name 'John Doe', got '%s'", name)
	}

	// Close first connection
	db1.Close()

	// Second connection - update the data
	db2, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Update the name
	result, err := db2.Exec("UPDATE users SET name = 'Jane Doe' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify update in same session
	err = db2.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query updated data: %v", err)
	}
	if name != "Jane Doe" {
		t.Errorf("Expected name 'Jane Doe' after update, got '%s'", name)
	}

	// Force a snapshot creation
	engine := stoolap.GetEngineByDSN(dbPath)
	if engine != nil {
		err = engine.CreateSnapshot()
		if err != nil {
			t.Logf("Warning: Failed to create snapshot: %v", err)
		} else {
			t.Log("Snapshot created successfully")
		}
	} else {
		t.Log("Warning: Could not get engine to create snapshot")
	}

	// Close second connection
	db2.Close()

	// Third connection - verify update persisted
	db3, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database again: %v", err)
	}
	defer db3.Close()

	// Query the data after restart
	err = db3.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query data after restart: %v", err)
	}

	// This is the critical test - the update should persist!
	if name != "Jane Doe" {
		t.Errorf("UPDATE not persisted! Expected name 'Jane Doe' after restart, got '%s'", name)
		t.Error("This indicates that UPDATE operations are lost when snapshots exist")
	}

	// Also test multiple updates
	_, err = db3.Exec("UPDATE users SET name = 'Alice Smith' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update data again: %v", err)
	}

	// Force another snapshot
	if engine != nil {
		engine.CreateSnapshot()
	}

	db3.Close()

	// Fourth connection - verify second update
	db4, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database final time: %v", err)
	}
	defer db4.Close()

	err = db4.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("Failed to query data after final restart: %v", err)
	}

	if name != "Alice Smith" {
		t.Errorf("Second UPDATE not persisted! Expected name 'Alice Smith' after restart, got '%s'", name)
	}
}

// TestUpdateChainWithSnapshot tests multiple updates to the same row
func TestUpdateChainWithSnapshot(t *testing.T) {
	tempDir := common.TempDir(t)
	dbPath := fmt.Sprintf("file://%s/test.db", tempDir)

	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create table
	_, err = db.Exec(`CREATE TABLE counter (
		id INTEGER PRIMARY KEY,
		value INTEGER NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial value
	_, err = db.Exec("INSERT INTO counter (id, value) VALUES (1, 0)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Perform multiple updates
	for i := 1; i <= 5; i++ {
		_, err = db.Exec("UPDATE counter SET value = ? WHERE id = 1", i)
		if err != nil {
			t.Fatalf("Failed to update counter to %d: %v", i, err)
		}
	}

	// Verify final value
	var value int
	err = db.QueryRow("SELECT value FROM counter WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query counter: %v", err)
	}
	if value != 5 {
		t.Errorf("Expected counter value 5, got %d", value)
	}

	// Force snapshot and close
	engine := stoolap.GetEngineByDSN(dbPath)
	if engine != nil {
		engine.CreateSnapshot()
	}
	db.Close()

	// Reopen and verify
	db2, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	err = db2.QueryRow("SELECT value FROM counter WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query counter after restart: %v", err)
	}

	if value != 5 {
		t.Errorf("UPDATE chain not persisted! Expected counter value 5 after restart, got %d", value)
		t.Error("This indicates that the latest UPDATE in a chain of updates is lost")
	}
}
