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
	"os"
	"path/filepath"
	"testing"
	"time"

	// Import stoolap driver
	"github.com/stoolap/stoolap"
	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestAutoIncrementPKTable tests auto-increment functionality for a table with an explicit primary key
func TestAutoIncrementPKTable(t *testing.T) {
	// Create a memory database for the test
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with an INTEGER PRIMARY KEY
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		price FLOAT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert records with explicit ID values
	_, err = db.Exec(`INSERT INTO products (id, name, price) VALUES 
		(1, 'Product A', 10.99),
		(2, 'Product B', 20.50),
		(3, 'Product C', 30.75)`)
	if err != nil {
		t.Fatalf("Failed to insert records with explicit IDs: %v", err)
	}

	// Test auto-increment behavior by inserting without providing ID
	// Since we've already used IDs 1-3, the auto-generated ID should be 4
	result, err := db.Exec(`INSERT INTO products (name, price) VALUES ('Product D', 40.25)`)
	if err != nil {
		t.Fatalf("Failed to insert without ID: %v", err)
	}

	// Check that the auto-generated ID is 4 (next after 3)
	id, err := result.LastInsertId()
	if err != nil {
		t.Logf("Driver doesn't support LastInsertId, will verify through query: %v", err)
	} else {
		if id != 4 {
			t.Errorf("Expected auto-generated ID to be 4, got %d", id)
		}
	}

	// Verify through a query
	var maxID int64
	err = db.QueryRow("SELECT MAX(id) FROM products").Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to query max ID: %v", err)
	}
	if maxID != 4 {
		t.Errorf("Expected max ID to be 4, got %d", maxID)
	}

	// Insert a record with an ID that exceeds the current auto-increment counter
	_, err = db.Exec(`INSERT INTO products (id, name, price) VALUES (10, 'Product E', 50.00)`)
	if err != nil {
		t.Fatalf("Failed to insert with higher ID: %v", err)
	}

	// Insert another record without ID to check if auto-increment counter was updated
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Product F', 60.50)`)
	if err != nil {
		t.Fatalf("Failed to insert second record without ID: %v", err)
	}

	// Verify that the auto-increment counter was updated after inserting ID 10
	err = db.QueryRow("SELECT MAX(id) FROM products").Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to query max ID: %v", err)
	}
	if maxID != 11 {
		t.Errorf("Expected max ID to be 11, got %d", maxID)
	}
}

// TestAutoIncrementNonPKTable tests auto-increment functionality for a table without a primary key
func TestAutoIncrementNonPKTable(t *testing.T) {
	// Create a memory database for the test
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table without a primary key
	_, err = db.Exec(`CREATE TABLE events (
		event_time TIMESTAMP NOT NULL,
		event_type TEXT NOT NULL,
		description TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert several records
	_, err = db.Exec(`INSERT INTO events (event_time, event_type, description) VALUES 
		('2023-01-01 10:00:00', 'START', 'System started'),
		('2023-01-01 10:05:00', 'LOG', 'Operation logged'),
		('2023-01-01 10:10:00', 'ALERT', 'Alert triggered')`)
	if err != nil {
		t.Fatalf("Failed to insert records: %v", err)
	}

	// Query to verify the records were inserted
	rows, err := db.Query("SELECT COUNT(*) FROM events")
	if err != nil {
		t.Fatalf("Failed to query record count: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}
	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}

	// Use a query that returns the hidden rowid to verify auto-increment behavior
	// This assumes rowids are exposed through an internal hidden column or API
	// For stoolap, we can verify this through a workaround using a dummy table with known IDs

	// Create a lookup table to potentially link records by content
	_, err = db.Exec(`CREATE TABLE event_lookup (
		id INTEGER PRIMARY KEY,
		event_description TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create lookup table: %v", err)
	}

	// Insert references to the event records
	for i := 1; i <= 3; i++ {
		description := fmt.Sprintf("Reference to event %d", i)
		_, err = db.Exec(`INSERT INTO event_lookup (id, event_description) VALUES (?, ?)`,
			i, description)
		if err != nil {
			t.Fatalf("Failed to insert lookup record: %v", err)
		}
	}

	// Insert a new record to check if the counter continues
	_, err = db.Exec(`INSERT INTO events (event_time, event_type, description) VALUES 
		('2023-01-01 10:15:00', 'LOG', 'New operation logged')`)
	if err != nil {
		t.Fatalf("Failed to insert fourth record: %v", err)
	}

	// Verify record count is now 4
	err = db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query updated count: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 records after insertion, got %d", count)
	}

	// Insert reference to the fourth event
	_, err = db.Exec(`INSERT INTO event_lookup (id, event_description) VALUES (?, ?)`,
		4, "Reference to event 4")
	if err != nil {
		t.Fatalf("Failed to insert fourth lookup record: %v", err)
	}
}

// TestAutoIncrementWALReplay tests that auto-increment values are correctly persisted
// and restored when replaying the WAL (Write-Ahead Log)
func TestAutoIncrementWALReplay(t *testing.T) {
	// Create a temporary database path
	tempDir := t.TempDir()

	dbPath := filepath.Join(tempDir, "test.db")
	// Testing with file:// (no extra slash)
	connString := fmt.Sprintf("file://%s", dbPath)

	t.Log("Using connection string:", connString)

	// First database connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a test table with an INTEGER PRIMARY KEY
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		username TEXT NOT NULL,
		email TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert records with explicit IDs
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(1, 'user1', 'user1@example.com'),
		(2, 'user2', 'user2@example.com'),
		(3, 'user3', 'user3@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert records with explicit IDs: %v", err)
	}

	// Test auto-increment by inserting without providing ID
	_, err = db.Exec(`INSERT INTO users (username, email) VALUES ('user4', 'user4@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert without ID: %v", err)
	}

	// Insert record with a much higher ID to advance the counter
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(100, 'user100', 'user100@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert with higher ID: %v", err)
	}

	// Force a transaction to ensure WAL is updated
	_, err = db.Exec("BEGIN TRANSACTION; COMMIT;")
	if err != nil {
		t.Logf("Note: Transaction commit returned: %v", err)
	}

	// Close the database to ensure everything is flushed to disk
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database to test WAL replay
	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Insert another record without ID to check if auto-increment counter was restored correctly
	_, err = db.Exec(`INSERT INTO users (username, email) VALUES ('user101', 'user101@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert after reopening: %v", err)
	}

	// Verify that the auto-increment counter was correctly restored from WAL
	var maxID int64
	err = db.QueryRow("SELECT MAX(id) FROM users").Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to query max ID: %v", err)
	}
	if maxID != 101 {
		t.Errorf("Expected max ID to be 101 after WAL replay, got %d", maxID)
	}

	// Create a table without a primary key
	_, err = db.Exec(`CREATE TABLE logs (
		event_time TIMESTAMP NOT NULL,
		message TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create logs table: %v", err)
	}

	// Insert records to test auto-increment for non-PK table
	for i := 0; i < 5; i++ {
		_, err = db.Exec(`INSERT INTO logs (event_time, message) VALUES (?, ?)`,
			time.Now(), fmt.Sprintf("Log message %d", i+1))
		if err != nil {
			t.Fatalf("Failed to insert log record %d: %v", i+1, err)
		}
	}

	// Force a transaction to ensure WAL is updated
	_, err = db.Exec("BEGIN TRANSACTION; COMMIT;")
	if err != nil {
		t.Logf("Note: Transaction commit returned: %v", err)
	}

	// Close and reopen the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Insert another log record
	_, err = db.Exec(`INSERT INTO logs (event_time, message) VALUES (?, ?)`,
		time.Now(), "Log message after reopen")
	if err != nil {
		t.Fatalf("Failed to insert log record after reopening: %v", err)
	}

	// Check count to make sure we have 6 log records total
	var logCount int
	err = db.QueryRow("SELECT COUNT(*) FROM logs").Scan(&logCount)
	if err != nil {
		t.Fatalf("Failed to query log count: %v", err)
	}
	if logCount != 6 {
		t.Errorf("Expected 6 log records, got %d", logCount)
	}

	// Close database
	db.Close()

	// Give Windows time to release file handles
	time.Sleep(100 * time.Millisecond)
}

// TestAutoIncrementSnapshotLoading tests that auto-increment values are correctly persisted
// and restored when loading from a snapshot
func TestAutoIncrementSnapshotLoading(t *testing.T) {
	// Create a temporary database path
	tempDir := t.TempDir()

	dbPath := filepath.Join(tempDir, "test.db")

	// Don't use snapshot interval, we'll manually trigger snapshots
	connString := fmt.Sprintf("file://%s", dbPath)

	// First database connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a test table with an INTEGER PRIMARY KEY
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		price FLOAT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert records with auto-incrementing IDs
	_, err = db.Exec(`INSERT INTO products (id, name, price) VALUES 
		(1, 'Product A', 10.99),
		(2, 'Product B', 20.50),
		(3, 'Product C', 30.75)`)
	if err != nil {
		t.Fatalf("Failed to insert initial records: %v", err)
	}

	// Insert a record with a much higher ID to advance the counter
	_, err = db.Exec(`INSERT INTO products (id, name, price) VALUES 
		(1000, 'Product X', 99.99)`)
	if err != nil {
		t.Fatalf("Failed to insert with higher ID: %v", err)
	}

	// Insert records without specifying ID to test auto-increment
	for i := 0; i < 5; i++ {
		_, err = db.Exec(`INSERT INTO products (name, price) VALUES (?, ?)`,
			fmt.Sprintf("Auto Product %d", i+1), 50.0+float64(i))
		if err != nil {
			t.Fatalf("Failed to insert auto-increment record %d: %v", i+1, err)
		}
	}

	// Create a table without primary key
	_, err = db.Exec(`CREATE TABLE activity (
		action TEXT NOT NULL,
		event_time TIMESTAMP NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create activity table: %v", err)
	}

	// Insert records into the non-PK table
	for i := 0; i < 10; i++ {
		_, err = db.Exec(`INSERT INTO activity (action, event_time) VALUES (?, ?)`,
			fmt.Sprintf("Action %d", i+1), time.Now())
		if err != nil {
			t.Fatalf("Failed to insert activity record %d: %v", i+1, err)
		}
	}

	// Force a transaction to ensure everything is committed
	_, err = db.Exec("BEGIN TRANSACTION; COMMIT;")
	if err != nil {
		t.Logf("Note: Transaction commit returned: %v", err)
	}

	// Manually create snapshot
	t.Log("Creating snapshot...")
	engine := stoolap.GetEngineByDSN(connString)
	if engine == nil {
		t.Fatalf("Failed to get engine for DSN: %s", connString)
	}
	if err := engine.CreateSnapshot(); err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Close the database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Verify the snapshot directory exists
	snapshotDir := filepath.Join(dbPath, "products")
	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Logf("Note: Could not read snapshot directory: %v (this might be expected if snapshots are stored differently)", err)
	} else {
		t.Logf("Found %d files in product snapshot directory", len(files))
		for _, file := range files {
			t.Logf("  %s", file.Name())
		}
	}

	// Reopen the database to load from snapshot
	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Insert another record without ID to test if auto-increment counter was restored
	_, err = db.Exec(`INSERT INTO products (name, price) VALUES ('Product After Snapshot', 200.00)`)
	if err != nil {
		t.Fatalf("Failed to insert after reopening: %v", err)
	}

	// Verify the auto-increment counter was correctly restored
	var maxID int64
	err = db.QueryRow("SELECT MAX(id) FROM products").Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to query max ID: %v", err)
	}
	if maxID < 1000 {
		t.Errorf("Expected max ID to be at least 1000, got %d", maxID)
	}

	// Check the count of records in the products table
	var productCount int
	err = db.QueryRow("SELECT COUNT(*) FROM products").Scan(&productCount)
	if err != nil {
		t.Fatalf("Failed to query product count: %v", err)
	}
	if productCount != 10 { // 4 explicit + 5 auto + 1 after snapshot
		t.Errorf("Expected 10 products, got %d", productCount)
	}

	// Insert another record into the non-PK table
	_, err = db.Exec(`INSERT INTO activity (action, event_time) VALUES (?, ?)`,
		"Action after snapshot", time.Now())
	if err != nil {
		t.Fatalf("Failed to insert activity after reopening: %v", err)
	}

	// Check the count of records in the activity table
	var activityCount int
	err = db.QueryRow("SELECT COUNT(*) FROM activity").Scan(&activityCount)
	if err != nil {
		t.Fatalf("Failed to query activity count: %v", err)
	}
	if activityCount != 11 { // 10 initial + 1 after snapshot
		t.Errorf("Expected 11 activities, got %d", activityCount)
	}

	// Close database
	db.Close()

	// Give Windows time to release file handles
	time.Sleep(100 * time.Millisecond)
}
