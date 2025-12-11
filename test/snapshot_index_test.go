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

	// Import stoolap driver
	"github.com/stoolap/stoolap-go"
	"github.com/stoolap/stoolap-go/internal/common"
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestSnapshotIndexPersistence tests that both regular and columnar indexes
// with custom names are properly persisted in snapshots and correctly
// restored after database restart.
func TestSnapshotIndexPersistence(t *testing.T) {
	// Create a temporary database path
	tempDir := common.TempDir(t)

	dbPath := filepath.Join(tempDir, "test.db")

	// Don't use snapshot interval, we'll manually trigger snapshots
	connString := fmt.Sprintf("file://%s", dbPath)
	t.Logf("Using connection string: %s", connString)

	// First database connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a test table
	_, err = db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		region TEXT,
		active BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`INSERT INTO customers (id, name, email, region, active) VALUES 
		(1, 'John Doe', 'john@example.com', 'North', true),
		(2, 'Jane Smith', 'jane@example.com', 'South', true),
		(3, 'Bob Johnson', 'bob@example.com', 'East', false),
		(4, 'Alice Brown', 'alice@example.com', 'West', true),
		(5, 'Charlie Wilson', 'charlie@example.com', 'North', false)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create a regular index with a custom name
	t.Log("Creating regular index with a custom name")
	_, err = db.Exec(`CREATE INDEX idx_customer_region ON customers(region)`)
	if err != nil {
		t.Fatalf("Failed to create regular index: %v", err)
	}

	// Create a unique columnar index
	t.Log("Creating unique columnar index on email")
	_, err = db.Exec(`CREATE UNIQUE COLUMNAR INDEX ON customers(email)`)
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Verify both indexes exist
	t.Log("Verifying indexes after creation")
	rows, err := db.Query(`SHOW INDEXES FROM customers`)
	if err != nil {
		t.Fatalf("Failed to show indexes: %v", err)
	}

	var foundIndexes = make(map[string]bool)
	var uniqueFlag = make(map[string]bool)
	var indexTypes = make(map[string]string)

	// Scan indexes before restart
	for rows.Next() {
		var table, indexName, column, indexType string
		var isUnique bool
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Found index: table=%s, name=%s, column=%s, type=%s, unique=%v",
			table, indexName, column, indexType, isUnique)
		foundIndexes[indexName] = true
		uniqueFlag[indexName] = isUnique
		indexTypes[indexName] = indexType
	}
	rows.Close()

	// Check regular index
	if !foundIndexes["idx_customer_region"] {
		t.Fatalf("Regular index idx_customer_region not found")
	}
	if uniqueFlag["idx_customer_region"] {
		t.Fatalf("idx_customer_region should not be a unique index")
	}

	// Check columnar index
	var columnarIndexName string
	for indexName := range foundIndexes {
		if indexName != "idx_customer_region" {
			columnarIndexName = indexName
			break
		}
	}
	if columnarIndexName == "" {
		t.Fatalf("Columnar index not found")
	}
	if !uniqueFlag[columnarIndexName] {
		t.Fatalf("Columnar index %s should be a unique index", columnarIndexName)
	}
	if indexTypes[columnarIndexName] != "columnar" {
		t.Fatalf("Expected index type 'columnar', got '%s'", indexTypes[columnarIndexName])
	}

	// Force a transaction to make sure all pending changes are flushed
	t.Log("Forcing a transaction to ensure all pending changes are flushed")
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

	// Verify the snapshot directory exists
	snapshotDir := filepath.Join(dbPath, "customers")
	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Logf("Note: Could not read snapshot directory: %v (this might be expected if snapshots are stored differently)", err)
	} else {
		t.Logf("Found %d files in snapshot directory", len(files))
		for _, file := range files {
			t.Logf("  %s", file.Name())
		}
	}

	// Check WAL directory
	walDir := filepath.Join(dbPath, "wal")
	walFiles, err := os.ReadDir(walDir)
	if err != nil {
		t.Logf("Note: Could not read WAL directory: %v (this might be expected if WAL is stored differently)", err)
	} else {
		t.Logf("Found %d files in WAL directory", len(walFiles))
		for _, file := range walFiles {
			t.Logf("  %s", file.Name())
		}
	}

	// Close the database before reopening
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database to ensure data is loaded from snapshots
	t.Log("Closing and reopening the database to test snapshot persistence")

	// Use a different connection string to force WAL replay for debugging
	reopenConnString := fmt.Sprintf("file://%s", dbPath)
	t.Logf("Reopening with connection string: %s", reopenConnString)

	db, err = sql.Open("stoolap", reopenConnString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify indexes are preserved after reopen
	t.Log("Verifying indexes after reopen")
	rows, err = db.Query(`SHOW INDEXES FROM customers`)
	if err != nil {
		t.Fatalf("Failed to show indexes after reopen: %v", err)
	}

	// Reset tracking maps
	foundIndexes = make(map[string]bool)
	uniqueFlag = make(map[string]bool)
	indexTypes = make(map[string]string)

	// Debug dump the index details directly from storage
	t.Log("Directly querying index details from storage:")
	result, err := db.Query(`SELECT * FROM customers LIMIT 1`)
	if err != nil {
		t.Logf("Note: Failed to execute simple query: %v", err)
	} else {
		result.Close()
	}

	// Scan indexes after restart
	for rows.Next() {
		var table, indexName, column, indexType string
		var isUnique bool
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Found index after reopen: table=%s, name=%s, column=%s, type=%s, unique=%v",
			table, indexName, column, indexType, isUnique)
		foundIndexes[indexName] = true
		uniqueFlag[indexName] = isUnique
		indexTypes[indexName] = indexType
	}
	rows.Close()

	// Check regular index after restart
	if !foundIndexes["idx_customer_region"] {
		t.Fatalf("Regular index idx_customer_region not found after reopen")
	}
	if uniqueFlag["idx_customer_region"] {
		t.Fatalf("idx_customer_region should not be a unique index after reopen")
	}

	// Check columnar index after restart
	var columnarIndexNameAfterReopen string
	for indexName := range foundIndexes {
		if indexName != "idx_customer_region" {
			columnarIndexNameAfterReopen = indexName
			break
		}
	}
	if columnarIndexNameAfterReopen == "" {
		t.Fatalf("Columnar index not found after reopen")
	}
	if !uniqueFlag[columnarIndexNameAfterReopen] {
		t.Fatalf("Columnar index %s should be a unique index after reopen", columnarIndexNameAfterReopen)
	}
	if indexTypes[columnarIndexNameAfterReopen] != "columnar" {
		t.Fatalf("Expected index type 'columnar', got '%s' after reopen", indexTypes[columnarIndexNameAfterReopen])
	}

	// Verify columnar index name is preserved
	if columnarIndexNameAfterReopen != columnarIndexName {
		t.Fatalf("Columnar index name changed after reopen: %s -> %s", columnarIndexName, columnarIndexNameAfterReopen)
	}

	// Verify uniqueness constraint on email index is still enforced
	t.Log("Verifying uniqueness constraint is still enforced")

	// First check if the email index is reported as unique
	rows, err = db.Query(`SHOW INDEXES FROM customers`)
	if err != nil {
		t.Fatalf("Failed to show indexes for uniqueness check: %v", err)
	}

	var emailIndexUnique bool
	for rows.Next() {
		var table, indexName, column, indexType string
		var isUnique bool
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}

		if column == "email" {
			t.Logf("Email index details: name=%s, unique=%v", indexName, isUnique)
			emailIndexUnique = isUnique
		}
	}
	rows.Close()

	if !emailIndexUnique {
		t.Fatalf("Email index is not marked as unique after reopening")
	}

	// Try to insert a duplicate email to see if constraint is enforced
	_, err = db.Exec(`INSERT INTO customers (id, name, email, region, active) VALUES 
		(6, 'Duplicate Email', 'john@example.com', 'South', true)`)
	if err == nil {
		t.Fatalf("Expected unique constraint violation, but insert succeeded")
	}
	t.Logf("Got expected error on duplicate insert: %v", err)

	// Test successful insert with a different email
	_, err = db.Exec(`INSERT INTO customers (id, name, email, region, active) VALUES 
		(6, 'Mark Davis', 'mark@example.com', 'East', true)`)
	if err != nil {
		t.Fatalf("Failed to insert valid data after reopen: %v", err)
	}
	t.Log("Successfully inserted non-duplicate data after reopen")

	// Close database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database at end: %v", err)
	}

	t.Log("Test completed successfully")
}
