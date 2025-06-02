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

	"github.com/stoolap/stoolap/internal/common"
	_ "github.com/stoolap/stoolap/pkg/driver" // Register stoolap driver
)

// TestIndexDirectCreate directly tests index name preservation
// without the complexities of database reopening
func TestIndexDirectCreate(t *testing.T) {
	// Create a temporary directory for our database
	tempDir := common.TempDir(t)

	// Use file:// to test persistence
	connString := "file://" + tempDir

	// Open a standard database/sql connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open SQL connection: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create an index with a special name
	fmt.Println("DEBUG: Creating index idx_users_email on users(email)")
	_, err = db.Exec("CREATE INDEX idx_users_email ON users (email)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	fmt.Println("DEBUG: Successfully created index")

	// Try to create a duplicate index - this should fail
	_, err = db.Exec("CREATE INDEX idx_users_email ON users (email)")
	if err == nil {
		t.Fatal("Creating duplicate index should have failed")
	} else {
		t.Logf("Expected error when creating duplicate index: %v", err)
	}

	// Get the list of indexes via SHOW INDEXES
	rows, err := db.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to query indexes: %v", err)
	}

	// Check if our index exists with the right name
	var found bool
	for rows.Next() {
		var tableName, indexName, columnName, indexType string
		var isUnique bool

		err = rows.Scan(&tableName, &indexName, &columnName, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		t.Logf("Found index: table=%s, name=%s, column=%s, type=%s, unique=%v",
			tableName, indexName, columnName, indexType, isUnique)

		if indexName == "idx_users_email" {
			found = true
		}
	}
	rows.Close()

	if !found {
		t.Fatal("Could not find index with name 'idx_users_email'")
	}

	// Close and reopen the database to test persistence
	t.Log("--- Closing and reopening the database ---")
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Check if our index still exists after reopening
	fmt.Println("DEBUG: After reopening, checking for indexes...")
	rows, err = db.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to query indexes after reopen: %v", err)
	}
	defer rows.Close()

	found = false
	for rows.Next() {
		var tableName, indexName, columnName, indexType string
		var isUnique bool

		err = rows.Scan(&tableName, &indexName, &columnName, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		t.Logf("Found index after reopen: table=%s, name=%s, column=%s, type=%s, unique=%v",
			tableName, indexName, columnName, indexType, isUnique)

		if indexName == "idx_users_email" {
			found = true
		}
	}

	if !found {
		t.Fatal("Could not find index with name 'idx_users_email' after reopening database")
	}

	// Test dropping the index and verify it's gone
	_, err = db.Exec("DROP INDEX idx_users_email ON users")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Check that the index is gone
	rows, err = db.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to query indexes after drop: %v", err)
	}
	defer rows.Close()

	found = false
	for rows.Next() {
		var tableName, indexName, columnName, indexType string
		var isUnique bool

		err = rows.Scan(&tableName, &indexName, &columnName, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		t.Logf("Index after drop: table=%s, name=%s, column=%s, type=%s, unique=%v",
			tableName, indexName, columnName, indexType, isUnique)

		if indexName == "idx_users_email" {
			found = true
		}
	}

	if found {
		t.Fatal("Index 'idx_users_email' still exists after DROP INDEX")
	}
}
