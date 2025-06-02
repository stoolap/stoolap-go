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

// TestUniqueIndexPersistence tests that unique index properties are properly preserved
// when using CREATE UNIQUE INDEX across database reopens
func TestUniqueIndexPersistence(t *testing.T) {
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

	// Create a unique index with a special name
	fmt.Println("DEBUG: Creating unique index idx_users_email_unique on users(email)")
	_, err = db.Exec("CREATE UNIQUE INDEX idx_users_email_unique ON users (email)")
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}
	fmt.Println("DEBUG: Successfully created unique index")

	// Insert a row to test uniqueness constraint
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (1, 'User1', 'user1@example.com')")
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}

	// Try to insert a row with same email - should fail due to unique constraint
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (2, 'User2', 'user1@example.com')")
	if err == nil {
		t.Fatal("Inserting duplicate email should have failed")
	} else {
		t.Logf("Expected error when inserting duplicate email: %v", err)
	}

	// Get the list of indexes via SHOW INDEXES
	rows, err := db.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to query indexes: %v", err)
	}

	// Check if our unique index exists with the right name and uniqueness property
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

		if indexName == "idx_users_email_unique" {
			found = true
			if !isUnique {
				t.Errorf("Index idx_users_email_unique should be unique, but isUnique=%v", isUnique)
			}
		}
	}
	rows.Close()

	if !found {
		t.Fatal("Could not find index with name 'idx_users_email_unique'")
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

	// Check if our unique index still exists after reopening
	fmt.Println("DEBUG: After reopening, checking for unique indexes...")
	rows, err = db.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to query indexes after reopen: %v", err)
	}

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

		if indexName == "idx_users_email_unique" {
			found = true
			if !isUnique {
				t.Errorf("Index idx_users_email_unique should still be unique after reopen, but isUnique=%v", isUnique)
			}
		}
	}
	rows.Close()

	if !found {
		t.Fatal("Could not find unique index with name 'idx_users_email_unique' after reopening database")
	}

	// Test that uniqueness constraint is still enforced after reopen
	// Try to insert another row with the same email
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (3, 'User3', 'user1@example.com')")
	if err == nil {
		t.Fatal("Inserting duplicate email after reopen should have failed")
	} else {
		t.Logf("Expected error when inserting duplicate email after reopen: %v", err)
	}

	// Test dropping the unique index and verify it's gone
	_, err = db.Exec("DROP INDEX idx_users_email_unique ON users")
	if err != nil {
		t.Fatalf("Failed to drop unique index: %v", err)
	}

	// Check that the unique index is gone
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

		if indexName == "idx_users_email_unique" {
			found = true
		}
	}

	if found {
		t.Fatal("Unique index 'idx_users_email_unique' still exists after DROP INDEX")
	}

	// Now we should be able to insert duplicate emails since the unique constraint is gone
	_, err = db.Exec("INSERT INTO users (id, name, email) VALUES (3, 'User3', 'user1@example.com')")
	if err != nil {
		t.Fatalf("Inserting previously duplicate email after dropping unique index should succeed, but got: %v", err)
	}

	// Verify we now have two rows with the same email
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE email = 'user1@example.com'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("Expected 2 rows with the same email after dropping unique index, got %d", count)
	}
}
