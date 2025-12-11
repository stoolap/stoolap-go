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
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestDriverMultiColumnUniqueIndex(t *testing.T) {
	// Open a connection to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with multi-column unique constraint
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			first_name TEXT NOT NULL,
			last_name TEXT,
			email TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a multi-column unique index
	_, err = db.Exec(`
		CREATE UNIQUE INDEX idx_users_name ON users(first_name, last_name)
	`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Insert a row that should succeed
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}

	// Insert a row with different names - should succeed
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', 25)
	`)
	if err != nil {
		t.Fatalf("Failed to insert second row: %v", err)
	}

	// Insert another row with same first name but different last name - should succeed
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (3, 'John', 'Smith', 'john.smith@example.com', 35)
	`)
	if err != nil {
		t.Fatalf("Failed to insert third row: %v", err)
	}

	// Insert another row with different first name but same last name - should succeed
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (4, 'Jane', 'Doe', 'jane.doe@example.com', 28)
	`)
	if err != nil {
		t.Fatalf("Failed to insert fourth row: %v", err)
	}

	// Try to insert a row that violates the unique constraint - should fail
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (5, 'John', 'Doe', 'john.doe2@example.com', 40)
	`)
	if err == nil {
		t.Errorf("Expected unique constraint violation but got success")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	// Test with NULL values (NULL values don't violate uniqueness)
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (6, 'John', NULL, 'john.null@example.com', 45)
	`)
	if err != nil {
		t.Fatalf("Failed to insert row with NULL last_name: %v", err)
	}

	// Another NULL last_name should work
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (7, 'John', NULL, 'john.null2@example.com', 50)
	`)
	if err != nil {
		t.Fatalf("Failed to insert second row with NULL last_name: %v", err)
	}

	// Let's verify the data
	rows, err := db.Query("SELECT id, first_name, last_name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}
	defer rows.Close()

	t.Log("Users in database:")
	var id int
	var firstName, lastName sql.NullString
	for rows.Next() {
		err = rows.Scan(&id, &firstName, &lastName)
		if err != nil {
			t.Errorf("Error scanning row: %v", err)
			continue
		}
		lastNameStr := "NULL"
		if lastName.Valid {
			lastNameStr = lastName.String
		}
		t.Logf("ID: %d, Name: %s %s", id, firstName.String, lastNameStr)
	}

	// Test the delete + insert works (checking that constraints are properly removed)
	_, err = db.Exec("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Now we should be able to insert with the same name
	_, err = db.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (8, 'John', 'Doe', 'john.doe3@example.com', 55)
	`)
	if err != nil {
		t.Fatalf("Failed to reuse name after deletion: %v", err)
	}

	// Test transaction support
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (9, 'Alice', 'Johnson', 'alice@example.com', 33)
	`)
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Should fail because of duplication, even in transaction
	_, err = tx.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (10, 'John', 'Doe', 'another.john@example.com', 60)
	`)
	if err == nil {
		t.Errorf("Expected unique constraint violation in transaction but got success")
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Try one more time with a complete transaction
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Insert new unique values and commit
	_, err = tx.Exec(`
		INSERT INTO users (id, first_name, last_name, email, age) 
		VALUES (11, 'Bob', 'Wilson', 'bob@example.com', 42)
	`)
	if err != nil {
		t.Fatalf("Failed to insert in second transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify Bob was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE first_name = 'Bob'").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count Bob: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 Bob, got %d", count)
	}
}
