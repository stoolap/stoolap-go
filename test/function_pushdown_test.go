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

	_ "github.com/stoolap/stoolap-go/internal/functions/scalar" // Import for init registration
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestFunctionPushdown(t *testing.T) {
	// We're not manually registering functions since they're already
	// registered in the init functions of the scalar package

	// Connect to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create test table with string data
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY, 
		username TEXT, 
		email TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, username, email) VALUES 
		(1, 'John_Smith', 'john.smith@example.com'),
		(2, 'SARAH_JONES', 'sarah.jones@example.com'),
		(3, 'mike_brown', 'mike.brown@example.com'),
		(4, 'LINDA_WILSON', 'linda.wilson@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test case-insensitive username search (UPPER function pushdown)
	t.Run("UPPER function pushdown", func(t *testing.T) {
		rows, err := db.Query("SELECT id, username FROM users WHERE UPPER(username) = 'JOHN_SMITH'")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var username string
			if err := rows.Scan(&id, &username); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
			if id != 1 || username != "John_Smith" {
				t.Errorf("Expected id=1, username='John_Smith', got id=%d, username='%s'", id, username)
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	// Test LOWER function pushdown
	t.Run("LOWER function pushdown", func(t *testing.T) {
		rows, err := db.Query("SELECT id, username FROM users WHERE LOWER(username) = 'sarah_jones'")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var username string
			if err := rows.Scan(&id, &username); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
			if id != 2 || username != "SARAH_JONES" {
				t.Errorf("Expected id=2, username='SARAH_JONES', got id=%d, username='%s'", id, username)
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})

	// Test function in more complex query with other conditions
	t.Run("Function with additional conditions", func(t *testing.T) {
		rows, err := db.Query("SELECT id, username FROM users WHERE LOWER(username) LIKE '%smith' AND id <= 2")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var username string
			if err := rows.Scan(&id, &username); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++

			// Only John_Smith should match
			if id != 1 || username != "John_Smith" {
				t.Errorf("Expected id=1, username='John_Smith', got id=%d, username='%s'", id, username)
			}
		}

		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	})
}
