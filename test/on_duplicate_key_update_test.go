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
	"strings"
	"testing"

	"github.com/stoolap/stoolap-go/internal/parser"
	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import the driver for side effects
)

func TestInsertOnDuplicateKeyUpdate(t *testing.T) {
	// Set up a new database using the standard sql driver
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with primary key
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			username TEXT NOT NULL,
			email TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a unique columnar index on username
	_, err = db.Exec(`
		CREATE UNIQUE INDEX idx_username ON users (username)
	`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Test case 1: Basic insert
	_, err = db.Exec(`
		INSERT INTO users (id, username, email, age) 
		VALUES (1, 'user1', 'user1@example.com', 25)
	`)
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}

	// Test case 2: Test PRIMARY KEY constraint with ON DUPLICATE KEY UPDATE
	_, err = db.Exec(`
		INSERT INTO users (id, username, email, age) 
		VALUES (1, 'different_user', 'new_email@example.com', 40)
		ON DUPLICATE KEY UPDATE 
		username = 'primary_key_updated',
		email = 'pk_updated@example.com', 
		age = 45
	`)
	if err != nil {
		t.Fatalf("Failed to update with ON DUPLICATE KEY for primary key violation: %v", err)
	}

	// Verify the primary key update worked correctly
	rows, err := db.Query("SELECT id, username, email, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("Expected to find user with id=1, but got no rows")
	}

	var id int64
	var username, email string
	var age int64
	err = rows.Scan(&id, &username, &email, &age)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify that the data was updated by primary key
	if id != 1 {
		t.Errorf("Expected id to be 1, got %d", id)
	}
	if username != "primary_key_updated" {
		t.Errorf("Expected username to be 'primary_key_updated', got '%s'", username)
	}
	if email != "pk_updated@example.com" {
		t.Errorf("Expected email to be 'pk_updated@example.com', got '%s'", email)
	}
	if age != 45 {
		t.Errorf("Expected age to be 45, got %d", age)
	}

	// Test case 3: Insert a second row for unique constraint testing
	_, err = db.Exec(`
		INSERT INTO users (id, username, email, age) 
		VALUES (2, 'user2', 'user2@example.com', 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert second row: %v", err)
	}

	// Test case 4: First, create a table with a UNIQUE constraint that isn't a PRIMARY KEY
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			code TEXT NOT NULL,
			name TEXT,
			price FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	// Create a unique columnar index on code
	_, err = db.Exec(`
		CREATE UNIQUE INDEX idx_code ON products (code)
	`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Insert an initial row
	_, err = db.Exec(`
		INSERT INTO products (id, code, name, price)
		VALUES (1, 'PROD-001', 'Initial Product', 19.99)
	`)
	if err != nil {
		t.Fatalf("Failed to insert product: %v", err)
	}

	// Now test ON DUPLICATE KEY UPDATE with the unique constraint on 'code'
	// Print a debug message to help us understand
	t.Log("Testing ON DUPLICATE KEY UPDATE with unique columnar index...")

	// Enable debug logging to trace the execution
	_, err = db.Exec(`
		INSERT INTO products (id, code, name, price)
		VALUES (999, 'PROD-001', 'Duplicate Code Product', 29.99)
		ON DUPLICATE KEY UPDATE
		name = 'Updated Product',
		price = 39.99
	`)

	// Intentionally allow errors to debug the issue
	if err != nil {
		t.Fatalf("Error with ON DUPLICATE KEY UPDATE: %v", err)
	}

	// Verify the product was updated, not inserted
	prodRows, err := db.Query("SELECT id, code, name, price FROM products WHERE code = 'PROD-001'")
	if err != nil {
		t.Fatalf("Failed to query products: %v", err)
	}
	defer prodRows.Close()

	if !prodRows.Next() {
		t.Fatalf("Expected to find product with code='PROD-001', but got no rows")
	}

	var prodID int64
	var code, name string
	var price float64
	err = prodRows.Scan(&prodID, &code, &name, &price)
	if err != nil {
		t.Fatalf("Failed to scan product row: %v", err)
	}

	t.Logf("Found product with id=%d, code=%s, name=%s, price=%.2f", prodID, code, name, price)

	// Verify the product was updated based on the unique constraint
	if prodID != 1 {
		t.Errorf("Expected product ID to be 1 (original), got %d", prodID)
	}
	if name != "Updated Product" {
		t.Errorf("Expected product name to be 'Updated Product', got '%s'", name)
	}
	if price != 39.99 {
		t.Errorf("Expected price to be 39.99, got %.2f", price)
	}

	// Test case 5: Insert a completely new row with ON DUPLICATE KEY (no duplicate, should insert)
	_, err = db.Exec(`
		INSERT INTO users (id, username, email, age) 
		VALUES (3, 'user3', 'user3@example.com', 35)
		ON DUPLICATE KEY UPDATE 
		email = 'not_used@example.com'
	`)
	if err != nil {
		t.Fatalf("Failed to insert new row with ON DUPLICATE KEY: %v", err)
	}

	// Test case 6: Test UNIQUE constraint with ON DUPLICATE KEY UPDATE on username column
	_, err = db.Exec(`
		INSERT INTO users (id, username, email, age) 
		VALUES (999, 'user2', 'different_email@example.com', 50)
		ON DUPLICATE KEY UPDATE 
		id = 2,
		email = 'updated_by_unique_constraint@example.com', 
		age = 55
	`)
	if err != nil {
		t.Fatalf("Failed to update with ON DUPLICATE KEY for unique constraint violation: %v", err)
	}

	// Verify all rows exist now by listing them
	rows, err = db.Query("SELECT id, username, email, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query users: %v", err)
	}
	defer rows.Close()

	// Log all rows for debugging purposes
	t.Log("Current rows in the table:")
	rowCount := 0
	usernames := make(map[string]bool)
	uniqueRows := make(map[string]struct {
		id    int64
		email string
		age   int64
	})

	for rows.Next() {
		rowCount++
		var id int64
		var username, email string
		var age int64
		err = rows.Scan(&id, &username, &email, &age)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Row %d: id=%d, username=%s, email=%s, age=%d",
			rowCount, id, username, email, age)

		// Track unique usernames and their associated data
		usernames[username] = true
		uniqueRows[username] = struct {
			id    int64
			email string
			age   int64
		}{id, email, age}
	}

	// Verify there's exactly one row with username='user2' that has been updated by the unique constraint test
	user2Data, exists := uniqueRows["user2"]
	if !exists {
		t.Errorf("Expected to find user with username='user2', but didn't find any")
	} else {
		if user2Data.id != 2 {
			t.Errorf("Expected user2 to have id=2, got id=%d", user2Data.id)
		}
		if user2Data.email != "updated_by_unique_constraint@example.com" {
			t.Errorf("Expected user2's email to be 'updated_by_unique_constraint@example.com', got '%s'", user2Data.email)
		}
		if user2Data.age != 55 {
			t.Errorf("Expected user2's age to be 55, got %d", user2Data.age)
		}
	}

	// The test should have exactly 3 rows in total (user1, user2, user3)
	if rowCount != 3 {
		t.Errorf("Expected 3 rows in total, got %d", rowCount)
	}

	// Verify there are exactly 3 distinct usernames
	if len(usernames) != 3 {
		t.Errorf("Expected 3 distinct usernames, but found %d", len(usernames))
	}

	t.Logf("Found %d rows with %d distinct usernames", rowCount, len(usernames))

	// Test case 4: Parse SQL with ON DUPLICATE KEY UPDATE
	stmt, err := parser.Parse("INSERT INTO users (id, username) VALUES (4, 'user3') ON DUPLICATE KEY UPDATE username = 'updated'")
	if err != nil {
		t.Fatalf("Failed to parse ON DUPLICATE KEY statement: %v", err)
	}

	// Verify the AST structure has ON DUPLICATE KEY fields
	insertStmt, ok := stmt.(*parser.InsertStatement)
	if !ok {
		t.Fatalf("Expected InsertStatement, got %T", stmt)
	}

	if !insertStmt.OnDuplicate {
		t.Errorf("Expected OnDuplicate to be true")
	}

	if len(insertStmt.UpdateColumns) != 1 || insertStmt.UpdateColumns[0].Value != "username" {
		t.Errorf("Expected update column to be 'username', got %v", insertStmt.UpdateColumns)
	}

	// Test result String() method has ON DUPLICATE KEY
	sql := insertStmt.String()
	if !strings.Contains(strings.ToUpper(sql), "ON DUPLICATE KEY UPDATE") {
		t.Errorf("Expected SQL string to contain ON DUPLICATE KEY UPDATE, got: %s", sql)
	}
}
