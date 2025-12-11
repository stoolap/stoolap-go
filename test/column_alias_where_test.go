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

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import for database registration
)

func TestColumnAliasInWhereClause(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER,
		price INTEGER,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO products (id, price, name) VALUES (1, 50, 'Widget')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	_, err = db.Exec(`INSERT INTO products (id, price, name) VALUES (2, 150, 'Gadget')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	_, err = db.Exec(`INSERT INTO products (id, price, name) VALUES (3, 80, 'Thing')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Test 1: Use column alias in WHERE clause
	t.Run("UseAliasInWhere", func(t *testing.T) {
		// Query using alias in WHERE clause
		rows, err := db.Query(`SELECT price AS cost FROM products WHERE cost > 100`)
		if err != nil {
			t.Fatalf("Error executing query with alias in WHERE: %v", err)
		}
		defer rows.Close()

		// Count rows and verify values
		var count int
		var cost int
		for rows.Next() {
			err := rows.Scan(&cost)
			if err != nil {
				t.Fatalf("Error scanning row: %v", err)
			}
			count++

			// Verify it's the correct value
			if cost != 150 {
				t.Errorf("Expected cost = 150, got %d", cost)
			}
		}

		// We should only get one row (Gadget with price 150)
		if count != 1 {
			t.Errorf("Expected 1 row matching WHERE cost > 100, got %d", count)
		}
	})

	// Test 2: Multiple aliases in SELECT and WHERE
	t.Run("MultipleAliases", func(t *testing.T) {
		// Query using multiple aliases
		rows, err := db.Query(`SELECT id, price AS cost, name AS product_name 
			FROM products 
			WHERE cost > 60 AND product_name = 'Thing'`)
		if err != nil {
			t.Fatalf("Error executing query with multiple aliases: %v", err)
		}
		defer rows.Close()

		// Count rows and verify values
		var count int
		var id int
		var cost int
		var productName string
		for rows.Next() {
			err := rows.Scan(&id, &cost, &productName)
			if err != nil {
				t.Fatalf("Error scanning row: %v", err)
			}
			count++

			// Verify the values match what we expect
			if id != 3 || cost != 80 || productName != "Thing" {
				t.Errorf("Expected id=3, cost=80, product_name='Thing', got id=%d, cost=%d, product_name='%s'",
					id, cost, productName)
			}
		}

		// We should only get one row (Thing with id=3, price=80)
		if count != 1 {
			t.Errorf("Expected 1 row matching complex condition, got %d", count)
		}
	})
}
