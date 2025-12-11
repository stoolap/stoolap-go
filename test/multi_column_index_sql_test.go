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

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestMultiColumnIndexSQL tests the implementation of multi-column indexes using SQL
func TestMultiColumnIndexSQL(t *testing.T) {
	// Create a test database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			category TEXT NOT NULL,
			price FLOAT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a multi-column index
	_, err = db.Exec(`
		CREATE INDEX name_category_idx ON products (name, category)
	`)
	if err != nil {
		t.Fatalf("Failed to create multi-column index: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price) VALUES 
		(1, 'Apple', 'Fruit', 1.99),
		(2, 'Banana', 'Fruit', 0.99),
		(3, 'Carrot', 'Vegetable', 0.75),
		(4, 'Apple', 'Vegetable', 2.50)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify the index by querying
	rows, err := db.Query(`
		SELECT id, name, category, price FROM products 
		WHERE name = 'Apple' AND category = 'Fruit'
	`)
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}
	defer rows.Close()

	// Check that we got the correct row
	count := 0
	for rows.Next() {
		var id int
		var name, category string
		var price float64
		if err := rows.Scan(&id, &name, &category, &price); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if id != 1 || name != "Apple" || category != "Fruit" || price != 1.99 {
			t.Errorf("Unexpected row: id=%d, name=%s, category=%s, price=%f", id, name, category, price)
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	// Drop the existing non-unique index first
	_, err = db.Exec(`DROP INDEX name_category_idx ON products`)
	if err != nil {
		t.Fatalf("Failed to drop existing index: %v", err)
	}

	// Create a unique multi-column index
	_, err = db.Exec(`
		CREATE UNIQUE INDEX unique_name_category_idx ON products (name, category)
	`)
	if err != nil {
		t.Fatalf("Failed to create unique multi-column index: %v", err)
	}

	// Attempt to insert a row that would violate the unique constraint
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price) VALUES 
		(5, 'Apple', 'Fruit', 2.99)
	`)
	if err == nil {
		t.Error("Expected unique constraint violation, but insert succeeded")
	} else if !strings.Contains(err.Error(), "constraint") {
		t.Errorf("Expected unique constraint error, got: %v", err)
	}

	// Test with different column values (should succeed)
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price) VALUES 
		(5, 'Apple', 'Tropical', 2.99)
	`)
	if err != nil {
		t.Errorf("Failed to insert row with unique values: %v", err)
	}

	// Show index information
	rows, err = db.Query("SHOW INDEXES FROM products")
	if err != nil {
		t.Fatalf("Failed to query indexes: %v", err)
	}
	defer rows.Close()

	t.Log("Indexes on products table:")
	for rows.Next() {
		var table, indexName, columnName, indexType string
		var unique bool
		if err := rows.Scan(&table, &indexName, &columnName, &indexType, &unique); err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Table: %s, Index: %s, Column: %s, Type: %s, Unique: %v",
			table, indexName, columnName, indexType, unique)
	}
}
