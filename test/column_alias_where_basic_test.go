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

func TestColumnAliasBasic(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a test table with unique name to avoid conflicts
	_, err = db.Exec(`CREATE TABLE basic_alias_test_items (id INTEGER, price INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO basic_alias_test_items (id, price, name) VALUES (1, 50, 'Widget')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
	_, err = db.Exec(`INSERT INTO basic_alias_test_items (id, price, name) VALUES (2, 150, 'Gadget')`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Debug: check how many rows we actually have
	debugRows, err := db.Query(`SELECT COUNT(*) FROM basic_alias_test_items`)
	if err != nil {
		t.Fatalf("Error in COUNT(*) query: %v", err)
	}

	var debugCount int
	if debugRows.Next() {
		if err := debugRows.Scan(&debugCount); err != nil {
			t.Fatalf("Error scanning debug row: %v", err)
		}
	}
	debugRows.Close()
	t.Logf("DEBUG: basic_alias_test_items table has %d rows", debugCount)

	// First test: basic SELECT with column alias
	t.Run("BasicAliasSelect", func(t *testing.T) {
		rows, err := db.Query(`SELECT price AS cost FROM basic_alias_test_items`)
		if err != nil {
			t.Fatalf("Error in SELECT with column alias: %v", err)
		}
		defer rows.Close()

		// Check column names
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Error getting columns: %v", err)
		}

		if len(cols) != 1 || cols[0] != "cost" {
			t.Fatalf("Expected column name 'cost', got: %v", cols)
		}

		// Count rows and check values
		var count int
		for rows.Next() {
			var price int
			if err := rows.Scan(&price); err != nil {
				t.Fatalf("Error scanning row: %v", err)
			}
			t.Logf("DEBUG: Row %d, price=%d", count+1, price)
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	// Add a test for a very simple WHERE clause using column alias
	t.Run("BasicWhereFilter", func(t *testing.T) {
		// Normal WHERE with no alias
		rows, err := db.Query(`SELECT * FROM basic_alias_test_items WHERE price > 100`)
		if err != nil {
			t.Fatalf("Error in simple WHERE: %v", err)
		}
		defer rows.Close()

		// Count rows
		count := 0
		for rows.Next() {
			count++
		}
		if count != 1 {
			t.Fatalf("Expected 1 row with price > 100, got %d", count)
		}
	})

	t.Run("AliasInWHERE", func(t *testing.T) {
		// Column alias in WHERE should now work
		// Try with a column alias in the WHERE clause
		rows, err := db.Query(`SELECT price AS cost FROM basic_alias_test_items WHERE cost > 100`)
		if err != nil {
			t.Fatalf("Error in WHERE with column alias: %v", err)
		}
		defer rows.Close()

		// Count rows
		count := 0
		for rows.Next() {
			count++
		}
		if count != 1 {
			t.Fatalf("Expected 1 row with cost > 100, got %d", count)
		}
	})
}
