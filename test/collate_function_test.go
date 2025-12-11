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

func TestCollateFunction(t *testing.T) {
	// Connect to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE collate_test (
			id INTEGER,
			text_value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO collate_test (id, text_value) VALUES 
		(1, 'Apple'),
		(2, 'apple'),
		(3, 'APPLE'),
		(4, 'Banana'),
		(5, 'banana'),
		(6, 'BANANA'),
		(7, 'Café'),
		(8, 'cafe'),
		(9, 'CAFE'),
		(10, 'Nação'),
		(11, 'nacao'),
		(12, 'NACAO')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test BINARY collation (case-sensitive)
	t.Run("Binary collation", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, text_value 
			FROM collate_test 
			WHERE COLLATE(text_value, 'BINARY') = 'Apple'
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to query with BINARY collation: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			if id != 1 || value != "Apple" {
				t.Errorf("Expected id=1 and value='Apple', got id=%d and value='%s'", id, value)
			}
			count++
		}
		if count != 1 {
			t.Errorf("Expected 1 row from BINARY collation, got %d", count)
		}
	})

	// Test NOCASE collation (case-insensitive)
	t.Run("Case-insensitive collation", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, text_value 
			FROM collate_test 
			WHERE COLLATE(text_value, 'NOCASE') = COLLATE('apple', 'NOCASE')
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to query with NOCASE collation: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			// Should match Apple, apple, and APPLE
			if id < 1 || id > 3 {
				t.Errorf("Unexpected id: %d", id)
			}
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 rows from NOCASE collation, got %d", count)
		}
	})

	// Test NOACCENT collation (accent-insensitive)
	t.Run("Accent-insensitive collation", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, text_value 
			FROM collate_test 
			WHERE COLLATE(LOWER(text_value), 'NOACCENT') = COLLATE('cafe', 'NOACCENT')
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to query with NOACCENT collation: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			// Should match café, cafe, CAFE
			if id < 7 || id > 9 {
				t.Errorf("Unexpected id: %d with value %s", id, value)
			}
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 rows from NOACCENT collation, got %d", count)
		}
	})

	// Test combined NOCASE and NOACCENT
	t.Run("Combined case and accent-insensitive collation", func(t *testing.T) {
		// We need to combine both collation types
		rows, err := db.Query(`
			SELECT id, text_value 
			FROM collate_test 
			WHERE COLLATE(COLLATE(text_value, 'NOCASE'), 'NOACCENT') = COLLATE(COLLATE('nação', 'NOCASE'), 'NOACCENT')
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to query with combined collation: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			// Should match Nação, nação, nacao, Nacao, NACAO, NAÇÃO
			if id < 10 || id > 12 {
				t.Errorf("Unexpected id: %d with value %s", id, value)
			}
			count++
		}
		if count != 3 {
			t.Errorf("Expected 3 rows from combined collation, got %d", count)
		}
	})

	// Test ordering with collation
	t.Run("Ordering with COLLATE", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, text_value 
			FROM collate_test 
			ORDER BY COLLATE(text_value, 'NOCASE')
			LIMIT 6
		`)
		if err != nil {
			t.Fatalf("Failed to query with ORDER BY COLLATE: %v", err)
		}
		defer rows.Close()

		// First 6 should be all forms of "Apple" and "Banana", in some order
		appleCount := 0
		bananaCount := 0

		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			// Check the value, ignoring case
			lowerValue := strings.ToLower(value)
			if lowerValue == "apple" {
				appleCount++
			} else if lowerValue == "banana" {
				bananaCount++
			} else {
				t.Errorf("Unexpected value: %s", value)
			}
		}

		if appleCount != 3 {
			t.Errorf("Expected 3 'Apple' entries, got %d", appleCount)
		}
		if bananaCount != 3 {
			t.Errorf("Expected 3 'Banana' entries, got %d", bananaCount)
		}
	})

	// Test with NULL values
	t.Run("NULL handling", func(t *testing.T) {
		// Insert NULL value
		_, err = db.Exec(`INSERT INTO collate_test (id, text_value) VALUES (13, NULL)`)
		if err != nil {
			t.Fatalf("Failed to insert NULL value: %v", err)
		}

		// Just check if the function can process NULL without crashing
		rows, err := db.Query(`
			SELECT COLLATE(text_value, 'BINARY') 
			FROM collate_test 
			WHERE id = 13
		`)
		if err != nil {
			t.Fatalf("Failed to query NULL value: %v", err)
		}
		defer rows.Close()

		// The test passes if we can execute the query successfully
		t.Log("COLLATE function handled NULL input without errors")

		// Just for observation purposes, check what's returned
		if rows.Next() {
			var collatedValue sql.NullString
			if err := rows.Scan(&collatedValue); err != nil {
				t.Logf("Scan error: %v", err)
			} else {
				t.Logf("collatedValue valid: %v", collatedValue.Valid)
			}
		}
	})
}
