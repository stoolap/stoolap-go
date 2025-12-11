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

// TestMultiColumnIndexQuery tests SQL queries that use multi-column indexes
func TestMultiColumnIndexQuery(t *testing.T) {
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
			price FLOAT NOT NULL,
			quantity INTEGER NOT NULL
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

	// Create an index for range queries
	_, err = db.Exec(`
		CREATE INDEX price_quantity_idx ON products (price, quantity)
	`)
	if err != nil {
		t.Fatalf("Failed to create index for range queries: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price, quantity) VALUES 
		(1, 'Apple', 'Fruit', 1.99, 100),
		(2, 'Banana', 'Fruit', 0.99, 150),
		(3, 'Carrot', 'Vegetable', 0.75, 200),
		(4, 'Potato', 'Vegetable', 0.50, 300),
		(5, 'Apple', 'Vegetable', 2.50, 50),
		(6, 'Kiwi', 'Fruit', 1.50, 75),
		(7, 'Orange', 'Fruit', 1.25, 125),
		(8, 'Grape', 'Fruit', 2.99, 80),
		(9, 'Lettuce', 'Vegetable', 1.75, 60),
		(10, 'Tomato', 'Vegetable', 1.99, 90)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test 1: Query with exact match on both columns (should use index)
	t.Run("ExactMatchBothColumns", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, name, category, price FROM products 
			WHERE name = 'Apple' AND category = 'Fruit'
		`)
		if err != nil {
			t.Fatalf("Failed to query with index: %v", err)
		}
		defer rows.Close()

		// Should return only id 1
		count := 0
		for rows.Next() {
			var id int
			var name, category string
			var price float64
			if err := rows.Scan(&id, &name, &category, &price); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
			if id != 1 || name != "Apple" || category != "Fruit" || price != 1.99 {
				t.Errorf("Unexpected row: id=%d, name=%s, category=%s, price=%f", id, name, category, price)
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 row for exact match, got %d", count)
		}
	})

	// Test 2: Query with match on first column only (should use index partially)
	t.Run("MatchFirstColumnOnly", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, name, category, price FROM products 
			WHERE name = 'Apple'
		`)
		if err != nil {
			t.Fatalf("Failed to query with partial index: %v", err)
		}
		defer rows.Close()

		// Should return ids 1 and 5 (both 'Apple' products)
		ids := make(map[int]bool)
		count := 0
		for rows.Next() {
			var id int
			var name, category string
			var price float64
			if err := rows.Scan(&id, &name, &category, &price); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
			ids[id] = true
			if name != "Apple" {
				t.Errorf("Expected name 'Apple', got '%s'", name)
			}
		}
		if count != 2 {
			t.Errorf("Expected 2 rows for first column match, got %d", count)
		}
		if !ids[1] || !ids[5] {
			t.Errorf("Expected IDs 1 and 5, got %v", ids)
		}
	})

	// Test 3: Range query on first column of multi-column index
	t.Run("RangeQueryFirstColumn", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, price, quantity FROM products 
			WHERE price BETWEEN 1.00 AND 2.00
		`)
		if err != nil {
			t.Fatalf("Failed to query with range on indexed column: %v", err)
		}
		defer rows.Close()

		// Should return all products with price between 1.00 and 2.00
		count := 0
		ids := make(map[int]bool)

		for rows.Next() {
			var id int
			var price float64
			var quantity int
			if err := rows.Scan(&id, &price, &quantity); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			count++
			ids[id] = true

			// Verify price is in range
			if price < 1.00 || price > 2.00 {
				t.Errorf("Price %f is outside expected range 1.00-2.00", price)
			}
		}

		// Should match IDs 1, 6, 7, 9, 10
		expectedIds := []int{1, 6, 7, 9, 10}
		if count != len(expectedIds) {
			t.Errorf("Expected %d rows for price range query, got %d", len(expectedIds), count)
		}

		for _, id := range expectedIds {
			if !ids[id] {
				t.Errorf("Expected ID %d in result set, but it was missing", id)
			}
		}
	})

	// Test 4: Complex query with range on first column and filter on second column
	t.Run("RangeQueryWithSecondColumn", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, price, quantity FROM products 
			WHERE price BETWEEN 1.00 AND 3.00 AND quantity > 70
		`)
		if err != nil {
			t.Fatalf("Failed to query with combined index conditions: %v", err)
		}
		defer rows.Close()

		// Count and collect results
		count := 0
		ids := make(map[int]bool)

		for rows.Next() {
			var id int
			var price float64
			var quantity int
			if err := rows.Scan(&id, &price, &quantity); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			// Verify both conditions are met
			if price < 1.00 || price > 3.00 {
				t.Errorf("Price %f is outside expected range 1.00-3.00", price)
			}
			if quantity <= 70 {
				t.Errorf("Quantity %d is not greater than 70", quantity)
			}

			count++
			ids[id] = true
		}

		// Should match IDs 1, 6, 7, 8, 10
		expectedIds := []int{1, 6, 7, 8, 10}
		if count != len(expectedIds) {
			t.Errorf("Expected %d rows for complex query, got %d", len(expectedIds), count)
		}

		for _, id := range expectedIds {
			if !ids[id] {
				t.Errorf("Expected ID %d in result set, but it was missing", id)
			}
		}
	})

	// Test 5: Create unique multi-column index
	_, err = db.Exec(`
		CREATE UNIQUE INDEX unique_name_price_idx ON products (name, price)
	`)
	if err != nil {
		t.Fatalf("Failed to create unique multi-column index: %v", err)
	}

	// Test 6: Try to insert a record that would violate the unique constraint
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price, quantity) VALUES 
		(11, 'Apple', 'Other', 1.99, 200)
	`)
	if err == nil {
		t.Errorf("Expected unique constraint violation, but insert succeeded")
	}

	// Test 7: Insert a record that doesn't violate the unique constraint
	_, err = db.Exec(`
		INSERT INTO products (id, name, category, price, quantity) VALUES 
		(11, 'Apple', 'Other', 3.99, 200)
	`)
	if err != nil {
		t.Errorf("Insert that doesn't violate unique constraint failed: %v", err)
	}

	// Test 8: Order by query using the index
	t.Run("OrderByIndexedColumns", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT id, name, category FROM products 
			WHERE category = 'Fruit'
			ORDER BY name
		`)
		if err != nil {
			t.Fatalf("Failed to query with order by: %v", err)
		}
		defer rows.Close()

		// Expected ordered names
		expectedNames := []string{"Apple", "Banana", "Grape", "Kiwi", "Orange"}

		// Check the order of results
		actualNames := make([]string, 0)
		for rows.Next() {
			var id int
			var name, category string
			if err := rows.Scan(&id, &name, &category); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			actualNames = append(actualNames, name)

			if category != "Fruit" {
				t.Errorf("Expected category 'Fruit', got '%s'", category)
			}
		}

		// Verify we got correct number of results in expected order
		if len(actualNames) != len(expectedNames) {
			t.Errorf("Expected %d items, got %d", len(expectedNames), len(actualNames))
		}

		for i := 0; i < len(actualNames) && i < len(expectedNames); i++ {
			if actualNames[i] != expectedNames[i] {
				t.Errorf("Expected ordered name '%s' at position %d, got '%s'",
					expectedNames[i], i, actualNames[i])
			}
		}
	})
}
