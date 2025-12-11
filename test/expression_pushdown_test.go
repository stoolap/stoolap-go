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
	"context"
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import the stoolap SQL driver
)

func TestExpressionPushdown(t *testing.T) {
	// Create an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	createTableSQL := `CREATE TABLE test_products (
		id INTEGER,
		name TEXT,
		category TEXT,
		price FLOAT,
		in_stock BOOLEAN,
		tags TEXT,
		supply_date TIMESTAMP
	)`
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	insertRows := []string{
		`INSERT INTO test_products VALUES (1, 'Laptop', 'Electronics', 1200.00, true, 'premium,tech', '2023-01-15')`,
		`INSERT INTO test_products VALUES (2, 'Smartphone', 'Electronics', 800.00, true, 'mobile,tech', '2023-02-20')`,
		`INSERT INTO test_products VALUES (3, 'Headphones', 'Electronics', 150.00, true, 'audio,tech', '2023-03-10')`,
		`INSERT INTO test_products VALUES (4, 'T-shirt', 'Clothing', 25.00, true, 'casual,cotton', '2023-01-25')`,
		`INSERT INTO test_products VALUES (5, 'Jeans', 'Clothing', 50.00, false, 'denim,casual', NULL)`,
		`INSERT INTO test_products VALUES (6, 'Sneakers', 'Footwear', 80.00, true, 'casual,sports', '2023-02-05')`,
		`INSERT INTO test_products VALUES (7, 'Boots', 'Footwear', 120.00, false, 'winter,leather', NULL)`,
		`INSERT INTO test_products VALUES (8, 'Desk', 'Furniture', 250.00, true, 'office,wood', '2023-03-25')`,
		`INSERT INTO test_products VALUES (9, 'Chair', 'Furniture', 150.00, true, 'office,comfort', '2023-03-25')`,
		`INSERT INTO test_products VALUES (10, NULL, NULL, NULL, NULL, NULL, NULL)`,
	}

	// Insert test data
	for _, query := range insertRows {
		_, err := db.ExecContext(context.Background(), query)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Run test suite for NOT expressions
	t.Run("NOT Expressions", func(t *testing.T) {
		testNOTExpressions(t, db)
	})

	// Run test suite for IN expressions
	t.Run("IN Expressions", func(t *testing.T) {
		testINExpressions(t, db)
	})

	// Run test suite for NOT IN expressions
	t.Run("NOT IN Expressions", func(t *testing.T) {
		testNOTINExpressions(t, db)
	})

	// Run test suite for parameter binding in expressions
	t.Run("Parameter Binding", func(t *testing.T) {
		testParameterBindingInExpressions(t, db)
	})
}

func testNOTExpressions(t *testing.T, db *sql.DB) {
	tests := []struct {
		name     string
		query    string
		expected int // Expected number of rows
	}{
		{
			name:     "NOT with equality",
			query:    "SELECT * FROM test_products WHERE NOT category = 'Electronics'",
			expected: 7, // All non-Electronics categories (including NULL)
		},
		{
			name:     "NOT with inequality",
			query:    "SELECT * FROM test_products WHERE NOT price > 100",
			expected: 4, // Price <= 100 or NULL
		},
		{
			name:     "NOT with AND condition",
			query:    "SELECT * FROM test_products WHERE NOT (category = 'Electronics' AND price > 500)",
			expected: 8, // Not (Electronics AND expensive)
		},
		{
			name:     "NOT with OR condition",
			query:    "SELECT * FROM test_products WHERE NOT (category = 'Electronics' OR category = 'Clothing')",
			expected: 5, // Not Electronics or Clothing
		},
		{
			name:     "NOT with NULL check",
			query:    "SELECT * FROM test_products WHERE NOT (category IS NULL)",
			expected: 9, // All rows with non-NULL category
		},
		// Double NOT isn't supported by the parser
		// {
		// 	name:     "Double NOT",
		// 	query:    "SELECT * FROM test_products WHERE NOT NOT category = 'Electronics'",
		// 	expected: 3, // Same as category = 'Electronics'
		// },
		{
			name:     "NOT with composite condition",
			query:    "SELECT * FROM test_products WHERE NOT (category = 'Electronics' AND price > 500 AND in_stock = true)",
			expected: 8, // Not (expensive Electronics in stock)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Count rows in the result
			rowCount := 0
			for result.Next() {
				rowCount++
			}

			if rowCount != tc.expected {
				t.Errorf("Expected %d rows, got %d rows for query: %s", tc.expected, rowCount, tc.query)
			}
		})
	}
}

func testINExpressions(t *testing.T, db *sql.DB) {
	tests := []struct {
		name     string
		query    string
		expected int // Expected number of rows
	}{
		{
			name:     "Simple IN with multiple values",
			query:    "SELECT * FROM test_products WHERE category IN ('Electronics', 'Clothing')",
			expected: 5, // Electronics (3) + Clothing (2)
		},
		{
			name:     "IN with single value",
			query:    "SELECT * FROM test_products WHERE category IN ('Furniture')",
			expected: 2, // Same as category = 'Furniture'
		},
		{
			name:     "IN with numeric values",
			query:    "SELECT * FROM test_products WHERE price IN (50.00, 150.00, 250.00)",
			expected: 4, // Products with these exact prices
		},
		{
			name:     "IN with no matching values",
			query:    "SELECT * FROM test_products WHERE category IN ('Unknown', 'NonExistent')",
			expected: 0, // No matches
		},
		{
			name:     "IN with NULL in list",
			query:    "SELECT * FROM test_products WHERE category IN ('Electronics', NULL)",
			expected: 3, // Only Electronics, NULL in list doesn't match NULL values
		},
		{
			name:     "IN with NULL column value",
			query:    "SELECT * FROM test_products WHERE NULL IN ('Electronics', 'Clothing')",
			expected: 0, // NULL comparison always returns false
		},
		{
			name:     "IN with mixed data types",
			query:    "SELECT * FROM test_products WHERE price IN (50, 150, 250)",
			expected: 4, // Should handle type conversion
		},
		{
			name:     "IN combined with other conditions",
			query:    "SELECT * FROM test_products WHERE category IN ('Electronics', 'Clothing') AND in_stock = true",
			expected: 4, // In-stock Electronics and Clothing
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Count rows in the result
			rowCount := 0
			for result.Next() {
				rowCount++
			}

			if rowCount != tc.expected {
				t.Errorf("Expected %d rows, got %d rows for query: %s", tc.expected, rowCount, tc.query)
			}
		})
	}
}

func testNOTINExpressions(t *testing.T, db *sql.DB) {
	tests := []struct {
		name     string
		query    string
		expected int // Expected number of rows
	}{
		{
			name:     "Simple NOT IN",
			query:    "SELECT * FROM test_products WHERE category NOT IN ('Electronics', 'Clothing')",
			expected: 5, // All categories except Electronics and Clothing (including NULL)
		},
		{
			name:     "NOT IN with NULL values",
			query:    "SELECT * FROM test_products WHERE category NOT IN ('Electronics', NULL)",
			expected: 7, // Implementation differs from standard SQL here
		},
		{
			name:     "NOT IN combined with AND",
			query:    "SELECT * FROM test_products WHERE category NOT IN ('Electronics', 'Clothing') AND in_stock = true",
			expected: 3, // In-stock products not in Electronics or Clothing
		},
		{
			name:     "NOT IN combined with OR",
			query:    "SELECT * FROM test_products WHERE category NOT IN ('Electronics', 'Clothing') OR price > 200",
			expected: 7, // Not (Electronics or Clothing) OR expensive products
		},
		{
			name:     "NOT IN with numeric values",
			query:    "SELECT * FROM test_products WHERE id NOT IN (1, 2, 3, 4, 5)",
			expected: 5, // IDs 6-10
		},
		{
			name:     "NOT with IN expression",
			query:    "SELECT * FROM test_products WHERE NOT (category IN ('Electronics', 'Clothing'))",
			expected: 5, // Same as category NOT IN ('Electronics', 'Clothing')
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Count rows in the result
			rowCount := 0
			for result.Next() {
				rowCount++
			}

			if rowCount != tc.expected {
				t.Errorf("Expected %d rows, got %d rows for query: %s", tc.expected, rowCount, tc.query)
			}
		})
	}
}

func testParameterBindingInExpressions(t *testing.T, db *sql.DB) {
	// Create a new table for parameter binding tests to avoid conflicts
	setupSQL := []string{
		`CREATE TABLE test_params (
			id INTEGER,
			name TEXT,
			category TEXT,
			price FLOAT,
			in_stock BOOLEAN,
			tags TEXT,
			supply_date TIMESTAMP
		)`,
		`INSERT INTO test_params VALUES (1, 'Laptop', 'Electronics', 1200.00, true, 'premium,tech', '2023-01-15')`,
		`INSERT INTO test_params VALUES (2, 'Smartphone', 'Electronics', 800.00, true, 'mobile,tech', '2023-02-20')`,
		`INSERT INTO test_params VALUES (3, 'Headphones', 'Electronics', 150.00, true, 'audio,tech', '2023-03-10')`,
		`INSERT INTO test_params VALUES (4, 'T-shirt', 'Clothing', 25.00, true, 'casual,cotton', '2023-01-25')`,
		`INSERT INTO test_params VALUES (5, 'Jeans', 'Clothing', 50.00, false, 'denim,casual', NULL)`,
		`INSERT INTO test_params VALUES (6, 'Sneakers', 'Footwear', 80.00, true, 'casual,sports', '2023-02-05')`,
		`INSERT INTO test_params VALUES (7, 'Boots', 'Footwear', 120.00, false, 'winter,leather', NULL)`,
		`INSERT INTO test_params VALUES (8, 'Desk', 'Furniture', 250.00, true, 'office,wood', '2023-03-25')`,
		`INSERT INTO test_params VALUES (9, 'Chair', 'Furniture', 150.00, true, 'office,comfort', '2023-03-25')`,
		`INSERT INTO test_params VALUES (10, NULL, NULL, NULL, NULL, NULL, NULL)`,
	}

	for _, q := range setupSQL {
		_, err := db.Exec(q)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}
	}

	// Test IN with parameters
	t.Run("IN with parameters", func(t *testing.T) {
		// Prepare a statement with IN clause and parameters
		stmt, err := db.Prepare("SELECT * FROM test_params WHERE category IN (?, ?, ?)")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		// Execute with parameters
		rows, err := stmt.Query("Electronics", "Clothing", "Furniture")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		// Count rows
		rowCount := 0
		for rows.Next() {
			rowCount++
		}

		expected := 7 // Electronics (3) + Clothing (2) + Furniture (2)
		if rowCount != expected {
			t.Errorf("Expected %d rows, got %d rows for IN query with parameters", expected, rowCount)
		}
	})

	// Test NOT with parameters
	t.Run("NOT with parameters", func(t *testing.T) {
		// Prepare a statement with NOT and parameter
		stmt, err := db.Prepare("SELECT * FROM test_params WHERE NOT (category = ?)")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		// Execute with parameter
		rows, err := stmt.Query("Electronics")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		// Count rows
		rowCount := 0
		for rows.Next() {
			rowCount++
		}

		expected := 7 // All non-Electronics (including NULL)
		if rowCount != expected {
			t.Errorf("Expected %d rows, got %d rows for NOT query with parameter", expected, rowCount)
		}
	})

	// Test IN with NULL parameter
	t.Run("IN with NULL parameter", func(t *testing.T) {
		// Prepare a statement with IN clause including NULL
		stmt, err := db.Prepare("SELECT * FROM test_params WHERE category IN (?, ?)")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		// Execute with NULL parameter
		var nullParam interface{} = nil
		rows, err := stmt.Query(nullParam, "Furniture")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		// Count rows
		rowCount := 0
		for rows.Next() {
			rowCount++
		}

		expected := 2 // Only Furniture (NULL in list doesn't match NULL values)
		if rowCount != expected {
			t.Errorf("Expected %d rows, got %d rows for IN query with NULL parameter", expected, rowCount)
		}
	})
}
