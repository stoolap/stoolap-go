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
	"testing"

	"github.com/stoolap/stoolap-go/internal/sql"
	"github.com/stoolap/stoolap-go/internal/storage"

	// Import necessary packages to register factory functions
	_ "github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestWhereClause(t *testing.T) {
	// Get the engine factory
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatalf("Failed to get db engine factory")
	}

	// Create the engine with the connection string
	engine, err := factory.Create("memory://")
	if err != nil {
		t.Fatalf("Failed to create db engine: %v", err)
	}

	// Open the engine
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a SQL executor
	executor := sql.NewExecutor(engine)

	// Create a test table
	createTableSQL := `CREATE TABLE products (
		id INTEGER,
		name TEXT,
		category TEXT,
		price FLOAT,
		in_stock BOOLEAN,
		tags TEXT
	)`
	_, err = executor.Execute(context.Background(), nil, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	insertRows := []string{
		// 0 rows with price < 100: Products 4, 5, 6 (25, 50, 80)
		// 3 rows with price <= 150: Products 3, 9, 7 (150, 150, 120)
		`INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 1200.00, true, 'portable,computer')`,
		`INSERT INTO products VALUES (2, 'Smartphone', 'Electronics', 800.00, true, 'mobile,phone')`,
		`INSERT INTO products VALUES (3, 'Headphones', 'Electronics', 150.00, true, 'audio,accessory')`,
		`INSERT INTO products VALUES (4, 'T-shirt', 'Clothing', 25.00, true, 'apparel,cotton')`,
		`INSERT INTO products VALUES (5, 'Jeans', 'Clothing', 50.00, false, 'apparel,denim')`,
		`INSERT INTO products VALUES (6, 'Sneakers', 'Footwear', 80.00, true, 'shoes,casual')`,
		`INSERT INTO products VALUES (7, 'Boots', 'Footwear', 120.00, false, 'shoes,winter')`,
		`INSERT INTO products VALUES (8, 'Desk', 'Furniture', 250.00, true, 'home,office')`,
		`INSERT INTO products VALUES (9, 'Chair', 'Furniture', 150.00, true, 'home,office')`,
		`INSERT INTO products VALUES (10, 'Bookshelf', 'Furniture', 180.00, false, 'home,storage')`,
	}

	// Insert test data
	for _, query := range insertRows {
		_, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Test various WHERE clause scenarios
	tests := []struct {
		name     string
		query    string
		expected int // Expected number of rows
	}{
		// Basic comparisons
		{
			name:     "Equality operator (=)",
			query:    "SELECT * FROM products WHERE category = 'Electronics'",
			expected: 3,
		},
		{
			name:     "Inequality operator (!=)",
			query:    "SELECT * FROM products WHERE category != 'Electronics'",
			expected: 7,
		},
		{
			name:     "Greater than operator (>)",
			query:    "SELECT * FROM products WHERE price > 200",
			expected: 3,
		},
		{
			name:     "Greater than or equal operator (>=)",
			query:    "SELECT * FROM products WHERE price >= 150",
			expected: 6,
		},
		{
			name:     "Less than operator (<)",
			query:    "SELECT * FROM products WHERE price < 100",
			expected: 3, // T-shirt (25), Jeans (50), Sneakers (80)
		},
		{
			name:     "Less than or equal operator (<=)",
			query:    "SELECT * FROM products WHERE price <= 150",
			expected: 6, // Above 3 plus: Headphones (150), Boots (120), Chair (150)
		},

		// Boolean operators
		{
			name:     "Boolean TRUE comparison",
			query:    "SELECT * FROM products WHERE in_stock = true",
			expected: 7,
		},
		{
			name:     "Boolean FALSE comparison",
			query:    "SELECT * FROM products WHERE in_stock = false",
			expected: 3,
		},

		// Logical operators
		{
			name:     "AND operator",
			query:    "SELECT * FROM products WHERE category = 'Electronics' AND price > 500",
			expected: 2,
		},
		{
			name:     "OR operator",
			query:    "SELECT * FROM products WHERE category = 'Furniture' OR category = 'Footwear'",
			expected: 5,
		},
		{
			name:     "Complex AND/OR combination",
			query:    "SELECT * FROM products WHERE (category = 'Electronics' AND price > 1000) OR (category = 'Furniture' AND price < 200)",
			expected: 3, // Laptop, Chair, Bookshelf
		},

		// LIKE operator
		{
			name:     "LIKE operator - starts with",
			query:    "SELECT * FROM products WHERE name LIKE 'B%'",
			expected: 2,
		},
		{
			name:     "LIKE operator - contains",
			query:    "SELECT * FROM products WHERE name LIKE '%o%'",
			expected: 5,
		},
		{
			name:     "LIKE operator - ends with",
			query:    "SELECT * FROM products WHERE name LIKE '%s'",
			expected: 4, // Headphones, Jeans, Boots, Sneakers
		},
		{
			name:     "LIKE operator - exact character count",
			query:    "SELECT * FROM products WHERE name LIKE 'Des_'",
			expected: 1,
		},

		// NOT operator
		{
			name:     "NOT operator",
			query:    "SELECT * FROM products WHERE NOT category = 'Electronics'",
			expected: 7,
		},
		{
			name:     "NOT with other conditions",
			query:    "SELECT * FROM products WHERE NOT in_stock AND price > 100",
			expected: 2, // Boots, Bookshelf
		},

		// IN operator
		{
			name:     "IN operator",
			query:    "SELECT * FROM products WHERE category IN ('Electronics', 'Furniture')",
			expected: 6,
		},
		{
			name:     "NOT IN operator",
			query:    "SELECT * FROM products WHERE category NOT IN ('Electronics', 'Furniture')",
			expected: 4,
		},

		// ORDER BY and LIMIT
		{
			name:     "ORDER BY ASC with LIMIT",
			query:    "SELECT * FROM products ORDER BY price ASC LIMIT 3",
			expected: 3,
		},
		{
			name:     "ORDER BY DESC with LIMIT",
			query:    "SELECT * FROM products ORDER BY price DESC LIMIT 2",
			expected: 2,
		},
		{
			name:     "WHERE with ORDER BY and LIMIT",
			query:    "SELECT * FROM products WHERE price > 100 ORDER BY price DESC LIMIT 3",
			expected: 3,
		},

		// Combined complex queries
		{
			name:     "Complex query 1",
			query:    "SELECT * FROM products WHERE (category = 'Electronics' OR category = 'Furniture') AND price > 200 AND in_stock = true",
			expected: 3, // Laptop, Smartphone, Desk
		},
		{
			name:     "Complex query 2",
			query:    "SELECT * FROM products WHERE name LIKE '%e%' AND price BETWEEN 100 AND 300 AND category != 'Clothing'",
			expected: 3,
		},
		{
			name:     "Complex query 3",
			query:    "SELECT * FROM products WHERE (price < 100 OR price > 1000) AND in_stock = true ORDER BY price DESC LIMIT 4",
			expected: 3, // Laptop, T-shirt, Sneakers
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), nil, tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Count rows in the result
			rowCount := 0
			for result.Next() {
				rowCount++
			}

			result.Close()

			if rowCount != tc.expected {
				t.Errorf("Expected %d rows, got %d rows", tc.expected, rowCount)
			}
		})
	}
}
