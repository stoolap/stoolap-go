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

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestFilterPushdown tests that filters are properly pushed down to the storage engine
func TestFilterPushdown(t *testing.T) {
	// Create a new database connection using database/sql API
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection parameters
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create a test table with data
	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE test_pushdown (
			id INTEGER,
			name TEXT,
			age INTEGER,
			active BOOLEAN,
			salary FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO test_pushdown VALUES
		(1, 'Alice', 30, true, 60000.0),
		(2, 'Bob', 25, true, 55000.0),
		(3, 'Charlie', 35, false, 70000.0),
		(4, 'Dave', 40, true, 80000.0),
		(5, 'Eve', 28, false, 65000.0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test cases for different filter conditions
	testCases := []struct {
		name     string
		query    string
		expected int // expected number of rows
	}{
		{
			name:     "Simple equality filter",
			query:    "SELECT * FROM test_pushdown WHERE id = 3",
			expected: 1,
		},
		{
			name:     "Greater than filter",
			query:    "SELECT * FROM test_pushdown WHERE age > 30",
			expected: 2,
		},
		{
			name:     "AND condition",
			query:    "SELECT * FROM test_pushdown WHERE active = true AND salary > 60000.0",
			expected: 1,
		},
		{
			name:     "IN condition",
			query:    "SELECT * FROM test_pushdown WHERE id IN (1, 3, 5)",
			expected: 3,
		},
		{
			name:     "LIKE condition",
			query:    "SELECT * FROM test_pushdown WHERE name LIKE 'A%'",
			expected: 1,
		},
		{
			name:     "IS NULL condition",
			query:    "SELECT * FROM test_pushdown WHERE age IS NOT NULL",
			expected: 5,
		},
		{
			name:     "BETWEEN condition",
			query:    "SELECT * FROM test_pushdown WHERE age BETWEEN 25 AND 35",
			expected: 4,
		},
		{
			name:     "Complex condition",
			query:    "SELECT * FROM test_pushdown WHERE (age > 30 OR salary > 60000.0) AND active = true",
			expected: 1,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query using database/sql API
			rows, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			// Count the rows
			var count int
			for rows.Next() {
				count++
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("Error iterating over rows: %v", err)
			}

			// Verify the result
			if count != tc.expected {
				t.Errorf("Expected %d rows, got %d", tc.expected, count)
			}
		})
	}

	// Create an index to test index-based filter pushdown
	_, err = db.ExecContext(context.Background(), "CREATE INDEX idx_age ON test_pushdown(age)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Test index-based filter pushdown
	indexTestCases := []struct {
		name     string
		query    string
		expected int // expected number of rows
	}{
		{
			name:     "Indexed equality filter",
			query:    "SELECT * FROM test_pushdown WHERE age = 30",
			expected: 1,
		},
		{
			name:     "Indexed range filter",
			query:    "SELECT * FROM test_pushdown WHERE age > 30",
			expected: 2,
		},
		{
			name:     "Indexed BETWEEN filter",
			query:    "SELECT * FROM test_pushdown WHERE age BETWEEN 28 AND 35",
			expected: 3,
		},
	}

	// Run index-based test cases
	for _, tc := range indexTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query using database/sql API
			rows, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			// Count the rows
			var count int
			for rows.Next() {
				count++
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("Error iterating over rows: %v", err)
			}

			// Verify the result
			if count != tc.expected {
				t.Errorf("Expected %d rows, got %d", tc.expected, count)
			}
		})
	}

	// Clean up
	_, err = db.ExecContext(context.Background(), "DROP TABLE test_pushdown")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

// TestCountWithPushdown tests COUNT(*) queries with filter pushdown
func TestCountWithPushdown(t *testing.T) {
	// Create a new database connection using database/sql API
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection parameters
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create a test table with data
	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE test_count_pushdown (
			id INTEGER,
			name TEXT,
			active BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO test_count_pushdown VALUES
		(1, 'Alice', true),
		(2, 'Bob', true),
		(3, 'Charlie', false),
		(4, 'Dave', true),
		(5, 'Eve', false)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test cases for COUNT(*) with different filters
	testCases := []struct {
		name     string
		query    string
		expected int64 // expected count
	}{
		{
			name:     "COUNT(*) with no filter",
			query:    "SELECT COUNT(*) FROM test_count_pushdown",
			expected: 5,
		},
		{
			name:     "COUNT(*) with simple filter",
			query:    "SELECT COUNT(*) FROM test_count_pushdown WHERE active = true",
			expected: 3,
		},
		{
			name:     "COUNT(*) with complex filter",
			query:    "SELECT COUNT(*) FROM test_count_pushdown WHERE id > 2 AND active = false",
			expected: 2,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query using database/sql API's QueryRow for single value results
			var count int64
			err := db.QueryRowContext(context.Background(), tc.query).Scan(&count)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Verify the result
			if count != tc.expected {
				t.Errorf("Expected count %d, got %d", tc.expected, count)
			}
		})
	}

	// Create an index to test index-based filter pushdown
	_, err = db.ExecContext(context.Background(), "CREATE INDEX idx_active ON test_count_pushdown(active)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Test index-based filter pushdown with COUNT(*)
	indexTestCases := []struct {
		name     string
		query    string
		expected int64 // expected count
	}{
		{
			name:     "COUNT(*) with indexed filter",
			query:    "SELECT COUNT(*) FROM test_count_pushdown WHERE active = false",
			expected: 2,
		},
	}

	// Run index-based test cases
	for _, tc := range indexTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query using database/sql API's QueryRow
			var count int64
			err := db.QueryRowContext(context.Background(), tc.query).Scan(&count)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Verify the result
			if count != tc.expected {
				t.Errorf("Expected count %d, got %d", tc.expected, count)
			}
		})
	}

	// Clean up
	_, err = db.ExecContext(context.Background(), "DROP TABLE test_count_pushdown")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}

// TestComplexFilterPushdown tests the advanced pushdown optimization for complex AND/OR expressions
func TestComplexFilterPushdown(t *testing.T) {
	// Create a new database connection using database/sql API
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection parameters
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Create a test table with data
	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE test_complex_pushdown (
			id INTEGER,
			category TEXT,
			price FLOAT,
			in_stock BOOLEAN,
			rating INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.ExecContext(context.Background(), `
		INSERT INTO test_complex_pushdown VALUES
		(1, 'Electronics', 499.99, true, 4),
		(2, 'Books', 29.99, true, 5),
		(3, 'Electronics', 1299.99, false, 4),
		(4, 'Clothing', 49.99, true, 3),
		(5, 'Books', 19.99, false, 5),
		(6, 'Electronics', 799.99, true, 2),
		(7, 'Clothing', 89.99, true, 4),
		(8, 'Books', 9.99, true, 3),
		(9, 'Electronics', 999.99, false, 5),
		(10, 'Clothing', 129.99, false, 2)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create indexes for testing
	_, err = db.ExecContext(context.Background(), "CREATE INDEX idx_category ON test_complex_pushdown(category)")
	if err != nil {
		t.Fatalf("Failed to create category index: %v", err)
	}

	_, err = db.ExecContext(context.Background(), "CREATE INDEX idx_price ON test_complex_pushdown(price)")
	if err != nil {
		t.Fatalf("Failed to create price index: %v", err)
	}

	_, err = db.ExecContext(context.Background(), "CREATE INDEX idx_in_stock ON test_complex_pushdown(in_stock)")
	if err != nil {
		t.Fatalf("Failed to create in_stock index: %v", err)
	}

	// Test cases for complex filter conditions
	testCases := []struct {
		name     string
		query    string
		expected int // expected number of rows
	}{
		{
			name:     "Complex AND with OR",
			query:    "SELECT * FROM test_complex_pushdown WHERE (category = 'Electronics' OR category = 'Books') AND in_stock = true",
			expected: 4, // Items 1, 2, 6, 8
		},
		{
			name:     "OR with multiple ANDs",
			query:    "SELECT * FROM test_complex_pushdown WHERE (category = 'Electronics' AND price > 500) OR (category = 'Books' AND rating = 5)",
			expected: 5, // Items 3, 5, 6, 9 and item 2 or 9
		},
		{
			name:     "Complex three-way OR with indexed columns",
			query:    "SELECT * FROM test_complex_pushdown WHERE category = 'Electronics' OR category = 'Books' OR in_stock = false",
			expected: 8, // All except items 4, 7
		},
		{
			name:     "Mixed conditions with range query",
			query:    "SELECT * FROM test_complex_pushdown WHERE (category = 'Clothing' OR price > 500) AND rating <= 4",
			expected: 5, // Items 3, 4, 6, 7, 10
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the query using database/sql API
			rows, err := db.QueryContext(context.Background(), tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			// Count the rows
			var count int
			for rows.Next() {
				count++
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("Error iterating over rows: %v", err)
			}

			// Verify the result
			if count != tc.expected {
				t.Errorf("Expected %d rows, got %d", tc.expected, count)
			}
		})
	}

	// Clean up
	_, err = db.ExecContext(context.Background(), "DROP TABLE test_complex_pushdown")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}
}
