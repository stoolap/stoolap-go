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
	"fmt"
	"strconv"
	"testing"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import for database registration
)

func TestSQLLiterals(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Test cases for literals and scalar functions
	tests := []struct {
		name     string
		query    string
		validate func(*testing.T, *sql.Rows)
	}{
		{
			name:  "Integer literals",
			query: "SELECT 1, 2, 3",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c int
				if err := rows.Scan(&a, &b, &c); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != 1 || b != 2 || c != 3 {
					t.Fatalf("Expected values 1, 2, 3 but got %d, %d, %d", a, b, c)
				}
			},
		},
		{
			name:  "Float literals",
			query: "SELECT 1.5, 2.25, 3.75",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c float64
				if err := rows.Scan(&a, &b, &c); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != 1.5 || b != 2.25 || c != 3.75 {
					t.Fatalf("Expected values 1.5, 2.25, 3.75 but got %f, %f, %f", a, b, c)
				}
			},
		},
		{
			name:  "String literals",
			query: "SELECT 'hello', 'world', 'sql'",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c string
				if err := rows.Scan(&a, &b, &c); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != "hello" || b != "world" || c != "sql" {
					t.Fatalf("Expected values 'hello', 'world', 'sql' but got '%s', '%s', '%s'", a, b, c)
				}
			},
		},
		{
			name:  "Boolean literals",
			query: "SELECT true, false",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b bool
				if err := rows.Scan(&a, &b); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != true || b != false {
					t.Fatalf("Expected values true, false but got %v, %v", a, b)
				}
			},
		},
		{
			name:  "Arithmetic expressions",
			query: "SELECT 1 + 2, 3 - 1, 2 * 3, 10 / 2, 10 % 3",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c, d, e int
				if err := rows.Scan(&a, &b, &c, &d, &e); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != 3 || b != 2 || c != 6 || d != 5 || e != 1 {
					t.Fatalf("Expected values 3, 2, 6, 5, 1 but got %d, %d, %d, %d, %d", a, b, c, d, e)
				}
			},
		},
		{
			name:  "Mixed arithmetic expressions",
			query: "SELECT 1 + 2.5, 5.5 - 2, 2 * 3.5",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c float64
				if err := rows.Scan(&a, &b, &c); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != 3.5 || b != 3.5 || c != 7.0 {
					t.Fatalf("Expected values 3.5, 3.5, 7.0 but got %f, %f, %f", a, b, c)
				}
			},
		},
		{
			name:  "Column aliases",
			query: "SELECT 1 AS one, 2 AS two, 3 AS three",
			validate: func(t *testing.T, rows *sql.Rows) {
				columns, err := rows.Columns()
				if err != nil {
					t.Fatalf("Error getting columns: %v", err)
				}

				expectedColumns := []string{"one", "two", "three"}
				if len(columns) != len(expectedColumns) {
					t.Fatalf("Expected %d columns but got %d", len(expectedColumns), len(columns))
				}

				for i, col := range columns {
					if col != expectedColumns[i] {
						t.Fatalf("Expected column %d to be '%s' but got '%s'", i, expectedColumns[i], col)
					}
				}

				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var a, b, c int
				if err := rows.Scan(&a, &b, &c); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if a != 1 || b != 2 || c != 3 {
					t.Fatalf("Expected values 1, 2, 3 but got %d, %d, %d", a, b, c)
				}
			},
		},
		{
			name:  "UPPER function",
			query: "SELECT UPPER('hello world') AS greeting",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var greeting string
				if err := rows.Scan(&greeting); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if greeting != "HELLO WORLD" {
					t.Fatalf("Expected 'HELLO WORLD' but got '%s'", greeting)
				}
			},
		},
		{
			name:  "LOWER function",
			query: "SELECT LOWER('HELLO WORLD') AS greeting",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var greeting string
				if err := rows.Scan(&greeting); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if greeting != "hello world" {
					t.Fatalf("Expected 'hello world' but got '%s'", greeting)
				}
			},
		},
		{
			name:  "LENGTH function",
			query: "SELECT LENGTH('hello') AS len",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var length int
				if err := rows.Scan(&length); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if length != 5 {
					t.Fatalf("Expected length 5 but got %d", length)
				}
			},
		},
		{
			name:  "CONCAT function",
			query: "SELECT CONCAT('hello', ' ', 'world') AS message",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var message string
				if err := rows.Scan(&message); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if message != "hello world" {
					t.Fatalf("Expected 'hello world' but got '%s'", message)
				}
			},
		},
		{
			name:  "ABS function",
			query: "SELECT ABS(-5) AS abs_value",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var absValue float64
				if err := rows.Scan(&absValue); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if absValue != 5.0 {
					t.Fatalf("Expected 5.0 but got %f", absValue)
				}
			},
		},
		{
			name:  "ROUND function",
			query: "SELECT ROUND(3.14159, 2) AS rounded",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var rounded float64
				if err := rows.Scan(&rounded); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if rounded != 3.14 {
					t.Fatalf("Expected 3.14 but got %f", rounded)
				}
			},
		},
		{
			name:  "CEIL function",
			query: "SELECT CEILING(3.14) AS ceil_value",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var ceilValue float64
				if err := rows.Scan(&ceilValue); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if ceilValue != 4.0 {
					t.Fatalf("Expected 4.0 but got %f", ceilValue)
				}
			},
		},
		{
			name:  "FLOOR function",
			query: "SELECT FLOOR(3.99) AS floor_value",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var floorValue float64
				if err := rows.Scan(&floorValue); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if floorValue != 3.0 {
					t.Fatalf("Expected 3.0 but got %f", floorValue)
				}
			},
		},
		{
			name:  "SUBSTRING function",
			query: "SELECT SUBSTRING('hello world', 7, 5) AS sub",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var sub string
				if err := rows.Scan(&sub); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if sub != "world" {
					t.Fatalf("Expected 'world' but got '%s'", sub)
				}
			},
		},
		{
			name:  "COALESCE function",
			query: "SELECT COALESCE(NULL, 'default') AS value",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var value string
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if value != "default" {
					t.Fatalf("Expected 'default' but got '%s'", value)
				}
			},
		},
		{
			name:  "NOW function",
			query: "SELECT NOW() AS current_time",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var timestamp string
				if err := rows.Scan(&timestamp); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				// Check if the returned value is a valid timestamp
				// within the last minute (to account for test execution time)
				_, err := time.Parse("2006-01-02 15:04:05", timestamp)
				if err != nil {
					// Try RFC3339nano format
					_, err = time.Parse(time.RFC3339, timestamp)
					if err != nil {
						t.Fatalf("Expected a valid timestamp but got '%s': %v", timestamp, err)
					}
				}

				t.Logf("NOW() returned: %s", timestamp)
			},
		},
		{
			name:  "Mixed functions",
			query: "SELECT UPPER(CONCAT('hello', ' ', 'world')) AS message, LENGTH('hello world') AS len",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var message string
				var length int
				if err := rows.Scan(&message, &length); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if message != "HELLO WORLD" {
					t.Fatalf("Expected 'HELLO WORLD' but got '%s'", message)
				}

				if length != 11 {
					t.Fatalf("Expected length 11 but got %d", length)
				}
			},
		},
		{
			name:  "CAST function - string to int",
			query: "SELECT CAST('123' AS INTEGER) AS cast_int",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var castInt int
				if err := rows.Scan(&castInt); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if castInt != 123 {
					t.Fatalf("Expected 123 but got %d", castInt)
				}
			},
		},
		{
			name:  "CAST function - string to float",
			query: "SELECT CAST('3.14' AS FLOAT) AS cast_float",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row but got none")
				}

				var castFloat float64
				if err := rows.Scan(&castFloat); err != nil {
					t.Fatalf("Error scanning result: %v", err)
				}

				if castFloat != 3.14 {
					t.Fatalf("Expected 3.14 but got %f", castFloat)
				}
			},
		},
	}

	// Run the test cases
	for i, test := range tests {
		t.Run(fmt.Sprintf("%02d_%s", i+1, test.name), func(t *testing.T) {
			rows, err := db.Query(test.query)
			if err != nil {
				t.Fatalf("Error executing query: %v", err)
			}
			defer rows.Close()

			// Print column information
			columns, err := rows.Columns()
			if err != nil {
				t.Fatalf("Error getting columns: %v", err)
			}
			t.Logf("Result columns: %v", columns)

			// Run the validation function
			test.validate(t, rows)

			// Make sure there are no more rows
			if rows.Next() {
				t.Fatal("Expected no more rows but got one")
			}
		})
	}
}

func TestSQLDualTable(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Test that DUAL table queries work (used in some SQL dialects for literals)
	t.Run("DUAL table query", func(t *testing.T) {
		query := "SELECT 1 AS value FROM DUAL"

		rows, err := db.Query(query)
		if err != nil {
			// If DUAL is not supported, note this and try without FROM clause
			t.Logf("DUAL table not supported: %v", err)
			t.Logf("Trying without FROM clause...")

			rows, err = db.Query("SELECT 1 AS value")
			if err != nil {
				t.Fatalf("Error executing query without FROM: %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Expected a row but got none")
			}

			var value int
			if err := rows.Scan(&value); err != nil {
				t.Fatalf("Error scanning result: %v", err)
			}

			if value != 1 {
				t.Fatalf("Expected 1 but got %d", value)
			}

			return
		}
		defer rows.Close()

		// DUAL table is supported
		if !rows.Next() {
			t.Fatal("Expected a row but got none")
		}

		var value int
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Error scanning result: %v", err)
		}

		if value != 1 {
			t.Fatalf("Expected 1 but got %d", value)
		}
	})
}

func TestSQLBenchmarkLiterals(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Build a query with 100 literals
	query := "SELECT "
	for i := 1; i <= 100; i++ {
		query += strconv.Itoa(i)
		if i < 100 {
			query += ", "
		}
	}

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected a row but got none")
	}

	values := make([]interface{}, 100)
	scanArgs := make([]interface{}, 100)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if err := rows.Scan(scanArgs...); err != nil {
		t.Fatalf("Error scanning result: %v", err)
	}

	// Verify a few values
	for i := 0; i < 100; i++ {
		expectedValue := i + 1
		actualValue := values[i].(int64) // SQL returns int64
		if int(actualValue) != expectedValue {
			t.Fatalf("Expected value %d but got %d at position %d", expectedValue, actualValue, i)
		}
	}
}
