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
	"testing"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestSQLExpressionCapabilities tests various SQL expressions to determine their support
func TestSQLExpressionCapabilities(t *testing.T) {
	// Connect to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with data for testing
	_, err = db.Exec(`
		CREATE TABLE expression_test (
			id INTEGER PRIMARY KEY,
			int_value INTEGER,
			float_value FLOAT,
			string_value TEXT,
			bool_value BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO expression_test 
		(id, int_value, float_value, string_value, bool_value)
		VALUES
		(1, 100, 1.1, 'hello', true),
		(2, 200, 2.2, 'world', false),
		(3, 300, 3.3, 'SQL', true),
		(4, 400, 4.4, 'test', false),
		(5, 500, 5.5, 'EXPRESSIONS', true)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Define test cases
	tests := []struct {
		name     string
		query    string
		expected bool // Whether we expect this to work or not
	}{
		// Basic literals
		{name: "Integer literal", query: "SELECT 42", expected: true},
		{name: "Float literal", query: "SELECT 3.14", expected: true},
		{name: "String literal", query: "SELECT 'hello world'", expected: true},
		{name: "Boolean literal - true", query: "SELECT true", expected: true},
		{name: "Boolean literal - false", query: "SELECT false", expected: true},

		// Arithmetic expressions
		{name: "Addition", query: "SELECT 1 + 1", expected: true},
		{name: "Subtraction", query: "SELECT 5 - 2", expected: true},
		{name: "Multiplication", query: "SELECT 3 * 4", expected: true},
		{name: "Division", query: "SELECT 10 / 2", expected: true},

		// String functions
		{name: "UPPER function", query: "SELECT UPPER('hello')", expected: true},
		{name: "LOWER function", query: "SELECT LOWER('WORLD')", expected: true},
		{name: "LENGTH function", query: "SELECT LENGTH('test')", expected: true},
		{name: "SUBSTRING function", query: "SELECT SUBSTRING('hello world', 1, 5)", expected: true},

		// Math functions
		{name: "ABS function", query: "SELECT ABS(-42)", expected: true},
		{name: "CEILING function", query: "SELECT CEILING(3.14)", expected: true},
		{name: "FLOOR function", query: "SELECT FLOOR(3.99)", expected: true},
		{name: "ROUND function", query: "SELECT ROUND(3.14159, 2)", expected: true},

		// Date/time functions
		{name: "NOW function", query: "SELECT NOW()", expected: true},

		// Expressions with column values
		{name: "Column arithmetic", query: "SELECT int_value + 10 FROM expression_test WHERE id = 1", expected: true},
		{name: "String concatenation", query: "SELECT string_value || ' test' FROM expression_test WHERE id = 1", expected: true},
		{name: "String function on column", query: "SELECT UPPER(string_value) FROM expression_test WHERE id = 1", expected: true},

		// Aliased expressions
		{name: "Aliased literal", query: "SELECT 42 AS answer", expected: true},
		{name: "Aliased expression", query: "SELECT int_value * 2 AS doubled FROM expression_test WHERE id = 1", expected: true},
		{name: "Aliased function", query: "SELECT UPPER(string_value) AS shouting FROM expression_test WHERE id = 1", expected: true},

		// Complex expressions
		{name: "Nested functions", query: "SELECT UPPER(SUBSTRING(string_value, 1, 3)) FROM expression_test WHERE id = 1", expected: true},
		{name: "Compound arithmetic", query: "SELECT (int_value + 5) * 2 FROM expression_test WHERE id = 1", expected: true},
		{name: "Mixed types", query: "SELECT int_value + float_value FROM expression_test WHERE id = 1", expected: true},
	}

	// Run the tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(tt.query)
			if tt.expected {
				if err != nil {
					t.Fatalf("Expected query to work, but got error: %v", err)
				}

				// Print column names
				cols, err := rows.Columns()
				if err != nil {
					t.Fatalf("Failed to get columns: %v", err)
				}
				t.Logf("Columns: %v", cols)

				// Try to read first row
				if rows.Next() {
					// Create a slice of interface{} to hold the values
					values := make([]interface{}, len(cols))
					valuePtrs := make([]interface{}, len(cols))
					for i := range values {
						valuePtrs[i] = &values[i]
					}

					err := rows.Scan(valuePtrs...)
					if err != nil {
						t.Fatalf("Failed to scan row: %v", err)
					}

					// Print the values
					rowStr := "Values: "
					for i, val := range values {
						if val == nil {
							rowStr += fmt.Sprintf("%s: NULL, ", cols[i])
						} else {
							switch v := val.(type) {
							case []byte:
								rowStr += fmt.Sprintf("%s: %s, ", cols[i], string(v))
							case int64, float64, string, bool:
								rowStr += fmt.Sprintf("%s: %v, ", cols[i], v)
							case time.Time:
								rowStr += fmt.Sprintf("%s: %v, ", cols[i], v.Format(time.RFC3339))
							default:
								rowStr += fmt.Sprintf("%s: %v (%T), ", cols[i], v, v)
							}
						}
					}

					t.Logf("%s", rowStr)
				} else {
					t.Logf("No rows returned")
				}
			} else {
				if err == nil {
					rows.Close()
					t.Logf("Query succeeded unexpectedly")
				} else {
					t.Logf("Query failed as expected with error: %v", err)
				}
			}

			if rows != nil {
				rows.Close()
			}
		})
	}
}
