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
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/parser"
)

// Helper function to convert interface slice to driver.NamedValue slice
func toNamedValues(params []interface{}) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(params))
	for i, value := range params {
		namedValues[i] = driver.NamedValue{
			Name:    "",    // Using positional parameters
			Ordinal: i + 1, // 1-based index
			Value:   value, // The value itself
		}
	}
	return namedValues
}

func TestParameterBinding(t *testing.T) {
	// Create a memory engine for testing
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer db.Close()

	// Debug
	t.Logf("Engine created: %T", db.Engine())
	if db.Engine() == nil {
		t.Fatalf("Engine is nil")
	}

	// Use the Executor method from DB
	exec := db.Executor()
	t.Logf("Executor created: %T", exec)

	// Create a test table with different data types
	createTbl := "CREATE TABLE test_params (id INTEGER, name TEXT, salary FLOAT, active BOOLEAN, hire_date DATE, meta JSON)"
	t.Logf("About to execute CREATE TABLE query")
	result, err := exec.Execute(context.Background(), nil, createTbl)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer result.Close()

	testCases := []struct {
		name     string
		query    string
		params   []interface{}
		hasError bool
	}{
		{
			name:     "Integer parameter",
			query:    "INSERT INTO test_params (id) VALUES (?)",
			params:   []interface{}{42},
			hasError: false,
		},
		{
			name:     "String parameter",
			query:    "INSERT INTO test_params (name) VALUES (?)",
			params:   []interface{}{"John Doe"},
			hasError: false,
		},
		{
			name:     "Float parameter",
			query:    "INSERT INTO test_params (salary) VALUES (?)",
			params:   []interface{}{75000.50},
			hasError: false,
		},
		{
			name:     "Boolean parameter",
			query:    "INSERT INTO test_params (active) VALUES (?)",
			params:   []interface{}{true},
			hasError: false,
		},
		{
			name:     "Time parameter",
			query:    "INSERT INTO test_params (hire_date) VALUES (?)",
			params:   []interface{}{time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
			hasError: false,
		},
		{
			name:     "JSON parameter",
			query:    "INSERT INTO test_params (id, meta) VALUES (?, ?)",
			params:   []interface{}{999, map[string]interface{}{"department": "Engineering", "level": 3}},
			hasError: false,
		},
		{
			name:     "Multiple parameters",
			query:    "INSERT INTO test_params (id, name, salary, active) VALUES (?, ?, ?, ?)",
			params:   []interface{}{101, "Jane Smith", 85000.75, false},
			hasError: false,
		},
		{
			name:     "WHERE clause with parameter",
			query:    "SELECT * FROM test_params WHERE id = ?",
			params:   []interface{}{42},
			hasError: false,
		},
		{
			name:     "Multiple WHERE parameters",
			query:    "SELECT * FROM test_params WHERE salary > ? AND active = ?",
			params:   []interface{}{50000.0, true},
			hasError: false,
		},
		{
			name:     "Parameters in complex expression",
			query:    "SELECT * FROM test_params WHERE (id = ? OR id = ?) AND active = ?",
			params:   []interface{}{42, 101, true},
			hasError: false,
		},
		{
			name:     "UPDATE with parameters",
			query:    "UPDATE test_params SET name = ?, salary = ? WHERE id = ?",
			params:   []interface{}{"Updated Name", 90000.0, 42},
			hasError: false,
		},
		{
			name:     "DELETE with parameter",
			query:    "DELETE FROM test_params WHERE id = ?",
			params:   []interface{}{101},
			hasError: false,
		},
		{
			name:     "Incorrect parameter count (too few)",
			query:    "INSERT INTO test_params (id, name) VALUES (?, ?)",
			params:   []interface{}{42},
			hasError: true,
		},
		{
			name:     "NULL parameter",
			query:    "INSERT INTO test_params (id, name) VALUES (?, ?)",
			params:   []interface{}{777, nil},
			hasError: false,
		},
		{
			name:     "Parameter in ORDER BY",
			query:    "SELECT * FROM test_params ORDER BY salary DESC LIMIT ?",
			params:   []interface{}{10},
			hasError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Convert interface params to driver.NamedValue
			namedParams := toNamedValues(tc.params)
			result, err := exec.ExecuteWithParams(context.Background(), nil, tc.query, namedParams)

			// Check if error was expected
			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				// Skip further testing for this case
				return
			}

			// Check for unexpected errors
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Close the result after use
			defer result.Close()

			// For SELECT queries, do some basic validation on the result
			if isSelect(tc.query) {
				// Verify we got a valid result with columns
				columns := result.Columns()
				if len(columns) == 0 {
					t.Errorf("Expected columns in result but got none")
				}

				// Try to fetch a row
				rowCount := 0
				for result.Next() {
					rowCount++
					// Scan values into an array of interfaces
					values := make([]interface{}, len(columns))
					for i := range values {
						values[i] = new(interface{})
					}

					// Check that we can scan values
					err = result.Scan(values...)
					if err != nil {
						t.Errorf("Failed to scan row: %v", err)
					}
				}

				// Report row count - for debugging
				t.Logf("Query returned %d rows", rowCount)
			}
		})
	}

	// Verify the final state by selecting all rows
	result, err = exec.Execute(context.Background(), nil, "SELECT COUNT(*) FROM test_params")
	if err != nil {
		t.Fatalf("Failed to execute final SELECT: %v", err)
	}
	defer result.Close()

	// Report the final state of the table
	var rowCount int
	if result.Next() {
		var count interface{}
		if err := result.Scan(&count); err != nil {
			t.Errorf("Failed to scan count: %v", err)
		} else if countVal, ok := count.(int64); ok {
			rowCount = int(countVal)
		}
	}

	t.Logf("Final table has %d rows", rowCount)
}

// Helper function to check if a query is a SELECT statement
func isSelect(query string) bool {
	l := parser.NewLexer(query)
	p := parser.NewParser(l)

	program := p.ParseProgram()
	if len(p.Errors()) > 0 || len(program.Statements) == 0 {
		return false
	}

	_, isSelect := program.Statements[0].(*parser.SelectStatement)
	return isSelect
}

func TestBindExpressionTraversal(t *testing.T) {
	// Create a memory engine for testing
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer db.Close()

	// Debug
	t.Logf("Engine created: %T", db.Engine())
	if db.Engine() == nil {
		t.Fatalf("Engine is nil")
	}

	// Use the Executor method from DB
	exec := db.Executor()
	t.Logf("Executor created: %T", exec)

	// Create a test table
	createTbl := "CREATE TABLE test_expr (id INTEGER, name TEXT, value FLOAT)"
	result, err := exec.Execute(context.Background(), nil, createTbl)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer result.Close()

	// Insert test data
	insertSQL := "INSERT INTO test_expr VALUES (1, 'Item 1', 100), (2, 'Item 2', 200), (3, 'Item 3', 300)"
	result, err = exec.Execute(context.Background(), nil, insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
	defer result.Close()

	// Test parameter binding in various complex expressions
	testCases := []struct {
		name     string
		query    string
		params   []interface{}
		expected int // Expected number of rows
	}{
		{
			name:     "Parameter in function call",
			query:    "SELECT * FROM test_expr WHERE ABS(value - ?) < 150",
			params:   []interface{}{250.0},
			expected: 2, // Items with values 100 and 300
		},
		{
			name:     "Parameter in nested expression",
			query:    "SELECT * FROM test_expr WHERE (id + ?) > (value / ?)",
			params:   []interface{}{5, 50.0},
			expected: 3, // All items should match
		},
		{
			name:     "Parameters in expression comparisons",
			query:    "SELECT * FROM test_expr WHERE value > ? AND id < ?",
			params:   []interface{}{150.0, 10},
			expected: 2, // Items with value > 150 and id < 10 (based on actual data)
		},
		{
			name:     "Parameters in ORDER BY and LIMIT",
			query:    "SELECT * FROM test_expr ORDER BY ABS(value - ?) LIMIT ?",
			params:   []interface{}{200.0, 2},
			expected: 2, // Limited to 2 rows
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Convert interface params to driver.NamedValue
			namedParams := toNamedValues(tc.params)
			result, err := exec.ExecuteWithParams(context.Background(), nil, tc.query, namedParams)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer result.Close()

			// Count the rows in the result
			rowCount := 0
			for result.Next() {
				rowCount++
				// Scan values (not using them, just checking we can scan)
				values := make([]interface{}, len(result.Columns()))
				for i := range values {
					values[i] = new(interface{})
				}
				if err := result.Scan(values...); err != nil {
					t.Errorf("Failed to scan row: %v", err)
				}
			}

			// Verify row count matches expected
			if rowCount != tc.expected {
				t.Errorf("Expected %d rows, but got %d", tc.expected, rowCount)
			}
		})
	}
}
