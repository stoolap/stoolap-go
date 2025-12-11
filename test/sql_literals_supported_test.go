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

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import for database registration
)

func TestSQLLiteralsSupported(t *testing.T) {
	// Open an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Create a simple test table
	_, err = db.Exec(`CREATE TABLE literals_test (
		id INTEGER,
		int_value INTEGER,
		float_value FLOAT,
		string_value TEXT,
		bool_value BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO literals_test (id, int_value, float_value, string_value, bool_value) 
		VALUES (1, 100, 3.14, 'hello', true)`)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

	// Basic tests for currently supported SQL literals
	tests := []struct {
		name     string
		query    string
		validate func(*testing.T, *sql.Rows)
	}{
		{
			name:  "Integer literal",
			query: "SELECT 42",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value int64
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if value != 42 {
					t.Errorf("Expected 42, got %d", value)
				}
			},
		},
		{
			name:  "Float literal",
			query: "SELECT 3.14",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value float64
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if value != 3.14 {
					t.Errorf("Expected 3.14, got %f", value)
				}
			},
		},
		{
			name:  "String literal",
			query: "SELECT 'hello world'",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value string
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if value != "hello world" {
					t.Errorf("Expected 'hello world', got '%s'", value)
				}
			},
		},
		{
			name:  "Boolean literal - true",
			query: "SELECT true",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value bool
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if !value {
					t.Errorf("Expected true, got %v", value)
				}
			},
		},
		{
			name:  "Boolean literal - false",
			query: "SELECT false",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value bool
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if value {
					t.Errorf("Expected false, got %v", value)
				}
			},
		},
		{
			name:  "NULL literal",
			query: "SELECT NULL",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var value interface{}
				if err := rows.Scan(&value); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if value != nil {
					t.Errorf("Expected nil, got %v", value)
				}
			},
		},
		{
			name:  "Multiple literals",
			query: "SELECT 1, 2.5, 'three', true, NULL",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var int_val int64
				var float_val float64
				var str_val string
				var bool_val bool
				var null_val interface{}

				if err := rows.Scan(&int_val, &float_val, &str_val, &bool_val, &null_val); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if int_val != 1 || float_val != 2.5 || str_val != "three" || !bool_val || null_val != nil {
					t.Errorf("Unexpected values: %d, %f, %s, %v, %v", int_val, float_val, str_val, bool_val, null_val)
				}
			},
		},
		{
			name:  "Column values",
			query: "SELECT id, int_value, float_value, string_value, bool_value FROM literals_test",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var id, int_val int64
				var float_val float64
				var str_val string
				var bool_val bool

				if err := rows.Scan(&id, &int_val, &float_val, &str_val, &bool_val); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if id != 1 || int_val != 100 || float_val != 3.14 || str_val != "hello" || !bool_val {
					t.Errorf("Unexpected values: %d, %d, %f, %s, %v", id, int_val, float_val, str_val, bool_val)
				}
			},
		},
		{
			name:  "Column aliases",
			query: "SELECT id AS identifier, int_value AS number FROM literals_test",
			validate: func(t *testing.T, rows *sql.Rows) {
				// Check column names
				columns, err := rows.Columns()
				if err != nil {
					t.Fatalf("Error getting columns: %v", err)
				}

				if len(columns) != 2 || columns[0] != "identifier" || columns[1] != "number" {
					t.Errorf("Expected columns [identifier, number], got %v", columns)
				}

				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var id, num int64
				if err := rows.Scan(&id, &num); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if id != 1 || num != 100 {
					t.Errorf("Expected [1, 100], got [%d, %d]", id, num)
				}
			},
		},
		{
			name:  "WHERE with column alias",
			query: "SELECT int_value AS num FROM literals_test WHERE num > 50",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatal("Expected a row")
				}

				var num int64
				if err := rows.Scan(&num); err != nil {
					t.Fatalf("Error scanning: %v", err)
				}

				if num != 100 {
					t.Errorf("Expected 100, got %d", num)
				}
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%02d_%s", i+1, test.name), func(t *testing.T) {
			t.Logf("Query: %s", test.query)

			rows, err := db.Query(test.query)
			if err != nil {
				t.Fatalf("Error executing query: %v", err)
			}
			defer rows.Close()

			columns, err := rows.Columns()
			if err != nil {
				t.Fatalf("Error getting columns: %v", err)
			}
			t.Logf("Columns: %v", columns)

			// Run the validation function
			test.validate(t, rows)

			// Make sure there are no more rows
			if rows.Next() {
				t.Error("Expected no more rows")
			}
		})
	}
}
