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

	"github.com/stoolap/stoolap"
)

func TestCastExpression(t *testing.T) {
	// CAST expressions are now supported
	ctx := context.Background()

	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get the SQL executor
	executor := db.Executor()

	// Create a test table
	_, err = executor.Execute(ctx, nil, `
		CREATE TABLE test_cast (
			id INTEGER,
			int_val INTEGER,
			float_val FLOAT,
			text_val TEXT,
			bool_val BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some test data
	_, err = executor.Execute(ctx, nil, `
		INSERT INTO test_cast (id, int_val, float_val, text_val, bool_val)
		VALUES
			(1, 123, 123.456, '123', true),
			(2, 0, 0.0, '0', false),
			(3, -50, -50.5, '-50', true),
			(4, NULL, NULL, NULL, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		// Integer casts
		{
			name:     "CAST text to INTEGER",
			query:    "SELECT CAST(text_val AS INTEGER) FROM test_cast WHERE id = 1",
			expected: int64(123),
		},
		{
			name:     "CAST float to INTEGER",
			query:    "SELECT CAST(float_val AS INTEGER) FROM test_cast WHERE id = 1",
			expected: int64(123),
		},
		{
			name:     "CAST boolean to INTEGER",
			query:    "SELECT CAST(bool_val AS INTEGER) FROM test_cast WHERE id = 1",
			expected: int64(1),
		},
		{
			name:     "CAST false to INTEGER",
			query:    "SELECT CAST(bool_val AS INTEGER) FROM test_cast WHERE id = 2",
			expected: int64(0),
		},
		// Float casts
		{
			name:     "CAST text to FLOAT",
			query:    "SELECT CAST(text_val AS FLOAT) FROM test_cast WHERE id = 1",
			expected: float64(123),
		},
		{
			name:     "CAST integer to FLOAT",
			query:    "SELECT CAST(int_val AS FLOAT) FROM test_cast WHERE id = 1",
			expected: float64(123),
		},
		{
			name:     "CAST boolean to FLOAT",
			query:    "SELECT CAST(bool_val AS FLOAT) FROM test_cast WHERE id = 1",
			expected: float64(1),
		},
		{
			name:     "CAST false to FLOAT",
			query:    "SELECT CAST(bool_val AS FLOAT) FROM test_cast WHERE id = 2",
			expected: float64(0),
		},
		// String casts
		{
			name:     "CAST integer to TEXT",
			query:    "SELECT CAST(int_val AS TEXT) FROM test_cast WHERE id = 1",
			expected: "123",
		},
		{
			name:     "CAST float to TEXT",
			query:    "SELECT CAST(float_val AS TEXT) FROM test_cast WHERE id = 1",
			expected: "123.456000",
		},
		{
			name:     "CAST boolean to TEXT",
			query:    "SELECT CAST(bool_val AS TEXT) FROM test_cast WHERE id = 1",
			expected: "true",
		},
		{
			name:     "CAST false to TEXT",
			query:    "SELECT CAST(bool_val AS TEXT) FROM test_cast WHERE id = 2",
			expected: "false",
		},
		// Boolean casts
		{
			name:     "CAST integer to BOOLEAN (true)",
			query:    "SELECT CAST(int_val AS BOOLEAN) FROM test_cast WHERE id = 1",
			expected: true,
		},
		{
			name:     "CAST integer to BOOLEAN (false)",
			query:    "SELECT CAST(int_val AS BOOLEAN) FROM test_cast WHERE id = 2",
			expected: false,
		},
		{
			name:     "CAST text to BOOLEAN (true)",
			query:    "SELECT CAST(text_val AS BOOLEAN) FROM test_cast WHERE id = 1",
			expected: true,
		},
		{
			name:     "CAST zero text to BOOLEAN (false)",
			query:    "SELECT CAST(text_val AS BOOLEAN) FROM test_cast WHERE id = 2",
			expected: false,
		},
		// NULL casts
		{
			name:     "CAST NULL to INTEGER",
			query:    "SELECT CAST(int_val AS INTEGER) FROM test_cast WHERE id = 4",
			expected: int64(0),
		},
		{
			name:     "CAST NULL to TEXT",
			query:    "SELECT CAST(text_val AS TEXT) FROM test_cast WHERE id = 4",
			expected: "",
		},
		// Filter using CAST
		{
			name:     "Filter using CAST",
			query:    "SELECT id FROM test_cast WHERE id = CAST('1' AS INTEGER)",
			expected: int64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, nil, tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if !result.Next() {
				t.Fatalf("no records returned")
			}

			// Scan the first column value
			var actual interface{}
			if err := result.Scan(&actual); err != nil {
				t.Fatalf("failed to scan result: %v", err)
			}

			// For NULL values, we need to check if the value is nil
			if tt.expected == nil {
				if actual != nil {
					t.Errorf("expected nil, got %v", actual)
				}
				return
			}

			// Check the result type and value
			switch expected := tt.expected.(type) {
			case int64:
				if actualInt, ok := actual.(int64); !ok {
					t.Errorf("expected int64, got %T", actual)
				} else if actualInt != expected {
					t.Errorf("expected %v, got %v", expected, actualInt)
				}
			case float64:
				if actualFloat, ok := actual.(float64); !ok {
					t.Errorf("expected float64, got %T", actual)
				} else if actualFloat != expected {
					t.Errorf("expected %v, got %v", expected, actualFloat)
				}
			case string:
				if actualStr, ok := actual.(string); !ok {
					t.Errorf("expected string, got %T", actual)
				} else if actualStr != expected {
					t.Errorf("expected %v, got %v", expected, actualStr)
				}
			case bool:
				if actualBool, ok := actual.(bool); !ok {
					t.Errorf("expected bool, got %T", actual)
				} else if actualBool != expected {
					t.Errorf("expected %v, got %v", expected, actualBool)
				}
			default:
				t.Errorf("unexpected expected type: %T", expected)
			}
		})
	}
}
