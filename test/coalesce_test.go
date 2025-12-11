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

// TestCoalesceFunction tests the SQL COALESCE function specifically with literals
func TestCoalesceFunction(t *testing.T) {
	// Get the block storage engine factory
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

	// Create a simple test table to run the queries against
	createQuery := `CREATE TABLE test_table (id INTEGER)`
	result, err := executor.Execute(context.Background(), nil, createQuery)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Insert a single row to be able to run the queries against
	insertQuery := `INSERT INTO test_table (id) VALUES (1)`
	result, err = executor.Execute(context.Background(), nil, insertQuery)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Test cases for COALESCE with literals
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "First value non-null",
			query:    "SELECT COALESCE('Value', 'Default') FROM test_table",
			expected: "Value",
		},
		{
			name:     "First value null, second non-null",
			query:    "SELECT COALESCE(NULL, 'Default') FROM test_table",
			expected: "Default",
		},
		{
			name:     "Empty string treated as null",
			query:    "SELECT COALESCE('', 'Default') FROM test_table",
			expected: "Default",
		},
		{
			name:     "Multiple values with NULL and empty string",
			query:    "SELECT COALESCE(NULL, '', 'Value', 'Other') FROM test_table",
			expected: "Value",
		},
		{
			name:     "Single value",
			query:    "SELECT COALESCE('Single') FROM test_table",
			expected: "Single",
		},
	}

	// Run all test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), nil, tc.query)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			defer result.Close()

			if !result.Next() {
				t.Fatal("Expected a result row")
			}

			var value string
			if err := result.Scan(&value); err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}

			if value != tc.expected {
				t.Errorf("Expected '%s', got '%s'", tc.expected, value)
			}
		})
	}
}
