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

// TestSimpleVectorizedExecution tests basic vectorized arithmetic operations
func TestSimpleVectorizedExecution(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Enable vectorized execution
	_, err = db.Exec("SET VECTORIZED = true")
	if err != nil {
		// If SET VECTORIZED is not supported, we'll need to modify the connection string
		t.Logf("Warning: Failed to enable vectorized mode via SQL: %v", err)
		t.Logf("Test will proceed but might not use vectorized execution")
	}

	// Create test table
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec("INSERT INTO test_table VALUES (?, ?)", i, float64(i)*1.5)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Test simple arithmetic operations to verify basic execution
	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Vector-Vector Addition",
			query: "SELECT COUNT(*) FROM test_table WHERE id + value > 50",
		},
		{
			name:  "Vector-Scalar Multiplication",
			query: "SELECT COUNT(*) FROM test_table WHERE value * 3.14159 > 100",
		},
		{
			name:  "Vector-Vector Division",
			query: "SELECT COUNT(*) FROM test_table WHERE value / id > 1",
		},
		{
			name:  "Complex Arithmetic Chain",
			query: "SELECT COUNT(*) FROM test_table WHERE ((value * 2) + (id / 10.0)) > 100",
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var count int
			err := db.QueryRow(tc.query).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Just verify we got a reasonable result (non-zero count)
			if count == 0 {
				t.Errorf("Query returned 0 rows")
			} else {
				t.Logf("Query returned count: %d", count)
			}
		})
	}

	// Create a larger table for more realistic tests
	_, err = db.Exec("CREATE TABLE benchmark_table (id INTEGER, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create benchmark table: %v", err)
	}

	// Insert 1000 rows for better measurement
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec("INSERT INTO benchmark_table VALUES (?, ?)", i, float64(i)*1.5)
		if err != nil {
			t.Fatalf("Failed to insert benchmark row %d: %v", i, err)
		}
	}

	// Run a complex query with multiple arithmetic operations
	t.Run("Complex Vector Operations", func(t *testing.T) {
		query := `
			SELECT COUNT(*) 
			FROM benchmark_table 
			WHERE (value * 2 + id) / 3.0 - 10 > 100
		`

		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to execute complex query: %v", err)
		}

		t.Logf("Complex query returned count: %d", count)

		// This query should return rows for higher values of id and value
		if count == 0 || count == 1000 {
			t.Errorf("Unexpected count for complex query: %d", count)
		}
	})

	// Test all the SIMD-optimized arithmetic operations in combination
	t.Run("Combined SIMD Operations", func(t *testing.T) {
		query := `
			SELECT COUNT(*) 
			FROM benchmark_table 
			WHERE (id * value) + (value / 2) - (id * 0.1) > 1000
		`

		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to execute combined SIMD operations query: %v", err)
		}

		t.Logf("Combined SIMD operations query returned count: %d", count)

		// Again, just verify we got a reasonable count
		if count == 0 || count == 1000 {
			t.Errorf("Unexpected count for combined SIMD query: %d", count)
		}
	})
}
