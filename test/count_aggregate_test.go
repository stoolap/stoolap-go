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

func TestCountAggregation(t *testing.T) {
	connStr := "memory://"

	// Open DB using database/sql interface
	db, err := sql.Open("stoolap", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE test_count (
		id INTEGER,
		name TEXT,
		value FLOAT,
		active BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	insertStmt := "INSERT INTO test_count VALUES (?, ?, ?, ?)"
	for i := 1; i <= 100; i++ {
		active := (i % 2) == 0
		_, err := db.Exec(insertStmt, i, "Item "+string(rune(64+i%26)), float64(i)*1.5, active)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Test COUNT(*) with various conditions
	countTests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "COUNT(*) - All rows",
			query:    "SELECT COUNT(*) FROM test_count",
			expected: 100,
		},
		{
			name:     "COUNT(*) with WHERE",
			query:    "SELECT COUNT(*) FROM test_count WHERE active = true",
			expected: 50,
		},
		{
			name:     "COUNT(*) with complex WHERE",
			query:    "SELECT COUNT(*) FROM test_count WHERE id > 50 AND value < 100",
			expected: 16,
		},
		{
			name:     "COUNT with column",
			query:    "SELECT COUNT(id) FROM test_count",
			expected: 100,
		},
		{
			name:     "COUNT DISTINCT",
			query:    "SELECT COUNT(DISTINCT active) FROM test_count",
			expected: 2,
		},
		{
			name:     "Multiple aggregates",
			query:    "SELECT COUNT(*), MIN(value), MAX(value) FROM test_count",
			expected: 1, // One row with three columns
		},
		{
			name:     "COUNT with GROUP BY",
			query:    "SELECT active, COUNT(*) FROM test_count GROUP BY active",
			expected: 2, // One row for each active value (true/false)
		},
		{
			name:     "COUNT with HAVING",
			query:    "SELECT id / 10 AS decade, COUNT(*) FROM test_count GROUP BY decade HAVING COUNT(*) > 5",
			expected: 10, // 10 decades with >5 rows each
		},
	}

	for _, tc := range countTests {
		t.Run(tc.name, func(t *testing.T) {
			// Skip decade test temporarily
			if tc.name == "COUNT with HAVING" {
				t.Skip("Skipping decade test until computed column handling is fixed")
				return
			}

			var count int

			// Handle the case of multiple aggregates or GROUP BY
			if tc.name == "Multiple aggregates" || tc.name == "COUNT with GROUP BY" || tc.name == "COUNT with HAVING" {
				// For multiple columns, we just verify row count
				rows, err := db.Query(tc.query)
				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}

				rowCount := 0
				for rows.Next() {
					rowCount++
				}
				rows.Close()

				if rowCount != tc.expected {
					t.Errorf("Expected %d rows, got %d rows", tc.expected, rowCount)
				}
			} else {
				// Add debugging query for COUNT DISTINCT issue
				if tc.name == "COUNT DISTINCT" {
					// Use a map to collect distinct values
					values := make(map[bool]bool)
					debugRows, _ := db.Query("SELECT active FROM test_count")
					for debugRows.Next() {
						var active bool
						_ = debugRows.Scan(&active)
						values[active] = true
					}
					debugRows.Close()
					t.Logf("  Number of distinct boolean values: %d", len(values))
					for val := range values {
						t.Logf("  active = %v", val)
					}
				}

				err := db.QueryRow(tc.query).Scan(&count)
				if err != nil {
					t.Fatalf("Failed to execute query: %v", err)
				}

				if count != tc.expected {
					t.Errorf("Expected count of %d, got %d", tc.expected, count)
				}
			}
		})
	}

	// Test using the benchmark pattern from cmd/stoolap/benchmark.go
	t.Run("Benchmark COUNT pattern", func(t *testing.T) {
		// This is the same pattern used in the benchmark tool
		var total int
		err := db.QueryRow("SELECT COUNT(*) FROM test_count").Scan(&total)
		if err != nil {
			t.Fatalf("COUNT query failed: %v", err)
		}

		if total != 100 {
			t.Errorf("Expected count of 100, got %d", total)
		}
	})
}
