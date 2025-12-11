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
	"fmt"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// BenchmarkCTEMaterialization benchmarks CTE materialization performance
func BenchmarkCTEMaterialization(b *testing.B) {
	testCases := []struct {
		name    string
		rows    int
		cols    int
		queries int // Number of times CTE is referenced
	}{
		{"Small_100x10x1", 100, 10, 1},
		{"Small_100x10x3", 100, 10, 3},
		{"Medium_1000x20x1", 1000, 20, 1},
		{"Medium_1000x20x3", 1000, 20, 3},
		{"Large_10000x50x1", 10000, 50, 1},
		{"Large_10000x50x3", 10000, 50, 3},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			ctx := context.Background()

			// Create a table with multiple columns
			createTableSQL := "CREATE TABLE wide_table (id INTEGER PRIMARY KEY"
			for i := 0; i < tc.cols-1; i++ {
				createTableSQL += fmt.Sprintf(", col%d INTEGER", i)
			}
			createTableSQL += ")"

			if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			// Insert data
			for i := 0; i < tc.rows; i++ {
				insertSQL := fmt.Sprintf("INSERT INTO wide_table VALUES (%d", i)
				for j := 0; j < tc.cols-1; j++ {
					insertSQL += fmt.Sprintf(", %d", i*10+j)
				}
				insertSQL += ")"
				if _, err := db.ExecContext(ctx, insertSQL); err != nil {
					b.Fatalf("Failed to insert row %d: %v", i, err)
				}
			}

			// Build CTE query that references the CTE multiple times
			cteQuery := "WITH data AS (SELECT * FROM wide_table) "

			// First reference
			cteQuery += "SELECT COUNT(*) FROM data"

			// Additional references
			for i := 1; i < tc.queries; i++ {
				cteQuery += fmt.Sprintf(" UNION ALL SELECT COUNT(*) FROM data WHERE id > %d", i*tc.rows/tc.queries)
			}

			b.ResetTimer()

			// Benchmark the CTE execution
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx, cteQuery)
				if err != nil {
					b.Fatalf("Failed to execute CTE query: %v", err)
				}

				// Consume all results
				for rows.Next() {
					var count int
					if err := rows.Scan(&count); err != nil {
						b.Fatalf("Failed to scan: %v", err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkCTEColumnarMemoryUsage tests memory efficiency of columnar CTE storage
func BenchmarkCTEColumnarMemoryUsage(b *testing.B) {
	testCases := []struct {
		name string
		rows int
		cols int
	}{
		{"Small_1000x10", 1000, 10},
		{"Medium_10000x20", 10000, 20},
		{"Large_50000x50", 50000, 50},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			ctx := context.Background()

			// Create table
			createTableSQL := "CREATE TABLE test_table (id INTEGER PRIMARY KEY"
			for i := 0; i < tc.cols-1; i++ {
				createTableSQL += fmt.Sprintf(", col%d TEXT", i)
			}
			createTableSQL += ")"

			if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			// Insert data with text values (to increase memory usage)
			for i := 0; i < tc.rows; i++ {
				insertSQL := fmt.Sprintf("INSERT INTO test_table VALUES (%d", i)
				for j := 0; j < tc.cols-1; j++ {
					insertSQL += fmt.Sprintf(", 'value_%d_%d'", i, j)
				}
				insertSQL += ")"
				if _, err := db.ExecContext(ctx, insertSQL); err != nil {
					b.Fatalf("Failed to insert row %d: %v", i, err)
				}
			}

			// CTE that materializes all data
			cteQuery := `
				WITH materialized_data AS (
					SELECT * FROM test_table
				)
				SELECT COUNT(*) as total,
					   SUM(id) as sum_id,
					   MAX(id) as max_id,
					   MIN(id) as min_id
				FROM materialized_data
			`

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx, cteQuery)
				if err != nil {
					b.Fatalf("Failed to execute CTE query: %v", err)
				}

				// Consume result
				if rows.Next() {
					var total, sumId, maxId, minId int
					if err := rows.Scan(&total, &sumId, &maxId, &minId); err != nil {
						b.Fatalf("Failed to scan: %v", err)
					}
				}
				rows.Close()
			}
		})
	}
}
