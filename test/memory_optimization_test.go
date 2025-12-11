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
	"runtime"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestMemoryOptimizationProgress tests memory usage at different stages
func TestMemoryOptimizationProgress(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	if _, err := db.ExecContext(ctx, `CREATE TABLE test_table (
		id INTEGER PRIMARY KEY,
		value INTEGER,
		category TEXT
	)`); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	numRows := 10000
	for i := 0; i < numRows; i++ {
		_, err := db.ExecContext(ctx, "INSERT INTO test_table VALUES (?, ?, ?)",
			i, i*10, "cat"+string(rune(i%10)))
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple_Select",
			query: "SELECT * FROM test_table WHERE id < 100",
		},
		{
			name:  "Simple_Aggregation",
			query: "SELECT category, COUNT(*), SUM(value) FROM test_table GROUP BY category",
		},
		{
			name:  "CTE_Simple",
			query: `WITH data AS (SELECT * FROM test_table WHERE id < 1000) SELECT COUNT(*) FROM data`,
		},
		{
			name: "CTE_With_Aggregation",
			query: `WITH summary AS (
				SELECT category, SUM(value) as total 
				FROM test_table 
				GROUP BY category
			) SELECT * FROM summary WHERE total > 100000`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Force GC and measure memory before
			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			// Execute query
			rows, err := db.QueryContext(ctx, tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Consume all results
			count := 0
			for rows.Next() {
				count++
			}
			rows.Close()

			// Force GC and measure memory after
			runtime.GC()
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			// Calculate allocations
			allocBytes := m2.TotalAlloc - m1.TotalAlloc
			allocCount := m2.Mallocs - m1.Mallocs

			t.Logf("Rows: %d, Memory: %d bytes, Allocations: %d, Bytes/Row: %.2f",
				count, allocBytes, allocCount, float64(allocBytes)/float64(count))
		})
	}
}

// BenchmarkOptimizedCTE benchmarks the optimized CTE implementation
func BenchmarkOptimizedCTE(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	if _, err := db.ExecContext(ctx, `CREATE TABLE bench_table (
		id INTEGER PRIMARY KEY,
		val1 INTEGER,
		val2 INTEGER,
		val3 INTEGER,
		val4 INTEGER
	)`); err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	numRows := 10000
	for i := 0; i < numRows; i++ {
		_, err := db.ExecContext(ctx, "INSERT INTO bench_table VALUES (?, ?, ?, ?, ?)",
			i, i*10, i*20, i*30, i*40)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	query := `
		WITH aggregated AS (
			SELECT 
				(id / 100) as group_id,
				SUM(val1) as sum1,
				SUM(val2) as sum2,
				SUM(val3) as sum3,
				SUM(val4) as sum4
			FROM bench_table
			GROUP BY (id / 100)
		)
		SELECT * FROM aggregated WHERE sum1 > 0
	`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			var groupID, sum1, sum2, sum3, sum4 int64
			if err := rows.Scan(&groupID, &sum1, &sum2, &sum3, &sum4); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}
