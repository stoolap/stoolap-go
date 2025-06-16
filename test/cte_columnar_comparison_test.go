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
	"fmt"
	"runtime"
	"testing"

	"github.com/stoolap/stoolap/internal/sql/executor"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

// TestColumnarCTEMemoryEfficiency tests memory efficiency of columnar CTE storage
func TestColumnarCTEMemoryEfficiency(t *testing.T) {
	// Create test engine
	cfg := &storage.Config{
		Path: "",
		Persistence: storage.PersistenceConfig{
			Enabled: false,
		},
	}
	eng := mvcc.NewMVCCEngine(cfg)
	if err := eng.Open(); err != nil {
		t.Fatalf("Failed to open MVCC engine: %v", err)
	}
	defer eng.Close()

	exec := executor.NewExecutor(eng)
	ctx := context.Background()

	// Create a wide table
	createSQL := `CREATE TABLE wide_table (
		id INTEGER PRIMARY KEY,
		col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
		col6 INTEGER, col7 INTEGER, col8 INTEGER, col9 INTEGER, col10 INTEGER
	)`

	if _, err := exec.Execute(ctx, nil, createSQL); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	numRows := 1000
	for i := 0; i < numRows; i++ {
		insertSQL := fmt.Sprintf(`INSERT INTO wide_table VALUES (
			%d, 'text%d', 'value%d', 'data%d', 'info%d', 'detail%d',
			%d, %d, %d, %d, %d
		)`, i, i, i, i, i, i, i*10, i*20, i*30, i*40, i*50)

		if _, err := exec.Execute(ctx, nil, insertSQL); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Force GC and record initial memory
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Execute CTE query that materializes all data
	cteQuery := `
		WITH all_data AS (
			SELECT * FROM wide_table
		)
		SELECT COUNT(*) as cnt, SUM(id) as sum_id FROM all_data
	`

	result, err := exec.Execute(ctx, nil, cteQuery)
	if err != nil {
		t.Fatalf("Failed to execute CTE query: %v", err)
	}
	defer result.Close()

	// Consume result
	if !result.Next() {
		t.Fatal("Expected one row")
	}

	row := result.Row()
	if row == nil || len(row) != 2 {
		t.Fatal("Expected row with 2 columns")
	}

	// Verify results
	count := row[0].AsInterface().(int64)
	sumId := row[1].AsInterface().(float64) // SUM returns float64

	if count != int64(numRows) {
		t.Errorf("Expected count %d, got %d", numRows, count)
	}

	expectedSum := float64((numRows - 1) * numRows / 2) // Sum of 0 to numRows-1
	if sumId != expectedSum {
		t.Errorf("Expected sum %f, got %f", expectedSum, sumId)
	}

	// Force GC and check memory after
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Calculate memory used (handle potential overflow)
	var memUsed int64
	if m2.HeapAlloc >= m1.HeapAlloc {
		memUsed = int64(m2.HeapAlloc - m1.HeapAlloc)
	} else {
		// Memory was freed, use TotalAlloc instead
		memUsed = int64(m2.TotalAlloc - m1.TotalAlloc)
	}
	bytesPerRow := float64(memUsed) / float64(numRows)

	t.Logf("Memory used for %d rows: %d bytes (%.2f bytes/row)", numRows, memUsed, bytesPerRow)

	// With columnar storage, we expect much less memory per row
	// Old map-based: ~1000+ bytes per row
	// New columnar: ~200-300 bytes per row
	if bytesPerRow > 500 {
		t.Logf("Warning: Memory usage seems high: %.2f bytes/row", bytesPerRow)
	}
}

// BenchmarkColumnarVsMapCTE compares columnar vs map-based CTE storage
func BenchmarkColumnarVsMapCTE(b *testing.B) {
	testCases := []struct {
		name string
		rows int
		cols int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x20", 1000, 20},
		{"Large_5000x30", 5000, 30},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create test engine
			cfg := &storage.Config{
				Path: "",
				Persistence: storage.PersistenceConfig{
					Enabled: false,
				},
			}
			eng := mvcc.NewMVCCEngine(cfg)
			if err := eng.Open(); err != nil {
				b.Fatalf("Failed to open MVCC engine: %v", err)
			}
			defer eng.Close()

			exec := executor.NewExecutor(eng)
			ctx := context.Background()

			// Create table
			createSQL := "CREATE TABLE test_table (id INTEGER PRIMARY KEY"
			for i := 0; i < tc.cols-1; i++ {
				createSQL += fmt.Sprintf(", col%d INTEGER", i)
			}
			createSQL += ")"

			if _, err := exec.Execute(ctx, nil, createSQL); err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			// Insert data
			for i := 0; i < tc.rows; i++ {
				insertSQL := fmt.Sprintf("INSERT INTO test_table VALUES (%d", i)
				for j := 0; j < tc.cols-1; j++ {
					insertSQL += fmt.Sprintf(", %d", i*tc.cols+j)
				}
				insertSQL += ")"
				if _, err := exec.Execute(ctx, nil, insertSQL); err != nil {
					b.Fatalf("Failed to insert: %v", err)
				}
			}

			// CTE query
			cteQuery := `
				WITH data AS (
					SELECT * FROM test_table
				)
				SELECT COUNT(*) FROM data
			`

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, nil, cteQuery)
				if err != nil {
					b.Fatalf("Failed to execute CTE: %v", err)
				}

				// Consume result
				for result.Next() {
					_ = result.Row()
				}
				result.Close()
			}
		})
	}
}
