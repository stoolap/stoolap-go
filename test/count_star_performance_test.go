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
	"strconv"
	"testing"
	"time"

	// Import for side effects - driver registration
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// BenchmarkCountStarPerformance tests the performance of COUNT(*) queries
// This test specifically benchmarks the optimized COUNT(*) implementation
func BenchmarkCountStarPerformance(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("count_star_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with 10,000 rows for COUNT(*) benchmark
	count := 10000
	batchSize := 1000
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	// Use batches to speed up setup
	for batchStart := 0; batchStart < count; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > count {
			batchEnd = count
		}

		// Begin a transaction for the batch insert
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		// Prepare the statement once for all inserts in this batch
		stmt, err := tx.Prepare(insertQuery)
		if err != nil {
			tx.Rollback()
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		// Compute current time once for all rows to reduce allocations
		currentTime := time.Now()

		// Insert rows by reusing the prepared statement
		for i := batchStart; i < batchEnd; i++ {
			_, err := stmt.Exec(
				i+1,
				"item_"+strconv.Itoa(i+1),
				float64(i+1),
				i%2 == 0,
				currentTime)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				b.Fatalf("Setup insert failed: %v", err)
			}
		}

		// Clean up statement
		stmt.Close()

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}
	}

	// Also create an indexed table for comparison
	indexedTableName := fmt.Sprintf("count_star_indexed_bench_%d", time.Now().UnixNano())

	// Create the indexed test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", indexedTableName))
	if err != nil {
		b.Fatalf("Failed to create indexed table: %v", err)
	}

	// Create a columnar index on value
	_, err = db.Exec(fmt.Sprintf("CREATE COLUMNAR INDEX ON %s (value)", indexedTableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	// Insert the same test data into the indexed table
	insertQuery = fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", indexedTableName)

	// Use batches to speed up setup for indexed table
	for batchStart := 0; batchStart < count; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > count {
			batchEnd = count
		}

		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		stmt, err := tx.Prepare(insertQuery)
		if err != nil {
			tx.Rollback()
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		currentTime := time.Now()

		for i := batchStart; i < batchEnd; i++ {
			_, err := stmt.Exec(
				i+1,
				"item_"+strconv.Itoa(i+1),
				float64(i+1),
				i%2 == 0,
				currentTime)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				b.Fatalf("Setup insert failed for indexed table: %v", err)
			}
		}

		stmt.Close()

		if err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit transaction for indexed table: %v", err)
		}
	}

	// Benchmark 1: Simple COUNT(*) without WHERE clause
	// This should use the fast path with direct RowCount() method
	b.Run("Simple_COUNT_STAR", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) query failed: %v", err)
			}

			if count != 10000 {
				b.Fatalf("Expected count 10000, got %d", count)
			}
		}
	})

	// Benchmark 2: COUNT(*) with WHERE clause on primary key
	// This should use the optimized column selection
	b.Run("COUNT_STAR_With_PK_Filter", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id <= 5000", tableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) with WHERE query failed: %v", err)
			}

			if count != 5000 {
				b.Fatalf("Expected count 5000, got %d", count)
			}
		}
	})

	// Benchmark 3: COUNT(*) with WHERE clause on non-indexed column
	// This should use the optimized column selection but still scan all rows
	b.Run("COUNT_STAR_With_NonIndexed_Filter", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE active = true", tableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) with non-indexed WHERE query failed: %v", err)
			}

			if count != 5000 { // Half the rows are active=true
				b.Fatalf("Expected count 5000, got %d", count)
			}
		}
	})

	// Benchmark 4: COUNT(*) with WHERE clause on indexed column
	// This should leverage the columnar index for faster filtering
	b.Run("COUNT_STAR_With_Indexed_Filter", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value <= 5000", indexedTableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) with indexed WHERE query failed: %v", err)
			}

			if count != 5000 {
				b.Fatalf("Expected count 5000, got %d", count)
			}
		}
	})

	// Benchmark 5: COUNT(*) with BETWEEN expression
	// This tests our BetweenExpression optimization
	b.Run("COUNT_STAR_With_Between", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value BETWEEN 1000 AND 5000", indexedTableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) with BETWEEN query failed: %v", err)
			}

			if count != 4001 { // Values 1000 through 5000 inclusive
				b.Fatalf("Expected count 4001, got %d", count)
			}
		}
	})

	// Benchmark 6: COUNT(*) with complex condition
	// This tests multiple conditions that need to be evaluated
	b.Run("COUNT_STAR_With_Complex_Condition", func(b *testing.B) {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE value BETWEEN 1000 AND 5000 AND active = true", indexedTableName)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int64
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatalf("COUNT(*) with complex condition query failed: %v", err)
			}

			// Half of the rows between 1000 and 5000 should have active=true
			if count < 2000 || count > 2001 { // Allow for odd/even distribution
				b.Fatalf("Expected count ~2000, got %d", count)
			}
		}
	})
}
