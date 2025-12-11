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

// Testing update with columnar index on non-primary key column
func BenchmarkMVCCUpdateWithColumnarIndex(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name for each iteration
	tableName := fmt.Sprintf("mvcc_update_batch_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	updateCount := 100
	insertCount := updateCount * b.N

	// Insert a batch of rows (100) to test batch update
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"item_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	updateQuery := fmt.Sprintf("UPDATE %s SET value = value * 2, active = !active WHERE value > ? AND value <= ?", tableName)

	for i := 0; i < b.N; i++ {
		id := i * updateCount
		result, err := db.Exec(updateQuery, float64(id), float64(id+updateCount))
		if err != nil {
			b.Fatalf("Batch update failed: %v", err)
		}

		// Verify the update count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != int64(updateCount) {
			b.Fatalf("Expected to update %d rows, but updated %d", updateCount, rowsAffected)
		}
	}
}

// Run the same test but without columnar index
func BenchmarkMVCCUpdateWithoutIndex(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name for each iteration
	tableName := fmt.Sprintf("mvcc_update_no_index_bench_%d", time.Now().UnixNano())

	// Create the test table without any columnar index
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	updateCount := 100
	insertCount := updateCount * b.N

	// Insert a batch of rows (100) to test batch update
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"item_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Using the same query on value column but without any index
	updateQuery := fmt.Sprintf("UPDATE %s SET value = value * 2, active = !active WHERE value > ? AND value <= ?", tableName)

	for i := 0; i < b.N; i++ {
		id := i * updateCount
		result, err := db.Exec(updateQuery, float64(id), float64(id+updateCount))
		if err != nil {
			b.Fatalf("Batch update failed: %v", err)
		}

		// Verify the update count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != int64(updateCount) {
			b.Fatalf("Expected to update %d rows, but updated %d", updateCount, rowsAffected)
		}
	}
}

// Run with primary key instead to compare
func BenchmarkMVCCUpdateWithPrimaryKey(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name for each iteration
	tableName := fmt.Sprintf("mvcc_update_pk_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	updateCount := 100
	insertCount := updateCount * b.N

	// Insert a batch of rows (100) to test batch update
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"item_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Using primary key here instead of value column
	updateQuery := fmt.Sprintf("UPDATE %s SET value = value * 2, active = !active WHERE id > ? AND id <= ?", tableName)

	for i := 0; i < b.N; i++ {
		id := i * updateCount
		result, err := db.Exec(updateQuery, id, id+updateCount)
		if err != nil {
			b.Fatalf("Batch update failed: %v", err)
		}

		// Verify the update count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != int64(updateCount) {
			b.Fatalf("Expected to update %d rows, but updated %d", updateCount, rowsAffected)
		}
	}
}

// Run the same test but with deletion
func BenchmarkMVCCDeleteWithColumnarIndex(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name for each iteration
	tableName := fmt.Sprintf("mvcc_delete_batch_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	deleteCount := 100
	insertCount := deleteCount * b.N

	// Insert a batch of rows to test batch delete
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"item_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE value > ? AND value <= ?", tableName)

	for i := 0; i < b.N; i++ {
		id := i * deleteCount
		result, err := db.Exec(deleteQuery, float64(id), float64(id+deleteCount))
		if err != nil {
			b.Fatalf("Batch delete failed: %v", err)
		}

		// Verify the delete count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != int64(deleteCount) {
			b.Fatalf("Expected to delete %d rows, but deleted %d", deleteCount, rowsAffected)
		}
	}
}
