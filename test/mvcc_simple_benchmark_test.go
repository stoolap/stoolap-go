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
	_ "github.com/stoolap/stoolap/pkg/driver"
)

// BenchmarkMVCCInsert provides a simple benchmark for MVCC insert operations
func BenchmarkMVCCInsertPersistent(b *testing.B) {
	tempDir := b.TempDir()

	// Open a connection to the database
	db, err := sql.Open("stoolap", "file://"+tempDir+"?sync_mode=normal")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_insert_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := db.Exec(
			insertQuery,
			i+1,
			"item_"+strconv.Itoa(i+1),
			float64(i)*1.5,
			i%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}

		// Verify the insert count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != 1 {
			b.Fatalf("Expected to insert %d rows, but updated %d", 1, rowsAffected)
		}
	}
}

// BenchmarkMVCCInsertBatch provides a benchmark for batch insert operations
func BenchmarkMVCCInsertBatch(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_insert_batch_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Number of rows to insert in each batch
	insertCount := 100
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(insertCount), "rows/op")

	for i := 0; i < b.N; i++ {
		// Begin a transaction for the batch insert
		tx, err := db.Begin()
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		baseID := i * insertCount

		// Prepare the statement once for all inserts
		stmt, err := tx.Prepare(insertQuery)
		if err != nil {
			tx.Rollback()
			b.Fatalf("Failed to prepare statement: %v", err)
		}

		// Compute current time once for all rows to reduce allocations
		currentTime := time.Now()

		// Insert rows by reusing the prepared statement
		var totalRowsAffected int64
		for j := 0; j < insertCount; j++ {
			id := baseID + j + 1
			result, err := stmt.Exec(
				id,
				"batch_item_"+strconv.Itoa(id),
				float64(id)*1.5,
				id%2 == 0,
				currentTime)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				b.Fatalf("Failed to execute batch insert: %v", err)
			}

			// Accumulate rows affected
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				stmt.Close()
				tx.Rollback()
				b.Fatalf("Failed to get rows affected: %v", err)
			}
			totalRowsAffected += rowsAffected
		}

		// Clean up statement
		stmt.Close()

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify the insert count
		if totalRowsAffected != int64(insertCount) {
			b.Fatalf("Expected to insert %d rows, but inserted %d", insertCount, totalRowsAffected)
		}
	}
}

// BenchmarkMVCCInsertBatchBulk provides a benchmark for bulk insert operations using a single multi-value insert
func BenchmarkMVCCBulkInsertBatch(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_insert_batch_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Number of rows to insert in each batch
	insertCount := 100

	var valuesClause string
	for j := 0; j < insertCount; j++ {
		if j > 0 {
			valuesClause += ", "
		}
		valuesClause += "(?, ?, ?, ?, ?)"
	}

	// Execute the batch insert
	batchInsertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES %s",
		tableName, valuesClause)

	args := make([]interface{}, 0, 5*insertCount)

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(insertCount), "rows/op")

	for i := 0; i < b.N; i++ {
		// Build a batch insert query with VALUES for multiple rows

		baseID := i * insertCount
		args = args[:0] // Reset the args slice for reuse

		// Build the VALUES clause and parameters for 100 rows
		for j := 0; j < insertCount; j++ {
			id := baseID + j + 1
			args = append(args, id)
			args = append(args, "batch_item_"+strconv.Itoa(id))
			args = append(args, float64(id)*1.5)
			args = append(args, id%2 == 0)
			args = append(args, "NOW()")
		}

		result, err := db.Exec(batchInsertQuery, args...)
		if err != nil {
			b.Fatalf("Batch insert failed: %v", err)
		}

		// Verify the insert count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != int64(insertCount) {
			b.Fatalf("Expected to insert %d rows, but inserted %d", insertCount, rowsAffected)
		}
	}
}

// BenchmarkMVCCUpdate provides a simple benchmark for MVCC update operations
func BenchmarkMVCCPrimaryKeySingleUpdate(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_update_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert a sufficient number of rows for updates
	count := b.N * 100

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)
	for i := 0; i < count; i++ {
		_, err := db.Exec(
			insertQuery,
			i+1,
			"item_"+strconv.Itoa(i+1),
			float64(i)*1.5,
			i%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	updateQuery := fmt.Sprintf("UPDATE %s SET value = ?, active = ? WHERE id = ?", tableName)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := (i % count) + 1 // Cycle through existing IDs

		result, err := db.Exec(
			updateQuery,
			float64(i)*3.14,
			!(i%2 == 0), // Flip the active flag
			id)
		if err != nil {
			b.Fatalf("Update failed: %v", err)
		}

		// Verify the update count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != 1 {
			b.Fatalf("Expected to update %d rows, but updated %d", 1, rowsAffected)
		}
	}
}

// BenchmarkMVCCUpdateBatch provides a benchmark for batch update operations with a range condition
func BenchmarkMVCCPrimaryKeyUpdateBatch(b *testing.B) {
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
	b.ReportMetric(float64(updateCount), "rows/op")

	updateQuery := fmt.Sprintf("UPDATE %s SET value = value * 2, active = CASE WHEN active THEN false ELSE true END WHERE id > ? AND id <= ?", tableName)

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

// BenchmarkMVCCColumnarIndexUpdateBatch provides a benchmark for batch update operations with a range condition
func BenchmarkMVCCColumnarIndexUpdateBatch(b *testing.B) {
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
	b.ReportMetric(float64(updateCount), "rows/op")

	updateQuery := fmt.Sprintf("UPDATE %s SET active = CASE WHEN active THEN false ELSE true END WHERE value > ? AND value <= ?", tableName)

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

func BenchmarkMVCCNoIndexUpdateBatch(b *testing.B) {
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
	b.ReportMetric(float64(updateCount), "rows/op")

	updateQuery := fmt.Sprintf("UPDATE %s SET active = CASE WHEN active THEN false ELSE true END WHERE value > ? AND value <= ?", tableName)

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

// BenchmarkMVCCPrimaryKeyDelete provides a simple benchmark for MVCC delete operations
func BenchmarkMVCCPrimaryKeySingleDelete(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_delete_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert a batch of rows (1000) regardless of b.N
	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)
	count := b.N * 100
	for i := 0; i < count; i++ {
		_, err := db.Exec(
			insertQuery,
			i+1,
			"to_delete_"+strconv.Itoa(i+1),
			float64(i)*1.5,
			i%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	delQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := (i % count) + 1
		result, err := db.Exec(delQuery, id)
		if err != nil {
			b.Fatalf("Delete failed: %v", err)
		}

		// Verify the delete count
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			b.Fatalf("Failed to get rows affected: %v", err)
		}
		if rowsAffected != 1 {
			b.Fatalf("Expected to delete %d rows, but updated %d", 1, rowsAffected)
		}
	}
}

// BenchmarkMVCCDeleteBatch provides a benchmark for batch delete operations with a range condition
func BenchmarkMVCCPrimaryKeyDeleteBatch(b *testing.B) {
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

	deleteCount := 100
	insertCount := deleteCount * b.N

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"to_delete_"+strconv.Itoa(j+1),
			float64(j)*1.5,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id > ? AND id <= ?", tableName)

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(deleteCount), "rows/op")

	for i := 0; i < b.N; i++ {
		id := i * deleteCount
		result, err := db.Exec(deleteQuery, id, id+deleteCount)
		if err != nil {
			b.Fatalf("Batch delete failed: %v\n", err)
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

// BenchmarkMVCCColumnarIndexDeleteBatch provides a benchmark for batch delete operations with a range condition
func BenchmarkMVCCColumnarIndexDeleteBatch(b *testing.B) {
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

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"to_delete_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE value > ? AND value <= ?", tableName)

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(deleteCount), "rows/op")

	for i := 0; i < b.N; i++ {
		id := i * deleteCount
		result, err := db.Exec(deleteQuery, float64(id), float64(id+deleteCount))
		if err != nil {
			b.Fatalf("Batch delete failed: %v\n", err)
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

func BenchmarkMVCCNoIndexDeleteBatch(b *testing.B) {
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

	deleteCount := 100
	insertCount := deleteCount * b.N

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)

	for j := 0; j < insertCount; j++ {
		_, err := db.Exec(
			insertQuery,
			j+1,
			"to_delete_"+strconv.Itoa(j+1),
			float64(j)+1,
			j%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE value > ? AND value <= ?", tableName)

	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(deleteCount), "rows/op")

	for i := 0; i < b.N; i++ {
		id := i * deleteCount
		result, err := db.Exec(deleteQuery, float64(id), float64(id+deleteCount))
		if err != nil {
			b.Fatalf("Batch delete failed: %v\n", err)
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

	b.StopTimer()
}

// BenchmarkMVCCSelect provides a simple benchmark for MVCC select operations
func BenchmarkMVCCSelect(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a unique table name
	tableName := fmt.Sprintf("mvcc_select_bench_%d", time.Now().UnixNano())

	// Create the test table
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, name TEXT, value FLOAT, active BOOLEAN, created TEXT)", tableName))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (id, name, value, active, created) VALUES (?, ?, ?, ?, ?)", tableName)
	// Insert test data - 1000 rows is reasonable for select benchmark
	count := 1000
	for i := 0; i < count; i++ {
		_, err := db.Exec(
			insertQuery,
			i+1,
			"item_"+strconv.Itoa(i+1),
			float64(i+1),
			i%2 == 0,
			time.Now())
		if err != nil {
			b.Fatalf("Setup insert failed: %v", err)
		}
	}

	selectQuery := fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE id = ?", tableName)
	// Benchmark different select operations
	b.Run("PrimaryKey", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rowID := (i % count) + 1 // Cycle through existing IDs

			var id int
			var name string
			var value float64
			var active bool
			err := db.QueryRow(
				selectQuery,
				rowID).Scan(&id, &name, &value, &active)
			if err != nil {
				b.Fatalf("Select by ID failed: %v", err)
			}
		}
	})

	selectQuery = fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE CAST(id AS TEXT) = ?", tableName)
	// Benchmark different select operations
	b.Run("ComplexFilter", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rowID := (i % count) + 1 // Cycle through existing IDs

			var id int
			var name string
			var value float64
			var active bool
			err := db.QueryRow(
				selectQuery,
				strconv.Itoa(rowID)).Scan(&id, &name, &value, &active)
			if err != nil {
				b.Fatalf("Select by ID failed: %v", err)
			}
		}
	})

	_, err = db.Exec(fmt.Sprintf("CREATE COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	selectQuery = fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE value = ?", tableName)
	b.Run("ColumnarIndex", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := float64((i % count) + 1) // Cycle through existing IDs

			var id int
			var name string
			var value float64
			var active bool
			err := db.QueryRow(
				selectQuery,
				v).Scan(&id, &name, &value, &active)
			if err != nil {
				b.Fatalf("Select by columnar index of value failed: %v", err)
			}
		}
	})

	rangeQuery := fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE id > ? AND id <= ?", tableName)

	b.Run("RangePrimaryKey", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				rangeQuery,
				0, 100)
			if err != nil {
				b.Fatalf("Range select failed: %v", err)
			}

			// Process all rows to ensure query is fully executed
			var id int
			var name string
			var value float64
			var active bool
			rowCount := 0
			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &active)
				if err != nil {
					rows.Close()
					b.Fatalf("Row scan failed: %v", err)
				}
				rowCount++
			}
			rows.Close()

			if rowCount != 100 {
				b.Fatalf("Expected 100 rows, got %d", rowCount)
			}
		}
	})

	_, err = db.Exec(fmt.Sprintf("DROP COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to drop index: %v", err)
	}

	filteredQuery := fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE value > ? AND value <= ? AND active = true", tableName)

	b.Run("RangeNoIndex", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				filteredQuery,
				float64(0), float64(100))
			if err != nil {
				b.Fatalf("Range select failed: %v", err)
			}

			// Process all rows to ensure query is fully executed
			var id int
			var name string
			var value float64
			var active bool
			rowCount := 0
			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &active)
				if err != nil {
					rows.Close()
					b.Fatalf("Row scan failed: %v", err)
				}
				rowCount++
			}
			rows.Close()

			if rowCount != 50 {
				b.Fatalf("Expected 50 rows, got %d", rowCount)
			}
		}
	})

	_, err = db.Exec(fmt.Sprintf("CREATE COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	b.Run("RangeWithColumnarIndex", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				filteredQuery,
				float64(0), float64(100))
			if err != nil {
				b.Fatalf("Range select failed: %v", err)
			}

			// Process all rows to ensure query is fully executed
			var id int
			var name string
			var value float64
			var active bool
			rowCount := 0
			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &active)
				if err != nil {
					rows.Close()
					b.Fatalf("Row scan failed: %v", err)
				}
				rowCount++
			}
			rows.Close()

			if rowCount != 100 {
				b.Fatalf("Expected 100 rows, got %d", rowCount)
			}
		}
	})

	_, err = db.Exec(fmt.Sprintf("DROP COLUMNAR INDEX ON %s (value)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE INDEX multi_idx_test ON %s (value, active)", tableName))
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	filteredQuery = fmt.Sprintf("SELECT id, name, value, active FROM %s WHERE value > ? AND value <= ? AND active = true", tableName)

	b.Run("RangeWithMultiColumnarIndex", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				filteredQuery,
				float64(0), float64(100))
			if err != nil {
				b.Fatalf("Range select failed: %v", err)
			}

			// Process all rows to ensure query is fully executed
			var id int
			var name string
			var value float64
			var active bool
			rowCount := 0
			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &active)
				if err != nil {
					rows.Close()
					b.Fatalf("Row scan failed: %v", err)
				}
				rowCount++
			}
			rows.Close()

			if rowCount != 50 {
				b.Fatalf("Expected 50 rows, got %d", rowCount)
			}
		}
	})
}
