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
	"time"

	"github.com/stoolap/stoolap-go/internal/common"
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestBooleanColumnarIndexBulkDelete tests a bulk insert of 50,000 rows with a boolean column,
// deletion of rows where active=true (half of the rows), and verification of the count.
// The table has a columnar index on the boolean column.
func TestBooleanColumnarIndexBulkDelete(t *testing.T) {
	tempDir := common.TempDir(t)

	// Create a temporary database in memory
	dbPath := "file://" + tempDir + "/boolean_test.db?persistence=true&sync_mode=normal&snapshot_interval=1"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	start := time.Now()

	// Create a table with a boolean column
	_, err = db.Exec("CREATE TABLE boolean_test (active BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a columnar index on the active column
	_, err = db.Exec("CREATE COLUMNAR INDEX ON boolean_test (active)")
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Begin a transaction for bulk insert
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Prepare statement for bulk insert
	stmt, err := tx.Prepare("INSERT INTO boolean_test VALUES (?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Insert 50,000 rows, half with active=true, half with active=false
	const rowCount = 50000
	for i := 0; i < rowCount; i++ {
		active := i%2 == 0 // Even indices are active=true, odd are active=false
		_, err = stmt.Exec(active)
		if err != nil {
			t.Fatalf("Failed to insert data at row %d: %v", i, err)
		}
	}

	// Close the prepared statement
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Failed to close prepared statement: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	elapsed := time.Since(start)

	t.Logf("Bulk insert completed in %s", elapsed)

	time.Sleep(time.Second) // Sleep to ensure the data is persisted

	db.Close()

	db, err = sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database after commit: %v", err)
	}
	defer db.Close()

	// Verify the total number of rows before deletion
	var initialCount int
	err = db.QueryRow("SELECT COUNT(*) FROM boolean_test").Scan(&initialCount)
	if err != nil {
		t.Fatalf("Failed to count initial rows: %v", err)
	}

	if initialCount != rowCount {
		t.Errorf("Expected %d initial rows, got %d", rowCount, initialCount)
	}

	// Verify that half the rows have active=true
	var trueCount int
	err = db.QueryRow("SELECT COUNT(*) FROM boolean_test WHERE active = true").Scan(&trueCount)
	if err != nil {
		t.Fatalf("Failed to count active=true rows: %v", err)
	}

	expectedTrueCount := rowCount / 2
	if trueCount != expectedTrueCount {
		t.Errorf("Expected %d rows with active=true, got %d", expectedTrueCount, trueCount)
	}

	// Delete all rows where active=true
	result, err := db.Exec("DELETE FROM boolean_test WHERE active = true")
	if err != nil {
		t.Fatalf("Failed to delete active=true rows: %v", err)
	}

	// Check the number of affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if rowsAffected != int64(expectedTrueCount) {
		t.Errorf("Expected %d rows to be deleted, got %d", expectedTrueCount, rowsAffected)
	}

	// Verify the final count after deletion
	var finalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM boolean_test").Scan(&finalCount)
	if err != nil {
		t.Fatalf("Failed to count rows after deletion: %v", err)
	}

	expectedFinalCount := rowCount - expectedTrueCount
	if finalCount != expectedFinalCount {
		t.Errorf("Expected %d rows after deletion, got %d", expectedFinalCount, finalCount)
	}

	// Verify no rows with active=true remain
	var remainingTrueCount int
	err = db.QueryRow("SELECT COUNT(*) FROM boolean_test WHERE active = true").Scan(&remainingTrueCount)
	if err != nil {
		t.Fatalf("Failed to count remaining active=true rows: %v", err)
	}

	if remainingTrueCount != 0 {
		t.Errorf("Expected 0 rows with active=true to remain, got %d", remainingTrueCount)
	}

	// Verify all remaining rows have active=false
	var falseCount int
	err = db.QueryRow("SELECT COUNT(*) FROM boolean_test WHERE active = false").Scan(&falseCount)
	if err != nil {
		t.Fatalf("Failed to count active=false rows: %v", err)
	}

	if falseCount != expectedFinalCount {
		t.Errorf("Expected all %d remaining rows to have active=false, got %d", expectedFinalCount, falseCount)
	}
}
