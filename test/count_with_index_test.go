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
	"testing"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestCountWithWhereClauseAndIndex tests that COUNT(*) with a WHERE clause works correctly with indexes
func TestCountWithWhereClauseAndIndex(t *testing.T) {
	// Open connection to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with primary key and a boolean column
	_, err = db.Exec(`
		CREATE TABLE test_count (
			id INTEGER PRIMARY KEY,
			active BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on the boolean column
	_, err = db.Exec(`
		CREATE COLUMNAR INDEX ON test_count (active)
	`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert 50,000 records with a mix of true/false values
	t.Log("Inserting 50,000 records...")

	// Use a transaction for faster inserts
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Prepare statement for better performance
	stmt, err := tx.Prepare("INSERT INTO test_count (id, active) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Define the number of active records (true values) to insert
	numTrue := 25000
	numTotal := 50000

	// Insert records - half true, half false
	startTime := time.Now()
	for i := 1; i <= numTotal; i++ {
		// First half of the records will be active (true), second half inactive (false)
		isActive := i <= numTrue

		_, err := stmt.Exec(i, isActive)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	// Close statement and commit transaction
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	insertDuration := time.Since(startTime)
	t.Logf("Inserted %d records in %v", numTotal, insertDuration)

	// Test 1: Count all records
	var totalCount int
	startTime = time.Now()
	err = db.QueryRow("SELECT COUNT(*) FROM test_count").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count all records: %v", err)
	}
	countAllDuration := time.Since(startTime)
	t.Logf("Count all query took %v", countAllDuration)

	if totalCount != numTotal {
		t.Errorf("Expected %d total records, got %d", numTotal, totalCount)
	}

	// Test 2: Count active records with WHERE clause (should use the index)
	var activeCount int
	startTime = time.Now()
	err = db.QueryRow("SELECT COUNT(*) FROM test_count WHERE active = ?", true).Scan(&activeCount)
	if err != nil {
		t.Fatalf("Failed to count active records: %v", err)
	}
	countActiveDuration := time.Since(startTime)
	t.Logf("Count with WHERE clause took %v", countActiveDuration)

	if activeCount != numTrue {
		t.Errorf("Expected %d active records, got %d", numTrue, activeCount)
	}

	// Test 3: Count inactive records with WHERE clause (should use the index)
	var inactiveCount int
	startTime = time.Now()
	err = db.QueryRow("SELECT COUNT(*) FROM test_count WHERE active = ?", false).Scan(&inactiveCount)
	if err != nil {
		t.Fatalf("Failed to count inactive records: %v", err)
	}
	countInactiveDuration := time.Since(startTime)
	t.Logf("Count with WHERE clause (inactive) took %v", countInactiveDuration)

	if inactiveCount != (numTotal - numTrue) {
		t.Errorf("Expected %d inactive records, got %d", numTotal-numTrue, inactiveCount)
	}

	// Test 4: Verify that active count + inactive count equals total count
	if activeCount+inactiveCount != totalCount {
		t.Errorf("Sum of active (%d) and inactive (%d) counts doesn't equal total count (%d)",
			activeCount, inactiveCount, totalCount)
	}
}

// TestCountWithWhereClauseAndIndexMultipleQueries performs multiple COUNT queries
// to verify consistency
func TestCountWithWhereClauseAndIndexMultipleQueries(t *testing.T) {
	// Open connection to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with primary key and a boolean column
	_, err = db.Exec(`
		CREATE TABLE test_multiple (
			id INTEGER PRIMARY KEY,
			active BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on the boolean column
	_, err = db.Exec(`
		CREATE COLUMNAR INDEX ON test_multiple (active)
	`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert 10,000 records with alternating true/false values
	t.Log("Inserting 10,000 records with alternating values...")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO test_multiple (id, active) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	numRecords := 10000
	expectedTrue := 0

	for i := 1; i <= numRecords; i++ {
		isActive := i%2 == 0 // Even IDs are active
		if isActive {
			expectedTrue++
		}

		_, err := stmt.Exec(i, isActive)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Run COUNT queries multiple times to verify consistency
	const numRuns = 5

	// Test total counts
	for i := 0; i < numRuns; i++ {
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_multiple").Scan(&count)
		if err != nil {
			t.Fatalf("Run %d: Failed to count records: %v", i, err)
		}

		if count != numRecords {
			t.Errorf("Run %d: Expected %d total records, got %d", i, numRecords, count)
		}
	}

	// Test active counts
	for i := 0; i < numRuns; i++ {
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_multiple WHERE active = ?", true).Scan(&count)
		if err != nil {
			t.Fatalf("Run %d: Failed to count active records: %v", i, err)
		}

		if count != expectedTrue {
			t.Errorf("Run %d: Expected %d active records, got %d", i, expectedTrue, count)
		}
	}

	// Test inactive counts
	for i := 0; i < numRuns; i++ {
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_multiple WHERE active = ?", false).Scan(&count)
		if err != nil {
			t.Fatalf("Run %d: Failed to count inactive records: %v", i, err)
		}

		if count != (numRecords - expectedTrue) {
			t.Errorf("Run %d: Expected %d inactive records, got %d", i, numRecords-expectedTrue, count)
		}
	}
}

// TestCountWithWhereClauseAndIndexBulkDataModification tests COUNT with WHERE clause after data modifications
func TestCountWithWhereClauseAndIndexBulkDataModification(t *testing.T) {
	// Open connection to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with primary key and a boolean column
	_, err = db.Exec(`
		CREATE TABLE test_bulk (
			id INTEGER PRIMARY KEY,
			active BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index on the boolean column
	_, err = db.Exec(`
		CREATE COLUMNAR INDEX ON test_bulk (active)
	`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert initial 10,000 records - all active=true
	t.Log("Inserting 10,000 records (all active=true)...")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO test_bulk (id, active) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	numRecords := 10000

	for i := 1; i <= numRecords; i++ {
		_, err := stmt.Exec(i, true) // All active=true
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify initial state
	var count, totalCount int

	// Count total records first
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count total records: %v", err)
	}

	t.Logf("Initial total count: %d", totalCount)

	// Count active records
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk WHERE active = ?", true).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count active records: %v", err)
	}

	t.Logf("Initial active count: %d", count)

	if count != numRecords {
		t.Errorf("Initial state: Expected %d active records, got %d", numRecords, count)
	}

	// Modify half the records to active=false
	t.Log("Updating half the records to active=false...")

	updateStart := time.Now()
	result, err := db.Exec("UPDATE test_bulk SET active = ? WHERE id <= ?", false, numRecords/2)
	if err != nil {
		t.Fatalf("Failed to update records: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if int(rowsAffected) != numRecords/2 {
		t.Errorf("Expected to update %d records, but affected %d", numRecords/2, rowsAffected)
	}

	updateDuration := time.Since(updateStart)
	t.Logf("Update completed in %v", updateDuration)

	// Verify counts after update
	var activeCount, inactiveCount int
	var afterUpdateTotal int

	// Get total count first
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk").Scan(&afterUpdateTotal)
	if err != nil {
		t.Fatalf("Failed to get total count after update: %v", err)
	}
	t.Logf("After update - total count: %d", afterUpdateTotal)

	// Count active records
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk WHERE active = ?", true).Scan(&activeCount)
	if err != nil {
		t.Fatalf("Failed to count active records after update: %v", err)
	}
	t.Logf("After update - active count: %d", activeCount)

	// Count inactive records
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk WHERE active = ?", false).Scan(&inactiveCount)
	if err != nil {
		t.Fatalf("Failed to count inactive records after update: %v", err)
	}
	t.Logf("After update - inactive count: %d", inactiveCount)

	// Double-check by counting records directly for troubleshooting
	rows, err := db.Query("SELECT id, active FROM test_bulk")
	if err != nil {
		t.Fatalf("Failed to query records: %v", err)
	}

	var directActiveCount, directInactiveCount int
	for rows.Next() {
		var id int
		var active bool
		if err := rows.Scan(&id, &active); err != nil {
			rows.Close()
			t.Fatalf("Failed to scan row: %v", err)
		}

		if active {
			directActiveCount++
		} else {
			directInactiveCount++
		}
	}
	rows.Close()

	t.Logf("Direct counts - active: %d, inactive: %d, total: %d",
		directActiveCount, directInactiveCount, directActiveCount+directInactiveCount)

	expectedActive := numRecords / 2
	expectedInactive := numRecords / 2

	if activeCount != expectedActive {
		t.Errorf("After update: Expected %d active records, got %d", expectedActive, activeCount)
	}

	if inactiveCount != expectedInactive {
		t.Errorf("After update: Expected %d inactive records, got %d", expectedInactive, inactiveCount)
	}

	// Verify that counts add up to total
	if activeCount+inactiveCount != afterUpdateTotal {
		t.Errorf("After update: Sum of active (%d) and inactive (%d) doesn't equal total (%d)",
			activeCount, inactiveCount, afterUpdateTotal)
	}

	// Delete half of the active records
	t.Log("Deleting half of the active records...")

	directActiveCount = 0
	directInactiveCount = 0

	deleteStart := time.Now()
	result, err = db.Exec("DELETE FROM test_bulk WHERE active = ? AND id > ? AND id <= ?",
		true, numRecords/2, numRecords/2+numRecords/4)
	if err != nil {
		t.Fatalf("Failed to delete records: %v", err)
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected for delete: %v", err)
	}

	expectedDeleted := numRecords / 4
	if int(rowsAffected) != expectedDeleted {
		t.Errorf("Expected to delete %d records, but affected %d", expectedDeleted, rowsAffected)
	}

	deleteDuration := time.Since(deleteStart)
	t.Logf("Delete completed in %v", deleteDuration)

	// Final verification
	var finalTotal int
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk").Scan(&finalTotal)
	if err != nil {
		t.Fatalf("Failed to count total records after delete: %v", err)
	}
	t.Logf("After delete - total count: %d", finalTotal)

	expectedTotal := numRecords - expectedDeleted
	if finalTotal != expectedTotal {
		t.Errorf("Final total: Expected %d records, got %d", expectedTotal, finalTotal)
	}

	// Count active records
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk WHERE active = ?", true).Scan(&activeCount)
	if err != nil {
		t.Fatalf("Failed to count active records after delete: %v", err)
	}
	t.Logf("After delete - active count: %d", activeCount)

	// Count inactive records
	err = db.QueryRow("SELECT COUNT(*) FROM test_bulk WHERE active = ?", false).Scan(&inactiveCount)
	if err != nil {
		t.Fatalf("Failed to count inactive records after delete: %v", err)
	}
	t.Logf("After delete - inactive count: %d", inactiveCount)

	// Perform a detailed analysis for debugging
	// Double-check by counting records directly for troubleshooting
	rows, err = db.Query("SELECT id, active FROM test_bulk ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query records: %v", err)
	}

	var idCounts = make(map[int]bool)

	for rows.Next() {
		var id int
		var active bool
		if err := rows.Scan(&id, &active); err != nil {
			rows.Close()
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Check for duplicate IDs
		if _, exists := idCounts[id]; exists {
			t.Errorf("Duplicate ID found: %d", id)
		}
		idCounts[id] = true

		if active {
			directActiveCount++
		} else {
			directInactiveCount++
		}
	}
	rows.Close()

	t.Logf("Direct count after delete - active: %d, inactive: %d, total: %d",
		directActiveCount, directInactiveCount, directActiveCount+directInactiveCount)
	t.Logf("Unique IDs found: %d", len(idCounts))

	// Debug: Print ID ranges still in the database
	var minID, maxID int
	err = db.QueryRow("SELECT MIN(id), MAX(id) FROM test_bulk").Scan(&minID, &maxID)
	if err == nil {
		t.Logf("ID range in database: %d to %d", minID, maxID)
	}

	// Log the expected counts
	expectedActive = numRecords / 4 // Half of the remaining active records
	t.Logf("Expected active count: %d", expectedActive)

	if activeCount != expectedActive {
		t.Errorf("After delete: Expected %d active records, got %d", expectedActive, activeCount)
	}

	if activeCount+inactiveCount != finalTotal {
		t.Errorf("After delete: Sum of active (%d) and inactive (%d) doesn't equal total (%d)",
			activeCount, inactiveCount, finalTotal)
	}
}

// TestCountWithMoreComplexFiltersAndIndex tests COUNT with more complex WHERE conditions
func TestCountWithMoreComplexFiltersAndIndex(t *testing.T) {
	// Open connection to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with primary key, boolean, and integer columns
	_, err = db.Exec(`
		CREATE TABLE test_complex (
			id INTEGER PRIMARY KEY,
			active BOOLEAN,
			category INTEGER,
			value FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create indexes
	_, err = db.Exec(`CREATE COLUMNAR INDEX ON test_complex (active)`)
	if err != nil {
		t.Fatalf("Failed to create active index: %v", err)
	}

	_, err = db.Exec(`CREATE COLUMNAR INDEX ON test_complex (category)`)
	if err != nil {
		t.Fatalf("Failed to create category index: %v", err)
	}

	// Insert test data
	t.Log("Inserting test data with various values...")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare(
		"INSERT INTO test_complex (id, active, category, value) VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	numRecords := 20000
	var expectedCounts = map[string]int{
		"active_true":       0,
		"active_false":      0,
		"cat_1":             0,
		"cat_2":             0,
		"cat_3":             0,
		"cat_other":         0,
		"active_true_cat_1": 0,
		"value_gt_50":       0,
	}

	for i := 1; i <= numRecords; i++ {
		// Determine values based on record number
		isActive := i%3 != 0            // 2/3 of records are active
		category := (i % 5) + 1         // Categories 1-5, evenly distributed
		value := float64((i % 100) + 1) // Values 1-100, evenly distributed

		// Update expected counts
		if isActive {
			expectedCounts["active_true"]++
		} else {
			expectedCounts["active_false"]++
		}

		if category == 1 {
			expectedCounts["cat_1"]++
			if isActive {
				expectedCounts["active_true_cat_1"]++
			}
		} else if category == 2 {
			expectedCounts["cat_2"]++
		} else if category == 3 {
			expectedCounts["cat_3"]++
		} else {
			expectedCounts["cat_other"]++
		}

		if value > 50 {
			expectedCounts["value_gt_50"]++
		}

		_, err := stmt.Exec(i, isActive, category, value)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test 1: Basic active filter
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_complex WHERE active = ?", true).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count active records: %v", err)
	}

	if count != expectedCounts["active_true"] {
		t.Errorf("Active count: Expected %d, got %d", expectedCounts["active_true"], count)
	}

	// Test 2: Category filter
	err = db.QueryRow("SELECT COUNT(*) FROM test_complex WHERE category = ?", 1).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count category 1 records: %v", err)
	}

	if count != expectedCounts["cat_1"] {
		t.Errorf("Category 1 count: Expected %d, got %d", expectedCounts["cat_1"], count)
	}

	// Test 3: Combined filters (active AND category)
	err = db.QueryRow("SELECT COUNT(*) FROM test_complex WHERE active = ? AND category = ?",
		true, 1).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count active category 1 records: %v", err)
	}

	if count != expectedCounts["active_true_cat_1"] {
		t.Errorf("Active category 1 count: Expected %d, got %d",
			expectedCounts["active_true_cat_1"], count)
	}

	// Test 4: Range filter (value > 50)
	err = db.QueryRow("SELECT COUNT(*) FROM test_complex WHERE value > ?", 50.0).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records with value > 50: %v", err)
	}

	if count != expectedCounts["value_gt_50"] {
		t.Errorf("Value > 50 count: Expected %d, got %d",
			expectedCounts["value_gt_50"], count)
	}

	// Test 5: Complex filter (active AND (category = 1 OR category = 2))
	var complexCount int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM test_complex 
		WHERE active = ? AND (category = ? OR category = ?)
	`, true, 1, 2).Scan(&complexCount)
	if err != nil {
		t.Fatalf("Failed to execute complex count query: %v", err)
	}

	expectedComplex := 0
	// Manually calculate the expected result
	for _, cat := range []string{"cat_1", "cat_2"} {
		var catActiveCount int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM test_complex 
			WHERE active = ? AND category = ?
		`, true, cat[4:]).Scan(&catActiveCount)
		if err != nil {
			t.Fatalf("Failed to count active %s records: %v", cat, err)
		}
		expectedComplex += catActiveCount
	}

	if complexCount != expectedComplex {
		t.Errorf("Complex count: Expected %d, got %d", expectedComplex, complexCount)
	}

	// Verify that our counts are consistent
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM test_complex").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to get total count: %v", err)
	}

	if totalCount != numRecords {
		t.Errorf("Total count: Expected %d, got %d", numRecords, totalCount)
	}

	if expectedCounts["active_true"]+expectedCounts["active_false"] != numRecords {
		t.Errorf("Sum of active (%d) and inactive (%d) doesn't equal total (%d)",
			expectedCounts["active_true"], expectedCounts["active_false"], numRecords)
	}
}

// BenchmarkCountWithIndex benchmarks COUNT(*) with a WHERE clause using an index
func BenchmarkCountWithIndex(b *testing.B) {
	// Open connection to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with primary key and a boolean column
	_, err = db.Exec(`
		CREATE TABLE bench_count (
			id INTEGER PRIMARY KEY,
			active BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Create index on the boolean column
	_, err = db.Exec(`
		CREATE COLUMNAR INDEX ON bench_count (active)
	`)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	// Insert 50,000 records with a mix of true/false values
	fmt.Println("Inserting 50,000 records for benchmark...")

	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO bench_count (id, active) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		b.Fatalf("Failed to prepare statement: %v", err)
	}

	numRecords := 50000

	for i := 1; i <= numRecords; i++ {
		// Even IDs are active
		isActive := i%2 == 0

		_, err := stmt.Exec(i, isActive)
		if err != nil {
			tx.Rollback()
			b.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit transaction: %v", err)
	}

	// Reset timer before benchmarking
	b.ResetTimer()

	// Benchmark COUNT(*) with WHERE clause
	b.Run("CountWithWhereTrue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow("SELECT COUNT(*) FROM bench_count WHERE active = ?", true).Scan(&count)
			if err != nil {
				b.Fatalf("Failed to execute count query: %v", err)
			}
			if count != numRecords/2 {
				b.Fatalf("Incorrect count: expected %d, got %d", numRecords/2, count)
			}
		}
	})

	b.Run("CountWithWhereFalse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow("SELECT COUNT(*) FROM bench_count WHERE active = ?", false).Scan(&count)
			if err != nil {
				b.Fatalf("Failed to execute count query: %v", err)
			}
			if count != numRecords/2 {
				b.Fatalf("Incorrect count: expected %d, got %d", numRecords/2, count)
			}
		}
	})

	b.Run("CountTotal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow("SELECT COUNT(*) FROM bench_count").Scan(&count)
			if err != nil {
				b.Fatalf("Failed to execute count query: %v", err)
			}
			if count != numRecords {
				b.Fatalf("Incorrect count: expected %d, got %d", numRecords, count)
			}
		}
	})
}
