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

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestDirectDelete tests a simple DELETE operation with a direct equality condition
func TestDirectDelete(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE test_delete (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a test row
	_, err = db.Exec("INSERT INTO test_delete VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify the row exists
	var exists int
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete").Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if exists != 1 {
		t.Fatalf("Expected 1 row before delete, got %d", exists)
	}

	// Delete the row
	_, err = db.Exec("DELETE FROM test_delete WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to delete row: %v", err)
	}

	// Verify the row was deleted - directly check if the table is empty
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows after delete: %v", err)
	}

	// Check that the row was deleted
	if count != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", count)
	}
}

// TestIntegerComparison tests DELETE with different integer comparison operators
func TestIntegerComparison(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE int_delete (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO int_delete VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Debug to help us understand the schema
	rows, err := db.Query("SELECT * FROM int_delete LIMIT 1")
	if err != nil {
		t.Fatalf("Failed to query schema: %v", err)
	}
	// Get column names
	_, err = rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	rows.Close()

	// Delete rows where id > 5
	_, err = db.Exec("DELETE FROM int_delete WHERE id > 5")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify only rows with id <= 5 remain
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM int_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows to remain after DELETE WHERE id > 5, got %d", count)
	}

	// Verify the correct rows were kept (1-5)
	for i := 1; i <= 10; i++ {
		var rowExists int
		err = db.QueryRow("SELECT COUNT(*) FROM int_delete WHERE id = ?", i).Scan(&rowExists)
		if err != nil {
			t.Fatalf("Failed to check if row exists: %v", err)
		}

		expectedExists := 0
		if i <= 5 {
			expectedExists = 1
		}

		if rowExists != expectedExists {
			t.Errorf("For id=%d, expected %d rows, got %d", i, expectedExists, rowExists)
		}
	}
}

// TestBooleanCondition tests DELETE with boolean conditions
func TestBooleanCondition(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE bool_delete (id INTEGER, active BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows - alternating true/false values
	for i := 1; i <= 10; i++ {
		active := i%2 == 0 // Even ids are active, odd are not
		_, err = db.Exec("INSERT INTO bool_delete VALUES (?, ?)", i, active)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete active rows
	_, err = db.Exec("DELETE FROM bool_delete WHERE active = true")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify only inactive rows remain
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM bool_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows to remain after DELETE WHERE active = true, got %d", count)
	}

	// Verify all remaining rows have active = false
	var inactiveCount int
	err = db.QueryRow("SELECT COUNT(*) FROM bool_delete WHERE active = false").Scan(&inactiveCount)
	if err != nil {
		t.Fatalf("Failed to count inactive rows: %v", err)
	}

	if inactiveCount != 5 {
		t.Errorf("Expected 5 inactive rows to remain, got %d", inactiveCount)
	}
}

// TestStringComparison tests DELETE with string comparison
func TestStringComparison(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE str_delete (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows with different string values
	testData := []struct {
		id   int
		name string
	}{
		{1, "Apple"},
		{2, "Banana"},
		{3, "Cherry"},
		{4, "Date"},
		{5, "Elderberry"},
	}

	for _, d := range testData {
		_, err = db.Exec("INSERT INTO str_delete VALUES (?, ?)", d.id, d.name)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete a specific string
	_, err = db.Exec("DELETE FROM str_delete WHERE name = 'Cherry'")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify the correct row was deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM str_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 4 {
		t.Errorf("Expected 4 rows to remain after DELETE WHERE name = 'Cherry', got %d", count)
	}

	// Verify the 'Cherry' row is gone
	var cherryCount int
	err = db.QueryRow("SELECT COUNT(*) FROM str_delete WHERE name = 'Cherry'").Scan(&cherryCount)
	if err != nil {
		t.Fatalf("Failed to check if Cherry remains: %v", err)
	}

	if cherryCount != 0 {
		t.Errorf("Expected 0 'Cherry' rows to remain, got %d", cherryCount)
	}
}

// TestDeleteWithAnd tests DELETE with AND condition
func TestDeleteWithAnd(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE and_delete (id INTEGER, category TEXT, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	testData := []struct {
		id       int
		category string
		value    float64
	}{
		{1, "A", 10.5},
		{2, "A", 20.0},
		{3, "B", 15.5},
		{4, "B", 25.0},
		{5, "C", 30.5},
		{6, "C", 40.0},
	}

	for _, d := range testData {
		_, err = db.Exec("INSERT INTO and_delete VALUES (?, ?, ?)", d.id, d.category, d.value)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete rows matching both conditions
	_, err = db.Exec("DELETE FROM and_delete WHERE category = 'B' AND value > 20.0")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify the correct row was deleted (should be only id=4)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM and_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows to remain after DELETE with AND condition, got %d", count)
	}

	// Verify id=4 is gone and id=3 (which is also category B) remains
	var id4Count, id3Count int
	err = db.QueryRow("SELECT COUNT(*) FROM and_delete WHERE id = 4").Scan(&id4Count)
	if err != nil {
		t.Fatalf("Failed to check if id=4 remains: %v", err)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM and_delete WHERE id = 3").Scan(&id3Count)
	if err != nil {
		t.Fatalf("Failed to check if id=3 remains: %v", err)
	}

	if id4Count != 0 {
		t.Errorf("Expected 0 rows with id=4 to remain, got %d", id4Count)
	}
	if id3Count != 1 {
		t.Errorf("Expected 1 row with id=3 to remain, got %d", id3Count)
	}
}

// TestDeleteAllRows tests deleting all rows in a table without a WHERE clause
func TestDeleteAllRows(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE delete_all_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO delete_all_test VALUES (?, ?)", i, fmt.Sprintf("Name-%d", i))
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Verify initial row count
	var initialCount int
	err = db.QueryRow("SELECT COUNT(*) FROM delete_all_test").Scan(&initialCount)
	if err != nil {
		t.Fatalf("Failed to count initial rows: %v", err)
	}
	if initialCount != 10 {
		t.Fatalf("Expected 10 initial rows, got %d", initialCount)
	}

	// Delete all rows
	result, err := db.Exec("DELETE FROM delete_all_test")
	if err != nil {
		t.Fatalf("Failed to delete all rows: %v", err)
	}

	// Check affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if rowsAffected != 10 {
		t.Errorf("Expected 10 rows affected, got %d", rowsAffected)
	}

	// Verify no rows remain
	var remainingCount int
	err = db.QueryRow("SELECT COUNT(*) FROM delete_all_test").Scan(&remainingCount)
	if err != nil {
		t.Fatalf("Failed to count remaining rows: %v", err)
	}
	if remainingCount != 0 {
		t.Errorf("Expected 0 rows to remain after DELETE without WHERE, got %d", remainingCount)
	}
}

// TestDeleteWithOr tests DELETE with OR condition
func TestDeleteWithOr(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE or_delete (id INTEGER, category TEXT, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	testData := []struct {
		id       int
		category string
		value    float64
	}{
		{1, "A", 10.5},
		{2, "A", 20.0},
		{3, "B", 15.5},
		{4, "B", 25.0},
		{5, "C", 30.5},
		{6, "C", 40.0},
	}

	for _, d := range testData {
		_, err = db.Exec("INSERT INTO or_delete VALUES (?, ?, ?)", d.id, d.category, d.value)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete rows matching either condition
	_, err = db.Exec("DELETE FROM or_delete WHERE category = 'A' OR value > 30.0")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify the correct rows were deleted (should be id=1,2,5,6)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM or_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows to remain after DELETE with OR condition, got %d", count)
	}

	// Verify only category B rows with value <= 30.0 remain (id=3,4)
	var bCategoryCount int
	err = db.QueryRow("SELECT COUNT(*) FROM or_delete WHERE category = 'B'").Scan(&bCategoryCount)
	if err != nil {
		t.Fatalf("Failed to count category B rows: %v", err)
	}

	if bCategoryCount != 2 {
		t.Errorf("Expected 2 rows with category B to remain, got %d", bCategoryCount)
	}
}

// TestDeleteWithBetween tests DELETE with BETWEEN condition
func TestDeleteWithBetween(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE between_delete (id INTEGER, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	for i := 1; i <= 10; i++ {
		value := float64(i * 10)
		_, err = db.Exec("INSERT INTO between_delete VALUES (?, ?)", i, value)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete rows where value is between 30 and 70
	_, err = db.Exec("DELETE FROM between_delete WHERE value BETWEEN 30 AND 70")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Verify the correct rows were deleted (should be id 3-7)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM between_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows to remain after DELETE with BETWEEN, got %d", count)
	}

}

// TestDeleteWithIn tests DELETE with IN condition
func TestDeleteWithIn(t *testing.T) {
	// Create a temporary database in memory to avoid disk issues
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE in_delete (id INTEGER, category TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test rows
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 1; i <= 5; i++ {
		category := categories[i-1]
		_, err = db.Exec("INSERT INTO in_delete VALUES (?, ?)", i, category)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Delete rows with category in (A, C, E)
	_, err = db.Exec("DELETE FROM in_delete WHERE category IN ('A', 'C', 'E')")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	// Print all remaining rows for debugging
	t.Log("Remaining rows after DELETE with IN:")
	rows, err := db.Query("SELECT id, category FROM in_delete ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query remaining rows: %v", err)
	}
	defer rows.Close()

	var remainingRows int
	for rows.Next() {
		var id int
		var category string
		if err := rows.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Row: id=%d, category=%s", id, category)
		remainingRows++
	}

	// Verify the correct rows were deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM in_delete").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows to remain after DELETE with IN, got %d", count)
	}

	// Check only categories B and D remain
	t.Log("Checking rows with category B:")
	rowsB, err := db.Query("SELECT id, category FROM in_delete WHERE category = 'B'")
	if err != nil {
		t.Fatalf("Failed to query category B rows: %v", err)
	}
	defer rowsB.Close()

	var bCount int
	for rowsB.Next() {
		var id int
		var category string
		if err := rowsB.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Category B row: id=%d, category=%s", id, category)
		bCount++
	}

	t.Log("Checking rows with category D:")
	rowsD, err := db.Query("SELECT id, category FROM in_delete WHERE category = 'D'")
	if err != nil {
		t.Fatalf("Failed to query category D rows: %v", err)
	}
	defer rowsD.Close()

	var dCount int
	for rowsD.Next() {
		var id int
		var category string
		if err := rowsD.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Category D row: id=%d, category=%s", id, category)
		dCount++
	}

	if bCount != 1 || dCount != 1 {
		t.Errorf("Expected 1 row each for categories B and D, got %d and %d", bCount, dCount)
	}
} // For this test, simply check total count is correct

// TestDeleteWithNotIn tests DELETE with a NOT IN condition
func TestDeleteWithNotIn(t *testing.T) {
	// Create a database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE not_in_delete (id INTEGER, category TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO not_in_delete VALUES 
		(1, 'A'), 
		(2, 'B'), 
		(3, 'C'), 
		(4, 'D'), 
		(5, 'E')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Debug: Show all rows before delete
	rows, err := db.Query("SELECT * FROM not_in_delete ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query all rows: %v", err)
	}

	t.Log("All rows before DELETE with NOT IN:")
	for rows.Next() {
		var id int
		var category string
		if err := rows.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("  id=%d, category=%s", id, category)
	}
	rows.Close()

	// Delete rows with NOT IN
	query := "DELETE FROM not_in_delete WHERE category NOT IN ('B', 'D')"
	t.Logf("Executing: %s", query)

	result, err := db.Exec(query)
	if err != nil {
		t.Fatalf("Failed to execute DELETE: %v", err)
	}

	// Check that 3 rows were deleted (A, C, E)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if rowsAffected != 3 {
		t.Errorf("Expected 3 rows to be deleted, got %d", rowsAffected)
	}

	// Verify the remaining rows
	rows, err = db.Query("SELECT * FROM not_in_delete ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query remaining rows: %v", err)
	}
	defer rows.Close()

	t.Log("Remaining rows after DELETE with NOT IN:")
	var id int
	var category string
	remainingRows := 0
	for rows.Next() {
		if err := rows.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Row: id=%d, category=%s", id, category)
		remainingRows++
	}

	if remainingRows != 2 {
		t.Errorf("Expected 2 rows to remain, got %d", remainingRows)
	}

	// Additional check: Verify that only rows with categories B and D remain
	rows, err = db.Query("SELECT * FROM not_in_delete WHERE category IN ('B', 'D')")
	if err != nil {
		t.Fatalf("Failed to query categories B and D: %v", err)
	}

	categoriesFound := make(map[string]bool)
	for rows.Next() {
		if err := rows.Scan(&id, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		categoriesFound[category] = true
		t.Logf("Category %s row: id=%d", category, id)
	}
	rows.Close()

	if !categoriesFound["B"] || !categoriesFound["D"] {
		t.Errorf("Expected to find rows with categories B and D, but found: %v", categoriesFound)
	}

	if len(categoriesFound) != 2 {
		t.Errorf("Expected exactly 2 categories to remain, got %d: %v", len(categoriesFound), categoriesFound)
	}
}
