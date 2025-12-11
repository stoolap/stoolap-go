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

// TestSimpleUpdate tests a simple UPDATE operation with a direct ID comparison
func TestSimpleUpdate(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE simple_update (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a test row
	_, err = db.Exec("INSERT INTO simple_update VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Verify the row exists and has the original value
	var id, value int
	err = db.QueryRow("SELECT id, value FROM simple_update WHERE id = 1").Scan(&id, &value)
	if err != nil {
		t.Fatalf("Failed to query row: %v", err)
	}
	if value != 100 {
		t.Fatalf("Expected initial value=100, got %d", value)
	}

	// Update the row
	t.Logf("Attempting simple UPDATE operation...")
	_, err = db.Exec("UPDATE simple_update SET value = 200 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update row: %v", err)
	}

	// Verify the row was updated
	err = db.QueryRow("SELECT id, value FROM simple_update WHERE id = 1").Scan(&id, &value)
	if err != nil {
		t.Fatalf("Failed to query row after update: %v", err)
	}

	if value != 200 {
		t.Errorf("Expected value=200 after update, got %d", value)
	} else {
		t.Logf("Simple UPDATE successful: value updated to %d as expected", value)
	}
}

// TestUpdateWithComplexWhere tests UPDATE queries with complex WHERE clauses
// to examine which WHERE conditions work properly with UPDATE operations
func TestUpdateWithComplexWhere(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test NOT IN operator with UPDATE
	t.Run("NotInCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE not_in_update (id INTEGER, category TEXT, value INTEGER)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test data
		_, err = db.Exec(`INSERT INTO not_in_update VALUES 
			(1, 'A', 10), 
			(2, 'B', 20), 
			(3, 'C', 30), 
			(4, 'D', 40), 
			(5, 'E', 50)`)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Debug: Show all rows before update
		rows, err := db.Query("SELECT * FROM not_in_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}

		for rows.Next() {
			var id int
			var category string
			var value int
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
		}
		rows.Close()

		// Update with NOT IN condition
		query := "UPDATE not_in_update SET value = 999 WHERE category NOT IN ('B', 'D')"

		result, err := db.Exec(query)
		if err != nil {
			t.Fatalf("Failed to execute UPDATE: %v", err)
		}

		// Check that 3 rows were updated (A, C, E)
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("Failed to get rows affected: %v", err)
		}

		if rowsAffected != 3 {
			t.Errorf("Expected 3 rows to be updated, got %d", rowsAffected)
		}

		// Verify the updated rows
		rows, err = db.Query("SELECT * FROM not_in_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query rows after update: %v", err)
		}
		defer rows.Close()

		updatedRows := make(map[string]int) // Map of category to value

		for rows.Next() {
			var id int
			var category string
			var value int
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			updatedRows[category] = value
		}

		// Verify that only categories A, C, E were updated
		for _, cat := range []string{"A", "C", "E"} {
			if updatedRows[cat] != 999 {
				t.Errorf("Expected category %s to have value 999, got %d", cat, updatedRows[cat])
			}
		}

		// Verify that categories B, D were not updated
		if updatedRows["B"] != 20 {
			t.Errorf("Expected category B to maintain original value 20, got %d", updatedRows["B"])
		}
		if updatedRows["D"] != 40 {
			t.Errorf("Expected category D to maintain original value 40, got %d", updatedRows["D"])
		}
	})

	// 1. Test string comparison
	t.Run("StringComparison", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE str_update (id INTEGER, name TEXT)")
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
			_, err = db.Exec("INSERT INTO str_update VALUES (?, ?)", d.id, d.name)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Try to update the row with name='Cherry'
		_, err = db.Exec("UPDATE str_update SET name='Modified Cherry' WHERE name = 'Cherry'")
		if err != nil {
			t.Fatalf("Failed to update with string comparison: %v", err)
		}

		// Verify the update worked correctly
		var id int
		var name string
		err = db.QueryRow("SELECT id, name FROM str_update WHERE id = 3").Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to query updated row: %v", err)
		}

		if name != "Modified Cherry" {
			t.Errorf("Expected name='Modified Cherry' after update, got '%s'", name)
		}

		// Verify all 5 rows are still present, with one modified
		rows, err := db.Query("SELECT id, name FROM str_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}
		defer rows.Close()

		var rowCount int
		var cherryCount int
		var modifiedCount int
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			rowCount++
			if name == "Cherry" {
				cherryCount++
			}
			if name == "Modified Cherry" {
				modifiedCount++
				if id != 3 {
					t.Errorf("Expected 'Modified Cherry' to be for id=3, got id=%d", id)
				}
			}

			t.Logf("Row %d: id=%d, name=%s", rowCount, id, name)
		}

		if rowCount != 5 {
			t.Errorf("Expected 5 total rows, got %d", rowCount)
		}

		if cherryCount > 0 {
			t.Logf("Note: Found %d rows still with name='Cherry' (should be 0)", cherryCount)
		}

		if modifiedCount != 1 {
			t.Errorf("Expected 1 'Modified Cherry' row, got %d", modifiedCount)
		}
	})

	// 2. Test AND condition
	t.Run("AndCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE and_update (id INTEGER, category TEXT, value FLOAT)")
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
			_, err = db.Exec("INSERT INTO and_update VALUES (?, ?, ?)", d.id, d.category, d.value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Update with AND condition - should only update row with id=4
		_, err = db.Exec("UPDATE and_update SET value = 100.0 WHERE category = 'B' AND value > 20.0")
		if err != nil {
			t.Fatalf("Failed to update with AND condition: %v", err)
		}

		// Verify the update worked correctly
		var id int
		var category string
		var value float64
		err = db.QueryRow("SELECT id, category, value FROM and_update WHERE id = 4").Scan(&id, &category, &value)
		if err != nil {
			t.Fatalf("Failed to query updated row: %v", err)
		}

		if value != 100.0 {
			t.Errorf("Expected value=100.0 after update for id=4, got %g", value)
		}

		// Verify other 'B' category row wasn't affected (id=3)
		err = db.QueryRow("SELECT id, category, value FROM and_update WHERE id = 3").Scan(&id, &category, &value)
		if err != nil {
			t.Fatalf("Failed to query non-updated row: %v", err)
		}

		if value != 15.5 {
			t.Errorf("Expected value=15.5 to remain unchanged for id=3, got %g", value)
		}
	})

	// 3. Test OR condition
	t.Run("OrCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE or_update (id INTEGER, category TEXT, value FLOAT)")
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
			_, err = db.Exec("INSERT INTO or_update VALUES (?, ?, ?)", d.id, d.category, d.value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Update with OR condition - should update 4 rows (category A or value > 30)
		_, err = db.Exec("UPDATE or_update SET value = 200.0 WHERE category = 'A' OR value > 30.0")
		if err != nil {
			t.Fatalf("Failed to update with OR condition: %v", err)
		}

		// Verify all rows after update
		rows, err := db.Query("SELECT id, category, value FROM or_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}
		defer rows.Close()

		expectedUpdated := map[int]bool{1: true, 2: true, 5: true, 6: true}
		expectedNotUpdated := map[int]bool{3: true, 4: true}

		var updatedCount int
		var notUpdatedCount int

		for rows.Next() {
			var id int
			var category string
			var value float64
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if expectedUpdated[id] {
				if value == 200.0 {
					updatedCount++
				} else {
					t.Errorf("Expected id=%d to be updated to value=200.0, got %g", id, value)
				}
			} else if expectedNotUpdated[id] {
				if value != 200.0 {
					notUpdatedCount++
				} else {
					t.Errorf("Expected id=%d to NOT be updated (value should not be 200.0)", id)
				}
			}
		}

		if updatedCount != len(expectedUpdated) {
			t.Errorf("Expected %d rows to be updated with value=200.0, got %d", len(expectedUpdated), updatedCount)
		}

		if notUpdatedCount != len(expectedNotUpdated) {
			t.Errorf("Expected %d rows to remain unchanged, got %d", len(expectedNotUpdated), notUpdatedCount)
		}
	})

	// 4. Test BETWEEN condition
	t.Run("BetweenCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE between_update (id INTEGER, value FLOAT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test rows
		for i := 1; i <= 10; i++ {
			value := float64(i * 10)
			_, err = db.Exec("INSERT INTO between_update VALUES (?, ?)", i, value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Update with explicit comparison instead of BETWEEN
		_, err = db.Exec("UPDATE between_update SET value = 999.0 WHERE value BETWEEN 30 AND 70")
		if err != nil {
			t.Fatalf("Failed to update with BETWEEN condition: %v", err)
		}

		// Query all rows to verify the results
		rows, err := db.Query("SELECT id, value FROM between_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}
		defer rows.Close()

		var updatedRows int
		var totalRows int

		t.Logf("Rows after BETWEEN update:")
		for rows.Next() {
			var id int
			var value float64
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			totalRows++
			t.Logf("  id=%d, value=%g", id, value)

			// Check if this row should have been updated (id 3-7 have values 30-70)
			shouldBeUpdated := id >= 3 && id <= 7

			if shouldBeUpdated {
				if value == 999.0 {
					updatedRows++
				} else {
					t.Errorf("For id=%d, expected value=999.0, got %g", id, value)
				}
			} else {
				expected := float64(id * 10)
				if value != expected {
					t.Errorf("For id=%d, expected value=%g (unchanged), got %g", id, expected, value)
				}
			}
		}

		if updatedRows != 5 {
			t.Errorf("Expected 5 rows to be updated with BETWEEN condition, got %d", updatedRows)
		}

		if totalRows != 10 {
			t.Errorf("Expected 10 total rows, got %d", totalRows)
		}
	})

	// 5. Test IN condition
	t.Run("InCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE in_update (id INTEGER, category TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test rows
		categories := []string{"A", "B", "C", "D", "E"}
		for i := 1; i <= 5; i++ {
			category := categories[i-1]
			_, err = db.Exec("INSERT INTO in_update VALUES (?, ?)", i, category)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Update with IN condition - should update categories A, C, E
		_, err = db.Exec("UPDATE in_update SET category = 'X' WHERE category IN ('A', 'C', 'E')")
		if err != nil {
			t.Fatalf("Failed to update with IN condition: %v", err)
		}

		// Query all rows to verify the results
		rows, err := db.Query("SELECT id, category FROM in_update ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}
		defer rows.Close()

		expectedUpdated := map[int]bool{1: true, 3: true, 5: true}
		expectedNotUpdated := map[int]bool{2: true, 4: true}

		var updatedCount int
		var notUpdatedCount int
		var totalCount int

		t.Logf("Rows after IN update:")
		for rows.Next() {
			var id int
			var category string
			if err := rows.Scan(&id, &category); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			totalCount++
			t.Logf("  id=%d, category='%s'", id, category)

			if expectedUpdated[id] {
				if category == "X" {
					updatedCount++
				} else {
					t.Errorf("Expected id=%d to be updated to category='X', got '%s'", id, category)
				}
			} else if expectedNotUpdated[id] {
				if category != "X" {
					notUpdatedCount++
				} else {
					t.Errorf("Expected id=%d to NOT be updated (category should not be 'X')", id)
				}
			}
		}

		if updatedCount != len(expectedUpdated) {
			t.Errorf("Expected %d rows to be updated with category='X', got %d", len(expectedUpdated), updatedCount)
		}

		if notUpdatedCount != len(expectedNotUpdated) {
			t.Errorf("Expected %d rows to remain unchanged, got %d", len(expectedNotUpdated), notUpdatedCount)
		}

		if totalCount != 5 {
			t.Errorf("Expected 5 total rows, got %d", totalCount)
		}
	})
}
