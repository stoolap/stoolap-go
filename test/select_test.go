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

// TestSelectWithComplexWhere tests SELECT queries with complex WHERE clauses
// that are similar to the ones used in delete_test.go
func TestSelectWithComplexWhere(t *testing.T) {
	// Create a temporary database in memory
	dbPath := "memory://"
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// 1. Test string comparison
	t.Run("StringComparison", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE str_select (id INTEGER, name TEXT)")
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
			_, err = db.Exec("INSERT INTO str_select VALUES (?, ?)", d.id, d.name)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Let's debug with a query that shows all rows
		rows, err := db.Query("SELECT * FROM str_select")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}
		t.Logf("Table columns: %v", columns)

		// Print each row for debugging
		t.Logf("All table rows:")
		for rows.Next() {
			// Create values array with enough space for all columns
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			// Print all values for debugging
			t.Logf("  Row data:")
			for i, col := range columns {
				t.Logf("    %s: %v", col, values[i])
			}
		}
		rows.Close()

		// Now try the original query with string comparison
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM str_select WHERE name = 'Cherry'").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with string comparison: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 row with name='Cherry', got %d", count)
		} else {
			t.Logf("Found %d row(s) with name='Cherry' as expected", count)
		}

		// Debug query with direct id lookup
		rows, err = db.Query("SELECT * FROM str_select WHERE id = 3")
		if err != nil {
			t.Fatalf("Failed to query id=3: %v", err)
		}

		t.Logf("Rows with id=3 (should be Cherry):")
		rowCount := 0
		for rows.Next() {
			// Create values array with enough space for all columns
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			// Print all values
			t.Logf("  Row data:")
			for i, col := range columns {
				t.Logf("    %s: %v", col, values[i])
			}
			rowCount++
		}
		rows.Close()

		t.Logf("Found %d rows with id=3", rowCount)
	})

	// 2. Test AND condition
	t.Run("AndCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE and_select (id INTEGER, category TEXT, value FLOAT)")
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
			_, err = db.Exec("INSERT INTO and_select VALUES (?, ?, ?)", d.id, d.category, d.value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Debug: print all rows in the table first
		t.Log("All rows in and_select table:")
		rows, err := db.Query("SELECT * FROM and_select")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}
		for rows.Next() {
			var id int
			var category string
			var value float64
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, category=%s, value=%g", id, category, value)
		}
		rows.Close()

		// Try a simple query first
		rows, err = db.Query("SELECT * FROM and_select WHERE category = 'B'")
		if err != nil {
			t.Fatalf("Failed to query with simple condition: %v", err)
		}
		t.Log("Rows with category='B':")
		count1 := 0
		for rows.Next() {
			var id int
			var category string
			var value float64
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, category=%s, value=%g", id, category, value)
			count1++
		}
		rows.Close()
		t.Logf("Found %d rows with category='B'", count1)

		// Try another simple query
		rows, err = db.Query("SELECT * FROM and_select WHERE value > 20.0")
		if err != nil {
			t.Fatalf("Failed to query with simple condition: %v", err)
		}
		t.Log("Rows with value > 20.0:")
		count2 := 0
		for rows.Next() {
			var id int
			var category string
			var value float64
			if err := rows.Scan(&id, &category, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, category=%s, value=%g", id, category, value)
			count2++
		}
		rows.Close()
		t.Logf("Found %d rows with value > 20.0", count2)

		// Now try the full AND query
		var count int
		t.Log("Executing AND query: SELECT COUNT(*) FROM and_select WHERE category = 'B' AND value > 20.0")
		err = db.QueryRow("SELECT COUNT(*) FROM and_select WHERE category = 'B' AND value > 20.0").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with AND condition: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 row with category='B' AND value>20.0, got %d", count)
		} else {
			t.Logf("Found %d row(s) with category='B' AND value>20.0 as expected", count)
		}

		// Let's try a full row query to debug
		rows, err = db.Query("SELECT * FROM and_select WHERE category = 'B' AND value > 20.0")
		if err != nil {
			t.Fatalf("Failed to query rows: %v", err)
		}

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}
		t.Logf("Query returned columns: %v", columns)

		// Scan the rows with a generic scanner
		var ids []int
		for rows.Next() {
			// Create values array with enough space for all columns
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			// Print all values for debugging
			t.Logf("  Row data:")
			for i, col := range columns {
				t.Logf("    %s: %v", col, values[i])
			}

			// Extract the ID (first column)
			if v, ok := values[0].(int64); ok {
				ids = append(ids, int(v))
			} else {
				t.Logf("First column not an int64: %T %v", values[0], values[0])
			}
		}
		rows.Close()

		if len(ids) != 1 || ids[0] != 4 {
			t.Errorf("Expected exactly one row with id=4, got %v", ids)
		} else {
			t.Logf("Found row with id=%d as expected", ids[0])
		}
	})

	// 3. Test OR condition
	t.Run("OrCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE or_select (id INTEGER, category TEXT, value FLOAT)")
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
			_, err = db.Exec("INSERT INTO or_select VALUES (?, ?, ?)", d.id, d.category, d.value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Query with OR condition
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM or_select WHERE category = 'A' OR value > 30.0").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with OR condition: %v", err)
		}

		if count != 4 {
			t.Errorf("Expected 4 rows with category='A' OR value>30.0, got %d", count)
		} else {
			t.Logf("Found %d row(s) with category='A' OR value>30.0 as expected", count)
		}
	})

	// 4. Test BETWEEN condition
	t.Run("BetweenCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE between_select (id INTEGER, value FLOAT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test rows
		for i := 1; i <= 10; i++ {
			value := float64(i * 10)
			_, err = db.Exec("INSERT INTO between_select VALUES (?, ?)", i, value)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Debug: List all rows
		rows, err := db.Query("SELECT * FROM between_select ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}

		t.Logf("All rows in between_select:")
		for rows.Next() {
			var id int
			var value float64
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, value=%g", id, value)
		}
		rows.Close()

		// Query with BETWEEN condition
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM between_select WHERE value BETWEEN 30 AND 70").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with BETWEEN condition: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected 5 rows with value BETWEEN 30 AND 70, got %d", count)
		} else {
			t.Logf("Found %d row(s) with value BETWEEN 30 AND 70 as expected", count)
		}

		// Debug: Show the specific rows that match
		rows, err = db.Query("SELECT * FROM between_select WHERE value BETWEEN 30 AND 70 ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query BETWEEN rows: %v", err)
		}

		t.Logf("Rows matching BETWEEN 30 AND 70:")
		rowCount := 0
		for rows.Next() {
			var id int
			var value float64
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, value=%g", id, value)
			rowCount++
		}
		rows.Close()

		t.Logf("Found %d rows matching BETWEEN criteria", rowCount)
	})

	// 5. Test IN condition
	t.Run("InCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE in_select (id INTEGER, category TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test rows
		categories := []string{"A", "B", "C", "D", "E"}
		for i := 1; i <= 5; i++ {
			category := categories[i-1]
			_, err = db.Exec("INSERT INTO in_select VALUES (?, ?)", i, category)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Query with IN condition
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM in_select WHERE category IN ('A', 'C', 'E')").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with IN condition: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected 3 rows with category IN ('A', 'C', 'E'), got %d", count)
		} else {
			t.Logf("Found %d row(s) with category IN ('A', 'C', 'E') as expected", count)
		}
	})

	// 6. Test NOT IN condition
	t.Run("NotInCondition", func(t *testing.T) {
		// Create a table
		_, err = db.Exec("CREATE TABLE not_in_select (id INTEGER, category TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test rows
		categories := []string{"A", "B", "C", "D", "E"}
		for i := 1; i <= 5; i++ {
			category := categories[i-1]
			_, err = db.Exec("INSERT INTO not_in_select VALUES (?, ?)", i, category)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Debug: Show all rows first
		rows, err := db.Query("SELECT * FROM not_in_select ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query all rows: %v", err)
		}

		t.Logf("All rows in not_in_select:")
		for rows.Next() {
			var id int
			var category string
			if err := rows.Scan(&id, &category); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("  id=%d, category=%s", id, category)
		}
		rows.Close()

		// Query with NOT IN condition
		var count int
		query := "SELECT COUNT(*) FROM not_in_select WHERE category NOT IN ('A', 'C', 'E')"

		err = db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query with NOT IN condition: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected 2 rows with category NOT IN ('A', 'C', 'E'), got %d", count)
		} else {
			t.Logf("Found %d row(s) with category NOT IN ('A', 'C', 'E') as expected", count)
		}
	})
}
