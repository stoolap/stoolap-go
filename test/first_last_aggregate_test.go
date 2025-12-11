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

func TestFirstLastAggregateFunctions(t *testing.T) {
	// Connect to in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a test table with sample data
	_, err = db.Exec(`
		CREATE TABLE test_first_last (
			id INTEGER,
			group_id INTEGER,
			value INTEGER,
			text_value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data with a mix of values and groups
	_, err = db.Exec(`
		INSERT INTO test_first_last (id, group_id, value, text_value) VALUES
		(1, 1, 10, 'Apple'),
		(2, 1, 20, 'Banana'),
		(3, 1, 30, 'Cherry'),
		(4, 2, 15, 'Grape'),
		(5, 2, 25, 'Lemon'),
		(6, 2, NULL, 'Orange'),
		(7, 3, NULL, NULL),
		(8, 3, 35, 'Pear'),
		(9, 3, 45, 'Plum')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test FIRST function
	t.Run("FIRST function", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT group_id, FIRST(value), FIRST(text_value)
			FROM test_first_last
			GROUP BY group_id
			ORDER BY group_id
		`)
		if err != nil {
			t.Fatalf("Failed to execute FIRST query: %v", err)
		}
		defer rows.Close()

		expected := []struct {
			groupID   int
			firstVal  sql.NullInt64
			firstText sql.NullString
		}{
			{1, sql.NullInt64{Int64: 10, Valid: true}, sql.NullString{String: "Apple", Valid: true}},
			{2, sql.NullInt64{Int64: 15, Valid: true}, sql.NullString{String: "Grape", Valid: true}},
			{3, sql.NullInt64{Int64: 35, Valid: true}, sql.NullString{String: "Pear", Valid: true}},
		}

		idx := 0
		for rows.Next() {
			var groupID int
			var firstVal sql.NullInt64
			var firstText sql.NullString

			if err := rows.Scan(&groupID, &firstVal, &firstText); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if idx >= len(expected) {
				t.Fatalf("Got more rows than expected")
			}

			if groupID != expected[idx].groupID {
				t.Errorf("Expected group_id %d, got %d", expected[idx].groupID, groupID)
			}

			if firstVal.Int64 != expected[idx].firstVal.Int64 || firstVal.Valid != expected[idx].firstVal.Valid {
				t.Errorf("For group %d: Expected first value %v, got %v", groupID, expected[idx].firstVal, firstVal)
			}

			if firstText.String != expected[idx].firstText.String || firstText.Valid != expected[idx].firstText.Valid {
				t.Errorf("For group %d: Expected first text %v, got %v", groupID, expected[idx].firstText, firstText)
			}

			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	// Test LAST function
	t.Run("LAST function", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT group_id, LAST(value), LAST(text_value)
			FROM test_first_last
			GROUP BY group_id
			ORDER BY group_id
		`)
		if err != nil {
			t.Fatalf("Failed to execute LAST query: %v", err)
		}
		defer rows.Close()

		expected := []struct {
			groupID  int
			lastVal  sql.NullInt64
			lastText sql.NullString
		}{
			{1, sql.NullInt64{Int64: 30, Valid: true}, sql.NullString{String: "Cherry", Valid: true}},
			{2, sql.NullInt64{Int64: 25, Valid: true}, sql.NullString{String: "Orange", Valid: true}},
			{3, sql.NullInt64{Int64: 45, Valid: true}, sql.NullString{String: "Plum", Valid: true}},
		}

		idx := 0
		for rows.Next() {
			var groupID int
			var lastVal sql.NullInt64
			var lastText sql.NullString

			if err := rows.Scan(&groupID, &lastVal, &lastText); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if idx >= len(expected) {
				t.Fatalf("Got more rows than expected")
			}

			if groupID != expected[idx].groupID {
				t.Errorf("Expected group_id %d, got %d", expected[idx].groupID, groupID)
			}

			if lastVal.Int64 != expected[idx].lastVal.Int64 || lastVal.Valid != expected[idx].lastVal.Valid {
				t.Errorf("For group %d: Expected last value %v, got %v", groupID, expected[idx].lastVal, lastVal)
			}

			if lastText.String != expected[idx].lastText.String || lastText.Valid != expected[idx].lastText.Valid {
				t.Errorf("For group %d: Expected last text %v, got %v", groupID, expected[idx].lastText, lastText)
			}

			idx++
		}

		if idx != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), idx)
		}
	})

	// Test FIRST with all NULLs
	t.Run("FIRST with all NULLs", func(t *testing.T) {
		// Create table with all NULL values
		_, err = db.Exec(`
			CREATE TABLE all_nulls (id INTEGER, value INTEGER)
		`)
		if err != nil {
			t.Fatalf("Failed to create all_nulls table: %v", err)
		}

		_, err = db.Exec(`
			INSERT INTO all_nulls (id, value) VALUES (1, NULL), (2, NULL), (3, NULL)
		`)
		if err != nil {
			t.Fatalf("Failed to insert NULL data: %v", err)
		}

		// Query FIRST on all NULLs - should be returned as NULL
		row := db.QueryRow(`SELECT FIRST(value) FROM all_nulls`)
		var firstVal sql.NullInt64
		if err := row.Scan(&firstVal); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}

		if firstVal.Valid {
			t.Errorf("Expected FIRST to return NULL, got %v", firstVal.Int64)
		} else {
			t.Log("FIRST correctly returned NULL for all NULL values")
		}
	})

	// Test FIRST and LAST with mixed NULL and non-NULL values
	t.Run("FIRST and LAST with mixed NULL values", func(t *testing.T) {
		// Create a table with specific NULL pattern to test
		_, err = db.Exec(`
			CREATE TABLE mixed_nulls (
				id INTEGER,
				val INTEGER
			)
		`)
		if err != nil {
			t.Fatalf("Failed to create mixed_nulls table: %v", err)
		}

		// Use parameter binding for NULL values
		_, err = db.Exec(`
			INSERT INTO mixed_nulls (id, val) VALUES
			(?, ?),
			(?, ?),
			(?, ?),
			(?, ?),
			(?, ?)
		`,
			1, nil, // NULL value
			2, 10, // First non-NULL
			3, 20, // Another non-NULL
			4, nil, // Another NULL
			5, 30) // Last non-NULL
		if err != nil {
			t.Fatalf("Failed to insert mixed NULL data: %v", err)
		}

		// Test FIRST (should be 10)
		row := db.QueryRow(`SELECT FIRST(val) FROM mixed_nulls`)
		var firstVal sql.NullInt64
		if err := row.Scan(&firstVal); err != nil {
			t.Fatalf("Failed to scan FIRST result: %v", err)
		}

		if !firstVal.Valid || firstVal.Int64 != 10 {
			t.Errorf("Expected FIRST value 10, got %v", firstVal)
		} else {
			t.Logf("FIRST correctly returned 10 as expected")
		}

		// Test LAST (should be 30)
		row = db.QueryRow(`SELECT LAST(val) FROM mixed_nulls`)
		var lastVal sql.NullInt64
		if err := row.Scan(&lastVal); err != nil {
			t.Fatalf("Failed to scan LAST result: %v", err)
		}

		if !lastVal.Valid || lastVal.Int64 != 30 {
			t.Errorf("Expected LAST value 30, got %v", lastVal)
		} else {
			t.Logf("LAST correctly returned 30 as expected")
		}
	})
}
