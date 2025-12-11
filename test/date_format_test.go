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

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestDateFormatting(t *testing.T) {
	// Initialize in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a simple test table with date column
	_, err = db.Exec(`
		CREATE TABLE date_format_test (
			id INTEGER,
			date_val DATE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data using SQL
	_, err = db.Exec(`
		INSERT INTO date_format_test (id, date_val) VALUES
		(1, '2023-01-15')
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Log("Successfully inserted test data")

	// Verify the row count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM date_format_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	t.Logf("Table has %d rows", count)

	// Debug query with interface{} to see the raw type
	debugRows, err := db.Query("SELECT * FROM date_format_test")
	if err != nil {
		t.Fatalf("Failed to debug query all rows: %v", err)
	}

	t.Log("DEBUG: All rows with interface{} scan:")
	for debugRows.Next() {
		var id int
		var dateVal interface{}
		if err := debugRows.Scan(&id, &dateVal); err != nil {
			t.Fatalf("Failed to scan debug row: %v", err)
		}
		t.Logf("DEBUG: Row data: ID=%d, DATE=%v (type: %T)", id, dateVal, dateVal)

		// If it's a time.Time, format it properly for display
		if timeVal, ok := dateVal.(time.Time); ok {
			formatted := timeVal.Format("2006-01-02")
			t.Logf("DEBUG: Formatted date: %s", formatted)
		}
	}
	debugRows.Close()

	// Now try with a string type scan which is what tests expect
	strRows, err := db.Query("SELECT id, date_val FROM date_format_test")
	if err != nil {
		t.Fatalf("Failed to query rows: %v", err)
	}

	t.Log("DEBUG: All rows with string scan:")
	for strRows.Next() {
		var id int
		var dateVal string
		if err := strRows.Scan(&id, &dateVal); err != nil {
			// If this fails, it means the driver doesn't automatically convert to string
			t.Logf("Failed to scan as string: %v", err)
			break
		}
		t.Logf("DEBUG: Row data: ID=%d, DATE=%s", id, dateVal)
	}
	strRows.Close()
}
