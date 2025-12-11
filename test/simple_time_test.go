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

func TestSimpleTimeFunctions(t *testing.T) {
	// Connect to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE simple_time_test (
			id INTEGER,
			event_time TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO simple_time_test (id, event_time) VALUES 
		(1, '2021-03-15T09:15:30')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test basic TIME_TRUNC
	rows, err := db.Query(`
		SELECT TIME_TRUNC('1h', event_time) FROM simple_time_test WHERE id = 1
	`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Get column types
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	t.Logf("Columns: %v", cols)

	// Scan a simple result
	if rows.Next() {
		var timeVal interface{}
		if err := rows.Scan(&timeVal); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		t.Logf("TIME_TRUNC result: %v (type: %T)", timeVal, timeVal)
	} else {
		t.Fatalf("No rows returned")
	}
}
