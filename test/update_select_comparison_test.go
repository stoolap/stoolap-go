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

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import the Stoolap driver
)

func TestUpdateSelectComparison(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE comparison (id INTEGER PRIMARY KEY, original_value INTEGER, updated_value INTEGER, select_value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testValues := []int{10, 20, 5, 0, -3, 100}

	for i, val := range testValues {
		id := i + 1

		// Insert original value
		_, err = db.Exec("INSERT INTO comparison (id, original_value) VALUES (?, ?)", id, val)
		if err != nil {
			t.Fatalf("Failed to insert value %d: %v", val, err)
		}

		// Calculate what SELECT thinks the result should be
		var selectResult int
		err = db.QueryRow("SELECT (? * 2 + 5)", val).Scan(&selectResult)
		if err != nil {
			t.Fatalf("Failed to calculate SELECT result for %d: %v", val, err)
		}

		// Store SELECT result
		_, err = db.Exec("UPDATE comparison SET select_value = ? WHERE id = ?", selectResult, id)
		if err != nil {
			t.Fatalf("Failed to store SELECT result: %v", err)
		}

		t.Logf("Value %d: SELECT (%d * 2 + 5) = %d", val, val, selectResult)
	}

	// Now test UPDATE with arithmetic expressions
	_, err = db.Exec("UPDATE comparison SET updated_value = (original_value * 2 + 5)")
	if err != nil {
		t.Fatalf("Failed to execute UPDATE with arithmetic: %v", err)
	}

	// Compare results
	t.Log("\nComparison Results:")
	rows, err := db.Query("SELECT id, original_value, updated_value, select_value FROM comparison ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query comparison results: %v", err)
	}
	defer rows.Close()

	allMatch := true
	for rows.Next() {
		var id, original, updated, selectVal int
		err := rows.Scan(&id, &original, &updated, &selectVal)
		if err != nil {
			t.Fatalf("Failed to scan comparison results: %v", err)
		}

		match := updated == selectVal
		if !match {
			allMatch = false
		}

		status := "✅"
		if !match {
			status = "❌"
		}

		t.Logf("%s Original: %d, UPDATE result: %d, SELECT result: %d", status, original, updated, selectVal)
	}

	if !allMatch {
		t.Error("UPDATE and SELECT arithmetic expressions produce different results!")
	} else {
		t.Log("🎉 All UPDATE and SELECT arithmetic expressions match perfectly!")
	}
}
