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

func TestUpdateWithParentheses(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`CREATE TABLE paren_test (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO paren_test (id, value) VALUES (1, 10)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "Without parentheses (should fail)",
			query:    "UPDATE paren_test SET value = value * 2 + 5 WHERE id = 1",
			expected: 25, // (10 * 2) + 5
		},
		{
			name:     "With parentheses around entire expression",
			query:    "UPDATE paren_test SET value = (value * 2 + 5) WHERE id = 1",
			expected: 25, // (10 * 2) + 5
		},
		{
			name:     "With parentheses around multiplication",
			query:    "UPDATE paren_test SET value = (value * 2) + 5 WHERE id = 1",
			expected: 25, // (10 * 2) + 5
		},
		{
			name:     "With parentheses around addition",
			query:    "UPDATE paren_test SET value = value * (2 + 5) WHERE id = 1",
			expected: 70, // 10 * (2 + 5)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset value to 10 before each test
			_, err := db.Exec("UPDATE paren_test SET value = 10 WHERE id = 1")
			if err != nil {
				t.Fatalf("Failed to reset value: %v", err)
			}

			// Try the test query
			_, err = db.Exec(tt.query)
			if err != nil {
				t.Logf("❌ Query failed: %s", tt.query)
				t.Logf("Error: %v", err)
				return
			}

			t.Logf("✅ Query succeeded: %s", tt.query)

			// Check the result
			var value int
			err = db.QueryRow("SELECT value FROM paren_test WHERE id = 1").Scan(&value)
			if err != nil {
				t.Fatalf("Failed to query result: %v", err)
			}

			if value != tt.expected {
				t.Errorf("Expected value %d, got %d", tt.expected, value)
			} else {
				t.Logf("✅ Correct result: %d", value)
			}
		})
	}
}
