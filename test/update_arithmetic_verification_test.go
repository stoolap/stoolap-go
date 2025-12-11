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

func TestUpdateArithmeticVerification(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`CREATE TABLE arith_verify (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO arith_verify (id, value) VALUES (1, 10), (2, 20), (3, 5)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Log("Initial values:")
	rows, err := db.Query("SELECT id, value FROM arith_verify ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query initial values: %v", err)
	}
	defer rows.Close()

	initialValues := make(map[int]int)
	for rows.Next() {
		var id, value int
		err := rows.Scan(&id, &value)
		if err != nil {
			t.Fatalf("Failed to scan initial values: %v", err)
		}
		initialValues[id] = value
		t.Logf("  ID %d: value = %d", id, value)
	}

	// Execute UPDATE with arithmetic expression
	t.Log("\nExecuting: UPDATE arith_verify SET value = (value * 2 + 5)")
	result, err := db.Exec("UPDATE arith_verify SET value = (value * 2 + 5)")
	if err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	t.Logf("Rows affected: %d", rowsAffected)

	// Verify the results
	t.Log("\nPost-UPDATE values:")
	rows, err = db.Query("SELECT id, value FROM arith_verify ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query post-update values: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, actualValue int
		err := rows.Scan(&id, &actualValue)
		if err != nil {
			t.Fatalf("Failed to scan post-update values: %v", err)
		}

		// Calculate expected value: (original_value * 2 + 5)
		originalValue := initialValues[id]
		expectedValue := originalValue*2 + 5

		t.Logf("  ID %d: %d -> %d (expected: %d)", id, originalValue, actualValue, expectedValue)

		if actualValue != expectedValue {
			t.Errorf("ID %d: expected %d, got %d", id, expectedValue, actualValue)
		}
	}

	// Verify mathematical correctness manually
	t.Log("\nManual verification of arithmetic:")
	expectedResults := map[int]struct{ original, expected int }{
		1: {10, 25}, // 10 * 2 + 5 = 25
		2: {20, 45}, // 20 * 2 + 5 = 45
		3: {5, 15},  // 5 * 2 + 5 = 15
	}

	rows, err = db.Query("SELECT id, value FROM arith_verify ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query final results: %v", err)
	}
	defer rows.Close()

	allCorrect := true
	for rows.Next() {
		var id, actualResult int
		err := rows.Scan(&id, &actualResult)
		if err != nil {
			t.Fatalf("Failed to scan final results: %v", err)
		}

		expected := expectedResults[id]
		manualCalc := expected.original*2 + 5

		if actualResult != manualCalc {
			t.Errorf("ID %d: Manual calculation (%d * 2 + 5 = %d) != UPDATE result (%d)",
				id, expected.original, manualCalc, actualResult)
			allCorrect = false
		} else {
			t.Logf("  ID %d: ✅ %d * 2 + 5 = %d (UPDATE result: %d)",
				id, expected.original, manualCalc, actualResult)
		}
	}

	if allCorrect {
		t.Log("🎉 All UPDATE arithmetic expressions are mathematically correct!")
	}
}

func TestUpdateArithmeticEdgeCases(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE edge_test (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name           string
		initialValue   int
		expression     string
		expectedResult int
	}{
		{
			name:           "Zero value",
			initialValue:   0,
			expression:     "(value * 2 + 5)",
			expectedResult: 5, // 0 * 2 + 5 = 5
		},
		{
			name:           "Negative value",
			initialValue:   -3,
			expression:     "(value * 2 + 5)",
			expectedResult: -1, // -3 * 2 + 5 = -1
		},
		{
			name:           "Large value",
			initialValue:   1000,
			expression:     "(value * 2 + 5)",
			expectedResult: 2005, // 1000 * 2 + 5 = 2005
		},
		{
			name:           "Complex expression",
			initialValue:   7,
			expression:     "(value * 3 - 1) / 2",
			expectedResult: 10, // (7 * 3 - 1) / 2 = 20 / 2 = 10
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := i + 1

			// Insert test value
			_, err := db.Exec("INSERT INTO edge_test (id, value) VALUES (?, ?)", id, tt.initialValue)
			if err != nil {
				t.Fatalf("Failed to insert test value: %v", err)
			}

			// Execute UPDATE with arithmetic expression
			query := "UPDATE edge_test SET value = " + tt.expression + " WHERE id = ?"
			_, err = db.Exec(query, id)
			if err != nil {
				t.Fatalf("Failed to execute UPDATE: %v", err)
			}

			// Verify result
			var result int
			err = db.QueryRow("SELECT value FROM edge_test WHERE id = ?", id).Scan(&result)
			if err != nil {
				t.Fatalf("Failed to query result: %v", err)
			}

			if result != tt.expectedResult {
				t.Errorf("Test %s: expected %d, got %d", tt.name, tt.expectedResult, result)
			} else {
				t.Logf("✅ %s: %d -> %d (expression: %s)", tt.name, tt.initialValue, result, tt.expression)
			}
		})
	}
}
