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
	"context"
	"testing"

	sql "github.com/stoolap/stoolap-go/internal/sql/executor"
)

// TestExecResultImplementation verifies that the ExecResult implementation correctly
// implements the storage.Result interface, particularly the cursor mechanics
func TestExecResultImplementation(t *testing.T) {
	// Create a simple test result with some data
	ctx := context.Background()
	columns := []string{"ID", "Name"}
	rows := [][]interface{}{
		{1, "First"},
		{2, "Second"},
		{3, "Third"},
	}

	// Create ExecResult
	// Create a memory result - we need a way to properly create a result
	// Let's create it the proper way through the public API
	result := sql.NewExecMemoryResult(columns, rows, ctx)

	// Verify the columns are correct
	resultColumns := result.Columns()
	if len(resultColumns) != len(columns) {
		t.Errorf("Column count mismatch: got %d, want %d", len(resultColumns), len(columns))
	}
	for i, col := range resultColumns {
		if col != columns[i] {
			t.Errorf("Column %d mismatch: got %s, want %s", i, col, columns[i])
		}
	}

	// Iterate through the rows and verify we can access all rows
	rowCount := 0
	for result.Next() {
		rowCount++
		row := result.Row()
		if row == nil {
			t.Errorf("Got nil row at position %d", rowCount)
			continue
		}

		if len(row) != 2 {
			t.Errorf("Row %d has incorrect column count: got %d, want 2", rowCount, len(row))
			continue
		}

		// Verify row data using the Row() method
		intVal, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to convert ID to int64 in row %d", rowCount)
		} else if int(intVal) != rowCount {
			t.Errorf("Row %d has incorrect ID: got %d, want %d", rowCount, intVal, rowCount)
		}

		// Also test the Scan method
		var id int
		var name string
		if err := result.Scan(&id, &name); err != nil {
			t.Errorf("Scan error on row %d: %v", rowCount, err)
		} else {
			if id != rowCount {
				t.Errorf("Scanned ID mismatch for row %d: got %d, want %d", rowCount, id, rowCount)
			}

			expectedName := ""
			switch rowCount {
			case 1:
				expectedName = "First"
			case 2:
				expectedName = "Second"
			case 3:
				expectedName = "Third"
			}

			if name != expectedName {
				t.Errorf("Scanned name mismatch for row %d: got %s, want %s", rowCount, name, expectedName)
			}
		}
	}

	// Verify we got all the rows
	if rowCount != len(rows) {
		t.Errorf("Row count mismatch: got %d, want %d", rowCount, len(rows))
	}

	// Test context access
	if result.Context() != ctx {
		t.Errorf("Context mismatch")
	}

	// Test Close
	if err := result.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}

	// Test RowsAffected and LastInsertID (which should be 0 for this test case)
	if rowsAffected := result.RowsAffected(); rowsAffected != 0 {
		t.Errorf("RowsAffected mismatch: got %d, want 0", rowsAffected)
	}

	if lastInsertID := result.LastInsertID(); lastInsertID != 0 {
		t.Errorf("LastInsertID mismatch: got %d, want 0", lastInsertID)
	}

	// Test alias functionality
	aliases := map[string]string{
		"AliasID":   "ID",
		"AliasName": "Name",
	}
	aliasedResult := result.WithAliases(aliases)
	if aliasedResult == nil {
		t.Errorf("WithAliases returned nil")
	}
}
