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
	"fmt"
	"testing"

	"github.com/stoolap/stoolap"
)

func TestCastDebug(t *testing.T) {
	// CAST expressions are now supported
	ctx := context.Background()

	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get the SQL executor
	executor := db.Executor()

	// Create a test table
	_, err = executor.Execute(ctx, nil, `
		CREATE TABLE test_cast_debug (
			id INTEGER,
			int_val INTEGER,
			float_val FLOAT,
			text_val TEXT,
			bool_val BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some test data
	_, err = executor.Execute(ctx, nil, `
		INSERT INTO test_cast_debug (id, int_val, float_val, text_val, bool_val)
		VALUES
			(1, 123, 123.456, '123', true),
			(2, 0, 0.0, '0', false),
			(3, -50, -50.5, '-50', true),
			(4, NULL, NULL, NULL, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	t.Run("Debug basic CAST", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, "SELECT CAST(text_val AS INTEGER) FROM test_cast_debug WHERE id = 1")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Print all rows
		for result.Next() {
			var castVal interface{}
			if err := result.Scan(&castVal); err != nil {
				t.Fatalf("failed to scan result: %v", err)
			}
			t.Logf("CAST result: CAST(text_val AS INTEGER)=%v (type: %T)", castVal, castVal)
		}
	})

	t.Run("Debug WHERE with CAST", func(t *testing.T) {
		// First, query all text values to see what we're working with
		result, err := executor.Execute(ctx, nil, "SELECT id, text_val FROM test_cast_debug")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Print all rows
		t.Log("All rows in table:")
		for result.Next() {
			var id int64
			var textVal string
			if err := result.Scan(&id, &textVal); err != nil {
				t.Fatalf("failed to scan result: %v", err)
			}
			t.Logf("  Row: id=%d, text_val=%s", id, textVal)
		}
		result.Close()

		// Try the comparison without CAST first
		result, err = executor.Execute(ctx, nil, "SELECT id FROM test_cast_debug WHERE id = 1")
		if err != nil {
			t.Fatalf("simple query failed: %v", err)
		}

		var simpleCount int
		for result.Next() {
			simpleCount++
			var id int64
			if err := result.Scan(&id); err != nil {
				t.Fatalf("failed to scan result: %v", err)
			}
			t.Logf("Simple query match: id=%d", id)
		}
		result.Close()

		if simpleCount == 0 {
			t.Errorf("expected at least one match for simple query, got none")
		}

		// Debug directly what CAST returns for each row
		debugResult, _ := executor.Execute(ctx, nil, "SELECT CAST(text_val AS INTEGER) FROM test_cast_debug")
		t.Log("Debug CAST results:")
		for debugResult.Next() {
			var castVal interface{}
			if err := debugResult.Scan(&castVal); err != nil {
				t.Logf("ERROR scanning debug row: %v", err)
			} else {
				t.Logf("  Row: CAST(text_val AS INTEGER)=%v (type: %T)", castVal, castVal)
			}
		}
		debugResult.Close()

		for _, id := range []int{1, 2, 3, 4} {
			textResult, _ := executor.Execute(ctx, nil, fmt.Sprintf("SELECT text_val FROM test_cast_debug WHERE id = %d", id))
			if textResult.Next() {
				var textVal string
				textResult.Scan(&textVal)
				t.Logf("  Row id=%d: text_val=%q", id, textVal)

				// Now try to CAST this value directly
				castResult, _ := executor.Execute(ctx, nil, fmt.Sprintf("SELECT CAST('%s' AS INTEGER) FROM test_cast_debug LIMIT 1", textVal))
				if castResult.Next() {
					var castVal interface{}
					castResult.Scan(&castVal)
					t.Logf("CAST(%q AS INTEGER)=%v (type: %T)", textVal, castVal, castVal)
				}
			}
		}

		// Now try with CAST
		result, err = executor.Execute(ctx, nil, "SELECT id FROM test_cast_debug WHERE CAST(text_val AS INTEGER) > 100")
		if err != nil {
			t.Fatalf("CAST query failed: %v", err)
		}

		// Count matching rows
		var count int
		for result.Next() {
			count++
			var id int64
			if err := result.Scan(&id); err != nil {
				t.Fatalf("failed to scan result: %v", err)
			}
			t.Logf("CAST query match: id=%d", id)
		}

		// Verify we have at least one match
		if count == 0 {
			t.Errorf("expected at least one match for WHERE with CAST, got none")
		}
	})
}
