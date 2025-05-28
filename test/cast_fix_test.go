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

	"github.com/stoolap/stoolap"
)

// TestCastFix tests a direct fix for the CAST expression issue in WHERE clauses
func TestCastFix(t *testing.T) {
	// Create a test database
	ctx := context.Background()
	db, _ := stoolap.Open("memory://")
	defer db.Close()

	executor := db.Executor()

	// Create test table and insert data
	executor.Execute(ctx, nil, `
		CREATE TABLE cast_fix_test (
			id INTEGER PRIMARY KEY,
			text_val TEXT
		)
	`)

	executor.Execute(ctx, nil, `
		INSERT INTO cast_fix_test (id, text_val) VALUES 
		(1, '123'),
		(2, '456')
	`)

	// Test direct CAST in SELECT - this should work
	result, _ := executor.Execute(ctx, nil, `SELECT CAST(text_val AS INTEGER) FROM cast_fix_test WHERE id = 1`)
	if result.Next() {
		var castVal int64
		result.Scan(&castVal)
		if castVal != 123 {
			t.Errorf("Expected CAST(text_val AS INTEGER) to be 123, got %d", castVal)
		} else {
			t.Logf("CAST in SELECT works: CAST(text_val AS INTEGER) = %d", castVal)
		}
	}

	// Test WHERE with CAST - this is what we're fixing
	whereResult, _ := executor.Execute(ctx, nil, `
		SELECT id FROM cast_fix_test WHERE CAST(text_val AS INTEGER) > 200
	`)

	// Count matches
	matches := 0
	for whereResult.Next() {
		var id int64
		whereResult.Scan(&id)
		t.Logf("CAST in WHERE works: row id=%d matches CAST(text_val AS INTEGER) > 200", id)
		matches++
	}

	// We expect only one match (id=2, text_val=456)
	if matches != 1 {
		t.Errorf("Expected 1 match for WHERE CAST(text_val AS INTEGER) > 200, got %d", matches)
	}
}
