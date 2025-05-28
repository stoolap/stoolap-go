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

func TestCastSimpleDebug(t *testing.T) {
	// Create an in-memory database
	ctx := context.Background()
	db, _ := stoolap.Open("memory://")
	defer db.Close()

	executor := db.Executor()

	// Create a table with known values
	executor.Execute(ctx, nil, `
		CREATE TABLE test_simple (
			id INTEGER,
			text_val TEXT
		)
	`)

	// Insert a single value
	executor.Execute(ctx, nil, `
		INSERT INTO test_simple (id, text_val) 
		VALUES (1, '123')
	`)

	// Test a simple CAST using a literal
	result, _ := executor.Execute(ctx, nil, "SELECT CAST('123' AS INTEGER) FROM test_simple")
	if result.Next() {
		var val interface{}
		result.Scan(&val)
		t.Logf("CAST('123' AS INTEGER) = %v (type: %T)", val, val)
	}

	// Test a CAST on a column
	result, _ = executor.Execute(ctx, nil, "SELECT CAST(text_val AS INTEGER) FROM test_simple")
	if result.Next() {
		var val interface{}
		result.Scan(&val)
		t.Logf("CAST(text_val AS INTEGER) = %v (type: %T)", val, val)
	}

	// Test a CAST in a WHERE clause
	result, _ = executor.Execute(ctx, nil, "SELECT id FROM test_simple WHERE CAST(text_val AS INTEGER) > 100")
	if result.Next() {
		var id int64
		result.Scan(&id)
		t.Logf("Found id %d where CAST(text_val AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row where CAST(text_val AS INTEGER) > 100, but none found")
	}

	// Test a CAST with a fixed value in a WHERE clause
	result, _ = executor.Execute(ctx, nil, "SELECT id FROM test_simple WHERE CAST('123' AS INTEGER) > 100")
	if result.Next() {
		var id int64
		result.Scan(&id)
		t.Logf("Found id %d where CAST('123' AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row where CAST('123' AS INTEGER) > 100, but none found")
	}
}
