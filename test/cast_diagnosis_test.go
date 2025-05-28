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
	"github.com/stoolap/stoolap/internal/functions/scalar"
)

func TestCastDiagnosis(t *testing.T) {
	// Create a simple test database
	ctx := context.Background()
	db, _ := stoolap.Open("memory://")
	defer db.Close()

	executor := db.Executor()

	// Create table with a single row
	executor.Execute(ctx, nil, `
		CREATE TABLE test_cast_diagnosis (
			id INTEGER,
			text_val TEXT
		)
	`)

	executor.Execute(ctx, nil, `
		INSERT INTO test_cast_diagnosis (id, text_val) 
		VALUES (1, '123')
	`)

	// PART 1: Test CAST directly
	castFunc := scalar.NewCastFunction()

	// Test with a literal
	result, err := castFunc.Evaluate("123", "INTEGER")
	if err != nil {
		t.Errorf("CAST('123' AS INTEGER) error: %v", err)
	} else {
		t.Logf("Direct CAST('123' AS INTEGER) = %v (type: %T)", result, result)
	}

	// Test with different values
	values := []string{"123", "0", "-50", ""}
	for _, val := range values {
		result, err := castFunc.Evaluate(val, "INTEGER")
		if err != nil {
			t.Logf("CAST(%q AS INTEGER) error: %v", val, err)
		} else {
			t.Logf("CAST(%q AS INTEGER) = %v (type: %T)", val, result, result)
		}
	}

	// PART 2: Test SQL with CAST
	// Get the value directly
	result1, _ := executor.Execute(ctx, nil, `
		SELECT CAST(text_val AS INTEGER) FROM test_cast_diagnosis
	`)

	if result1.Next() {
		var val interface{}
		result1.Scan(&val)
		t.Logf("SQL CAST(text_val AS INTEGER) = %v (type: %T)", val, val)
	}

	// Try a WHERE clause with literal
	result2, _ := executor.Execute(ctx, nil, `
		SELECT id FROM test_cast_diagnosis WHERE CAST('123' AS INTEGER) > 100
	`)

	if result2.Next() {
		var id int64
		result2.Scan(&id)
		t.Logf("Found row %d with WHERE CAST('123' AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row with WHERE CAST('123' AS INTEGER) > 100")
	}

	// Print the value directly to verify
	valueResult, _ := executor.Execute(ctx, nil, `SELECT text_val FROM test_cast_diagnosis WHERE id = 1`)
	if valueResult.Next() {
		var textVal string
		valueResult.Scan(&textVal)
		t.Logf("Row id=1 has text_val=%q", textVal)
	}

	// Test with CAST in SELECT to verify it works
	castResult, _ := executor.Execute(ctx, nil, `SELECT CAST(text_val AS INTEGER) FROM test_cast_diagnosis WHERE id = 1`)
	if castResult.Next() {
		var castVal int64
		castResult.Scan(&castVal)
		t.Logf("CAST(text_val AS INTEGER) = %d", castVal)
	}

	// Try a WHERE clause with column
	result3, _ := executor.Execute(ctx, nil, `
		SELECT id FROM test_cast_diagnosis WHERE CAST(text_val AS INTEGER) > 100
	`)

	if result3.Next() {
		var id int64
		result3.Scan(&id)
		t.Logf("Found row %d with WHERE CAST(text_val AS INTEGER) > 100", id)
	} else {
		t.Errorf("Expected to find a row with WHERE CAST(text_val AS INTEGER) > 100")
	}

	// Compare with regular comparison
	result4, _ := executor.Execute(ctx, nil, `
		SELECT id FROM test_cast_diagnosis WHERE id > 0
	`)

	if result4.Next() {
		var id int64
		result4.Scan(&id)
		t.Logf("Found row %d with WHERE id > 0", id)
	} else {
		t.Errorf("Expected to find a row with WHERE id > 0")
	}
}
