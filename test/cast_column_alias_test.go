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

func TestColumnAlias(t *testing.T) {
	ctx := context.Background()

	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Get the SQL executor
	executor := db.Executor()

	// Create a test table with a simple schema
	_, err = executor.Execute(ctx, nil, `
		CREATE TABLE test_alias (
			id INTEGER,
			val TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a single row
	_, err = executor.Execute(ctx, nil, `
		INSERT INTO test_alias (id, val) VALUES (1, '123')
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test simple column aliases
	t.Run("Simple column alias", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, `
			SELECT id AS alias_id, val AS alias_val FROM test_alias
		`)
		if err != nil {
			t.Fatalf("failed to run query: %v", err)
		}

		// Print the column names to see what's being returned
		t.Logf("Column names: %v", result.Columns())

		// There should be exactly one row
		if !result.Next() {
			t.Fatal("expected one row, got none")
		}

		var id int64
		var val string
		if err := result.Scan(&id, &val); err != nil {
			t.Fatalf("failed to scan result: %v", err)
		}

		t.Logf("Alias result: id=%d, val=%s", id, val)
	})

	// Test expression with alias
	t.Run("Expression with alias", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, `
			SELECT (id + 10) AS calculated FROM test_alias
		`)
		if err != nil {
			t.Fatalf("failed to run query: %v", err)
		}

		// Print the column names to see what's being returned
		t.Logf("Column names: %v", result.Columns())

		// There should be exactly one row
		if !result.Next() {
			t.Fatal("expected one row, got none")
		}

		var calculated int64
		if err := result.Scan(&calculated); err != nil {
			t.Fatalf("failed to scan result: %v", err)
		}

		t.Logf("Calculated value: %d", calculated)

		// The calculated value should be id + 10 = 1 + 10 = 11
		if calculated != 11 {
			t.Errorf("expected calculated=11, got %d", calculated)
		}
	})
}
