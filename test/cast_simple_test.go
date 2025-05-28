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

func TestCastSimple(t *testing.T) {
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

	// Create a test table with a simple schema
	_, err = executor.Execute(ctx, nil, `
		CREATE TABLE test_cast_simple (
			id INTEGER,
			val TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a single row
	_, err = executor.Execute(ctx, nil, `
		INSERT INTO test_cast_simple (id, val) VALUES (1, '123')
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Simple query without CAST to verify table is working
	t.Run("Simple query", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, `
			SELECT id, val FROM test_cast_simple
		`)
		if err != nil {
			t.Fatalf("failed to run simple query: %v", err)
		}

		// Print the column names
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

		t.Logf("Simple query result: id=%d, val=%s", id, val)

		// There should be no more rows
		if result.Next() {
			t.Fatal("expected only one row, got more")
		}
	})

	// Try a literal CAST without referencing a column
	t.Run("Cast literal", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, `
			SELECT CAST('123' AS INTEGER) FROM test_cast_simple
		`)
		if err != nil {
			t.Fatalf("failed to run CAST query: %v", err)
		}

		// Print the column names
		t.Logf("Column names: %v", result.Columns())

		// There should be exactly one row
		if !result.Next() {
			t.Fatal("expected one row, got none")
		}

		var castVal int64
		if err := result.Scan(&castVal); err != nil {
			t.Fatalf("failed to scan result: %v", err)
		}

		t.Logf("CAST result: %d (type: %T)", castVal, castVal)
	})

	// Only try the column reference CAST if the literal CAST succeeded
	t.Run("Cast column", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, `
			SELECT CAST(val AS INTEGER) FROM test_cast_simple
		`)
		if err != nil {
			t.Fatalf("failed to run CAST query: %v", err)
		}

		// Print the column names
		t.Logf("Column names: %v", result.Columns())

		// There should be exactly one row
		if !result.Next() {
			t.Fatal("expected one row, got none")
		}

		var castVal int64
		if err := result.Scan(&castVal); err != nil {
			t.Fatalf("failed to scan result: %v", err)
		}

		t.Logf("CAST result: %d (type: %T)", castVal, castVal)
	})
}
