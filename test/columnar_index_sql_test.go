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

	"github.com/stoolap/stoolap-go/internal/sql"
	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestColumnarIndexSQL(t *testing.T) {
	// Create a new in-memory database
	config := &storage.Config{
		Path: "memory://",
	}
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a new SQL executor
	executor := sql.NewExecutor(engine)
	ctx := context.Background()

	// Start a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test table
	_, err = executor.Execute(ctx, tx, "CREATE TABLE test_columnar (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = executor.Execute(ctx, tx, "INSERT INTO test_columnar VALUES (1, 'Alice', 30, true)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	_, err = executor.Execute(ctx, tx, "INSERT INTO test_columnar VALUES (2, 'Bob', 25, false)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	_, err = executor.Execute(ctx, tx, "INSERT INTO test_columnar VALUES (3, 'Charlie', 40, true)")
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Create a columnar index on the 'age' column
	_, err = executor.Execute(ctx, tx, "CREATE COLUMNAR INDEX ON test_columnar (age)")
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Create a columnar index on the 'active' column
	_, err = executor.Execute(ctx, tx, "CREATE COLUMNAR INDEX ON test_columnar (active)")
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Test querying with the columnar index
	result, err := executor.Execute(ctx, tx, "SELECT * FROM test_columnar WHERE age = 25")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify results
	count := 0
	for result.Next() {
		var id int64
		var name string
		var age int64
		var active bool
		err := result.Scan(&id, &name, &age, &active)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		count++
		if id != 2 || name != "Bob" || age != 25 || active != false {
			t.Errorf("Unexpected row values: id=%d, name=%s, age=%d, active=%t", id, name, age, active)
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	// Test with the 'active' column
	activeResult, err := executor.Execute(ctx, tx, "SELECT * FROM test_columnar WHERE active = true")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify results
	activeCount := 0
	for activeResult.Next() {
		var id int64
		var name string
		var age int64
		var active bool
		err := activeResult.Scan(&id, &name, &age, &active)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		activeCount++

		if id != 1 && id != 3 {
			t.Errorf("Unexpected row ID: %d", id)
		}

		if !active {
			t.Errorf("Expected active to be true, got false for ID %d", id)
		}
	}

	if activeCount != 2 {
		t.Errorf("Expected 2 rows, got %d", activeCount)
	}

	// Drop the columnar index
	_, err = executor.Execute(ctx, tx, "DROP COLUMNAR INDEX ON test_columnar (age)")
	if err != nil {
		t.Fatalf("Failed to drop columnar index: %v", err)
	}

	// The query should still work even after dropping the index
	postDropResult, err := executor.Execute(ctx, tx, "SELECT * FROM test_columnar WHERE age = 25")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify results
	postDropCount := 0
	for postDropResult.Next() {
		postDropCount++
	}

	if postDropCount != 1 {
		t.Errorf("Expected 1 row, got %d", postDropCount)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
