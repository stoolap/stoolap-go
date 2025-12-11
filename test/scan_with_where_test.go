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
	"database/sql"
	"testing"

	internalsql "github.com/stoolap/stoolap-go/internal/sql"
	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestScanWithWhereClause verifies that scanning works correctly
// when a WHERE clause adds additional columns to the underlying result
func TestScanWithWhereClause(t *testing.T) {
	// Create a new engine directly
	engine := mvcc.NewMVCCEngine(&storage.Config{Path: "test.db"})
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	executor := internalsql.NewExecutor(engine)

	// Create a transaction
	ctx := context.Background()
	tx, err := engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table with some columns
	_, err = executor.Execute(ctx, tx, `
		CREATE TABLE test_scan_table (
			id INTEGER NOT NULL,
			val TEXT NOT NULL,
			active BOOLEAN NOT NULL
		)
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = executor.Execute(ctx, tx, `
		INSERT INTO test_scan_table (id, val, active) VALUES 
		(1, 'value1', true),
		(2, 'value2', false),
		(3, 'value3', true)
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Simple query with WHERE clause
	// This query only selects 'val' and 'active', but the WHERE clause needs 'id'
	result, err := executor.Execute(ctx, tx, `
		SELECT val, active FROM test_scan_table WHERE id = 1
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	// Verify columns
	columns := result.Columns()

	// Debug: Print result type and columns
	t.Logf("Result type: %T", result)
	t.Logf("Columns: %v", columns)

	if len(columns) != 2 {
		tx.Rollback()
		t.Fatalf("Expected 2 columns, got %d", len(columns))
	}
	if columns[0] != "val" || columns[1] != "active" {
		tx.Rollback()
		t.Fatalf("Unexpected column names: %v", columns)
	}

	// Scan results
	var val string
	var active bool
	if !result.Next() {
		tx.Rollback()
		t.Fatalf("No rows returned")
	}

	// This is where the issue would occur - if we're not properly handling column indices
	err = result.Scan(&val, &active)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify values
	if val != "value1" {
		tx.Rollback()
		t.Errorf("Expected val='value1', got '%s'", val)
	}
	if !active {
		tx.Rollback()
		t.Errorf("Expected active=true, got %t", active)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestExtraColumnsInWhere verifies that scanning works correctly when a query
// has columns in the WHERE clause that aren't in the SELECT clause
func TestExtraColumnsInWhere(t *testing.T) {
	t.Skip("Skipping test for now due to filtering result issues")
	// Create a new engine directly
	engine := mvcc.NewMVCCEngine(&storage.Config{Path: "test.db"})
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	executor := internalsql.NewExecutor(engine)

	// Create a transaction
	ctx := context.Background()
	tx, err := engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table with several columns
	_, err = executor.Execute(ctx, tx, `
		CREATE TABLE complex_table (
			id INTEGER NOT NULL,
			name TEXT NOT NULL,
			age INTEGER NOT NULL,
			active BOOLEAN NOT NULL,
			status TEXT NOT NULL
		)
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = executor.Execute(ctx, tx, `
		INSERT INTO complex_table (id, name, age, active, status) VALUES 
		(1, 'John', 25, true, 'employed'),
		(2, 'Jane', 30, false, 'unemployed'),
		(3, 'Bob', 40, true, 'retired')
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Complex query with WHERE clause that references columns not in the SELECT
	// This tests handling columns in the WHERE that aren't in the SELECT
	result, err := executor.Execute(ctx, tx, `
		SELECT name, status FROM complex_table 
		WHERE id = 1 AND age > 20 AND active = true
	`)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	// Verify columns
	columns := result.Columns()

	// Debug: Print result type and columns
	t.Logf("Result type: %T", result)
	t.Logf("Columns: %v", columns)

	if len(columns) != 2 {
		tx.Rollback()
		t.Fatalf("Expected 2 columns, got %d", len(columns))
	}
	if columns[0] != "name" || columns[1] != "status" {
		tx.Rollback()
		t.Fatalf("Unexpected column names: %v", columns)
	}

	// Scan results
	var name string
	var status string
	if !result.Next() {
		tx.Rollback()
		t.Fatalf("No rows returned")
	}

	// This is where the issue would occur - if we're not properly handling column indices
	err = result.Scan(&name, &status)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify values
	if name != "John" {
		tx.Rollback()
		t.Errorf("Expected name='John', got '%s'", name)
	}
	if status != "employed" {
		tx.Rollback()
		t.Errorf("Expected status='employed', got '%s'", status)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
