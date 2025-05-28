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
package executor

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

func TestShowIndexesMultiColumn(t *testing.T) {
	// Create a test database with MVCCEngine
	config := &storage.Config{
		Path: "", // In-memory mode
	}
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	executor := NewExecutor(engine)

	ctx := context.Background()
	tx, err := engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test table
	createTableSQL := "CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price FLOAT)"
	_, err = executor.Execute(ctx, tx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a single-column index
	createSingleIndexSQL := "CREATE INDEX idx_name ON test_products (name)"
	_, err = executor.Execute(ctx, tx, createSingleIndexSQL)
	if err != nil {
		t.Fatalf("Failed to create single-column index: %v", err)
	}

	// Create a multi-column index
	createMultiIndexSQL := "CREATE INDEX idx_name_category ON test_products (name, category)"
	_, err = executor.Execute(ctx, tx, createMultiIndexSQL)
	if err != nil {
		t.Fatalf("Failed to create multi-column index: %v", err)
	}

	// Create a unique multi-column index
	createUniqueMultiIndexSQL := "CREATE UNIQUE INDEX unique_cat_price ON test_products (category, price)"
	_, err = executor.Execute(ctx, tx, createUniqueMultiIndexSQL)
	if err != nil {
		t.Fatalf("Failed to create unique multi-column index: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Start a new transaction for querying
	tx, err = engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create and execute a SHOW INDEXES statement
	showIndexesStmt := &parser.ShowIndexesStatement{
		TableName: &parser.Identifier{Value: "test_products"},
	}

	result, err := executor.executeShowIndexes(ctx, tx, showIndexesStmt)
	if err != nil {
		t.Fatalf("Failed to execute SHOW INDEXES: %v", err)
	}

	// Check columns
	columns := result.Columns()
	expectedColumns := []string{"Table", "Index Name", "Column Name(s)", "Type", "Unique"}
	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}
	for i, col := range columns {
		if i < len(expectedColumns) && col != expectedColumns[i] {
			t.Errorf("Expected column %d to be %s, got %s", i, expectedColumns[i], col)
		}
	}

	// Verify we have the correct number of rows - we should have 3 rows for our indexes:
	// 1 row for single-column index idx_name
	// 1 row for multi-column index idx_name_category
	// 1 row for multi-column index unique_cat_price

	// Check number of rows
	rowCount := 0
	indexCounts := make(map[string]int)
	columnData := make(map[string]string)

	for result.Next() {
		row := result.Row()
		if len(row) < 3 {
			t.Fatalf("Row has fewer than 3 columns")
		}

		indexName, ok := row[1].AsString()
		if !ok {
			t.Fatalf("Failed to convert index name to string")
		}

		columnNames, ok := row[2].AsString()
		if ok {
			columnData[indexName] = columnNames
		}

		// Count occurrences of each index
		indexCounts[indexName]++
		rowCount++
	}
	result.Close()

	// Expect 3 total rows (one per index)
	if rowCount != 3 {
		t.Errorf("Expected 3 rows in SHOW INDEXES result, got %d", rowCount)
	}

	// Check single-column index
	if count, ok := indexCounts["idx_name"]; !ok || count != 1 {
		t.Errorf("Expected single-column index 'idx_name' to appear exactly once, got %d", count)
	}

	// Check multi-column indexes
	if count, ok := indexCounts["idx_name_category"]; !ok || count != 1 {
		t.Errorf("Expected multi-column index 'idx_name_category' to appear exactly once, got %d", count)
	}

	if count, ok := indexCounts["unique_cat_price"]; !ok || count != 1 {
		t.Errorf("Expected multi-column index 'unique_cat_price' to appear exactly once, got %d", count)
	}

	// Verify column names in the Column Name(s) field
	if colNames, ok := columnData["idx_name_category"]; !ok || !strings.Contains(colNames, "name") || !strings.Contains(colNames, "category") {
		t.Errorf("Expected 'idx_name_category' to have both column names, got: %s", colNames)
	}

	if colNames, ok := columnData["unique_cat_price"]; !ok || !strings.Contains(colNames, "category") || !strings.Contains(colNames, "price") {
		t.Errorf("Expected 'unique_cat_price' to have both column names, got: %s", colNames)
	}
}

func TestDropIndexMultiColumn(t *testing.T) {
	// Create a test database with MVCCEngine
	config := &storage.Config{
		Path: "", // In-memory mode
	}
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	executor := NewExecutor(engine)

	ctx := context.Background()
	tx, err := engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test table
	createTableSQL := "CREATE TABLE test_products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price FLOAT)"
	_, err = executor.Execute(ctx, tx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a single-column index
	createSingleIndexSQL := "CREATE INDEX idx_name ON test_products (name)"
	_, err = executor.Execute(ctx, tx, createSingleIndexSQL)
	if err != nil {
		t.Fatalf("Failed to create single-column index: %v", err)
	}

	// Create a multi-column index
	createMultiIndexSQL := "CREATE INDEX idx_name_category ON test_products (name, category)"
	_, err = executor.Execute(ctx, tx, createMultiIndexSQL)
	if err != nil {
		t.Fatalf("Failed to create multi-column index: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Start a new transaction for verification
	tx, err = engine.BeginTx(ctx, sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Verify both indexes exist
	showIndexesStmt := &parser.ShowIndexesStatement{
		TableName: &parser.Identifier{Value: "test_products"},
	}
	result, err := executor.executeShowIndexes(ctx, tx, showIndexesStmt)
	if err != nil {
		t.Fatalf("Failed to execute SHOW INDEXES: %v", err)
	}

	// Count rows to confirm we have 2 rows (1 for idx_name, 1 for idx_name_category)
	rowCount := 0
	for result.Next() {
		rowCount++
	}
	result.Close()

	if rowCount != 2 {
		t.Errorf("Expected 2 index rows before DROP, got %d", rowCount)
	}

	// Now drop the multi-column index
	dropIndexSQL := "DROP INDEX idx_name_category ON test_products"
	_, err = executor.Execute(ctx, tx, dropIndexSQL)
	if err != nil {
		t.Fatalf("Failed to drop multi-column index: %v", err)
	}

	// Verify only the single-column index remains
	result, err = executor.executeShowIndexes(ctx, tx, showIndexesStmt)
	if err != nil {
		t.Fatalf("Failed to execute SHOW INDEXES after DROP: %v", err)
	}

	// Should only have 1 row now for idx_name
	rowCount = 0
	indexNames := make(map[string]bool)

	for result.Next() {
		row := result.Row()
		indexName, _ := row[1].AsString()
		indexNames[indexName] = true
		rowCount++
	}
	result.Close()

	if rowCount != 1 {
		t.Errorf("Expected 1 index row after DROP, got %d", rowCount)
	}

	if _, exists := indexNames["idx_name"]; !exists {
		t.Errorf("Expected idx_name to still exist after DROP")
	}

	if _, exists := indexNames["idx_name_category"]; exists {
		t.Errorf("Expected idx_name_category to be removed after DROP")
	}

	// Try to drop the index again with IF EXISTS (should not error)
	dropAgainSQL := "DROP INDEX IF EXISTS idx_name_category ON test_products"
	_, err = executor.Execute(ctx, tx, dropAgainSQL)
	if err != nil {
		t.Errorf("DROP INDEX IF EXISTS should not error on non-existent index: %v", err)
	}

	// Try to drop without IF EXISTS (should error)
	dropAgainNoIfSQL := "DROP INDEX idx_name_category ON test_products"
	_, err = executor.Execute(ctx, tx, dropAgainNoIfSQL)
	if err == nil {
		t.Errorf("DROP INDEX without IF EXISTS should error on non-existent index")
	}

	tx.Rollback()
}
