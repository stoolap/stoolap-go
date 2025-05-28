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
	"testing"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

func setupTestEngine(t *testing.T) (storage.Engine, *Executor) {
	engine := mvcc.NewMVCCEngine(&storage.Config{Path: "memory://"})

	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}

	executor := NewExecutor(engine)
	return engine, executor
}

func TestExecuteInsert(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table first
	createSQL := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test INSERT
	insertSQL := "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
	result, err = executor.Execute(ctx, nil, insertSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}
	result.Close()
}

func TestExecuteUpdate(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup table with data
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		"INSERT INTO users VALUES (1, 'Alice', 30)",
		"INSERT INTO users VALUES (2, 'Bob', 25)",
		"INSERT INTO users VALUES (3, 'Charlie', 35)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test UPDATE
	updateSQL := "UPDATE users SET age = 31 WHERE name = 'Alice'"
	result, err := executor.Execute(ctx, nil, updateSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}
	result.Close()

	// Test UPDATE with arithmetic expression
	updateSQL = "UPDATE users SET age = age + 1 WHERE age < 30"
	result, err = executor.Execute(ctx, nil, updateSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 1 { // Only Bob's age < 30
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}
	result.Close()
}

func TestExecuteDelete(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup table with data
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		"INSERT INTO users VALUES (1, 'Alice', 30)",
		"INSERT INTO users VALUES (2, 'Bob', 25)",
		"INSERT INTO users VALUES (3, 'Charlie', 35)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test DELETE with WHERE clause
	deleteSQL := "DELETE FROM users WHERE age > 30"
	result, err := executor.Execute(ctx, nil, deleteSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 1 { // Only Charlie has age > 30
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}
	result.Close()

	// Test DELETE without WHERE (delete all)
	deleteSQL = "DELETE FROM users"
	result, err = executor.Execute(ctx, nil, deleteSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 2 { // Alice and Bob remaining
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected())
	}
	result.Close()
}

func TestExecuteCreateIndex(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table first
	createSQL := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test CREATE INDEX
	indexSQL := "CREATE INDEX idx_name ON users (name)"
	result, err = executor.Execute(ctx, nil, indexSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test CREATE UNIQUE INDEX
	uniqueIndexSQL := "CREATE UNIQUE INDEX idx_email ON users (email)"
	result, err = executor.Execute(ctx, nil, uniqueIndexSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()
}

func TestExecuteDropTable(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table first
	createSQL := "CREATE TABLE temp_table (id INTEGER)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test DROP TABLE
	dropSQL := "DROP TABLE temp_table"
	result, err = executor.Execute(ctx, nil, dropSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Verify table is dropped by trying to insert
	insertSQL := "INSERT INTO temp_table VALUES (1)"
	_, err = executor.Execute(ctx, nil, insertSQL)
	if err == nil {
		t.Error("Expected error when inserting into dropped table")
	}
}

func TestExecuteDropIndex(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup table with index
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE INDEX idx_name ON users (name)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test DROP INDEX
	dropIndexSQL := "DROP INDEX idx_name ON users"
	result, err := executor.Execute(ctx, nil, dropIndexSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()
}

func TestExecuteTruncate(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup table with data
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'Bob')",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test DELETE all (equivalent to TRUNCATE)
	deleteSQL := "DELETE FROM users"
	result, err := executor.Execute(ctx, nil, deleteSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Verify table is empty
	selectSQL := "SELECT COUNT(*) FROM users"
	result, err = executor.Execute(ctx, nil, selectSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	if result.Next() {
		var count int
		if err := result.Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("Expected 0 rows after DELETE all, got %d", count)
		}
	}
}

func TestExecuteMerge(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table with unique constraint
	createSQL := "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT UNIQUE, stock INTEGER)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Insert initial data
	insertSQL := "INSERT INTO products VALUES (1, 'PROD001', 100)"
	result, err = executor.Execute(ctx, nil, insertSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test MERGE (INSERT ... ON DUPLICATE KEY UPDATE)
	mergeSQL := "INSERT INTO products VALUES (1, 'PROD001', 50) ON DUPLICATE KEY UPDATE stock = stock + 50"
	result, err = executor.Execute(ctx, nil, mergeSQL)
	if err != nil {
		t.Fatal(err)
	}

	if result.RowsAffected() != 2 { // 2 rows affected for UPDATE in MySQL compatibility
		t.Errorf("Expected 2 rows affected for MERGE UPDATE, got %d", result.RowsAffected())
	}
	result.Close()

	// Verify stock was updated
	selectSQL := "SELECT stock FROM products WHERE sku = 'PROD001'"
	result, err = executor.Execute(ctx, nil, selectSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	if result.Next() {
		var stock int
		if err := result.Scan(&stock); err != nil {
			t.Fatal(err)
		}
		if stock != 150 {
			t.Errorf("Expected stock 150 after MERGE, got %d", stock)
		}
	}
}

func TestExecuteWithTransaction(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table
	createSQL := "CREATE TABLE test_tx (id INTEGER PRIMARY KEY, value TEXT)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test with explicit transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}

	// Insert in transaction
	insertSQL := "INSERT INTO test_tx VALUES (1, 'test')"
	result, err = executor.Execute(ctx, tx, insertSQL)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	result.Close()

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify data was committed
	selectSQL := "SELECT value FROM test_tx WHERE id = 1"
	result, err = executor.Execute(ctx, nil, selectSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	if result.Next() {
		var value string
		if err := result.Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != "test" {
			t.Errorf("Expected 'test', got '%s'", value)
		}
	} else {
		t.Error("Expected one row")
	}
}

func TestExecuteAlterTable(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table
	createSQL := "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT)"
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test ALTER TABLE ADD COLUMN
	alterSQL := "ALTER TABLE alter_test ADD COLUMN age INTEGER"
	result, err = executor.Execute(ctx, nil, alterSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Verify column was added by inserting with new column
	insertSQL := "INSERT INTO alter_test (id, name, age) VALUES (1, 'Test', 25)"
	result, err = executor.Execute(ctx, nil, insertSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()
}

func TestExecuteConstraints(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table with constraints
	createSQL := `CREATE TABLE constraint_test (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT,
		age INTEGER
	)`
	result, err := executor.Execute(ctx, nil, createSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Create unique index on email
	createIndexSQL := "CREATE UNIQUE INDEX idx_email ON constraint_test(email)"
	result, err = executor.Execute(ctx, nil, createIndexSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	// Test NOT NULL constraint
	insertSQL := "INSERT INTO constraint_test (id, email, age) VALUES (1, 'test@example.com', 25)"
	_, err = executor.Execute(ctx, nil, insertSQL)
	if err == nil {
		t.Error("Expected NOT NULL constraint violation")
	}

	// Test UNIQUE constraint
	insertSQL = "INSERT INTO constraint_test (id, name, email, age) VALUES (1, 'Test1', 'test@example.com', 25)"
	result, err = executor.Execute(ctx, nil, insertSQL)
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	insertSQL = "INSERT INTO constraint_test (id, name, email, age) VALUES (2, 'Test2', 'test@example.com', 30)"
	_, err = executor.Execute(ctx, nil, insertSQL)
	if err == nil {
		t.Error("Expected UNIQUE constraint violation")
	}

	// Test PRIMARY KEY constraint
	insertSQL = "INSERT INTO constraint_test (id, name, email, age) VALUES (1, 'Test3', 'test3@example.com', 35)"
	_, err = executor.Execute(ctx, nil, insertSQL)
	if err == nil {
		t.Error("Expected PRIMARY KEY constraint violation")
	}
}
