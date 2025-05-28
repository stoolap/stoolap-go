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

	// Import the mvcc storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// TestExecutorShowCommands tests SHOW commands
func TestExecutorShowCommands(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test_show")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	executor := NewExecutor(engine)
	ctx := context.Background()

	// Create some test tables
	_, err = executor.Execute(ctx, nil, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = executor.Execute(ctx, nil, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	// Test SHOW TABLES
	result, err := executor.Execute(ctx, nil, "SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to execute SHOW TABLES: %v", err)
	}

	tableCount := 0
	foundUsers := false
	foundProducts := false

	for result.Next() {
		var tableName string
		err = result.Scan(&tableName)
		if err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}

		if tableName == "users" {
			foundUsers = true
		}
		if tableName == "products" {
			foundProducts = true
		}
		tableCount++
	}
	result.Close()

	if tableCount < 2 {
		t.Errorf("Expected at least 2 tables, got %d", tableCount)
	}
	if !foundUsers {
		t.Error("Did not find 'users' table in SHOW TABLES")
	}
	if !foundProducts {
		t.Error("Did not find 'products' table in SHOW TABLES")
	}

	// Test SHOW CREATE TABLE
	result, err = executor.Execute(ctx, nil, "SHOW CREATE TABLE users")
	if err != nil {
		t.Fatalf("Failed to execute SHOW CREATE TABLE: %v", err)
	}

	if !result.Next() {
		t.Fatal("Expected SHOW CREATE TABLE to return a row")
	}

	var tableName, createStmt string
	err = result.Scan(&tableName, &createStmt)
	if err != nil {
		t.Fatalf("Failed to scan SHOW CREATE TABLE result: %v", err)
	}

	if tableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", tableName)
	}

	// The create statement should contain the table name and columns
	if createStmt == "" {
		t.Error("Expected non-empty CREATE TABLE statement")
	}
	result.Close()

	// Create indexes for testing SHOW INDEXES
	_, err = executor.Execute(ctx, nil, "CREATE INDEX idx_users_email ON users(email)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	_, err = executor.Execute(ctx, nil, "CREATE UNIQUE INDEX idx_users_name ON users(name)")
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Test SHOW INDEXES
	result, err = executor.Execute(ctx, nil, "SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to execute SHOW INDEXES: %v", err)
	}

	indexCount := 0
	for result.Next() {
		// We expect at least table name and index name columns
		// The exact schema depends on the implementation
		indexCount++
	}
	result.Close()

	if indexCount < 2 {
		t.Errorf("Expected at least 2 indexes, got %d", indexCount)
	}

	// Test SHOW CREATE TABLE for non-existent table
	_, err = executor.Execute(ctx, nil, "SHOW CREATE TABLE non_existent")
	if err == nil {
		t.Error("Expected error for SHOW CREATE TABLE on non-existent table")
	}

	// Test SHOW INDEXES for non-existent table
	_, err = executor.Execute(ctx, nil, "SHOW INDEXES FROM non_existent")
	if err == nil {
		t.Error("Expected error for SHOW INDEXES on non-existent table")
	}
}

// TestExecutorPragmaCommands tests PRAGMA commands
func TestExecutorPragmaCommands(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test_pragma")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	executor := NewExecutor(engine)
	ctx := context.Background()

	// Test PRAGMA version
	result, err := executor.Execute(ctx, nil, "PRAGMA version")
	if err != nil {
		t.Fatalf("Failed to execute PRAGMA version: %v", err)
	}

	if !result.Next() {
		t.Fatal("Expected PRAGMA version to return a row")
	}

	var version string
	err = result.Scan(&version)
	if err != nil {
		t.Fatalf("Failed to scan version: %v", err)
	}

	if version == "" {
		t.Error("Expected non-empty version string")
	}
	t.Logf("Got version: %s", version)
	result.Close()

	// Test other PRAGMA commands
	// Test PRAGMA snapshot_interval (read)
	result, err = executor.Execute(ctx, nil, "PRAGMA snapshot_interval")
	if err != nil {
		t.Fatalf("Failed to execute PRAGMA snapshot_interval: %v", err)
	}

	if !result.Next() {
		t.Fatal("Expected PRAGMA snapshot_interval to return a row")
	}

	var interval int
	err = result.Scan(&interval)
	if err != nil {
		t.Fatalf("Failed to scan snapshot_interval: %v", err)
	}
	t.Logf("Got snapshot_interval: %d", interval)
	result.Close()

	// Test PRAGMA snapshot_interval (write)
	_, err = executor.Execute(ctx, nil, "PRAGMA snapshot_interval = 5000")
	if err != nil {
		t.Fatalf("Failed to set PRAGMA snapshot_interval: %v", err)
	}

	// Verify the change
	result, err = executor.Execute(ctx, nil, "PRAGMA snapshot_interval")
	if err != nil {
		t.Fatalf("Failed to read PRAGMA snapshot_interval after setting: %v", err)
	}

	if !result.Next() {
		t.Fatal("Expected result after setting snapshot_interval")
	}

	err = result.Scan(&interval)
	if err != nil {
		t.Fatalf("Failed to scan new snapshot_interval: %v", err)
	}

	if interval != 5000 {
		t.Errorf("Expected snapshot_interval to be 5000, got %d", interval)
	}
	result.Close()
}
