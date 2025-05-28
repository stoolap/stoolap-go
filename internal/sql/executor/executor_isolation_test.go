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

// TestExecutorBasicOperations tests basic executor operations with a real engine
func TestExecutorBasicOperations(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	executor := NewExecutor(engine)

	// Test creating a table
	ctx := context.Background()
	result, err := executor.Execute(ctx, nil, "CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify the result
	rowsAffected := result.RowsAffected()
	if rowsAffected != 0 {
		t.Errorf("Expected 0 rows affected for CREATE TABLE, got %d", rowsAffected)
	}

	// Test inserting data
	result, err = executor.Execute(ctx, nil, "INSERT INTO test_table (id, name) VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	rowsAffected = result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected for INSERT, got %d", rowsAffected)
	}

	// Test selecting data
	result, err = executor.Execute(ctx, nil, "SELECT id, name FROM test_table WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	// Verify we can iterate over results
	if !result.Next() {
		t.Error("Expected to have at least one row")
	}

	var id int
	var name string
	err = result.Scan(&id, &name)
	if err != nil {
		t.Fatalf("Failed to scan result: %v", err)
	}

	if id != 1 || name != "test" {
		t.Errorf("Expected id=1, name='test', got id=%d, name='%s'", id, name)
	}

	// Close the result
	result.Close()
}

// TestExecutorVectorizedModeToggle tests enabling/disabling vectorized mode
func TestExecutorVectorizedModeToggle(t *testing.T) {
	mockEngine := &mockEngine{}
	executor := NewExecutor(mockEngine)

	// Should be disabled by default
	if executor.vectorizedMode {
		t.Error("Vectorized mode should be disabled by default")
	}

	// Enable it
	executor.EnableVectorizedMode()
	if !executor.vectorizedMode {
		t.Error("Vectorized mode should be enabled after EnableVectorizedMode()")
	}

	// Get status
	status := executor.GetVectorizedStatus()
	if status != "Vectorized mode is enabled" {
		t.Errorf("Unexpected status: %s", status)
	}

	// Disable it
	executor.DisableVectorizedMode()
	if executor.vectorizedMode {
		t.Error("Vectorized mode should be disabled after DisableVectorizedMode()")
	}
}

// TestExecutorDefaultIsolationLevel tests the default isolation level
func TestExecutorDefaultIsolationLevel(t *testing.T) {
	mockEngine := &mockEngine{}
	executor := NewExecutor(mockEngine)

	// Check default
	if executor.defaultIsolationLevel != storage.ReadCommitted {
		t.Errorf("Expected default isolation level to be ReadCommitted, got %v", executor.defaultIsolationLevel)
	}

	level := executor.GetDefaultIsolationLevel()
	if level != storage.ReadCommitted {
		t.Errorf("GetDefaultIsolationLevel() returned %v, expected ReadCommitted", level)
	}
}

// TestExecutorQueryCacheExists tests that query cache is initialized
func TestExecutorQueryCacheExists(t *testing.T) {
	mockEngine := &mockEngine{}
	executor := NewExecutor(mockEngine)

	if executor.queryCache == nil {
		t.Error("Query cache should not be nil")
	}

	if executor.functionRegistry == nil {
		t.Error("Function registry should not be nil")
	}
}
