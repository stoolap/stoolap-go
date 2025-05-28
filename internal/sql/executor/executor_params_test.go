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
	"database/sql/driver"
	"testing"

	"github.com/stoolap/stoolap/internal/storage"

	// Import the mvcc storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// TestExecutorParameterizedQueries tests parameterized query execution
func TestExecutorParameterizedQueries(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test_params")
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

	// Create a test table
	_, err = executor.Execute(ctx, nil, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT with parameters
	params := []driver.NamedValue{
		{Ordinal: 1, Value: 1},
		{Ordinal: 2, Value: "Alice"},
		{Ordinal: 3, Value: 25},
	}

	result, err := executor.ExecuteWithParams(ctx, nil, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", params)
	if err != nil {
		t.Fatalf("Failed to insert with parameters: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}

	// Test SELECT with parameters
	selectParams := []driver.NamedValue{
		{Ordinal: 1, Value: 1},
	}

	result, err = executor.ExecuteWithParams(ctx, nil, "SELECT name, age FROM users WHERE id = ?", selectParams)
	if err != nil {
		t.Fatalf("Failed to select with parameters: %v", err)
	}

	if !result.Next() {
		t.Fatal("Expected to find a row")
	}

	var name string
	var age int
	err = result.Scan(&name, &age)
	if err != nil {
		t.Fatalf("Failed to scan result: %v", err)
	}

	if name != "Alice" || age != 25 {
		t.Errorf("Expected name='Alice', age=25, got name='%s', age=%d", name, age)
	}

	result.Close()

	// Test UPDATE with parameters
	updateParams := []driver.NamedValue{
		{Ordinal: 1, Value: 30},
		{Ordinal: 2, Value: 1},
	}

	result, err = executor.ExecuteWithParams(ctx, nil, "UPDATE users SET age = ? WHERE id = ?", updateParams)
	if err != nil {
		t.Fatalf("Failed to update with parameters: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected())
	}
}

// TestExecutorInsufficientParameters tests error handling for missing parameters
func TestExecutorInsufficientParameters(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test_params_error")
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

	// Create a test table
	_, err = executor.Execute(ctx, nil, "CREATE TABLE test (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to execute with insufficient parameters
	params := []driver.NamedValue{
		{Ordinal: 1, Value: 1},
		// Missing second parameter
	}

	_, err = executor.ExecuteWithParams(ctx, nil, "INSERT INTO test (id, value) VALUES (?, ?)", params)
	if err == nil {
		t.Error("Expected error for insufficient parameters")
	}
}

// TestExecutorSetStatements tests SET statement execution
func TestExecutorSetStatements(t *testing.T) {
	// Create a real in-memory engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatal("Failed to get mvcc engine factory")
	}

	engine, err := factory.Create("memory://test_set")
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

	// Test SET VECTORIZED
	_, err = executor.Execute(ctx, nil, "SET VECTORIZED = true")
	if err != nil {
		t.Fatalf("Failed to set vectorized mode: %v", err)
	}

	if !executor.IsVectorizedModeEnabled() {
		t.Error("Vectorized mode should be enabled")
	}

	// Test SET VECTORIZED OFF
	_, err = executor.Execute(ctx, nil, "SET VECTORIZED = OFF")
	if err != nil {
		t.Fatalf("Failed to disable vectorized mode: %v", err)
	}

	if executor.IsVectorizedModeEnabled() {
		t.Error("Vectorized mode should be disabled")
	}

	// Test SET ISOLATIONLEVEL
	_, err = executor.Execute(ctx, nil, "SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	if executor.GetDefaultIsolationLevel() != storage.SnapshotIsolation {
		t.Errorf("Expected SnapshotIsolation, got %v", executor.GetDefaultIsolationLevel())
	}

	// Test invalid SET variable
	_, err = executor.Execute(ctx, nil, "SET INVALID_VAR = 'value'")
	if err == nil {
		t.Error("Expected error for invalid SET variable")
	}
}
