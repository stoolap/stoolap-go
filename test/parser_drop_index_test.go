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

	// Import necessary packages to register factory functions
	_ "github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestDropIndexIfExists tests the DROP INDEX IF EXISTS statement
func TestDropIndexIfExists(t *testing.T) {
	// Get the block storage engine factory
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatalf("Failed to get db engine factory")
	}

	// Create the engine with the connection string
	engine, err := factory.Create("memory://")
	if err != nil {
		t.Fatalf("Failed to create db engine: %v", err)
	}

	// Open the engine
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a SQL executor
	executor := sql.NewExecutor(engine)

	// Create a test table
	result, err := executor.Execute(context.Background(), nil, `
		CREATE TABLE test_drop_index (
			id INTEGER, 
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Try to drop a nonexistent index with IF EXISTS (should succeed)
	result, err = executor.Execute(context.Background(), nil, `
		DROP INDEX IF EXISTS nonexistent_idx ON test_drop_index
	`)
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS failed: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Try to drop a nonexistent index without IF EXISTS (should fail)
	result, err = executor.Execute(context.Background(), nil, `
		DROP INDEX nonexistent_idx ON test_drop_index
	`)
	if err == nil {
		if result != nil {
			result.Close()
		}
		t.Fatalf("Expected error when dropping nonexistent index without IF EXISTS, but got none")
	}

	t.Logf("DROP INDEX IF EXISTS test passed!")
}
