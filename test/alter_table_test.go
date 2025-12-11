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

// TestAlterTableOperations tests ALTER TABLE operations
func TestAlterTableOperations(t *testing.T) {
	t.Skip("The alter table currently not implemented")
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
	result, err := executor.Execute(context.Background(), nil, `CREATE TABLE test_alter (id INTEGER, name TEXT, value FLOAT)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Test adding a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter ADD COLUMN description TEXT`)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was added correctly
	schema, err := engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if the description column exists
	descriptionFound := false
	for _, col := range schema.Columns {
		if col.Name == "description" && col.Type == storage.TEXT {
			descriptionFound = true
			break
		}
	}

	if !descriptionFound {
		t.Fatalf("Added column 'description' not found in schema")
	}

	// Test dropping a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter DROP COLUMN description`)
	if err != nil {
		t.Fatalf("Failed to drop column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was dropped correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Make sure description column is no longer present
	for _, col := range schema.Columns {
		if col.Name == "description" {
			t.Fatalf("Dropped column 'description' still found in schema")
		}
	}

	// Test renaming a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter RENAME COLUMN name TO full_name`)
	if err != nil {
		t.Fatalf("Failed to rename column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was renamed correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if the full_name column exists and name doesn't
	fullNameFound := false
	for _, col := range schema.Columns {
		if col.Name == "name" {
			t.Fatalf("Old column name 'name' still found in schema")
		}
		if col.Name == "full_name" {
			fullNameFound = true
		}
	}

	if !fullNameFound {
		t.Fatalf("Renamed column 'full_name' not found in schema")
	}

	// Test modifying a column
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter MODIFY COLUMN value INTEGER`)
	if err != nil {
		t.Fatalf("Failed to modify column: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify column was modified correctly
	schema, err = engine.GetTableSchema("test_alter")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Check if value column now has INTEGER type
	valueFound := false
	for _, col := range schema.Columns {
		if col.Name == "value" {
			if col.Type != storage.INTEGER {
				t.Fatalf("Modified column 'value' has incorrect type: got %v, want %v", col.Type, storage.INTEGER)
			}
			valueFound = true
			break
		}
	}

	if !valueFound {
		t.Fatalf("Column 'value' not found in schema")
	}

	// Test renaming a table
	result, err = executor.Execute(context.Background(), nil, `ALTER TABLE test_alter RENAME TO test_renamed`)
	if err != nil {
		t.Fatalf("Failed to rename table: %v", err)
	}
	if result != nil {
		result.Close()
	}

	// Verify table was renamed correctly by checking if the tables exist directly
	t.Log("Checking if the table was renamed correctly...")

	// Check if old table exists
	oldTableExists, err := engine.TableExists("test_alter")
	if err != nil {
		t.Fatalf("Error checking if old table exists: %v", err)
	}
	if oldTableExists {
		t.Fatalf("Old table name 'test_alter' still exists")
	}

	// Check if new table exists
	newTableExists, err := engine.TableExists("test_renamed")
	if err != nil {
		t.Fatalf("Error checking if new table exists: %v", err)
	}
	if !newTableExists {
		t.Fatalf("Renamed table 'test_renamed' not found")
	}

	t.Log("Table renaming verified successfully")
}
