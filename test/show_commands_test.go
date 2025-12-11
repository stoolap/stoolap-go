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

func TestShowCommands(t *testing.T) {
	// Create an in-memory engine
	config := &storage.Config{
		Path: "",
	}
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create executor
	executor := sql.NewExecutor(engine)

	ctx := context.Background()

	// Step 1: Create a test table - use a separate transaction for each command
	_, err := executor.Execute(ctx, nil, "CREATE TABLE test_show (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Step 2: Create an index on the table
	_, err = executor.Execute(ctx, nil, "CREATE INDEX idx_test_name ON test_show (name)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Step 3: Test SHOW TABLES - use a new transaction so we can see the committed objects
	result, err := executor.Execute(ctx, nil, "SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to execute SHOW TABLES: %v", err)
	}

	// Verify columns
	expectedColumns := []string{"Tables"}
	columns := result.Columns()
	if len(columns) != len(expectedColumns) || columns[0] != expectedColumns[0] {
		t.Errorf("SHOW TABLES result columns mismatch: got %v, want %v", columns, expectedColumns)
	}

	// Verify it returns at least one row with our table
	foundTable := false
	for result.Next() {
		row := result.Row()
		if len(row) < 1 {
			t.Errorf("SHOW TABLES returned empty row")
			continue
		}

		tableName, ok := row[0].AsString()
		if !ok {
			t.Errorf("Failed to convert table name to string")
			continue
		}

		if tableName == "test_show" {
			foundTable = true
			break
		}
	}
	if !foundTable {
		t.Errorf("SHOW TABLES did not return our test table")
	}
	result.Close()

	// Step 4: Test SHOW CREATE TABLE
	result, err = executor.Execute(ctx, nil, "SHOW CREATE TABLE test_show")
	if err != nil {
		t.Fatalf("Failed to execute SHOW CREATE TABLE: %v", err)
	}

	// Verify columns
	expectedColumns = []string{"Table", "Create Table"}
	columns = result.Columns()
	if len(columns) != len(expectedColumns) || columns[0] != expectedColumns[0] || columns[1] != expectedColumns[1] {
		t.Errorf("SHOW CREATE TABLE result columns mismatch: got %v, want %v", columns, expectedColumns)
	}

	// Verify it returns the expected CREATE TABLE statement
	if !result.Next() {
		t.Errorf("SHOW CREATE TABLE did not return any rows")
	} else {
		row := result.Row()
		if len(row) < 2 {
			t.Errorf("SHOW CREATE TABLE returned incomplete row")
		} else {
			tableName, ok := row[0].AsString()
			if !ok || tableName != "test_show" {
				t.Errorf("SHOW CREATE TABLE returned incorrect table name: %v", tableName)
			}

			createStmt, ok := row[1].AsString()
			if !ok || len(createStmt) == 0 {
				t.Errorf("SHOW CREATE TABLE returned empty CREATE TABLE statement")
			} else {
				// Just check for some expected parts in the CREATE TABLE statement
				for _, expectedPart := range []string{"CREATE TABLE", "test_show", "id", "INTEGER", "PRIMARY KEY", "name", "TEXT"} {
					if !stringContains(createStmt, expectedPart) {
						t.Errorf("CREATE TABLE statement missing expected part %q: %s", expectedPart, createStmt)
					}
				}
			}
		}
	}
	result.Close()

	// Step 5: Test SHOW INDEXES
	result, err = executor.Execute(ctx, nil, "SHOW INDEXES FROM test_show")
	if err != nil {
		t.Fatalf("Failed to execute SHOW INDEXES: %v", err)
	}

	// Verify columns
	expectedColumns = []string{"Table", "Index Name", "Column Name(s)", "Type", "Unique"}
	columns = result.Columns()
	if len(columns) != len(expectedColumns) {
		t.Errorf("SHOW INDEXES result columns mismatch: got %v, want %v", columns, expectedColumns)
	}

	// Process the rows to verify that we have indexes
	var indexNames []string

	for result.Next() {
		row := result.Row()
		if len(row) >= 2 {
			tableName, tableOk := row[0].AsString()
			indexName, indexOk := row[1].AsString()

			if tableOk && indexOk && tableName == "test_show" {
				indexNames = append(indexNames, indexName)
			}
		}
	}

	// Check if our expected indexes are in the result
	if len(indexNames) == 0 {
		t.Errorf("SHOW INDEXES did not return any indexes for test_show table")
	}

	// We're expecting at least our created index or a similar one
	// Note: The system might create indexes with different naming patterns
	t.Logf("Found indexes: %v", indexNames)

	// Check if any of the index names contains terms from our created index
	foundRelatedIndex := false
	for _, name := range indexNames {
		if stringContains(name, "name") || stringContains(name, "idx") ||
			stringContains(name, "test_show") {
			foundRelatedIndex = true
			break
		}
	}

	if !foundRelatedIndex {
		t.Errorf("SHOW INDEXES did not return any index related to our created index")
	}
	result.Close()
}

// Helper function to check if a string contains a substring
func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
