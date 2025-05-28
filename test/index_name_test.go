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
	"os"
	"path/filepath"
	"testing"

	"github.com/stoolap/stoolap"
	_ "github.com/stoolap/stoolap/pkg/driver" // Register stoolap driver
)

// TestIndexNamePersistence tests that index names are properly preserved
// when using CREATE INDEX IF NOT EXISTS across database reopens
func TestIndexNamePersistence(t *testing.T) {
	// Create a temporary directory for our database
	tempDir, err := os.MkdirTemp("", "stoolap-index-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	// Use file:// protocol to ensure persistence
	connString := "file://" + dbPath

	// Create and set up the database
	db, err := stoolap.Open(connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a context
	ctx := context.Background()

	// Create a test table
	_, err = db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create an index with a special name using IF NOT EXISTS
	_, err = db.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Close the database to simulate restart
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	db, err = stoolap.Open(connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Try to create the same index again with IF NOT EXISTS - this should not error
	_, err = db.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)")
	if err != nil {
		t.Fatalf("Failed to create index after reopen: %v", err)
	}

	// Close the first DB connection before opening a new one for verification
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Instead of SHOW INDEXES, let's try a more direct approach
	// by querying the db engine to see exactly what's happening
	sqlDB, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open SQL connection: %v", err)
	}
	defer sqlDB.Close()

	// Try a raw SQL query to directly examine the index
	rows, err := sqlDB.Query("CREATE INDEX idx_test2 ON users (name)")
	if err != nil {
		t.Logf("Trying to create a new index: %v", err)
	} else {
		rows.Close()
	}

	// Using a direct SQL query to check all available tables
	rows, err = sqlDB.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("Failed to show tables: %v", err)
	}
	t.Log("--- Available Tables ---")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("Failed to scan table name: %v", err)
		}
		t.Logf("Table: %s", tableName)
	}
	rows.Close()

	// Let's try the SHOW INDEXES command and display everything we get
	rows, err = sqlDB.Query("SHOW INDEXES FROM users")
	if err != nil {
		t.Fatalf("Failed to show indexes: %v", err)
	}

	// Get column names from the query
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("SHOW INDEXES columns: %v", cols)

	t.Log("--- All Indexes Found ---")
	rowCount := 0
	for rows.Next() {
		rowCount++
		// Use slice of interfaces to scan any data
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Print all column values
		rowMap := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			rowMap[colName] = *val
			t.Logf("Column %s data type: %T", colName, *val)
		}

		t.Logf("Row data: %v", rowMap)

		// Check if we found our index
		if indexName, ok := rowMap["Index Name"]; ok {
			if name, ok := indexName.(string); ok && name == "idx_users_email" {
				t.Logf("Found our index: %s", name)
				return // Test passes if we found our index
			}
		}
	}
	rows.Close()

	if rowCount == 0 {
		t.Logf("SHOW INDEXES returned zero rows")
	}

	t.Fatalf("Could not find index with name 'idx_users_email'")
}

// This part of the test won't be reached if the first part passes
// We'll have a separate test for the drop/recreate functionality
