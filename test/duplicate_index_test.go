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
	"database/sql"
	"strings"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestDuplicateIndexCreation(t *testing.T) {
	// Create a new in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE duplicate_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("TestUniqueFirst", func(t *testing.T) {
		// First create a unique index
		_, err = db.Exec("CREATE UNIQUE INDEX idx_name_unique ON duplicate_test(name)")
		if err != nil {
			t.Fatalf("Failed to create unique index: %v", err)
		}

		// Now try to create a non-unique index on the same column
		_, err = db.Exec("CREATE INDEX idx_name ON duplicate_test(name)")
		if err == nil {
			t.Fatal("Expected error when creating duplicate index, but got none")
		}

		// Check if the error message contains the expected text
		expectedErrorText := "cannot create non-unique index; a unique index already exists for column"
		if !strings.Contains(err.Error(), expectedErrorText) {
			t.Errorf("Unexpected error message. Expected to contain '%s', got: %v",
				expectedErrorText, err)
		}

		// Verify we still have only one index
		rows, err := db.Query("SHOW INDEXES FROM duplicate_test")
		if err != nil {
			t.Fatalf("Failed to query indexes: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 index, found %d", count)
		}
	})

	t.Run("TestNonUniqueFirst", func(t *testing.T) {
		// Create a new table for this test
		_, err = db.Exec("CREATE TABLE duplicate_test2 (id INTEGER, name TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// First create a non-unique index
		_, err = db.Exec("CREATE INDEX idx_name2 ON duplicate_test2(name)")
		if err != nil {
			t.Fatalf("Failed to create non-unique index: %v", err)
		}

		// Now try to create a unique index on the same column
		_, err = db.Exec("CREATE UNIQUE INDEX idx_name2_unique ON duplicate_test2(name)")
		if err == nil {
			t.Fatal("Expected error when creating duplicate index, but got none")
		}

		// Check if the error message contains the expected text
		expectedErrorText := "cannot create unique index; a non-unique index already exists for column"
		if !strings.Contains(err.Error(), expectedErrorText) {
			t.Errorf("Unexpected error message. Expected to contain '%s', got: %v",
				expectedErrorText, err)
		}

		// Verify we still have only one index
		rows, err := db.Query("SHOW INDEXES FROM duplicate_test2")
		if err != nil {
			t.Fatalf("Failed to query indexes: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 index, found %d", count)
		}
	})

	// Test the IF NOT EXISTS clause works properly
	t.Run("TestIfNotExists", func(t *testing.T) {
		// Create a new table for this test
		_, err = db.Exec("CREATE TABLE duplicate_test3 (id INTEGER, name TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Create a unique index
		_, err = db.Exec("CREATE UNIQUE INDEX idx_name3 ON duplicate_test3(name)")
		if err != nil {
			t.Fatalf("Failed to create unique index: %v", err)
		}

		// Try to create a non-unique index with IF NOT EXISTS
		_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_name3_nonunique ON duplicate_test3(name)")
		if err != nil {
			t.Errorf("Expected IF NOT EXISTS to succeed, but got error: %v", err)
		}

		// Verify we still have only one index
		rows, err := db.Query("SHOW INDEXES FROM duplicate_test3")
		if err != nil {
			t.Fatalf("Failed to query indexes: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 index, found %d", count)
		}
	})
}
