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
	"fmt"
	"path/filepath"
	"testing"

	// Import stoolap driver
	"github.com/stoolap/stoolap/internal/common"
	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestIndexIfExistsOperations tests CREATE/DROP INDEX IF EXISTS operations
// and verifies that indexes are properly preserved across database reopens
func TestIndexIfExistsOperations(t *testing.T) {
	// Create a temporary database path
	tempDir := common.TempDir(t)

	dbPath := filepath.Join(tempDir, "test.db")
	connString := fmt.Sprintf("file://%s", dbPath)

	// First database connection
	db, err := sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a test table
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price FLOAT,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`INSERT INTO products (id, name, price, category) VALUES 
		(1, 'Laptop', 999.99, 'Electronics'),
		(2, 'Smartphone', 599.99, 'Electronics'),
		(3, 'Headphones', 149.99, 'Accessories'),
		(4, 'Backpack', 79.99, 'Accessories'),
		(5, 'Monitor', 349.99, 'Electronics')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test 1: Create a new index with IF NOT EXISTS (should succeed)
	fmt.Println("TEST 1: Creating index with IF NOT EXISTS (should succeed)")
	_, err = db.Exec(`CREATE COLUMNAR INDEX IF NOT EXISTS ON products(category)`)
	if err != nil {
		t.Fatalf("Failed to create index with IF NOT EXISTS: %v", err)
	}

	// Verify index exists
	rows, err := db.Query(`SHOW INDEXES FROM products`)
	if err != nil {
		t.Fatalf("Failed to show indexes: %v", err)
	}

	var indexFound bool
	var table, indexName, column, indexType string
	var isUnique bool

	for rows.Next() {
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		if indexName == "columnar_products_category" {
			t.Logf("Found index: table=%s, name=%s, column=%s, type=%s, unique=%v",
				table, indexName, column, indexType, isUnique)
			indexFound = true
		}
	}
	rows.Close()

	if !indexFound {
		t.Fatalf("Index not found after creation")
	}

	// Test 2: Create the same index again with IF NOT EXISTS (should not error)
	fmt.Println("TEST 2: Creating same index again with IF NOT EXISTS (should not error)")
	_, err = db.Exec(`CREATE COLUMNAR INDEX IF NOT EXISTS ON products(category)`)
	if err != nil {
		t.Fatalf("Creating existing index with IF NOT EXISTS should not error: %v", err)
	}

	// Test 3: Create a unique index with IF NOT EXISTS
	fmt.Println("TEST 3: Creating unique index with IF NOT EXISTS")
	_, err = db.Exec(`CREATE UNIQUE COLUMNAR INDEX IF NOT EXISTS ON products(name)`)
	if err != nil {
		t.Fatalf("Failed to create unique index with IF NOT EXISTS: %v", err)
	}

	// Verify both indexes exist
	rows, err = db.Query(`SHOW INDEXES FROM products`)
	if err != nil {
		t.Fatalf("Failed to show indexes: %v", err)
	}

	var foundIndexes = make(map[string]bool)
	var uniqueFlag = make(map[string]bool)

	for rows.Next() {
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Found index: table=%s, name=%s, column=%s, type=%s, unique=%v",
			table, indexName, column, indexType, isUnique)
		foundIndexes[indexName] = true
		uniqueFlag[indexName] = isUnique
	}
	rows.Close()

	if !foundIndexes["columnar_products_category"] {
		t.Fatalf("Index columnar_products_category not found")
	}
	if !foundIndexes["unique_columnar_products_name"] {
		t.Fatalf("Index unique_columnar_products_name not found")
	}
	if !uniqueFlag["unique_columnar_products_name"] {
		t.Fatalf("unique_columnar_products_name should be a unique index")
	}
	if uniqueFlag["columnar_products_category"] {
		t.Fatalf("columnar_products_category should not be a unique index")
	}

	// Close the database before reopening
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	t.Log("--- Closing and reopening the database ---")
	db, err = sql.Open("stoolap", connString)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Test 4: Verify indexes are preserved after reopen
	fmt.Println("TEST 4: Verifying indexes after database reopen")
	rows, err = db.Query(`SHOW INDEXES FROM products`)
	if err != nil {
		t.Fatalf("Failed to show indexes after reopen: %v", err)
	}

	foundIndexes = make(map[string]bool)
	uniqueFlag = make(map[string]bool)

	for rows.Next() {
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Found index after reopen: table=%s, name=%s, column=%s, type=%s, unique=%v",
			table, indexName, column, indexType, isUnique)
		foundIndexes[indexName] = true
		uniqueFlag[indexName] = isUnique
	}
	rows.Close()

	if !foundIndexes["columnar_products_category"] {
		t.Fatalf("Index columnar_products_category not found")
	}
	if !foundIndexes["unique_columnar_products_name"] {
		t.Fatalf("Index unique_columnar_products_name not found")
	}
	if !uniqueFlag["unique_columnar_products_name"] {
		t.Fatalf("unique_columnar_products_name should be a unique index")
	}
	if uniqueFlag["columnar_products_category"] {
		t.Fatalf("columnar_products_category should not be a unique index")
	}

	// Test 5: Test DROP INDEX IF EXISTS on an existing index
	fmt.Println("TEST 5: Testing DROP INDEX IF EXISTS on existing index")
	_, err = db.Exec(`DROP COLUMNAR INDEX IF EXISTS ON products(category)`)
	if err != nil {
		t.Fatalf("Failed to drop existing index with IF EXISTS: %v", err)
	}

	// Test 6: Test DROP INDEX IF EXISTS on a non-existing index
	fmt.Println("TEST 6: Testing DROP INDEX IF EXISTS on non-existing index")
	_, err = db.Exec(`DROP COLUMNAR INDEX IF EXISTS ON products(nonexist)`)
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS on non-existing index should not error: %v", err)
	}

	// Verify only one index remains
	rows, err = db.Query(`SHOW INDEXES FROM products`)
	if err != nil {
		t.Fatalf("Failed to show indexes after drops: %v", err)
	}

	foundIndexes = make(map[string]bool)

	for rows.Next() {
		err = rows.Scan(&table, &indexName, &column, &indexType, &isUnique)
		if err != nil {
			t.Fatalf("Failed to scan index row: %v", err)
		}
		t.Logf("Remaining index: table=%s, name=%s, column=%s, type=%s, unique=%v",
			table, indexName, column, indexType, isUnique)
		foundIndexes[indexName] = true
	}
	rows.Close()

	if foundIndexes["columnar_products_category"] {
		t.Fatalf("Index columnar_products_category should have been dropped")
	}
	if !foundIndexes["unique_columnar_products_name"] {
		t.Fatalf("Index unique_columnar_products_name should still exist")
	}

	// Close database
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database at end: %v", err)
	}
}
