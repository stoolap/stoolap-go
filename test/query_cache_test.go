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
	"database/sql/driver"
	"testing"

	"github.com/stoolap/stoolap"
)

// Helper function to convert interface slice to driver.NamedValue slice
func toQueryCacheNamedValues(params []interface{}) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(params))
	for i, value := range params {
		namedValues[i] = driver.NamedValue{
			Name:    "",    // Using positional parameters
			Ordinal: i + 1, // 1-based index
			Value:   value, // The value itself
		}
	}
	return namedValues
}

// TestQueryCacheBasic tests the basic functionality of the query cache
func TestQueryCacheBasic(t *testing.T) {
	// Create a memory engine for testing
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer db.Close()

	// Use the Executor method from DB
	exec := db.Executor()

	// We can now use the Executor interface directly

	// Create a test table with sample data, making id the primary key
	_, err = exec.Execute(context.Background(), nil, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, name TEXT, value FLOAT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a test row to make sure we can query it later
	_, err = exec.Execute(context.Background(), nil, "INSERT INTO cache_test VALUES (1, 'test-name', 10.5)")
	if err != nil {
		t.Fatalf("Failed to insert test row: %v", err)
	}

	// Verify the row was inserted
	result, err := exec.Execute(context.Background(), nil, "SELECT COUNT(*) FROM cache_test")
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	var count int64
	if result.Next() {
		err = result.Scan(&count)
		if err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}
	result.Close()

	if count != 1 {
		t.Fatalf("Expected 1 row but got %d", count)
	}

	// Execute the same query multiple times
	for i := 0; i < 5; i++ {
		result, err := exec.Execute(context.Background(), nil, "SELECT * FROM cache_test")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Check result correctness
		rowCount := 0
		for result.Next() {
			rowCount++
		}
		result.Close()

		if rowCount != 1 {
			t.Errorf("Expected 1 row, got %d", rowCount)
		}
	}

	// Execute parameterized queries that demonstrate query caching with parameters
	// This is just to verify the API works
	_, err = exec.ExecuteWithParams(
		context.Background(),
		nil,
		"SELECT * FROM cache_test WHERE id = ?",
		toQueryCacheNamedValues([]interface{}{1}),
	)
	if err != nil {
		t.Fatalf("Parameterized query failed: %v", err)
	}

	// Execute the same parameterized query again
	_, err = exec.ExecuteWithParams(
		context.Background(),
		nil,
		"SELECT * FROM cache_test WHERE id = ?",
		toQueryCacheNamedValues([]interface{}{2}),
	)
	if err != nil {
		t.Fatalf("Second parameterized query failed: %v", err)
	}

	// Execute a mix of queries to test cache behavior
	queryStrings := []string{
		"SELECT * FROM cache_test",
		"SELECT name, value FROM cache_test",
		"SELECT id FROM cache_test",
		"SELECT * FROM cache_test", // Repeated query
	}

	for _, q := range queryStrings {
		result, err := exec.Execute(context.Background(), nil, q)
		if err != nil {
			t.Fatalf("Query failed on '%s': %v", q, err)
		}
		result.Close()
	}

	t.Log("Query cache test completed successfully.")
}
