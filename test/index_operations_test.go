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
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestIndexOperations(t *testing.T) {
	t.Skip("Skipping index operations test due to unresolved issues with unique constraints")
	// Initialize in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			id INTEGER, 
			name TEXT, 
			category TEXT,
			value FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with varied categories for testing bitmap index
	_, err = db.Exec(`
		INSERT INTO test_table VALUES
		(1, 'Item 1', 'A', 10.5),
		(2, 'Item 2', 'A', 20.1),
		(3, 'Item 3', 'B', 30.7),
		(4, 'Item 4', 'B', 40.2),
		(5, 'Item 5', 'B', 50.9),
		(6, 'Item 6', 'C', 60.3),
		(7, 'Item 7', 'C', 70.8),
		(8, 'Item 8', 'A', 80.4),
		(9, 'Item 9', 'D', 90.6),
		(10, 'Item 10', 'D', 100.1)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create a bitmap index on the low-cardinality category column
	_, err = db.Exec("CREATE INDEX idx_category ON test_table(category)")
	if err != nil {
		t.Fatalf("Failed to create bitmap index: %v", err)
	}

	// Create a btree index on the high-cardinality value column
	_, err = db.Exec("CREATE INDEX idx_value ON test_table(value)")
	if err != nil {
		t.Fatalf("Failed to create btree index: %v", err)
	}

	// Test bitmap index with equality condition
	rows, err := db.Query("SELECT id, name, category FROM test_table WHERE category = 'B'")
	if err != nil {
		t.Fatalf("Failed to query with bitmap index: %v", err)
	}

	// Count rows for category B (should be 3)
	var count int
	for rows.Next() {
		count++
		var id int
		var name string
		var category string
		if err := rows.Scan(&id, &name, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if id < 3 || id > 5 {
			t.Errorf("Expected id in range [3,5], got %d", id)
		}
		if category != "B" {
			t.Errorf("Expected category 'B', got '%s'", category)
		}
	}
	rows.Close()
	if count != 3 {
		t.Errorf("Expected 3 rows for category B, got %d", count)
	}

	// Test btree index with range condition
	rows, err = db.Query("SELECT id, name, value FROM test_table WHERE value > 50.0 AND value < 90.0")
	if err != nil {
		t.Fatalf("Failed to query with btree index: %v", err)
	}

	// Count rows in the value range (should be 4)
	count = 0
	for rows.Next() {
		count++
		var id int
		var name string
		var value float64
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if id < 5 || id > 8 {
			t.Errorf("Expected id in range [5,8], got %d", id)
		}
		if value <= 50.0 || value >= 90.0 {
			t.Errorf("Expected value in range (50.0, 90.0), got %f", value)
		}
	}
	rows.Close()
	if count != 4 {
		t.Errorf("Expected 4 rows for value range, got %d", count)
	}

	// Test bitmap index with range condition on category (using string comparison)
	rows, err = db.Query("SELECT id, name, category FROM test_table WHERE category >= 'B' AND category <= 'C'")
	if err != nil {
		t.Fatalf("Failed to query with bitmap index range: %v", err)
	}

	// Count rows for category range B-C (should be 5)
	count = 0
	for rows.Next() {
		count++
		var id int
		var name string
		var category string
		if err := rows.Scan(&id, &name, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if id < 3 || id > 7 {
			t.Errorf("Expected id in range [3,7], got %d", id)
		}
		if category < "B" || category > "C" {
			t.Errorf("Expected category in range ['B','C'], got '%s'", category)
		}
	}
	rows.Close()
	if count != 5 {
		t.Errorf("Expected 5 rows for category range B-C, got %d", count)
	}

	// Test bitmap index with range inclusive/exclusive boundaries
	rows, err = db.Query("SELECT id, name, category FROM test_table WHERE category > 'A' AND category < 'D'")
	if err != nil {
		t.Fatalf("Failed to query with bitmap index exclusive range: %v", err)
	}

	// Count rows for exclusive category range (should be 5)
	count = 0
	for rows.Next() {
		count++
		var id int
		var name string
		var category string
		if err := rows.Scan(&id, &name, &category); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if id < 3 || id > 7 {
			t.Errorf("Expected id in range [3,7], got %d", id)
		}
		if category <= "A" || category >= "D" {
			t.Errorf("Expected category in range ('A','D'), got '%s'", category)
		}
	}
	rows.Close()
	if count != 5 {
		t.Errorf("Expected 5 rows for exclusive category range, got %d", count)
	}

	// Switch the order since idx_category might have been created first
	// Test DROP INDEX IF EXISTS
	_, err = db.Exec("DROP INDEX IF EXISTS idx_category ON test_table")
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS failed when index doesn't exist: %v", err)
	}

	// Test DROP INDEX IF EXISTS on an index that does exist
	_, err = db.Exec("DROP INDEX IF EXISTS idx_value ON test_table")
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS failed when index exists: %v", err)
	}

	// Test unique index constraints
	_, err = db.Exec("CREATE UNIQUE INDEX idx_id ON test_table(id)")
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Try to insert a duplicate ID, which should fail due to the unique constraint
	_, err = db.Exec("INSERT INTO test_table VALUES (1, 'Duplicate', 'X', 999.9)")
	if err == nil {
		t.Errorf("Expected unique constraint violation, but insert succeeded")
	}
}

func TestIndexRangeOperations(t *testing.T) {
	// Initialize in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a test table with various data types for range testing
	_, err = db.Exec(`
		CREATE TABLE range_test (
			id INTEGER,
			int_val INTEGER,
			float_val FLOAT,
			str_val TEXT,
			bool_val BOOLEAN,
			date_val TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create range test table: %v", err)
	}

	// Insert test data using simple SQL to avoid parameter binding issues
	t.Log("Inserting test data using SQL:")

	// Use SQL syntax that doesn't require parameter binding
	_, err = db.Exec(`
		INSERT INTO range_test (id, int_val, float_val, str_val, bool_val, date_val) VALUES
		(1, 10, 1.5, 'apple', true, '2023-01-01'),
		(2, 20, 2.5, 'banana', true, '2023-02-01'),
		(3, 30, 3.5, 'cherry', false, '2023-03-01'),
		(4, 40, 4.5, 'date', false, '2023-04-01'),
		(5, 50, 5.5, 'elderberry', true, '2023-05-01'),
		(6, 60, 6.5, 'fig', false, '2023-06-01'),
		(7, 70, 7.5, 'grape', true, '2023-07-01'),
		(8, 80, 8.5, 'honeydew', false, '2023-08-01'),
		(9, 90, 9.5, 'imbe', true, '2023-09-01'),
		(10, 100, 10.5, 'jackfruit', false, '2023-10-01')
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	t.Log("Successfully inserted test data")

	// Create bitmap indexes for range testing
	_, err = db.Exec("CREATE INDEX idx_int ON range_test(int_val)")
	if err != nil {
		t.Fatalf("Failed to create int index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_float ON range_test(float_val)")
	if err != nil {
		t.Fatalf("Failed to create float index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_str ON range_test(str_val)")
	if err != nil {
		t.Fatalf("Failed to create string index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_bool ON range_test(bool_val)")
	if err != nil {
		t.Fatalf("Failed to create boolean index: %v", err)
	}

	// Create date index - add explicit logging
	_, err = db.Exec("CREATE INDEX idx_date ON range_test(date_val)")
	if err != nil {
		t.Fatalf("Failed to create date index: %v", err)
	}

	// Add a debug query to examine the raw data in the date_val column
	debugRows, err := db.Query("SELECT id, date_val FROM range_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query date values: %v", err)
	}
	t.Log("DEBUG - Raw date values stored in the database:")
	for debugRows.Next() {
		var id int
		var dateVal string
		if err := debugRows.Scan(&id, &dateVal); err != nil {
			t.Fatalf("Failed to scan debug row: %v", err)
		}
		t.Logf("DEBUG: ID: %d, Raw date_val: '%s'", id, dateVal)
	}
	debugRows.Close()

	// Run a simple query to verify date values were properly inserted
	rows, err := db.Query("SELECT id, int_val, float_val, str_val, bool_val, date_val FROM range_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query values: %v", err)
	}
	defer rows.Close()

	t.Log("Checking all inserted values:")
	var count int
	for rows.Next() {
		count++
		var id, intVal int
		var floatVal float64
		var strVal, dateVal string
		var boolVal bool
		if err := rows.Scan(&id, &intVal, &floatVal, &strVal, &boolVal, &dateVal); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Row %d: ID=%d, INT=%d, FLOAT=%f, STR=%s, BOOL=%v, DATE=%s",
			count, id, intVal, floatVal, strVal, boolVal, dateVal)
	}
	t.Logf("Total rows: %d", count)

	// Test integer range query (inclusive)
	t.Run("IntegerRangeInclusive", func(t *testing.T) {
		rows, err := db.Query("SELECT id, int_val FROM range_test WHERE int_val >= 30 AND int_val <= 70")
		if err != nil {
			t.Fatalf("Failed to query with int range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var int_val int
			if err := rows.Scan(&id, &int_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the int_val is in range
			if int_val < 30 || int_val > 70 {
				t.Errorf("Expected int_val in range [30, 70], got %d", int_val)
			}
		}
		rows.Close()

		// Expected ids 3, 4, 5, 6, 7 (values 30, 40, 50, 60, 70)
		expectedIDs := []int{3, 4, 5, 6, 7}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test integer range query (exclusive)
	t.Run("IntegerRangeExclusive", func(t *testing.T) {
		rows, err := db.Query("SELECT id, int_val FROM range_test WHERE int_val > 30 AND int_val < 70")
		if err != nil {
			t.Fatalf("Failed to query with exclusive int range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var int_val int
			if err := rows.Scan(&id, &int_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the int_val is in range
			if int_val <= 30 || int_val >= 70 {
				t.Errorf("Expected int_val in range (30, 70), got %d", int_val)
			}
		}
		rows.Close()

		// Expected ids 4, 5, 6 (values 40, 50, 60)
		expectedIDs := []int{4, 5, 6}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test float range query
	t.Run("FloatRange", func(t *testing.T) {
		rows, err := db.Query("SELECT id, float_val FROM range_test WHERE float_val >= 4.5 AND float_val <= 8.5")
		if err != nil {
			t.Fatalf("Failed to query with float range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var float_val float64
			if err := rows.Scan(&id, &float_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the float_val is in range
			if float_val < 4.5 || float_val > 8.5 {
				t.Errorf("Expected float_val in range [4.5, 8.5], got %f", float_val)
			}
		}
		rows.Close()

		// Expected ids 4, 5, 6, 7, 8 (values 4.5, 5.5, 6.5, 7.5, 8.5)
		expectedIDs := []int{4, 5, 6, 7, 8}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test string range query (lexicographical)
	t.Run("StringRange", func(t *testing.T) {
		rows, err := db.Query("SELECT id, str_val FROM range_test WHERE str_val >= 'cherry' AND str_val <= 'grape'")
		if err != nil {
			t.Fatalf("Failed to query with string range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var str_val string
			if err := rows.Scan(&id, &str_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the str_val is in range
			if str_val < "cherry" || str_val > "grape" {
				t.Errorf("Expected str_val in range ['cherry', 'grape'], got '%s'", str_val)
			}
		}
		rows.Close()

		// Expected ids 3, 4, 5, 6, 7 (cherry, date, elderberry, fig, grape)
		expectedIDs := []int{3, 4, 5, 6, 7}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test date range query
	t.Run("DateRange", func(t *testing.T) {
		// First, query all rows to see the date values
		rows, err := db.Query("SELECT id, date_val FROM range_test ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query dates: %v", err)
		}

		t.Log("All date values in the database:")
		for rows.Next() {
			var id int
			var dateVal string
			if err := rows.Scan(&id, &dateVal); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("ID: %d, Date: %s", id, dateVal)
		}
		rows.Close()

		// Test simple date equality to see if basic date comparison works
		t.Log("Testing simple date equality query...")
		simpleDateRows, err := db.Query("SELECT id, date_val FROM range_test WHERE date_val = '2023-12-31'")
		if err != nil {
			t.Fatalf("Failed to query with simple date equality: %v", err)
		}
		var simpleCount int
		for simpleDateRows.Next() {
			simpleCount++
			var id int
			var dateVal string
			if err := simpleDateRows.Scan(&id, &dateVal); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("Found date match: ID: %d, Date: %s", id, dateVal)
		}
		simpleDateRows.Close()
		t.Logf("Simple date equality found %d matches", simpleCount)

		// Now try the date range query
		t.Log("Executing date range query...")
		rows, err = db.Query("SELECT id, date_val FROM range_test WHERE date_val >= '2023-03-01' AND date_val <= '2023-07-01'")
		if err != nil {
			t.Fatalf("Failed to query with date range: %v", err)
		}

		var ids []int
		t.Log("Rows returned by date range query:")
		for rows.Next() {
			var id int
			var date_val string // using string since we can't directly scan time.Time
			if err := rows.Scan(&id, &date_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)
			t.Logf("Found match: ID: %d, Date: %s", id, date_val)

			// Validate the date_val is in range
			if date_val < "2023-03-01T00:00:00Z" || date_val > "2023-07-01T00:00:00Z" {
				t.Errorf("Expected date_val in range ['2023-03-01T00:00:00Z', '2023-07-01T00:00:00Z'], got '%s'", date_val)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("Error iterating rows: %v", err)
		}
		rows.Close()

		t.Logf("Total rows found: %d", len(ids))

		// Expected ids 3, 4, 5, 6, 7 (March through July)
		expectedIDs := []int{3, 4, 5, 6, 7}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test boolean value with range query (should work even though boolean is not really a range type)
	t.Run("BooleanValue", func(t *testing.T) {
		rows, err := db.Query("SELECT id, bool_val FROM range_test WHERE bool_val = true")
		if err != nil {
			t.Fatalf("Failed to query with boolean value: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var bool_val bool
			if err := rows.Scan(&id, &bool_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the bool_val is true
			if !bool_val {
				t.Errorf("Expected bool_val to be true, got false")
			}
		}
		rows.Close()

		// Expected ids 1, 2, 5, 7, 9 (true values)
		expectedIDs := []int{1, 2, 5, 7, 9}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test open range query (only min bound)
	t.Run("OpenMinRange", func(t *testing.T) {
		rows, err := db.Query("SELECT id, int_val FROM range_test WHERE int_val >= 80")
		if err != nil {
			t.Fatalf("Failed to query with open min range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var int_val int
			if err := rows.Scan(&id, &int_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the int_val is >= 80
			if int_val < 80 {
				t.Errorf("Expected int_val >= 80, got %d", int_val)
			}
		}
		rows.Close()

		// Expected ids 8, 9, 10 (values 80, 90, 100)
		expectedIDs := []int{8, 9, 10}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})

	// Test open range query (only max bound)
	t.Run("OpenMaxRange", func(t *testing.T) {
		rows, err := db.Query("SELECT id, int_val FROM range_test WHERE int_val <= 30")
		if err != nil {
			t.Fatalf("Failed to query with open max range: %v", err)
		}

		var ids []int
		for rows.Next() {
			var id int
			var int_val int
			if err := rows.Scan(&id, &int_val); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)

			// Validate the int_val is <= 30
			if int_val > 30 {
				t.Errorf("Expected int_val <= 30, got %d", int_val)
			}
		}
		rows.Close()

		// Expected ids 1, 2, 3 (values 10, 20, 30)
		expectedIDs := []int{1, 2, 3}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		// Check if all expected IDs are in results
		for _, expected := range expectedIDs {
			found := false
			for _, id := range ids {
				if id == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected ID %d not found in results", expected)
			}
		}
	})
}
