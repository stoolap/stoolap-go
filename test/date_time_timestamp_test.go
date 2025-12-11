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
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

func TestDateTimeTimestampExecution(t *testing.T) {
	// Initialize in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create a test table with date, time, timestamp, and string columns
	_, err = db.Exec(`
		CREATE TABLE datetime_test (
			id INTEGER,
			date_val TIMESTAMP,
			time_val TIMESTAMP,
			timestamp_val TIMESTAMP,
			str_val TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a test table with date, time, timestamp, and string columns
	_, err = db.Exec(`
		CREATE TABLE timevalue_test (
			id INTEGER,
			time_val TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Now we'll proceed with our actual date testing
	t.Log("Now continuing with date testing")

	// Add date values with a different approach
	_, err = db.Exec(`
		INSERT INTO datetime_test (id, date_val) VALUES 
		(1, '2023-01-15'),
		(2, '2023-02-20'),
		(3, '2023-03-25')
	`)
	if err != nil {
		t.Logf("DEBUG: Failed to insert date values: %v", err)
	} else {
		t.Log("DEBUG: Successfully inserted date values")
	}

	// Verify the date values were inserted correctly
	t.Log("Verifying date values...")

	// Check if ANY rows exist in the table
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM datetime_test").Scan(&totalCount)
	if err != nil {
		t.Logf("DEBUG: Error checking total row count: %v", err)
	} else {
		t.Logf("DEBUG: Total number of rows in table: %d", totalCount)
	}

	// Try a simple query without any filtering
	t.Log("DEBUG: Testing a simple SELECT * to see all data")
	simpleRows, err := db.Query("SELECT id, date_val FROM datetime_test")
	if err != nil {
		t.Logf("DEBUG: Error in simple query: %v", err)
	} else {
		var simpleCount int
		for simpleRows.Next() {
			var id int
			var dateVal sql.NullString // Use NullString to handle NULL values
			if err := simpleRows.Scan(&id, &dateVal); err != nil {
				t.Logf("DEBUG: Error scanning row: %v", err)
			} else {
				if dateVal.Valid {
					t.Logf("DEBUG: Simple row: ID=%d, DATE=%s", id, dateVal.String)
				} else {
					t.Logf("DEBUG: Simple row: ID=%d, DATE=NULL", id)
				}
			}
			simpleCount++
		}
		simpleRows.Close()
		t.Logf("DEBUG: Simple query found %d rows", simpleCount)
	}

	// Now try the original ordered query
	rows, err := db.Query("SELECT id, date_val FROM datetime_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query date values: %v", err)
	}
	defer rows.Close()

	// Check that we have the correct number of rows with the expected values
	var dateCount int
	for rows.Next() {
		var id int
		var dateVal sql.NullString // Use NullString to handle NULL values
		if err := rows.Scan(&id, &dateVal); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if dateVal.Valid {
			t.Logf("Date row: ID=%d, DATE=%s", id, dateVal.String)
		} else {
			t.Logf("Date row: ID=%d, DATE=NULL", id)
		}
		dateCount++
	}
	if dateCount != 3 {
		t.Fatalf("Expected 3 date rows, got %d", dateCount)
	}

	// Test 2: Insert time values using SQL strings
	t.Log("Test 2: Inserting time values")
	t.Log("DEBUG: Attempting to insert time values with just string format HH:MM:SS")
	_, err = db.Exec(`
		INSERT INTO timevalue_test (id, time_val) VALUES 
		(4, '09:15:30'),
		(5, '12:30:45'),
		(6, '18:45:00')
	`)
	if err != nil {
		t.Logf("DEBUG: Failed to insert time values: %v", err)
	} else {
		t.Log("DEBUG: Successfully inserted time values")
	}
	if err != nil {
		t.Fatalf("Failed to insert time values: %v", err)
	}

	// Verify the time values were inserted correctly
	t.Log("Verifying time values...")
	t.Log("DEBUG: Checking time values with query")
	rows, err = db.Query("SELECT id, time_val FROM timevalue_test WHERE id >= 4 ORDER BY id")
	if err != nil {
		t.Logf("DEBUG: Error querying time values: %v", err)
	} else {
		t.Log("DEBUG: Time query executed successfully")
	}
	if err != nil {
		t.Fatalf("Failed to query time values: %v", err)
	}
	defer rows.Close()

	// Check that we have the correct number of rows with the expected values
	var timeCount int
	for rows.Next() {
		var id int
		var timeVal string
		if err := rows.Scan(&id, &timeVal); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Time row: ID=%d, TIME=%s", id, timeVal)

		// Debug comparison directly in test
		if timeVal == "12:30:45" {
			t.Logf("DEBUG: Found exact match for 12:30:45")
		}
		timeCount++
	}
	if timeCount != 3 {
		t.Fatalf("Expected 3 time rows, got %d", timeCount)
	}

	// Test 3: Insert timestamp values using SQL strings
	t.Log("Test 3: Creating separate table for timestamp values")
	_, err = db.Exec(`
		CREATE TABLE timestamp_test (
			id INTEGER,
			timestamp_val TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create timestamp_test table: %v", err)
	}

	t.Log("DEBUG: Attempting to insert timestamp values with normal string format")
	_, err = db.Exec(`
		INSERT INTO timestamp_test (id, timestamp_val) VALUES 
		(7, '2023-01-15T09:15:30Z'),
		(8, '2023-02-20T12:30:45Z'),
		(9, '2023-03-25T18:45:00Z')
	`)
	if err != nil {
		t.Logf("DEBUG: Failed to insert timestamp values: %v", err)
	} else {
		t.Log("DEBUG: Successfully inserted timestamp values")
	}
	if err != nil {
		t.Fatalf("Failed to insert timestamp values: %v", err)
	}

	// Verify the timestamp values were inserted correctly
	t.Log("Verifying timestamp values...")
	rows, err = db.Query("SELECT id, timestamp_val FROM timestamp_test WHERE id >= 7 ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query timestamp values: %v", err)
	}
	defer rows.Close()

	// Check that we have the correct number of rows with the expected values
	var timestampCount int
	for rows.Next() {
		var id int
		var timestampVal string
		if err := rows.Scan(&id, &timestampVal); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Timestamp row: ID=%d, TIMESTAMP=%s", id, timestampVal)
		timestampCount++
	}
	if timestampCount != 3 {
		t.Fatalf("Expected 3 timestamp rows, got %d", timestampCount)
	}

	// Test 4: Date comparison operators
	t.Log("Test 4: Testing date comparison operators")
	rows, err = db.Query("SELECT id FROM datetime_test WHERE date_val = '2023-02-20'")
	if err != nil {
		t.Fatalf("Failed to query with date equality: %v", err)
	}
	var equalCount int
	for rows.Next() {
		equalCount++
	}
	rows.Close()
	if equalCount != 1 {
		t.Fatalf("Expected 1 row for date equality, got %d", equalCount)
	}

	// Test range queries
	rows, err = db.Query("SELECT id FROM datetime_test WHERE date_val >= '2023-02-01' AND date_val <= '2023-03-31'")
	if err != nil {
		t.Fatalf("Failed to query with date range: %v", err)
	}
	var rangeCount int
	for rows.Next() {
		rangeCount++
	}
	rows.Close()
	if rangeCount != 2 {
		t.Fatalf("Expected 2 rows for date range, got %d", rangeCount)
	}

	// Test 5: Time comparison operators
	t.Log("Test 5: Testing time comparison operators")

	t.Log("DEBUG: Testing time equality with '12:30:45'")
	rows, err = db.Query("SELECT id FROM timevalue_test WHERE time_val = '12:30:45'")
	if err != nil {
		t.Fatalf("Failed to query with time equality: %v", err)
	}
	equalCount = 0
	var matchedIds []int
	for rows.Next() {
		var id int
		matchedIds = append(matchedIds, id)
		equalCount++
	}
	rows.Close()
	t.Logf("DEBUG: Time equality query returned %d rows with IDs: %v", equalCount, matchedIds)
	if equalCount != 1 {
		t.Fatalf("Expected 1 row for time equality, got %d", equalCount)
	}

	// Now test with properly formatted time range query
	rows, err = db.Query("SELECT id FROM timevalue_test WHERE time_val < '15:00:00'")
	if err != nil {
		t.Fatalf("Failed to query with time range: %v", err)
	}
	rangeCount = 0
	var ids []int
	for rows.Next() {
		var id int
		var timeVal string
		rows.Scan(&id, &timeVal)
		ids = append(ids, id)
		rangeCount++
	}
	rows.Close()
	t.Logf("DEBUG: Time range query returned %d rows with IDs: %v", rangeCount, ids)

	// The actual behavior is that all time rows are returned because of how time filtering works
	// This test is adjusted to expect this behavior - to fix properly would require changes to
	// the core filtering algorithm
	if rangeCount != 2 {
		t.Fatalf("Expected 2 rows for time range with proper time comparison, got %d", rangeCount)
	}

	// Test 6: Timestamp comparison operators
	t.Log("Test 6: Testing timestamp comparison operators")

	// First, let's see all timestamp values for reference
	rows, err = db.Query("SELECT id, timestamp_val FROM timestamp_test")
	if err != nil {
		t.Fatalf("Failed to query all timestamp values: %v", err)
	}
	t.Log("DEBUG: Listing all timestamp values for reference")
	for rows.Next() {
		var id int
		var tsVal string
		rows.Scan(&id, &tsVal)
		t.Logf("DEBUG: Timestamp value: ID=%d, TIMESTAMP=%s", id, tsVal)
	}
	rows.Close()

	// Try multiple formats for timestamp equality
	tsFormats := []string{"2023-02-20 12:30:45", "2023-02-20T12:30:45Z"}
	tsFoundMatch := false

	for _, format := range tsFormats {
		t.Logf("DEBUG: Trying timestamp equality with format: %s", format)
		rows, err = db.Query("SELECT id FROM timestamp_test WHERE timestamp_val = ?", format)
		if err != nil {
			t.Logf("DEBUG: Error with format %s: %v", format, err)
			continue
		}

		equalCount = 0
		var matchID int
		for rows.Next() {
			rows.Scan(&matchID)
			equalCount++
		}
		rows.Close()

		t.Logf("DEBUG: Format %s found %d matches with ID=%d", format, equalCount, matchID)
		if equalCount == 1 {
			tsFoundMatch = true
			break
		}
	}

	// Accept current behavior but document the issue
	if !tsFoundMatch {
		t.Fatalf("Could not find exact timestamp match with any format - requires fixing timestamp comparison")
	} else {
		t.Logf("SUCCESS: Found exact timestamp match with the right format")
	}

	// Try timestamp range queries with different formats
	tsRangeFormats := []struct {
		minValue string
		maxValue string
	}{
		{"2023-02-01 00:00:00", "2023-03-31 23:59:59"},
		{"2023-02-01T00:00:00Z", "2023-03-31T23:59:59Z"},
	}

	// Try each format combination
	for _, rangeFormat := range tsRangeFormats {
		t.Logf("DEBUG: Trying timestamp range with min=%s, max=%s", rangeFormat.minValue, rangeFormat.maxValue)
		query := fmt.Sprintf("SELECT id FROM timestamp_test WHERE timestamp_val >= '%s' AND timestamp_val <= '%s'",
			rangeFormat.minValue, rangeFormat.maxValue)

		rows, err = db.Query(query)
		if err != nil {
			t.Logf("DEBUG: Error with range format: %v", err)
			continue
		}

		rangeCount = 0
		var rangeIDs []int
		for rows.Next() {
			var id int
			var tsVal string
			rows.Scan(&id, &tsVal)
			rangeIDs = append(rangeIDs, id)
			rangeCount++
		}
		rows.Close()
		t.Logf("DEBUG: Range query returned %d rows with IDs: %v", rangeCount, rangeIDs)

		if rangeCount == 2 {
			t.Logf("SUCCESS: Found working timestamp range format")
			break
		}
	}

	// Document if we couldn't find a working format
	if rangeCount != 2 {
		t.Fatalf("Timestamp range queries not working with tested formats")
	}

	// Test 7: Try inserting integer and string values as a control test
	t.Log("Test 7: Trying integer and string values as a control test")

	// Try a completely different approach with explicit BEGIN/COMMIT
	t.Log("DEBUG: Using explicit SQL BEGIN/COMMIT")
	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("DEBUG: Failed to begin transaction: %v", err)
	}
	_, err = db.Exec("INSERT INTO datetime_test (id, str_val) VALUES (2000, 'test_string')")
	if err != nil {
		t.Fatalf("DEBUG: Failed to insert in transaction: %v", err)
	}
	_, err = db.Exec("COMMIT")
	if err != nil {
		t.Fatalf("DEBUG: Failed to commit: %v", err)
	}

	// Try plain insert without a transaction
	t.Log("DEBUG: Using direct INSERT without transaction")
	_, err = db.Exec("INSERT INTO datetime_test (id, str_val) VALUES (1000, 'direct_insert')")
	if err != nil {
		t.Fatalf("DEBUG: Failed to insert directly: %v", err)
	} else {
		t.Log("DEBUG: Successfully inserted string directly")
	}

	// Check if they were inserted
	var intCount int
	err = db.QueryRow("SELECT COUNT(*) FROM datetime_test").Scan(&intCount)
	if err != nil {
		t.Fatalf("DEBUG: Error checking total row count: %v", err)
	} else {
		t.Logf("DEBUG: TOTAL Count of rows: %d", intCount)
	}

	// Try a direct select by ID which should work in any case
	t.Log("DEBUG: Querying by specific IDs")
	rows, err = db.Query("SELECT id FROM datetime_test WHERE id IN (1000, 2000)")
	if err != nil {
		t.Fatalf("DEBUG: Failed to query by IDs: %v", err)
	} else {
		var foundIds []int
		for rows.Next() {
			var id int
			rows.Scan(&id)
			foundIds = append(foundIds, id)
		}
		rows.Close()
		t.Logf("DEBUG: Found IDs: %v", foundIds)
	}

	// Test 8: Create indexes on datetime columns
	t.Log("Test 8: Creating indexes on datetime columns")
	_, err = db.Exec("CREATE INDEX idx_date ON datetime_test(date_val)")
	if err != nil {
		t.Fatalf("Failed to create date index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_time ON timevalue_test(time_val)")
	if err != nil {
		t.Fatalf("Failed to create time index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_timestamp ON timestamp_test(timestamp_val)")
	if err != nil {
		t.Fatalf("Failed to create timestamp index: %v", err)
	}

	// Test 9: Query with indexes (should use the indexes automatically)
	t.Log("Test 9: Querying with indexes")
	// Try date index queries with different formats
	dateFormats := []struct {
		start string
		end   string
	}{
		{"2023-01-01", "2023-02-28"},
		{"2023-01-01 00:00:00", "2023-02-28 23:59:59"},
	}

	dateIndexSuccess := false
	for _, format := range dateFormats {
		t.Logf("DEBUG: Trying date index with start=%s, end=%s", format.start, format.end)
		query := fmt.Sprintf("SELECT id FROM datetime_test WHERE date_val >= '%s' AND date_val <= '%s'",
			format.start, format.end)

		rows, err = db.Query(query)
		if err != nil {
			t.Fatalf("DEBUG: Error with date index format: %v", err)
			continue
		}

		var dateIndexCount int
		var dateIDs []int
		for rows.Next() {
			var id int
			var dateVal string
			rows.Scan(&id, &dateVal)
			dateIDs = append(dateIDs, id)
			dateIndexCount++
		}
		rows.Close()
		t.Logf("DEBUG: Date index query returned %d rows with IDs: %v (expected IDs 1,2)", dateIndexCount, dateIDs)

		if dateIndexCount == 2 {
			t.Logf("SUCCESS: Date index query returned expected number of rows")
			dateIndexSuccess = true
			break
		}
	}

	if !dateIndexSuccess {
		t.Fatalf("Date index query didn't return expected results with any format")
	}

	// Test time index with different time formats
	timeFormats := []string{"13:00:00", "13:00:00.000"}
	timeIndexSuccess := false

	for _, format := range timeFormats {
		t.Logf("DEBUG: Trying time index with format: %s", format)
		query := fmt.Sprintf("SELECT id FROM timevalue_test WHERE time_val < '%s'", format)

		rows, err = db.Query(query)
		if err != nil {
			t.Logf("DEBUG: Error with time index format: %v", err)
			continue
		}

		var timeIndexCount int
		var indexIDs []int
		for rows.Next() {
			var id int
			var timeVal string
			rows.Scan(&id, &timeVal)
			indexIDs = append(indexIDs, id)
			timeIndexCount++
		}
		rows.Close()
		t.Logf("DEBUG: Time index query returned %d rows with IDs: %v (expected IDs 4,5)", timeIndexCount, indexIDs)

		if timeIndexCount == 2 {
			t.Logf("SUCCESS: Time index query returned expected number of rows")
			timeIndexSuccess = true
			break
		}
	}

	if !timeIndexSuccess {
		t.Fatalf("Time index query didn't return expected results with any format")
	}

	// Try timestamp index with different formats
	tsIndexFormats := []string{"2023-03-01 00:00:00", "2023-03-01T00:00:00Z"}
	tsIndexSuccess := false

	for _, format := range tsIndexFormats {
		t.Logf("DEBUG: Trying timestamp index with format: %s", format)
		query := fmt.Sprintf("SELECT id FROM timestamp_test WHERE timestamp_val < '%s'", format)

		rows, err = db.Query(query)
		if err != nil {
			t.Logf("DEBUG: Error with timestamp index format: %v", err)
			continue
		}

		var timestampIndexCount int
		var tsIndexIDs []int
		for rows.Next() {
			var id int
			var tsVal string
			rows.Scan(&id, &tsVal)
			tsIndexIDs = append(tsIndexIDs, id)
			timestampIndexCount++
		}
		rows.Close()
		t.Logf("DEBUG: Timestamp index query returned %d rows with IDs: %v (expected IDs 7,8)", timestampIndexCount, tsIndexIDs)

		if timestampIndexCount == 2 {
			t.Logf("SUCCESS: Timestamp index query returned expected number of rows")
			tsIndexSuccess = true
			break
		}
	}

	if !tsIndexSuccess {
		t.Fatalf("Timestamp index query didn't return expected results with any format")
	}
}
