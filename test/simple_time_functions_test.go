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

func TestSimpleV2TimeFunctions(t *testing.T) {
	// Connect to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with unique name
	_, err = db.Exec(`
		CREATE TABLE time_functions_test (
			id INTEGER,
			employee_name TEXT,
			department TEXT,
			event_timestamp TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO time_functions_test (id, employee_name, department, event_timestamp) VALUES 
		(1, 'Alice', 'Engineering', '2021-03-15T09:15:30'),
		(2, 'Bob', 'Engineering', '2021-03-15T10:25:45'),
		(3, 'Charlie', 'Marketing', '2021-03-15T11:35:15'),
		(4, 'Diana', 'Marketing', '2021-05-20T14:45:10'),
		(5, 'Eve', 'Finance', '2022-01-10T08:55:20')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test DATE_TRUNC function with different units
	t.Run("Test DATE_TRUNC function", func(t *testing.T) {
		// Test with year unit
		rows, err := db.Query(`
			SELECT DATE_TRUNC('year', event_timestamp) AS year_trunc
			FROM time_functions_test
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to execute DATE_TRUNC year query: %v", err)
		}
		defer rows.Close()

		// Collect the results
		var results []string

		for rows.Next() {
			var truncated string
			if err := rows.Scan(&truncated); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			results = append(results, truncated)
		}

		// Should have 5 rows
		if len(results) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(results))
		} else {
			// Check the first result (2021-03-15 should truncate to 2021-01-01)
			if !strings.Contains(results[0], "2021-01-01") {
				t.Errorf("Expected first date to be truncated to 2021-01-01, got %s", results[0])
			}
		}
	})

	// Test TIME_TRUNC function with duration formats
	t.Run("Test TIME_TRUNC function", func(t *testing.T) {
		// Test with 1-hour duration
		rows, err := db.Query(`
			SELECT TIME_TRUNC('1h', event_timestamp) AS hour_trunc
			FROM time_functions_test
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to execute TIME_TRUNC 1h query: %v", err)
		}
		defer rows.Close()

		// Collect the results
		var results []string

		for rows.Next() {
			var truncated string
			if err := rows.Scan(&truncated); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			results = append(results, truncated)
		}

		// Should have 5 rows
		if len(results) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(results))
		} else {
			// Check the first result (9:15:30 should truncate to 9:00:00)
			if !strings.Contains(results[0], "09:00:00") {
				t.Errorf("Expected first time to be truncated to 09:00:00, got %s", results[0])
			}
		}

		// Test with 15-minute duration
		minuteRows, err := db.Query(`
			SELECT TIME_TRUNC('15m', event_timestamp) AS minute_trunc
			FROM time_functions_test
			ORDER BY id
		`)
		if err != nil {
			t.Fatalf("Failed to execute TIME_TRUNC 15m query: %v", err)
		}
		defer minuteRows.Close()

		// Collect the results
		var minuteResults []string

		for minuteRows.Next() {
			var truncated string
			if err := minuteRows.Scan(&truncated); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			minuteResults = append(minuteResults, truncated)
		}

		// Check the first result (9:15:30 should truncate to 9:15:00)
		if len(minuteResults) > 0 && !strings.Contains(minuteResults[0], "09:15:00") {
			t.Errorf("Expected first time to be truncated to 09:15:00, got %s", minuteResults[0])
		}
	})

	// Create sales table for GROUP BY tests with unique name
	_, err = db.Exec(`
		CREATE TABLE time_function_sales (
			id INTEGER,
			product_id INTEGER,
			amount FLOAT,
			transaction_time TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create sales table: %v", err)
	}

	// Insert test data with different times
	_, err = db.Exec(`
		INSERT INTO time_function_sales (id, product_id, amount, transaction_time) VALUES 
		(1, 101, 50.0, '2023-01-01 09:15:00'),
		(2, 102, 25.0, '2023-01-01 09:30:00'),
		(3, 101, 75.0, '2023-01-01 10:15:00'),
		(4, 103, 100.0, '2023-01-01 10:45:00'),
		(5, 102, 35.0, '2023-01-01 11:15:00'),
		(6, 101, 60.0, '2023-01-01 11:45:00')
	`)
	if err != nil {
		t.Fatalf("Failed to insert sales data: %v", err)
	}

	// Test GROUP BY with TIME_TRUNC
	t.Run("Test TIME_TRUNC with GROUP BY", func(t *testing.T) {
		// Query using TIME_TRUNC with 1-hour intervals and GROUP BY
		rows, err := db.Query(`
			SELECT 
				TIME_TRUNC('1h', transaction_time) AS hour_bucket,
				SUM(amount) AS total_sales
			FROM time_function_sales
			GROUP BY TIME_TRUNC('1h', transaction_time)
			ORDER BY hour_bucket
		`)
		if err != nil {
			t.Fatalf("Failed to execute TIME_TRUNC with GROUP BY: %v", err)
		}
		defer rows.Close()

		// Collect the results
		var results []struct {
			HourBucket string
			TotalSales float64
		}

		for rows.Next() {
			var bucket string
			var sales float64
			if err := rows.Scan(&bucket, &sales); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			results = append(results, struct {
				HourBucket string
				TotalSales float64
			}{bucket, sales})
		}

		// Should have 3 different hour buckets
		if len(results) != 3 {
			t.Errorf("Expected 3 hour buckets, got %d", len(results))
		} else {
			// Log results for debugging
			for i, r := range results {
				t.Logf("Bucket %d: Time=%s, Sum=%v", i+1, r.HourBucket, r.TotalSales)
			}

			// Create a map to store the result by time bucket
			bucketSums := make(map[string]float64)
			for _, r := range results {
				// Extract the hour from the bucket time string
				hourPart := ""
				if strings.Contains(r.HourBucket, "09:") {
					hourPart = "09:00"
				} else if strings.Contains(r.HourBucket, "10:") {
					hourPart = "10:00"
				} else if strings.Contains(r.HourBucket, "11:") {
					hourPart = "11:00"
				}

				if hourPart != "" {
					bucketSums[hourPart] = r.TotalSales
				}
			}

			// Verify the totals per hour regardless of the order returned
			// 9:00-10:00 bucket should have 75.0 (50.0 + 25.0)
			if sum, ok := bucketSums["09:00"]; !ok || sum != 75.0 {
				t.Errorf("Expected 75.0 for 9:00 hour bucket, got %v", sum)
			}

			// 10:00-11:00 bucket should have 175.0 (75.0 + 100.0)
			if sum, ok := bucketSums["10:00"]; !ok || sum != 175.0 {
				t.Errorf("Expected 175.0 for 10:00 hour bucket, got %v", sum)
			}

			// 11:00-12:00 bucket should have 95.0 (35.0 + 60.0)
			if sum, ok := bucketSums["11:00"]; !ok || sum != 95.0 {
				t.Errorf("Expected 95.0 for 11:00 hour bucket, got %v", sum)
			}
		}
	})
}
