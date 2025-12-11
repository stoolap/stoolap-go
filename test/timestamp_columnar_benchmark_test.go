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
	"time"

	// Import for side effects - driver registration
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// BenchmarkTimestampOperations tests timestamp operations with and without columnar index
func BenchmarkTimestampOperations(b *testing.B) {
	// Open a connection to the database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Create a table with a timestamp column
	_, err = db.Exec(`
		CREATE TABLE timestamp_test (
			id INTEGER PRIMARY KEY,
			ts_val TIMESTAMP,
			value FLOAT,
			active BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data - one year of hourly timestamps
	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 365*24; i++ {
		timestamp := startTime.Add(time.Duration(i) * time.Hour)
		_, err := db.Exec(
			"INSERT INTO timestamp_test (id, ts_val, value, active) VALUES (?, ?, ?, ?)",
			i+1,
			timestamp.Format("2006-01-02 15:04:05"),
			float64(i%100)+0.5,
			i%2 == 0,
		)
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	// Test point query without index
	b.Run("PointQuery_NoIndex", func(b *testing.B) {
		// Sample 50 timestamps to query
		timestamps := make([]string, 50)
		for i := 0; i < 50; i++ {
			ts := startTime.AddDate(0, 0, i*7) // Weekly sample
			timestamps[i] = ts.Format("2006-01-02 15:04:05")
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ts := timestamps[i%len(timestamps)]
			var id int
			var tsVal string
			var value float64
			err := db.QueryRow("SELECT id, ts_val, value FROM timestamp_test WHERE ts_val = ?", ts).Scan(&id, &tsVal, &value)
			if err != nil && err != sql.ErrNoRows {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	// Create columnar index on timestamp column
	_, err = db.Exec("CREATE COLUMNAR INDEX ON timestamp_test (ts_val)")
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	// Test point query with index
	b.Run("PointQuery_WithIndex", func(b *testing.B) {
		// Use the same timestamps as before
		timestamps := make([]string, 50)
		for i := 0; i < 50; i++ {
			ts := startTime.AddDate(0, 0, i*7) // Weekly sample
			timestamps[i] = ts.Format("2006-01-02 15:04:05")
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ts := timestamps[i%len(timestamps)]
			var id int
			var tsVal string
			var value float64
			err := db.QueryRow("SELECT id, ts_val, value FROM timestamp_test WHERE ts_val = ?", ts).Scan(&id, &tsVal, &value)
			if err != nil && err != sql.ErrNoRows {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	// Test range query without index
	_, err = db.Exec("DROP COLUMNAR INDEX IF EXISTS ON timestamp_test (ts_val)")
	if err != nil {
		b.Fatalf("Failed to drop index: %v", err)
	}

	b.Run("RangeQuery_NoIndex", func(b *testing.B) {
		// Create 10 date ranges to benchmark
		ranges := make([][2]string, 10)
		for i := 0; i < 10; i++ {
			start := startTime.AddDate(0, i, 0) // Monthly start points
			end := start.AddDate(0, 0, 7)       // 1 week range
			ranges[i][0] = start.Format("2006-01-02 15:04:05")
			ranges[i][1] = end.Format("2006-01-02 15:04:05")
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rangeIdx := i % len(ranges)
			start := ranges[rangeIdx][0]
			end := ranges[rangeIdx][1]

			rows, err := db.Query(
				"SELECT id, ts_val, value FROM timestamp_test WHERE ts_val >= ? AND ts_val <= ?",
				start, end)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Process results
			var count int
			for rows.Next() {
				var id int
				var ts string
				var value float64
				if err := rows.Scan(&id, &ts, &value); err != nil {
					rows.Close()
					b.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			rows.Close()
		}
	})

	// Recreate index for range query test
	_, err = db.Exec("CREATE COLUMNAR INDEX IF NOT EXISTS ON timestamp_test (ts_val)")
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	b.Run("RangeQuery_WithIndex", func(b *testing.B) {
		// Use same ranges as before
		ranges := make([][2]string, 10)
		for i := 0; i < 10; i++ {
			start := startTime.AddDate(0, i, 0) // Monthly start points
			end := start.AddDate(0, 0, 7)       // 1 week range
			ranges[i][0] = start.Format("2006-01-02 15:04:05")
			ranges[i][1] = end.Format("2006-01-02 15:04:05")
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rangeIdx := i % len(ranges)
			start := ranges[rangeIdx][0]
			end := ranges[rangeIdx][1]

			rows, err := db.Query(
				"SELECT id, ts_val, value FROM timestamp_test WHERE ts_val >= ? AND ts_val <= ?",
				start, end)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Process results
			var count int
			for rows.Next() {
				var id int
				var ts string
				var value float64
				if err := rows.Scan(&id, &ts, &value); err != nil {
					rows.Close()
					b.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			rows.Close()
		}
	})

	// Test "recent data" query without index - common in time-series applications
	_, err = db.Exec("DROP COLUMNAR INDEX IF EXISTS ON timestamp_test (ts_val)")
	if err != nil {
		b.Fatalf("Failed to drop index: %v", err)
	}

	b.Run("RecentDataQuery_NoIndex", func(b *testing.B) {
		// Get the most recent 24 hours of data
		endTime := startTime.AddDate(0, 0, 364)  // Last day in dataset
		dayStart := endTime.Add(-24 * time.Hour) // 24 hours before
		queryStart := dayStart.Format("2006-01-02 15:04:05")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				"SELECT id, ts_val, value FROM timestamp_test WHERE ts_val >= ?",
				queryStart)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Process results
			var count int
			for rows.Next() {
				var id int
				var ts string
				var value float64
				if err := rows.Scan(&id, &ts, &value); err != nil {
					rows.Close()
					b.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			rows.Close()
		}
	})

	// Recreate index for the final test
	_, err = db.Exec("CREATE COLUMNAR INDEX IF NOT EXISTS ON timestamp_test (ts_val)")
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	b.Run("RecentDataQuery_WithIndex", func(b *testing.B) {
		// Use same query parameters as before
		endTime := startTime.AddDate(0, 0, 364)  // Last day in dataset
		dayStart := endTime.Add(-24 * time.Hour) // 24 hours before
		queryStart := dayStart.Format("2006-01-02 15:04:05")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(
				"SELECT id, ts_val, value FROM timestamp_test WHERE ts_val >= ?",
				queryStart)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Process results
			var count int
			for rows.Next() {
				var id int
				var ts string
				var value float64
				if err := rows.Scan(&id, &ts, &value); err != nil {
					rows.Close()
					b.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			rows.Close()
		}
	})
}
