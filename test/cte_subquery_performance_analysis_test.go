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
	"runtime"
	"testing"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// BenchmarkCTEMaterializationOverhead measures the overhead of CTE materialization
func BenchmarkCTEMaterializationOverhead(b *testing.B) {
	testCases := []struct {
		name string
		rows int
		cols int
	}{
		{"Small_100x5", 100, 5},
		{"Medium_1000x10", 1000, 10},
		{"Large_10000x20", 10000, 20},
		{"VeryLarge_100000x5", 100000, 5},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Create table with multiple columns
			createSQL := "CREATE TABLE test_data (id INTEGER PRIMARY KEY"
			for i := 1; i < tc.cols; i++ {
				createSQL += fmt.Sprintf(", col%d FLOAT", i)
			}
			createSQL += ")"

			_, err = db.Exec(createSQL)
			if err != nil {
				b.Fatal(err)
			}

			// Insert data
			tx, _ := db.Begin()
			insertSQL := "INSERT INTO test_data VALUES (?"
			for i := 1; i < tc.cols; i++ {
				insertSQL += ", ?"
			}
			insertSQL += ")"

			stmt, _ := tx.Prepare(insertSQL)
			for i := 0; i < tc.rows; i++ {
				args := make([]interface{}, tc.cols)
				args[0] = i
				for j := 1; j < tc.cols; j++ {
					args[j] = float64(i * j)
				}
				stmt.Exec(args...)
			}
			stmt.Close()
			tx.Commit()

			// Benchmark CTE that materializes all data
			query := `WITH materialized AS (
				SELECT * FROM test_data
			)
			SELECT COUNT(*) FROM materialized`

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var count int
				err := db.QueryRow(query).Scan(&count)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSubqueryVsDirectQuery compares subquery overhead vs direct queries
func BenchmarkSubqueryVsDirectQuery(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup tables
	setupPerformanceTestTables(b, db, 10000)

	testCases := []struct {
		name        string
		directQuery string
		subquery    string
	}{
		{
			name:        "Simple_IN",
			directQuery: "SELECT COUNT(*) FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.status = 'active'",
			subquery:    "SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')",
		},
		{
			name:        "Scalar_Comparison",
			directQuery: "SELECT COUNT(*) FROM orders WHERE amount > 250", // Pre-calculated average
			subquery:    "SELECT COUNT(*) FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
		},
		{
			name:        "EXISTS_Check",
			directQuery: "SELECT COUNT(DISTINCT customer_id) FROM orders WHERE amount > 100",
			subquery:    "SELECT COUNT(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name+"_Direct", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var count int
				err := db.QueryRow(tc.directQuery).Scan(&count)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"_Subquery", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var count int
				err := db.QueryRow(tc.subquery).Scan(&count)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCTEMemoryUsageProfile profiles memory usage patterns of CTEs
func BenchmarkCTEMemoryUsageProfile(b *testing.B) {
	testCases := []struct {
		name     string
		rows     int
		scenario string
		query    string
	}{
		{
			name:     "FullMaterialization_10K",
			rows:     10000,
			scenario: "full_scan",
			query: `WITH all_orders AS (
				SELECT * FROM orders
			)
			SELECT COUNT(*) FROM all_orders`,
		},
		{
			name:     "PartialUse_10K",
			rows:     10000,
			scenario: "limit",
			query: `WITH all_orders AS (
				SELECT * FROM orders ORDER BY amount DESC
			)
			SELECT * FROM all_orders LIMIT 10`,
		},
		{
			name:     "MultipleReferences_10K",
			rows:     10000,
			scenario: "multi_ref",
			query: `WITH high_value AS (
				SELECT * FROM orders WHERE amount > 500
			)
			SELECT 
				(SELECT COUNT(*) FROM high_value) as total,
				(SELECT AVG(amount) FROM high_value) as avg_amount,
				(SELECT MAX(amount) FROM high_value) as max_amount`,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			setupPerformanceTestTables(b, db, tc.rows)

			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			startAlloc := m.Alloc

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query(tc.query)
				if err != nil {
					b.Fatal(err)
				}

				// Consume results
				for rows.Next() {
					// Just scan to consume
				}
				rows.Close()
			}

			runtime.GC()
			runtime.ReadMemStats(&m)
			endAlloc := m.Alloc

			b.ReportMetric(float64(endAlloc-startAlloc)/float64(b.N), "bytes/op")
		})
	}
}

// BenchmarkSubqueryExecutionPaths profiles different subquery execution paths
func BenchmarkSubqueryExecutionPaths(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	setupPerformanceTestTables(b, db, 10000)

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "IN_Small_Result",
			query: "SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'vip' LIMIT 10)",
		},
		{
			name:  "IN_Large_Result",
			query: "SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')",
		},
		{
			name:  "Scalar_Aggregate",
			query: "SELECT COUNT(*) FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'completed')",
		},
		{
			name:  "Scalar_SingleRow",
			query: "SELECT COUNT(*) FROM orders WHERE amount > (SELECT amount FROM orders WHERE id = 1000)",
		},
		{
			name:  "EXISTS_EarlyExit",
			query: "SELECT COUNT(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id LIMIT 1)",
		},
		{
			name:  "NOT_EXISTS",
			query: "SELECT COUNT(*) FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Warm up
			var count int
			db.QueryRow(tc.query).Scan(&count)

			b.ResetTimer()
			b.ReportAllocs()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				err := db.QueryRow(tc.query).Scan(&count)
				if err != nil {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(start)

			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "ns/op")
		})
	}
}

// BenchmarkCTEVsTemporaryTable compares CTE performance vs temporary tables
func BenchmarkCTEVsTemporaryTable(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	setupPerformanceTestTables(b, db, 50000)

	b.Run("CTE_Approach", func(b *testing.B) {
		query := `
			WITH customer_summary AS (
				SELECT 
					customer_id,
					COUNT(*) as order_count,
					SUM(amount) as total_amount,
					AVG(amount) as avg_amount
				FROM orders
				GROUP BY customer_id
			)
			SELECT 
				c.name,
				cs.order_count,
				cs.total_amount
			FROM customers c
			JOIN customer_summary cs ON c.id = cs.customer_id
			WHERE cs.order_count > 5
		`

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(query)
			if err != nil {
				b.Fatal(err)
			}
			for rows.Next() {
				var name string
				var orderCount int
				var totalAmount float64
				rows.Scan(&name, &orderCount, &totalAmount)
			}
			rows.Close()
		}
	})

	b.Run("TempTable_Approach", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create temp table
			tempName := fmt.Sprintf("temp_summary_%d", i)
			_, err := db.Exec(fmt.Sprintf(`
				CREATE TEMP TABLE %s AS
				SELECT 
					customer_id,
					COUNT(*) as order_count,
					SUM(amount) as total_amount,
					AVG(amount) as avg_amount
				FROM orders
				GROUP BY customer_id
			`, tempName))
			if err != nil {
				b.Fatal(err)
			}

			// Query using temp table
			rows, err := db.Query(fmt.Sprintf(`
				SELECT 
					c.name,
					cs.order_count,
					cs.total_amount
				FROM customers c
				JOIN %s cs ON c.id = cs.customer_id
				WHERE cs.order_count > 5
			`, tempName))
			if err != nil {
				b.Fatal(err)
			}

			for rows.Next() {
				var name string
				var orderCount int
				var totalAmount float64
				rows.Scan(&name, &orderCount, &totalAmount)
			}
			rows.Close()

			// Drop temp table
			_, _ = db.Exec(fmt.Sprintf("DROP TABLE %s", tempName))
		}
	})
}

// Helper function for performance tests
func setupPerformanceTestTables(b testing.TB, db *sql.DB, orderCount int) {
	// Create customers table
	_, err := db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT,
		created_date INTEGER
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT,
		order_date INTEGER
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert customers (1/10th of orders)
	customerCount := orderCount / 10
	if customerCount < 100 {
		customerCount = 100
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO customers VALUES (?, ?, ?, ?)")
	for i := 0; i < customerCount; i++ {
		status := "active"
		if i%10 == 0 {
			status = "inactive"
		} else if i%50 == 0 {
			status = "vip"
		}
		stmt.Exec(i, fmt.Sprintf("Customer%d", i), status, i%365)
	}
	stmt.Close()
	tx.Commit()

	// Insert orders
	tx, _ = db.Begin()
	stmt, _ = tx.Prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?)")
	for i := 0; i < orderCount; i++ {
		custID := i % customerCount
		amount := float64((i % 1000) + 50)
		status := "pending"
		if i%3 == 0 {
			status = "completed"
		} else if i%7 == 0 {
			status = "cancelled"
		}
		stmt.Exec(i, custID, amount, status, i%365)
	}
	stmt.Close()
	tx.Commit()
}

// BenchmarkWorstCaseScenarios tests worst-case performance scenarios
func BenchmarkWorstCaseScenarios(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	setupPerformanceTestTables(b, db, 10000)

	b.Run("DeeplyNestedCTEs", func(b *testing.B) {
		query := `
			WITH level1 AS (
				SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id
			),
			level2 AS (
				SELECT * FROM level1 WHERE total > 1000
			),
			level3 AS (
				SELECT * FROM level2 WHERE total < 5000
			),
			level4 AS (
				SELECT AVG(total) as avg_total FROM level3
			)
			SELECT * FROM level4
		`

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var avgTotal float64
			err := db.QueryRow(query).Scan(&avgTotal)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("LargeINList", func(b *testing.B) {
		// Subquery that returns many values
		query := `
			SELECT COUNT(*) FROM orders 
			WHERE customer_id IN (
				SELECT id FROM customers WHERE id < 5000
			)
		`

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MultipleSubqueriesInWHERE", func(b *testing.B) {
		query := `
			SELECT COUNT(*) FROM orders o
			WHERE o.amount > (SELECT AVG(amount) FROM orders)
			  AND o.customer_id IN (SELECT id FROM customers WHERE status = 'active')
			  AND EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.created_date < 180)
		`

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
