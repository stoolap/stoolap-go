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

// BenchmarkSimpleCTE benchmarks basic CTE performance
func BenchmarkSimpleCTE(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
		{"VeryLarge_100000", 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup table
			setupCTEBenchmarkTable(b, db, tc.rows)

			// Prepare query
			query := `WITH active_customers AS (
				SELECT id, name, total_spent 
				FROM customers 
				WHERE status = 'active'
			)
			SELECT COUNT(*) FROM active_customers`

			b.ResetTimer()
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

// BenchmarkCTEWithAggregation benchmarks CTE with aggregation
func BenchmarkCTEWithAggregation(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
		{"VeryLarge_100000", 100000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup table
			setupCTEBenchmarkTable(b, db, tc.rows)

			// Prepare query
			query := `WITH customer_stats AS (
				SELECT 
					status,
					COUNT(*) as customer_count,
					AVG(total_spent) as avg_spent,
					MAX(total_spent) as max_spent
				FROM customers
				GROUP BY status
			)
			SELECT * FROM customer_stats WHERE avg_spent > 500`

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var status string
					var count int
					var avgSpent, maxSpent float64
					err := rows.Scan(&status, &count, &avgSpent, &maxSpent)
					if err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkMultipleCTEs benchmarks multiple CTEs in one query
func BenchmarkMultipleCTEs(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupCTEBenchmarkWithOrders(b, db, tc.rows)

			// Prepare query
			query := `WITH 
			high_value_customers AS (
				SELECT id, name 
				FROM customers 
				WHERE total_spent > 1000
			),
			recent_orders AS (
				SELECT customer_id, SUM(amount) as order_total
				FROM orders
				WHERE status = 'completed'
				GROUP BY customer_id
			)
			SELECT 
				hvc.name,
				ro.order_total
			FROM high_value_customers hvc
			JOIN recent_orders ro ON hvc.id = ro.customer_id`

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var name string
					var orderTotal float64
					err := rows.Scan(&name, &orderTotal)
					if err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkNestedCTEs benchmarks CTEs referencing other CTEs
func BenchmarkNestedCTEs(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup table
			setupCTEBenchmarkTable(b, db, tc.rows)

			// Prepare query
			query := `WITH 
			active_customers AS (
				SELECT id, name, total_spent 
				FROM customers 
				WHERE status = 'active'
			),
			high_spenders AS (
				SELECT id, name, total_spent
				FROM active_customers
				WHERE total_spent > 1000
			),
			top_tier AS (
				SELECT name, total_spent
				FROM high_spenders
				WHERE total_spent > 5000
			)
			SELECT COUNT(*) FROM top_tier`

			b.ResetTimer()
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

// BenchmarkCTEInSubquery benchmarks CTE used in subqueries
func BenchmarkCTEInSubquery(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupCTEBenchmarkWithOrders(b, db, tc.rows)

			// Prepare query
			query := `WITH avg_order AS (
				SELECT AVG(amount) as avg_amount
				FROM orders
			)
			SELECT COUNT(*) 
			FROM orders 
			WHERE amount > (SELECT avg_amount FROM avg_order)`

			b.ResetTimer()
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

// BenchmarkCTEVsSubquery compares CTE vs subquery performance
func BenchmarkCTEVsSubquery(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup tables
	setupCTEBenchmarkWithOrders(b, db, 10000)

	b.Run("CTE", func(b *testing.B) {
		query := `WITH high_value_customers AS (
			SELECT id FROM customers WHERE total_spent > 1000
		)
		SELECT COUNT(*) FROM orders 
		WHERE customer_id IN (SELECT id FROM high_value_customers)`

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Subquery", func(b *testing.B) {
		query := `SELECT COUNT(*) FROM orders 
		WHERE customer_id IN (SELECT id FROM customers WHERE total_spent > 1000)`

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCTEWithHaving benchmarks CTE with HAVING clause
func BenchmarkCTEWithHaving(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_100", 100},
		{"Medium_1000", 1000},
		{"Large_10000", 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupCTEBenchmarkWithOrders(b, db, tc.rows)

			// Prepare query
			query := `WITH customer_order_stats AS (
				SELECT 
					customer_id,
					COUNT(*) as order_count,
					SUM(amount) as total_amount
				FROM orders
				GROUP BY customer_id
				HAVING COUNT(*) > 5
			)
			SELECT COUNT(*) FROM customer_order_stats WHERE total_amount > 1000`

			b.ResetTimer()
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

// Helper function to setup CTE benchmark table
func setupCTEBenchmarkTable(b *testing.B, db *sql.DB, rows int) {
	// Create customers table
	_, err := db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT,
		total_spent FLOAT,
		created_date TEXT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert customers
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO customers (id, name, status, total_spent, created_date) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < rows; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		} else if i%5 == 0 {
			status = "pending"
		}
		totalSpent := float64(i * 10)
		createdDate := fmt.Sprintf("2025-%02d-%02d", (i%12)+1, (i%28)+1)
		_, err = stmt.Exec(i, fmt.Sprintf("Customer %d", i), status, totalSpent, createdDate)
		if err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()

	err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}
}

// Helper function to setup CTE benchmark with orders table
func setupCTEBenchmarkWithOrders(b *testing.B, db *sql.DB, customerRows int) {
	// First setup customers
	setupCTEBenchmarkTable(b, db, customerRows)

	// Create orders table
	_, err := db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT,
		order_date TEXT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert orders (10 orders per customer on average)
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO orders (id, customer_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	orderID := 0
	for i := 0; i < customerRows*10; i++ {
		customerID := i % customerRows
		amount := float64((i % 500) + 50)
		status := "pending"
		if i%2 == 0 {
			status = "completed"
		} else if i%5 == 0 {
			status = "cancelled"
		}
		orderDate := fmt.Sprintf("2025-%02d-%02d", (i%12)+1, (i%28)+1)
		_, err = stmt.Exec(orderID, customerID, amount, status, orderDate)
		if err != nil {
			b.Fatal(err)
		}
		orderID++
	}
	stmt.Close()

	err = tx.Commit()
	if err != nil {
		b.Fatal(err)
	}
}

// BenchmarkCTEMemoryUsage tests memory efficiency of CTEs
func BenchmarkCTEMemoryUsage(b *testing.B) {
	testCases := []struct {
		name string
		rows int
	}{
		{"Small_1000", 1000},
		{"Medium_10000", 10000},
		{"Large_50000", 50000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup table
			setupCTEBenchmarkTable(b, db, tc.rows)

			// Complex query with multiple CTEs to test memory usage
			query := `WITH 
			base_data AS (
				SELECT id, name, status, total_spent
				FROM customers
			),
			status_groups AS (
				SELECT status, COUNT(*) as count, AVG(total_spent) as avg_spent
				FROM base_data
				GROUP BY status
			),
			high_value AS (
				SELECT *
				FROM base_data
				WHERE total_spent > (SELECT AVG(total_spent) * 2 FROM base_data)
			)
			SELECT 
				sg.status, 
				sg.count, 
				sg.avg_spent,
				(SELECT COUNT(*) FROM high_value hv WHERE hv.status = sg.status) as high_value_count
			FROM status_groups sg`

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var status string
					var count, highValueCount int
					var avgSpent float64
					err := rows.Scan(&status, &count, &avgSpent, &highValueCount)
					if err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}
