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

// BenchmarkSubqueryIN benchmarks IN subquery performance with varying data sizes
func BenchmarkSubqueryIN(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
		{"VeryLarge_100000x10000", 100000, 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

			// Prepare query
			query := `SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')`

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

// BenchmarkSubqueryNOTIN benchmarks NOT IN subquery performance
func BenchmarkSubqueryNOTIN(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

			// Prepare query
			query := `SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers WHERE status = 'inactive')`

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

// BenchmarkSubqueryEXISTS benchmarks EXISTS subquery performance
func BenchmarkSubqueryEXISTS(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
		{"VeryLarge_100000x10000", 100000, 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

			// Prepare query
			query := `SELECT COUNT(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)`

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

// BenchmarkSubqueryScalar benchmarks scalar subquery performance
func BenchmarkSubqueryScalar(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

			// Prepare query
			query := `SELECT COUNT(*) FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'completed')`

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

// BenchmarkSubqueryScalarInSelect benchmarks scalar subquery in SELECT clause
func BenchmarkSubqueryScalarInSelect(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100", 100, 100},
		{"Medium_1000", 1000, 1000},
		{"Large_10000", 10000, 10000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Setup tables
			setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

			// Prepare query
			query := `SELECT id, name, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count FROM customers c LIMIT 100`

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var id int
					var name string
					var orderCount int
					err := rows.Scan(&id, &name, &orderCount)
					if err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkSubqueryUpdate benchmarks UPDATE with subquery
func BenchmarkSubqueryUpdate(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, err := sql.Open("stoolap", "memory://")
				if err != nil {
					b.Fatal(err)
				}

				// Setup tables
				setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

				// Prepare query
				query := `UPDATE orders SET status = 'premium' WHERE customer_id IN (SELECT id FROM customers WHERE total_spent > 1000)`

				b.StartTimer()
				_, err = db.Exec(query)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()

				db.Close()
			}
		})
	}
}

// BenchmarkSubqueryDelete benchmarks DELETE with subquery
func BenchmarkSubqueryDelete(b *testing.B) {
	testCases := []struct {
		name         string
		mainRows     int
		subqueryRows int
	}{
		{"Small_100x10", 100, 10},
		{"Medium_1000x100", 1000, 100},
		{"Large_10000x1000", 10000, 1000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, err := sql.Open("stoolap", "memory://")
				if err != nil {
					b.Fatal(err)
				}

				// Setup tables
				setupSubqueryBenchmarkTables(b, db, tc.mainRows, tc.subqueryRows)

				// Prepare query
				query := `DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'inactive')`

				b.StartTimer()
				_, err = db.Exec(query)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()

				db.Close()
			}
		})
	}
}

// Helper function to setup benchmark tables
func setupSubqueryBenchmarkTables(b *testing.B, db *sql.DB, mainRows, subqueryRows int) {
	// Create customers table
	_, err := db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		status TEXT,
		total_spent FLOAT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert customers
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO customers (id, name, status, total_spent) VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < subqueryRows; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		}
		totalSpent := float64(i * 100)
		_, err = stmt.Exec(i, fmt.Sprintf("Customer %d", i), status, totalSpent)
		if err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()

	// Insert orders
	stmt, err = tx.Prepare("INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < mainRows; i++ {
		customerID := i % subqueryRows
		amount := float64((i % 500) + 50)
		status := "pending"
		if i%2 == 0 {
			status = "completed"
		}
		_, err = stmt.Exec(i, customerID, amount, status)
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

// BenchmarkSubqueryVsJoin compares subquery vs JOIN performance
func BenchmarkSubqueryVsJoin(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup tables with 10000 orders and 1000 customers
	setupSubqueryBenchmarkTables(b, db, 10000, 1000)

	b.Run("Subquery", func(b *testing.B) {
		query := `SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = 'active')`
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Join", func(b *testing.B) {
		query := `SELECT COUNT(*) FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.status = 'active'`
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

// BenchmarkNestedSubqueries benchmarks nested subquery performance
func BenchmarkNestedSubqueries(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup tables
	setupSubqueryBenchmarkTables(b, db, 1000, 100)

	// Create a third table for nested subqueries
	_, err = db.Exec(`CREATE TABLE regions (
		id INTEGER PRIMARY KEY,
		name TEXT,
		customer_id INTEGER
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert regions
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO regions (id, name, customer_id) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("Region %d", i), i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()
	tx.Commit()

	// Nested subquery
	query := `SELECT COUNT(*) FROM orders WHERE customer_id IN 
		(SELECT id FROM customers WHERE id IN 
			(SELECT customer_id FROM regions WHERE name LIKE 'Region%'))`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			b.Fatal(err)
		}
	}
}
