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

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestCTESubqueryComparison creates a simple test to show what would happen without CTE/subquery support
func TestCTESubqueryComparison(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup tables
	setupComparisonTables(t, db)

	// Test 1: Complex query that would benefit from CTE
	t.Run("Without CTE - Multiple Queries", func(t *testing.T) {
		// Without CTE, we need multiple queries

		// First, get high value customers
		rows, err := db.Query(`
			SELECT id FROM customers WHERE total_spent > 1000
		`)
		if err != nil {
			t.Fatal(err)
		}

		var highValueCustomers []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			if err != nil {
				t.Fatal(err)
			}
			highValueCustomers = append(highValueCustomers, id)
		}
		rows.Close()

		// Then get their recent orders manually
		for _, custID := range highValueCustomers {
			rows, err := db.Query(`
				SELECT SUM(amount) FROM orders WHERE customer_id = ? AND status = 'completed'
			`, custID)
			if err != nil {
				t.Fatal(err)
			}
			rows.Close()
		}

		// This approach requires application-level logic and multiple round trips
	})

	t.Run("With CTE - Single Query", func(t *testing.T) {
		// With CTE, everything in one query
		query := `
			WITH high_value_customers AS (
				SELECT id, name FROM customers WHERE total_spent > 1000
			),
			recent_orders AS (
				SELECT customer_id, SUM(amount) as order_total
				FROM orders WHERE status = 'completed'
				GROUP BY customer_id
			)
			SELECT hvc.name, ro.order_total
			FROM high_value_customers hvc
			JOIN recent_orders ro ON hvc.id = ro.customer_id
		`

		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// Single query, no application logic needed
		var count int
		for rows.Next() {
			var name string
			var total float64
			err := rows.Scan(&name, &total)
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		if count == 0 {
			t.Error("Expected results from CTE query")
		}
	})

	// Test 2: Subquery that would require temporary table without support
	t.Run("Without Subquery - Temp Table", func(t *testing.T) {
		// Without subquery support, create temp table manually
		// First create the table
		_, err := db.Exec(`CREATE TABLE avg_order_amount (avg_amount FLOAT)`)
		if err != nil {
			t.Fatal(err)
		}

		// Then calculate and insert the average
		var avgAmount float64
		err = db.QueryRow(`SELECT AVG(amount) FROM orders`).Scan(&avgAmount)
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(`INSERT INTO avg_order_amount VALUES (?)`, avgAmount)
		if err != nil {
			t.Fatal(err)
		}

		// Then use it in main query
		var count int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM orders o
			CROSS JOIN avg_order_amount a
			WHERE o.amount > a.avg_amount
		`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}

		// Clean up
		_, _ = db.Exec(`DROP TABLE avg_order_amount`)

		if count == 0 {
			t.Error("Expected results from temp table approach")
		}
	})

	t.Run("With Subquery - Direct", func(t *testing.T) {
		// With subquery support, direct query
		var count int
		err := db.QueryRow(`
			SELECT COUNT(*) FROM orders 
			WHERE amount > (SELECT AVG(amount) FROM orders)
		`).Scan(&count)

		if err != nil {
			t.Fatal(err)
		}

		if count == 0 {
			t.Error("Expected results from subquery")
		}
	})
}

// BenchmarkAlternativeApproaches shows performance difference between CTE/subquery vs alternatives
func BenchmarkAlternativeApproaches(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup with more data
	setupLargeComparisonTables(b, db, 1000, 5000)

	b.Run("Multiple_Queries_Application_Logic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Get high value customers
			rows, err := db.Query(`SELECT id FROM customers WHERE total_spent > 1000`)
			if err != nil {
				b.Fatal(err)
			}

			var ids []int
			for rows.Next() {
				var id int
				rows.Scan(&id)
				ids = append(ids, id)
			}
			rows.Close()

			// For each customer, get their order sum
			totalSum := 0.0
			for _, id := range ids {
				var sum sql.NullFloat64
				err := db.QueryRow(`SELECT SUM(amount) FROM orders WHERE customer_id = ?`, id).Scan(&sum)
				if err != nil && err != sql.ErrNoRows {
					b.Fatal(err)
				}
				if sum.Valid {
					totalSum += sum.Float64
				}
			}
		}
	})

	b.Run("Single_CTE_Query", func(b *testing.B) {
		query := `
			WITH high_value_customers AS (
				SELECT id FROM customers WHERE total_spent > 1000
			)
			SELECT SUM(amount)
			FROM orders
			WHERE customer_id IN (SELECT id FROM high_value_customers)
		`

		for i := 0; i < b.N; i++ {
			var sum sql.NullFloat64
			err := db.QueryRow(query).Scan(&sum)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Temp_Table_Approach", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Create temp table with unique name
			tempTable := fmt.Sprintf("temp_avg_%d", i)

			// Create table structure
			_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (avg_amount FLOAT)`, tempTable))
			if err != nil {
				b.Fatal(err)
			}

			// Calculate and insert average
			var avgAmount float64
			err = db.QueryRow(`SELECT AVG(amount) FROM orders`).Scan(&avgAmount)
			if err != nil {
				b.Fatal(err)
			}

			_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (?)`, tempTable), avgAmount)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			var count int
			err = db.QueryRow(fmt.Sprintf(`
				SELECT COUNT(*) FROM orders o
				CROSS JOIN %s a
				WHERE o.amount > a.avg_amount
			`, tempTable)).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			_, _ = db.Exec(fmt.Sprintf(`DROP TABLE %s`, tempTable))
			b.StartTimer()
		}
	})

	b.Run("Direct_Subquery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := db.QueryRow(`
				SELECT COUNT(*) FROM orders 
				WHERE amount > (SELECT AVG(amount) FROM orders)
			`).Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func setupComparisonTables(t testing.TB, db *sql.DB) {
	// Create tables
	_, err := db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		total_spent FLOAT
	)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert sample data
	_, err = db.Exec(`INSERT INTO customers VALUES 
		(1, 'Alice', 1500),
		(2, 'Bob', 500),
		(3, 'Charlie', 2000),
		(4, 'David', 800)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO orders VALUES 
		(1, 1, 100, 'completed'),
		(2, 1, 200, 'completed'),
		(3, 2, 150, 'pending'),
		(4, 3, 300, 'completed'),
		(5, 3, 400, 'completed')`)
	if err != nil {
		t.Fatal(err)
	}
}

func setupLargeComparisonTables(b *testing.B, db *sql.DB, customers, orders int) {
	// Create tables
	_, err := db.Exec(`CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		total_spent FLOAT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Bulk insert customers
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO customers VALUES (?, ?, ?)")
	for i := 0; i < customers; i++ {
		spent := float64(i * 10 % 5000)
		stmt.Exec(i, fmt.Sprintf("Customer%d", i), spent)
	}
	stmt.Close()
	tx.Commit()

	// Bulk insert orders
	tx, _ = db.Begin()
	stmt, _ = tx.Prepare("INSERT INTO orders VALUES (?, ?, ?, ?)")
	for i := 0; i < orders; i++ {
		custID := i % customers
		amount := float64((i % 1000) + 50)
		status := "pending"
		if i%3 == 0 {
			status = "completed"
		}
		stmt.Exec(i, custID, amount, status)
	}
	stmt.Close()
	tx.Commit()
}
