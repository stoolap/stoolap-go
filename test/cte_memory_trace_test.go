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
	"database/sql"
	"runtime"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestCTEMemoryTrace traces memory allocations in CTE execution
func TestCTEMemoryTrace(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables
	if _, err := db.ExecContext(ctx, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY,
		name TEXT,
		total_spent FLOAT
	)`); err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		customer_id INTEGER,
		amount FLOAT,
		status TEXT
	)`); err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert test data
	numCustomers := 1000
	ordersPerCustomer := 10

	// Insert customers
	for i := 0; i < numCustomers; i++ {
		_, err := db.ExecContext(ctx, "INSERT INTO customers VALUES (?, ?, ?)",
			i, "Customer"+string(rune(i)), float64(i*100))
		if err != nil {
			t.Fatalf("Failed to insert customer: %v", err)
		}
	}

	// Insert orders
	orderID := 0
	for i := 0; i < numCustomers; i++ {
		for j := 0; j < ordersPerCustomer; j++ {
			status := "pending"
			if j%2 == 0 {
				status = "completed"
			}
			_, err := db.ExecContext(ctx, "INSERT INTO orders VALUES (?, ?, ?, ?)",
				orderID, i, float64(j*50+10), status)
			if err != nil {
				t.Fatalf("Failed to insert order: %v", err)
			}
			orderID++
		}
	}

	// Test different parts of the query separately
	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "Simple_CTE_No_Aggregation",
			query: `WITH high_value AS (
				SELECT id, name FROM customers WHERE total_spent > 50000
			)
			SELECT COUNT(*) FROM high_value`,
		},
		{
			name: "CTE_With_GROUP_BY",
			query: `WITH order_totals AS (
				SELECT customer_id, SUM(amount) as total
				FROM orders
				WHERE status = 'completed'
				GROUP BY customer_id
			)
			SELECT COUNT(*) FROM order_totals`,
		},
		{
			name: "Full_Query_With_JOIN",
			query: `WITH 
			high_value_customers AS (
				SELECT id, name 
				FROM customers 
				WHERE total_spent > 50000
			),
			recent_orders AS (
				SELECT customer_id, SUM(amount) as order_total
				FROM orders
				WHERE status = 'completed'
				GROUP BY customer_id
			)
			SELECT COUNT(*)
			FROM high_value_customers hvc
			JOIN recent_orders ro ON hvc.id = ro.customer_id`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Force GC and measure memory before
			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)

			// Execute query
			rows, err := db.QueryContext(ctx, tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Consume results
			for rows.Next() {
				var count int
				if err := rows.Scan(&count); err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
			}
			rows.Close()

			// Force GC and measure memory after
			runtime.GC()
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			// Calculate allocations
			allocBytes := m2.TotalAlloc - m1.TotalAlloc
			allocCount := m2.Mallocs - m1.Mallocs

			t.Logf("Memory allocated: %d bytes in %d allocations (%.2f bytes/alloc)",
				allocBytes, allocCount, float64(allocBytes)/float64(allocCount))
		})
	}
}
