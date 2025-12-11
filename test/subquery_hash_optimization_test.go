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
	"testing"

	"github.com/stoolap/stoolap-go/internal/parser"
	"github.com/stoolap/stoolap-go/internal/sql/executor"
	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestInExpressionHashOptimization tests the hash-based IN expression optimization
func TestInExpressionHashOptimization(t *testing.T) {
	// Create test engine
	cfg := &storage.Config{
		Path: "",
		Persistence: storage.PersistenceConfig{
			Enabled: false,
		},
	}
	eng := mvcc.NewMVCCEngine(cfg)
	if err := eng.Open(); err != nil {
		t.Fatalf("Failed to open MVCC engine: %v", err)
	}
	defer eng.Close()

	// Create executor
	exec := executor.NewExecutor(eng)
	ctx := context.Background()

	// Helper function to execute SQL
	executeSQL := func(query string) error {
		tx, err := eng.BeginTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = exec.Execute(ctx, tx, query)
		if err != nil {
			return err
		}

		return tx.Commit()
	}

	// Create test tables
	err := executeSQL(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)`)
	if err != nil {
		t.Fatal(err)
	}

	err = executeSQL(`CREATE TABLE active_ids (user_id INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	err = executeSQL(`INSERT INTO users VALUES 
		(1, 'Alice', 'active'),
		(2, 'Bob', 'inactive'),
		(3, 'Charlie', 'active'),
		(4, 'David', 'pending'),
		(5, 'Eve', 'active')`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert active IDs (with duplicates to test deduplication)
	err = executeSQL(`INSERT INTO active_ids VALUES (1), (3), (5), (1), (3)`)
	if err != nil {
		t.Fatal(err)
	}

	// Test hash optimization directly
	t.Run("DirectHashOptimization", func(t *testing.T) {
		tx, err := eng.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		// Parse the subquery
		stmt, err := parser.Parse(`SELECT user_id FROM active_ids`)
		if err != nil {
			t.Fatal(err)
		}

		selectStmt := stmt.(*parser.SelectStatement)
		subquery := &parser.ScalarSubquery{Subquery: selectStmt}

		// Execute the subquery (it will use hash optimization internally)
		values, err := exec.ExecuteSubquery(ctx, tx, subquery)
		if err != nil {
			t.Fatal(err)
		}

		// Verify we get correct values (deduped)
		// Convert to map to check uniqueness
		valueMap := make(map[int64]bool)
		for _, v := range values {
			if id, ok := v.(int64); ok {
				valueMap[id] = true
			}
		}

		if len(valueMap) != 3 {
			t.Errorf("Expected 3 unique values, got %d", len(valueMap))
		}

		// Check specific values
		for _, expectedID := range []int64{1, 3, 5} {
			if !valueMap[expectedID] {
				t.Errorf("Expected ID %d in value set", expectedID)
			}
		}
	})

	// Test IN subquery with hash optimization
	t.Run("INSubqueryPerformance", func(t *testing.T) {
		query := `SELECT id, name FROM users WHERE id IN (SELECT user_id FROM active_ids)`

		tx, err := eng.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		result, err := exec.Execute(ctx, tx, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		// Collect results
		var ids []int64
		for result.Next() {
			row := result.Row()
			if len(row) >= 1 {
				if idVal := row[0].AsInterface(); idVal != nil {
					if id, ok := idVal.(int64); ok {
						ids = append(ids, id)
					}
				}
			}
		}

		// Verify correct results
		expectedIDs := []int64{1, 3, 5}
		if len(ids) != len(expectedIDs) {
			t.Errorf("Expected %d results, got %d", len(expectedIDs), len(ids))
		}

		for i, id := range ids {
			if i < len(expectedIDs) && id != expectedIDs[i] {
				t.Errorf("Expected ID %d, got %d", expectedIDs[i], id)
			}
		}
	})

	// Test NOT IN with NULL handling
	t.Run("NOTINWithNULL", func(t *testing.T) {
		// Add a NULL value to active_ids
		err := executeSQL(`INSERT INTO active_ids VALUES (NULL)`)
		if err != nil {
			t.Fatal(err)
		}

		query := `SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM active_ids)`

		tx, err := eng.BeginTransaction()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		result, err := exec.Execute(ctx, tx, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		// NOT IN with NULL should return no results
		if result.Next() {
			t.Error("NOT IN with NULL should return no results")
		}
	})
}

// TestHashOptimizationCorrectness verifies the hash optimization produces same results
func TestHashOptimizationCorrectness(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount FLOAT)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE vip_customers (id INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO orders VALUES 
		(1, 100, 50.0),
		(2, 101, 75.0),
		(3, 102, 100.0),
		(4, 100, 25.0),
		(5, 103, 150.0)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO vip_customers VALUES (100), (102)`)
	if err != nil {
		t.Fatal(err)
	}

	// Debug: Check data was inserted
	var orderCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&orderCount)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Orders count: %d", orderCount)

	var vipCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM vip_customers`).Scan(&vipCount)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("VIP customers count: %d", vipCount)

	// Debug: Check direct IN list
	var directCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM orders WHERE customer_id IN (100, 102)`).Scan(&directCount)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Direct IN (100, 102) count: %d", directCount)

	// Test cases that should produce identical results
	testCases := []struct {
		name          string
		query         string
		expectedCount int
	}{
		{
			name:          "Simple IN subquery",
			query:         `SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT id FROM vip_customers)`,
			expectedCount: 3, // Orders 1, 3, 4
		},
		{
			name:          "NOT IN subquery",
			query:         `SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM vip_customers)`,
			expectedCount: 2, // Orders 2, 5
		},
		{
			name:          "IN with duplicates",
			query:         `SELECT COUNT(DISTINCT customer_id) FROM orders WHERE customer_id IN (SELECT id FROM vip_customers)`,
			expectedCount: 2, // Customers 100, 102
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var count int
			err := db.QueryRow(tc.query).Scan(&count)
			if err != nil {
				t.Fatal(err)
			}

			if count != tc.expectedCount {
				t.Errorf("Expected count %d, got %d", tc.expectedCount, count)
			}
		})
	}
}

// BenchmarkHashOptimizationReal benchmarks the actual performance improvement
func BenchmarkHashOptimizationReal(b *testing.B) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create tables with significant data
	_, err = db.Exec(`CREATE TABLE large_table (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE filter_values (value INTEGER)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert data
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO large_table VALUES (?, ?)")
	for i := 0; i < 10000; i++ {
		stmt.Exec(i, i%1000)
	}
	stmt.Close()

	// Insert filter values (500 unique values)
	stmt, _ = tx.Prepare("INSERT INTO filter_values VALUES (?)")
	for i := 0; i < 500; i++ {
		stmt.Exec(i * 2) // Even numbers only
	}
	stmt.Close()
	tx.Commit()

	// Benchmark the IN subquery
	query := `SELECT COUNT(*) FROM large_table WHERE value IN (SELECT value FROM filter_values)`

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			b.Fatal(err)
		}
	}
}
