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
	"fmt"
	"strconv"
	"testing"

	"github.com/stoolap/stoolap-go/internal/sql"
	"github.com/stoolap/stoolap-go/internal/storage"

	// Import necessary packages to register factory functions
	_ "github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestAggregateOrdering(t *testing.T) {
	// Get the block storage engine factory
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		t.Fatalf("Failed to get db engine factory")
	}

	// Create the engine with the connection string
	engine, err := factory.Create("memory://")
	if err != nil {
		t.Fatalf("Failed to create db engine: %v", err)
	}

	// Open the engine
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a SQL executor
	executor := sql.NewExecutor(engine)

	// Create test table
	createTable(t, executor, `CREATE TABLE sales (
		id INTEGER,
		category TEXT,
		product TEXT,
		amount FLOAT
	)`)

	// Insert test data
	salesData := []string{
		`INSERT INTO sales (id, category, product, amount) VALUES (1, 'Electronics', 'Laptop', 1200.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (2, 'Electronics', 'Phone', 800.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (3, 'Electronics', 'Laptop', 1500.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (4, 'Clothing', 'Shirt', 50.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (5, 'Clothing', 'Pants', 80.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (6, 'Clothing', 'Shirt', 45.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (7, 'Books', 'Fiction', 25.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (8, 'Books', 'Non-Fiction', 35.00)`,
		`INSERT INTO sales (id, category, product, amount) VALUES (9, 'Books', 'Fiction', 30.00)`,
	}

	t.Log("Inserting sales data")
	insertData(t, executor, salesData)

	// Verify data was inserted
	verifyQuery := `SELECT * FROM sales`
	verifyResult := executeQuery(t, executor, verifyQuery)

	t.Logf("Verification query result columns: %v", verifyResult.Columns())
	rowCount := 0
	for verifyResult.Next() {
		var id int
		var category, product string
		var amount float64
		if err := verifyResult.Scan(&id, &category, &product, &amount); err != nil {
			t.Fatalf("Failed to scan verification row: %v", err)
		}
		t.Logf("Verification row: id=%d, category=%s, product=%s, amount=%.2f",
			id, category, product, amount)
		rowCount++
	}
	t.Logf("Total rows: %d", rowCount)

	// Test aggregation with ORDER BY
	t.Run("Aggregation with ORDER BY", func(t *testing.T) {
		// Use a simpler query without the alias in ORDER BY
		query := `SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY SUM(amount) DESC`

		result := executeQuery(t, executor, query)

		// Print the result columns
		columns := result.Columns()
		t.Logf("Result columns: %v", columns)

		// Expected order: Electronics, Clothing, Books
		expectedCategories := []string{"Electronics", "Clothing", "Books"}
		expectedTotals := []float64{3500.00, 175.00, 90.00}

		rowIdx := 0
		for result.Next() {
			// We need to use interface{} then type assert because the executor
			// is setting values to interface{} types
			var categoryVal, totalSalesVal interface{}

			if err := result.Scan(&categoryVal, &totalSalesVal); err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}

			// Convert to expected types
			category, ok := categoryVal.(string)
			if !ok {
				t.Logf("DEBUG: Failed to convert category value %v (%T) to string", categoryVal, categoryVal)
				category = fmt.Sprintf("%v", categoryVal)
			}

			var totalSales float64
			switch v := totalSalesVal.(type) {
			case float64:
				totalSales = v
			case int64:
				totalSales = float64(v)
			case int:
				totalSales = float64(v)
			default:
				t.Logf("DEBUG: Failed to convert total_sales value %v (%T) to float64", totalSalesVal, totalSalesVal)
				if s, ok := totalSalesVal.(string); ok {
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						totalSales = f
					}
				}
			}

			t.Logf("Result row: category=%s, total_sales=%.2f", category, totalSales)

			if rowIdx < len(expectedCategories) {
				if category != expectedCategories[rowIdx] {
					t.Errorf("Expected category %s at position %d, got %s",
						expectedCategories[rowIdx], rowIdx, category)
				}

				if totalSales != expectedTotals[rowIdx] {
					t.Errorf("Expected total sales %.2f at position %d, got %.2f",
						expectedTotals[rowIdx], rowIdx, totalSales)
				}
			}

			rowIdx++
		}

		if rowIdx != 3 {
			t.Errorf("Expected 3 result rows, got %d", rowIdx)
		}
	})

	// Test aggregation with LIMIT
	t.Run("Aggregation with LIMIT", func(t *testing.T) {
		// Simplify the query to avoid the alias
		query := `SELECT product, AVG(amount) FROM sales GROUP BY product LIMIT 3`

		result := executeQuery(t, executor, query)

		// Count rows to verify the limit is applied
		rowCount := 0
		for result.Next() {
			// We need to use interface{} then type assert because the executor
			// is setting values to interface{} types
			var productVal, avgPriceVal interface{}

			if err := result.Scan(&productVal, &avgPriceVal); err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}

			// Convert to expected types
			product, ok := productVal.(string)
			if !ok {
				t.Logf("DEBUG: Failed to convert product value %v (%T) to string", productVal, productVal)
				product = fmt.Sprintf("%v", productVal)
			}

			var avgPrice float64
			switch v := avgPriceVal.(type) {
			case float64:
				avgPrice = v
			case int64:
				avgPrice = float64(v)
			case int:
				avgPrice = float64(v)
			default:
				t.Logf("DEBUG: Failed to convert avg_price value %v (%T) to float64", avgPriceVal, avgPriceVal)
				if s, ok := avgPriceVal.(string); ok {
					if f, err := strconv.ParseFloat(s, 64); err == nil {
						avgPrice = f
					}
				}
			}

			t.Logf("Result row: product=%s, avg_price=%.2f", product, avgPrice)
			rowCount++
		}

		if rowCount != 3 {
			t.Errorf("Expected 3 rows with LIMIT 3, got %d", rowCount)
		}
	})
}

// Helper functions
func createTable(t *testing.T, executor *sql.Executor, query string) {
	result, err := executor.Execute(context.Background(), nil, query)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result != nil {
		result.Close()
	}
}

func insertData(t *testing.T, executor *sql.Executor, statements []string) {
	for i, statement := range statements {
		t.Logf("Inserting statement %d: %s", i+1, statement)
		result, err := executor.Execute(context.Background(), nil, statement)
		if err != nil {
			t.Fatalf("Failed to execute statement: %v", err)
		}
		if result != nil {
			result.Close()
		}
	}
}

func executeQuery(t *testing.T, executor *sql.Executor, query string) storage.Result {
	result, err := executor.Execute(context.Background(), nil, query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	t.Cleanup(func() {
		if result != nil {
			result.Close()
		}
	})

	return result
}
