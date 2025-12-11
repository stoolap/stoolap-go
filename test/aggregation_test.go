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
	"testing"

	"github.com/stoolap/stoolap-go/internal/sql"
	"github.com/stoolap/stoolap-go/internal/storage"

	// Import necessary packages to register factory functions
	_ "github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestAggregations(t *testing.T) {
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

	// Create the test table
	createQuery := `CREATE TABLE sales_aggr (id INTEGER, product TEXT, category TEXT, amount FLOAT, region TEXT)`
	_, err = executor.Execute(context.Background(), nil, createQuery)
	if err != nil {
		t.Fatalf("Failed to create sales_aggr table: %v", err)
	}

	// Insert test data
	sales_aggrData := []string{
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (1, 'Laptop', 'Electronics', 1200, 'North')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (2, 'Smartphone', 'Electronics', 800, 'North')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (3, 'TV', 'Electronics', 1500, 'South')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (4, 'Chair', 'Furniture', 150, 'East')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (5, 'Table', 'Furniture', 450, 'East')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (6, 'Sofa', 'Furniture', 950, 'West')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (7, 'Shirt', 'Clothing', 35, 'North')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (8, 'Jeans', 'Clothing', 60, 'South')`,
		`INSERT INTO sales_aggr (id, product, category, amount, region) VALUES (9, 'Shoes', 'Clothing', 90, 'West')`,
	}

	for _, query := range sales_aggrData {
		_, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Test COUNT
	t.Run("COUNT(*)", func(t *testing.T) {
		query := "SELECT COUNT(*) AS total FROM sales_aggr"

		result, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		if !result.Next() {
			t.Fatal("Expected a result row")
		}

		var count int
		if err := result.Scan(&count); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}

		if count != 9 {
			t.Errorf("Expected count of 9, got %d", count)
		}
	})

	// Test GROUP BY (simplified for now)
	t.Run("COUNT with GROUP BY", func(t *testing.T) {
		query := "SELECT COUNT(*) FROM sales_aggr GROUP BY category"

		result, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Just check that we got some results back for now
		found := false
		for result.Next() {
			found = true
			break
		}

		if !found {
			t.Fatal("Expected at least one result row")
		}
	})

	// Test DISTINCT
	t.Run("COUNT DISTINCT", func(t *testing.T) {
		// Insert duplicate data for testing DISTINCT
		_, err := executor.Execute(context.Background(), nil, `
			INSERT INTO sales_aggr (id, product, category, amount, region) 
			VALUES (10, 'Laptop', 'Electronics', 1200, 'North')
		`)
		if err != nil {
			t.Fatalf("Failed to insert duplicate data: %v", err)
		}

		query := "SELECT COUNT(DISTINCT product) FROM sales_aggr"

		result, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		if !result.Next() {
			t.Fatal("Expected a result row")
		}

		var distinctCount int
		if err := result.Scan(&distinctCount); err != nil {
			t.Fatalf("Failed to scan result: %v", err)
		}

		// We expect 9 distinct products (even though we have 10 rows)
		if distinctCount != 9 {
			t.Errorf("Expected 9 distinct products, got %d", distinctCount)
		}
	})

	// Test HAVING
	t.Run("GROUP BY with HAVING", func(t *testing.T) {
		query := "SELECT category, COUNT(*) FROM sales_aggr GROUP BY category HAVING COUNT(*) > 2"

		result, err := executor.Execute(context.Background(), nil, query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Count the number of rows returned
		rowCount := 0
		categories := make(map[string]bool)

		for result.Next() {
			rowCount++
			var category string
			var count int

			if err := result.Scan(&category, &count); err != nil {
				t.Fatalf("Failed to scan result: %v", err)
			}

			// Save the category
			categories[category] = true

			// Verify the count is > 2
			if count <= 2 {
				t.Errorf("Expected count > 2 for category %s, got %d", category, count)
			}
		}

		// We should have only returned categories with more than 2 products
		// (Electronics, Furniture, and Clothing each have 3 products, plus 1 duplicate for Electronics)
		if rowCount != 3 {
			t.Errorf("Expected 3 rows, got %d", rowCount)
		}

		// Check that we got all expected categories
		expectedCategories := []string{"Electronics", "Furniture", "Clothing"}
		for _, expectedCat := range expectedCategories {
			if !categories[expectedCat] {
				t.Errorf("Expected category %s in results, but it was not found", expectedCat)
			}
		}
	})
}
