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
	"sort"
	"testing"

	// Import stoolap driver
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestSelectDistinct tests the SELECT DISTINCT functionality
func TestSelectDistinct(t *testing.T) {
	// Create an in-memory database for testing
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with sample data
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		category TEXT NOT NULL,
		region TEXT NOT NULL,
		price FLOAT NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert sample data with duplicates in various columns
	_, err = db.Exec(`INSERT INTO products (id, category, region, price) VALUES 
		(1, 'Electronics', 'North', 100.00),
		(2, 'Electronics', 'South', 200.00),
		(3, 'Electronics', 'North', 150.00),
		(4, 'Clothing', 'East', 50.00),
		(5, 'Clothing', 'West', 75.00),
		(6, 'Clothing', 'East', 60.00),
		(7, 'Books', 'North', 20.00),
		(8, 'Books', 'South', 25.00),
		(9, 'Books', 'North', 20.00),
		(10, 'Electronics', 'West', 180.00)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test case 1: Basic SELECT DISTINCT for a single column
	t.Run("SingleColumnDistinct", func(t *testing.T) {
		// Query distinct categories
		rows, err := db.Query("SELECT DISTINCT category FROM products")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT: %v", err)
		}
		defer rows.Close()

		// Collect results
		var categories []string
		for rows.Next() {
			var category string
			if err := rows.Scan(&category); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			categories = append(categories, category)
		}

		// Verify results (should be 3 distinct categories)
		expectedCategories := []string{"Electronics", "Clothing", "Books"}
		sort.Strings(categories)
		sort.Strings(expectedCategories)

		if len(categories) != len(expectedCategories) {
			t.Errorf("Expected %d distinct categories, got %d", len(expectedCategories), len(categories))
			t.Logf("Actual categories: %v", categories)
		} else {
			for i, category := range categories {
				if i < len(expectedCategories) && category != expectedCategories[i] {
					t.Errorf("Expected category %s at position %d, got %s", expectedCategories[i], i, category)
				}
			}
		}

		// Verify with COUNT DISTINCT for comparison
		var distinctCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT category) FROM products").Scan(&distinctCount)
		if err != nil {
			t.Fatalf("Failed to execute COUNT DISTINCT: %v", err)
		}

		if distinctCount != len(expectedCategories) {
			t.Errorf("COUNT DISTINCT returned %d, but SELECT DISTINCT returned %d categories",
				distinctCount, len(categories))
		}
	})

	// Test case 2: SELECT DISTINCT with multiple columns
	t.Run("MultiColumnDistinct", func(t *testing.T) {
		// Query distinct category-region combinations
		rows, err := db.Query("SELECT DISTINCT category, region FROM products")
		if err != nil {
			t.Fatalf("Failed to execute multi-column SELECT DISTINCT: %v", err)
		}
		defer rows.Close()

		// Collect results
		type categoryRegion struct {
			Category string
			Region   string
		}
		var combinations []categoryRegion
		for rows.Next() {
			var category, region string
			if err := rows.Scan(&category, &region); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			combinations = append(combinations, categoryRegion{Category: category, Region: region})
		}

		// Expected distinct combinations (should be 8 unique combinations)
		expectedCombinations := []categoryRegion{
			{Category: "Electronics", Region: "North"},
			{Category: "Electronics", Region: "South"},
			{Category: "Electronics", Region: "West"},
			{Category: "Clothing", Region: "East"},
			{Category: "Clothing", Region: "West"},
			{Category: "Books", Region: "North"},
			{Category: "Books", Region: "South"},
		}

		// Helper function to check if a combination exists in expected list
		containsCombination := func(combo categoryRegion, list []categoryRegion) bool {
			for _, item := range list {
				if item.Category == combo.Category && item.Region == combo.Region {
					return true
				}
			}
			return false
		}

		if len(combinations) != len(expectedCombinations) {
			t.Errorf("Expected %d distinct category-region combinations, got %d",
				len(expectedCombinations), len(combinations))
			t.Logf("Actual combinations:")
			for _, combo := range combinations {
				t.Logf("  %s - %s", combo.Category, combo.Region)
			}
			t.Logf("Expected combinations:")
			for _, combo := range expectedCombinations {
				t.Logf("  %s - %s", combo.Category, combo.Region)
			}
		}

		// Verify each combination is in the expected list
		for _, combo := range combinations {
			if !containsCombination(combo, expectedCombinations) {
				t.Errorf("Unexpected combination: %s - %s", combo.Category, combo.Region)
			}
		}
	})

	// Test case 3: SELECT DISTINCT with ORDER BY
	t.Run("DistinctWithOrderBy", func(t *testing.T) {
		// Query distinct regions ordered by name
		rows, err := db.Query("SELECT DISTINCT region FROM products ORDER BY region")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT with ORDER BY: %v", err)
		}
		defer rows.Close()

		// Collect results
		var regions []string
		for rows.Next() {
			var region string
			if err := rows.Scan(&region); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			regions = append(regions, region)
		}

		// Expected result should be ordered alphabetically
		expectedRegions := []string{"East", "North", "South", "West"}

		if len(regions) != len(expectedRegions) {
			t.Errorf("Expected %d distinct regions, got %d", len(expectedRegions), len(regions))
		}

		// Check the order
		for i, region := range regions {
			if region != expectedRegions[i] {
				t.Errorf("Expected region %s at position %d, got %s", expectedRegions[i], i, region)
			}
		}
	})

	// Test case 4: SELECT DISTINCT on numeric columns
	t.Run("DistinctOnNumeric", func(t *testing.T) {
		// Query distinct prices
		rows, err := db.Query("SELECT DISTINCT price FROM products ORDER BY price")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT on numeric column: %v", err)
		}
		defer rows.Close()

		// Collect results
		var prices []float64
		for rows.Next() {
			var price float64
			if err := rows.Scan(&price); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			prices = append(prices, price)
		}

		// Expected distinct prices (there's a duplicate 20.00)
		expectedPrices := []float64{20.00, 25.00, 50.00, 60.00, 75.00, 100.00, 150.00, 180.00, 200.00}

		if len(prices) != len(expectedPrices) {
			t.Errorf("Expected %d distinct prices, got %d", len(expectedPrices), len(prices))
			t.Logf("Actual prices: %v", prices)
		}

		// Verify with COUNT DISTINCT for comparison
		var distinctCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT price) FROM products").Scan(&distinctCount)
		if err != nil {
			t.Fatalf("Failed to execute COUNT DISTINCT: %v", err)
		}

		if distinctCount != len(expectedPrices) {
			t.Errorf("COUNT DISTINCT returned %d, but SELECT DISTINCT returned %d prices",
				distinctCount, len(prices))
		}
	})

	// Test case 5: Add a more complex scenario with real-world data
	t.Run("ComplexDistinctTest", func(t *testing.T) {
		// Create a table for candle data similar to what the user mentioned in the TODO
		_, err = db.Exec(`CREATE TABLE candle_data (
			id INTEGER PRIMARY KEY,
			symbol TEXT NOT NULL,
			ts TIMESTAMP NOT NULL,
			open FLOAT NOT NULL,
			high FLOAT NOT NULL,
			low FLOAT NOT NULL,
			close FLOAT NOT NULL,
			volume FLOAT NOT NULL
		)`)
		if err != nil {
			t.Fatalf("Failed to create candle_data table: %v", err)
		}

		// Insert sample data with repeated symbols
		symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD", "BTC-USD", "ETH-USD", "BTC-USD"}
		for i, symbol := range symbols {
			_, err = db.Exec(`INSERT INTO candle_data (id, symbol, ts, open, high, low, close, volume)
				VALUES (?, ?, '2023-01-01 12:00:00', 100, 105, 95, 102, 1000)`,
				i+1, symbol)
			if err != nil {
				t.Fatalf("Failed to insert candle data: %v", err)
			}
		}

		// Test SELECT DISTINCT on symbol column
		rows, err := db.Query("SELECT DISTINCT symbol FROM candle_data ORDER BY symbol")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT on symbols: %v", err)
		}
		defer rows.Close()

		// Collect distinct symbols
		var distinctSymbols []string
		for rows.Next() {
			var symbol string
			if err := rows.Scan(&symbol); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			distinctSymbols = append(distinctSymbols, symbol)
		}

		// Expected distinct symbols
		expectedSymbols := []string{"BTC-USD", "ETH-USD", "SOL-USD"}

		if len(distinctSymbols) != len(expectedSymbols) {
			t.Errorf("Expected %d distinct symbols, got %d: %v",
				len(expectedSymbols), len(distinctSymbols), distinctSymbols)
		}

		// Verify the order
		for i, symbol := range distinctSymbols {
			if symbol != expectedSymbols[i] {
				t.Errorf("Expected symbol %s at position %d, got %s", expectedSymbols[i], i, symbol)
			}
		}

		// Double-check with COUNT DISTINCT
		var symbolCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT symbol) FROM candle_data").Scan(&symbolCount)
		if err != nil {
			t.Fatalf("Failed to execute COUNT DISTINCT on symbols: %v", err)
		}

		if symbolCount != len(expectedSymbols) {
			t.Errorf("COUNT DISTINCT returned %d, but SELECT DISTINCT returned %d symbols",
				symbolCount, len(distinctSymbols))
		}
	})

	// Test case 6: Test DISTINCT with no duplicates
	t.Run("DistinctWithNoDuplicates", func(t *testing.T) {
		// Create a table with all unique values
		_, err = db.Exec(`CREATE TABLE unique_data (id INTEGER PRIMARY KEY)`)
		if err != nil {
			t.Fatalf("Failed to create unique_data table: %v", err)
		}

		// Insert 10 unique values
		for i := 1; i <= 10; i++ {
			_, err = db.Exec("INSERT INTO unique_data (id) VALUES (?)", i)
			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}
		}

		// Test SELECT DISTINCT on a column with all unique values
		rows, err := db.Query("SELECT DISTINCT id FROM unique_data ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT: %v", err)
		}
		defer rows.Close()

		// Collect results
		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			ids = append(ids, id)
		}

		// Expect 10 ids
		if len(ids) != 10 {
			t.Errorf("Expected 10 distinct ids, got %d", len(ids))
		}

		// Verify they are in order
		for i := 0; i < len(ids); i++ {
			if ids[i] != i+1 {
				t.Errorf("Expected id %d at position %d, got %d", i+1, i, ids[i])
			}
		}
	})

	// Test case 7: DISTINCT with a large number of rows
	t.Run("DistinctWithLargeDataset", func(t *testing.T) {
		// Create a table for this test
		_, err = db.Exec(`CREATE TABLE large_data (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL
		)`)
		if err != nil {
			t.Fatalf("Failed to create large_data table: %v", err)
		}

		// Insert 1000 rows with only 5 distinct categories
		categories := []string{"A", "B", "C", "D", "E"}

		// Use batching for efficiency
		for i := 0; i < 1000; i += 100 {
			var valueStrings []string
			var args []interface{}

			for j := 0; j < 100; j++ {
				id := i + j + 1
				category := categories[id%len(categories)]
				valueStrings = append(valueStrings, fmt.Sprintf("(%d, ?)", id))
				args = append(args, category)
			}

			query := fmt.Sprintf("INSERT INTO large_data (id, category) VALUES %s",
				joinStrings(valueStrings, ", "))
			_, err = db.Exec(query, args...)
			if err != nil {
				t.Fatalf("Failed to insert batch: %v", err)
			}
		}

		// Query distinct categories
		rows, err := db.Query("SELECT DISTINCT category FROM large_data ORDER BY category")
		if err != nil {
			t.Fatalf("Failed to execute SELECT DISTINCT on large dataset: %v", err)
		}
		defer rows.Close()

		// Collect distinct categories
		var distinctCategories []string
		for rows.Next() {
			var category string
			if err := rows.Scan(&category); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			distinctCategories = append(distinctCategories, category)
		}

		// Should only have 5 distinct categories
		if len(distinctCategories) != 5 {
			t.Errorf("Expected 5 distinct categories, got %d", len(distinctCategories))
		}

		// Verify they match our expected categories
		expectedCategories := []string{"A", "B", "C", "D", "E"}
		for i, category := range distinctCategories {
			if category != expectedCategories[i] {
				t.Errorf("Expected category %s at position %d, got %s",
					expectedCategories[i], i, category)
			}
		}

		// Verify with COUNT DISTINCT
		var categoryCount int
		err = db.QueryRow("SELECT COUNT(DISTINCT category) FROM large_data").Scan(&categoryCount)
		if err != nil {
			t.Fatalf("Failed to execute COUNT DISTINCT: %v", err)
		}

		if categoryCount != 5 {
			t.Errorf("COUNT DISTINCT returned %d, expected 5", categoryCount)
		}
	})
}

// Helper function to join strings with a separator
func joinStrings(strings []string, separator string) string {
	result := ""
	for i, s := range strings {
		if i > 0 {
			result += separator
		}
		result += s
	}
	return result
}
