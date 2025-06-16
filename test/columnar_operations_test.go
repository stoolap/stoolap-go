package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stoolap/stoolap"
)

func TestColumnarOperations(t *testing.T) {
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table and data
	_, err = db.Exec(context.Background(), `
		CREATE TABLE products (
			id INT PRIMARY KEY,
			name TEXT,
			price FLOAT,
			category TEXT,
			in_stock BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 1000; i++ {
		category := "Category" + string(rune('A'+(i%5)))
		inStock := "false"
		if i%2 == 0 {
			inStock = "true"
		}
		query := fmt.Sprintf(
			"INSERT INTO products (id, name, price, category, in_stock) VALUES (%d, 'Product%d', %f, '%s', %s)",
			i, i, float64(i)*10.5, category, inStock)
		_, err = db.Exec(context.Background(), query)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	t.Run("CTE with columnar filtering", func(t *testing.T) {
		// Test that columnar filtering works
		rows, err := db.Query(context.Background(), `
			WITH expensive_products AS (
				SELECT id, name, price, category 
				FROM products 
				WHERE price > 5000
			)
			SELECT COUNT(*) as cnt FROM expensive_products
		`)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected one row")
		}

		var count int
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		// Products with price > 5000 are those with id > 476 (since price = id * 10.5)
		// So we expect 1000 - 476 = 524 products
		expectedCount := 524
		if count != expectedCount {
			t.Errorf("Expected %d products, got %d", expectedCount, count)
		}
	})

	t.Run("CTE with equality filter", func(t *testing.T) {
		rows, err := db.Query(context.Background(), `
			WITH category_products AS (
				SELECT id, name, price, category 
				FROM products 
				WHERE category = 'CategoryA'
			)
			SELECT COUNT(*) as cnt FROM category_products
		`)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected one row")
		}

		var count int
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		// CategoryA is for ids where id % 5 == 1 (1, 6, 11, 16, ..., 996)
		// That's 200 products
		expectedCount := 200
		if count != expectedCount {
			t.Errorf("Expected %d products in CategoryA, got %d", expectedCount, count)
		}
	})

	t.Run("CTE columnar MIN/MAX", func(t *testing.T) {
		rows, err := db.Query(context.Background(), `
			WITH all_products AS (
				SELECT id, name, price 
				FROM products
			)
			SELECT 
				(SELECT MIN(price) FROM all_products) as min_price,
				(SELECT MAX(price) FROM all_products) as max_price
		`)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected one row")
		}

		var minPrice, maxPrice float64
		if err := rows.Scan(&minPrice, &maxPrice); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		expectedMin := 10.5    // id=1, price=1*10.5
		expectedMax := 10500.0 // id=1000, price=1000*10.5

		if minPrice != expectedMin {
			t.Errorf("Expected min price %f, got %f", expectedMin, minPrice)
		}
		if maxPrice != expectedMax {
			t.Errorf("Expected max price %f, got %f", expectedMax, maxPrice)
		}
	})

	t.Run("CTE reused multiple times", func(t *testing.T) {
		// Test that Reset() works correctly
		rows, err := db.Query(context.Background(), `
			WITH cheap_products AS (
				SELECT id, name, price 
				FROM products 
				WHERE price < 1000
			)
			SELECT 
				(SELECT COUNT(*) FROM cheap_products) as total,
				(SELECT COUNT(*) FROM cheap_products WHERE price < 500) as very_cheap
		`)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("Expected one row")
		}

		var total, veryCheap int
		if err := rows.Scan(&total, &veryCheap); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		// price < 1000 means id < 96 (95 products)
		// price < 500 means id < 48 (47 products)
		if total != 95 {
			t.Errorf("Expected 95 total cheap products, got %d", total)
		}
		if veryCheap != 47 {
			t.Errorf("Expected 47 very cheap products, got %d", veryCheap)
		}
	})
}
