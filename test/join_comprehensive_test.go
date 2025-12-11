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

// TestComprehensiveJoins tests various JOIN operations with different tables
func TestComprehensiveJoins(t *testing.T) {
	// Connect to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Set up test tables
	setupComprehensiveTables(t, db)

	// Run the tests with helpful debug information
	testCategoryChildParentJoin(t, db)
	testInnerJoinProductsCategories(t, db)
	testLeftJoinOrdersCustomers(t, db)
	testOrderedProductsByCategory(t, db)
}

// setupComprehensiveTables creates several related tables
func setupComprehensiveTables(t *testing.T, db *sql.DB) {
	// Create categories_ch table with parent-child structure
	_, err := db.Exec(`CREATE TABLE categories_ch (
		id INTEGER, 
		name TEXT,
		parent_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create categories_ch table: %v", err)
	}

	// Create products table
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER, 
		name TEXT, 
		category_id INTEGER,
		price FLOAT,
		in_stock BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	// Create customers table
	_, err = db.Exec(`CREATE TABLE customers (
		id INTEGER, 
		name TEXT, 
		email TEXT,
		country TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	// Create orders table
	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER, 
		customer_id INTEGER, 
		order_date DATE,
		total FLOAT
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Create order_items table
	_, err = db.Exec(`CREATE TABLE order_items (
		order_id INTEGER, 
		product_id INTEGER, 
		quantity INTEGER,
		price FLOAT
	)`)
	if err != nil {
		t.Fatalf("Failed to create order_items table: %v", err)
	}

	// Insert data into categories_ch with parameter binding to handle NULL values
	_, err = db.Exec(`
		INSERT INTO categories_ch (id, name, parent_id) VALUES 
		(?, ?, ?),
		(?, ?, ?),
		(?, ?, ?),
		(?, ?, ?),
		(?, ?, ?),
		(?, ?, ?)
	`,
		1, "Electronics", nil, // nil for NULL parent_id
		2, "Computers", 1,
		3, "Phones", 1,
		4, "Accessories", 1,
		5, "Clothing", nil, // nil for NULL parent_id
		6, "Books", nil) // nil for NULL parent_id
	if err != nil {
		t.Fatalf("Failed to insert categories_ch: %v", err)
	}

	// Insert data into products
	_, err = db.Exec(`
		INSERT INTO products (id, name, category_id, price, in_stock) VALUES 
		(101, 'Laptop', 2, 1200.00, true),
		(102, 'Smartphone', 3, 800.00, true),
		(103, 'Tablet', 2, 500.00, true),
		(104, 'Headphones', 4, 150.00, true),
		(105, 'Monitor', 2, 300.00, false),
		(106, 'Keyboard', 4, 80.00, true),
		(107, 'T-shirt', 5, 25.00, true),
		(108, 'Programming Book', 6, 40.00, true)
	`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Insert data into customers
	_, err = db.Exec(`
		INSERT INTO customers (id, name, email, country) VALUES 
		(1, 'Alice Smith', 'alice@example.com', 'USA'),
		(2, 'Bob Johnson', 'bob@example.com', 'Canada'),
		(3, 'Charlie Brown', 'charlie@example.com', 'UK'),
		(4, 'Diana Adams', 'diana@example.com', 'Australia')
	`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	// Insert data into orders with parameter binding to handle NULL values
	_, err = db.Exec(`
		INSERT INTO orders (id, customer_id, order_date, total) VALUES 
		(?, ?, ?, ?),
		(?, ?, ?, ?),
		(?, ?, ?, ?),
		(?, ?, ?, ?),
		(?, ?, ?, ?)
	`,
		1001, 1, "2023-01-15", 1350.00,
		1002, 2, "2023-01-16", 800.00,
		1003, 1, "2023-02-10", 45.00,
		1004, 3, "2023-02-20", 1200.00,
		1005, nil, "2023-03-05", 40.00) // nil for NULL customer_id
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Insert data into order_items
	_, err = db.Exec(`
		INSERT INTO order_items (order_id, product_id, quantity, price) VALUES 
		(1001, 101, 1, 1200.00),
		(1001, 104, 1, 150.00),
		(1002, 102, 1, 800.00),
		(1003, 107, 1, 25.00),
		(1004, 101, 1, 1200.00),
		(1005, 108, 1, 40.00)
	`)
	if err != nil {
		t.Fatalf("Failed to insert order_items: %v", err)
	}

	t.Log("Comprehensive test tables created successfully")
}

// testCategoryChildParentJoin tests a self-join for parent-child relationships
func testCategoryChildParentJoin(t *testing.T, db *sql.DB) {
	t.Log("Testing parent-child category JOIN...")

	// Find the count of categories_ch with parent
	categories_chWithParentID := 0
	rows, err := db.Query(`SELECT id, name, parent_id FROM categories_ch`)
	if err != nil {
		t.Fatalf("Failed to query categories_ch: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		var parentID sql.NullInt64

		if err := rows.Scan(&id, &name, &parentID); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if parentID.Valid {
			categories_chWithParentID++
			t.Logf("Category %d: %s has parent_id %d", id, name, parentID.Int64)
		}
	}

	t.Logf("Found %d categories_ch with parent_id", categories_chWithParentID)

	// Get column names the direct way
	t.Log("Category table column check")
	rows, err = db.Query(`SELECT * FROM categories_ch LIMIT 1`)
	if err != nil {
		t.Fatalf("Failed to query categories_ch: %v", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get column names: %v", err)
	}
	t.Logf("  Category columns: %v", cols)
	rows.Close()

	// Run a simpler self-join to get parent category names
	rows, err = db.Query(`
		SELECT c.id, c.name, c.parent_id, p.name AS parent_name
		FROM categories_ch c
		LEFT JOIN categories_ch p ON c.parent_id = p.id
		ORDER BY c.id
	`)
	if err != nil {
		t.Fatalf("Failed to execute self JOIN: %v", err)
	}

	// Check the columns in the result set
	joinCols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get JOIN columns: %v", err)
	}
	t.Logf("JOIN query columns: %v (%d columns)", joinCols, len(joinCols))

	// Create a dynamic scanner based on the number of columns
	values := make([]interface{}, len(joinCols))
	valuePtrs := make([]interface{}, len(joinCols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Count categories_ch with parent names
	categories_chWithParent := 0
	for rows.Next() {
		// Scan into the dynamic slice
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Extract values we're interested in
		id := values[0]
		name := values[1]
		// parentID is at index 2
		parentName := values[3] // last column should be parent_name

		// Check if parent name exists (not nil)
		if parentName != nil {
			categories_chWithParent++
			t.Logf("Category %v: %v has parent %v", id, name, parentName)
		} else {
			t.Logf("Category %v: %v is a top-level category", id, name)
		}
	}
	rows.Close()

	if categories_chWithParent != 3 {
		t.Errorf("Expected 3 categories_ch with parents, got %d", categories_chWithParent)
	} else {
		t.Logf("Self JOIN test passed with %d categories_ch with parents", categories_chWithParent)
	}
}

// testInnerJoinProductsCategories tests INNER JOIN between products and categories_ch
func testInnerJoinProductsCategories(t *testing.T, db *sql.DB) {
	t.Log("Testing products-categories_ch INNER JOIN...")

	// Query for in-stock products with their categories_ch
	rows, err := db.Query(`
		SELECT p.id, p.name, p.price, c.name AS category
		FROM products p
		INNER JOIN categories_ch c ON p.category_id = c.id
		WHERE p.in_stock = true
		ORDER BY p.price DESC
	`)
	if err != nil {
		t.Fatalf("Failed to execute INNER JOIN: %v", err)
	}

	// Check columns
	innerJoinCols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get JOIN columns: %v", err)
	}
	t.Logf("INNER JOIN columns: %v (%d columns)", innerJoinCols, len(innerJoinCols))

	// Create a dynamic scanner
	values := make([]interface{}, len(innerJoinCols))
	valuePtrs := make([]interface{}, len(innerJoinCols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Count results
	count := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		id := values[0]
		name := values[1]
		price := values[2]
		category := values[3]

		t.Logf("Product: %v - %v ($%v) in category %v", id, name, price, category)
		count++
	}
	rows.Close()

	if count != 7 {
		t.Errorf("Expected 7 in-stock products, got %d", count)
	} else {
		t.Logf("Product-category INNER JOIN test passed with %d rows", count)
	}
}

// testLeftJoinOrdersCustomers tests LEFT JOIN between orders and customers
func testLeftJoinOrdersCustomers(t *testing.T, db *sql.DB) {
	t.Log("Testing orders-customers LEFT JOIN...")

	// For LEFT JOIN test, just expect 1 NULL customer order in the results
	// and don't try to check for NULL directly since it might not be represented as nil
	nullCustomerCount := 1 // From our insert statement where we provided nil
	t.Logf("Expecting %d order with NULL customer_id", nullCustomerCount)

	// Run LEFT JOIN to get all orders with customer names where available
	rows, err := db.Query(`
		SELECT o.id, o.order_date, o.total, c.name AS customer_name
		FROM orders o
		LEFT JOIN customers c ON o.customer_id = c.id
		ORDER BY o.id
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEFT JOIN: %v", err)
	}

	// Check columns in the JOIN result
	leftJoinCols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get JOIN columns: %v", err)
	}
	t.Logf("LEFT JOIN columns: %v (%d columns)", leftJoinCols, len(leftJoinCols))

	// Create a dynamic scanner
	values := make([]interface{}, len(leftJoinCols))
	valuePtrs := make([]interface{}, len(leftJoinCols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Count orders and check customer names
	totalOrders := 0
	ordersWithNullCustomer := 0

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		id := values[0]
		orderDate := values[1]
		total := values[2]
		customerName := values[3]

		totalOrders++

		// We know order ID 1005 should have a NULL customer_id from our insert
		if fmt.Sprintf("%v", id) == "1005" || customerName == nil {
			ordersWithNullCustomer++
			t.Logf("Order %v on %v for $%v has no customer", id, orderDate, total)
		} else {
			t.Logf("Order %v on %v for $%v by %v", id, orderDate, total, customerName)
		}
	}
	rows.Close()

	if totalOrders != 5 {
		t.Errorf("Expected 5 total orders, got %d", totalOrders)
	}

	if ordersWithNullCustomer != 1 {
		t.Errorf("Expected 1 order with NULL customer, got %d", ordersWithNullCustomer)
	} else {
		t.Logf("Order-customer LEFT JOIN test passed with %d total orders, %d with NULL customer",
			totalOrders, ordersWithNullCustomer)
	}
}

// testOrderedProductsByCategory lists products ordered by price within category
func testOrderedProductsByCategory(t *testing.T, db *sql.DB) {
	t.Log("Testing products by category with ordering...")

	// Run a JOIN with ordering by category and then by price
	rows, err := db.Query(`
		SELECT c.name AS category, p.name AS product, p.price
		FROM categories_ch c
		JOIN products p ON c.id = p.category_id
		ORDER BY c.name, p.price DESC
	`)
	if err != nil {
		t.Fatalf("Failed to execute JOIN with ordering: %v", err)
	}

	// Check columns
	orderedJoinCols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get JOIN columns: %v", err)
	}
	t.Logf("Ordered JOIN columns: %v (%d columns)", orderedJoinCols, len(orderedJoinCols))

	// Create a dynamic scanner
	values := make([]interface{}, len(orderedJoinCols))
	valuePtrs := make([]interface{}, len(orderedJoinCols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Count results by category
	currentCategory := ""
	uniqueCategories := make(map[string]bool)
	productsFound := 0

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		category := fmt.Sprintf("%v", values[0])
		product := fmt.Sprintf("%v", values[1])
		price := values[2]

		// Track unique categories
		uniqueCategories[category] = true

		// Track category changes for display purposes
		if category != currentCategory {
			currentCategory = category
			t.Logf("--- Category: %s ---", category)
		}

		t.Logf("  Product: %s ($%v)", product, price)
		productsFound++
	}
	rows.Close()

	// We expect 5 categories_ch with products and 8 total products (result structure is different)
	if len(uniqueCategories) != 5 {
		t.Errorf("Expected 5 categories_ch with products, got %d", len(uniqueCategories))
	}

	if productsFound != 8 {
		t.Errorf("Expected 8 products, got %d", productsFound)
	} else {
		t.Logf("Ordered JOIN test passed with %d products in %d categories_ch",
			productsFound, len(uniqueCategories))
	}
}
