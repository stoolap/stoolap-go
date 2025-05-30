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
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func TestIntegrationSQL(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("BasicAggregations", func(t *testing.T) {
		testBasicAggregations(t, db)
	})

	t.Run("GroupByOperations", func(t *testing.T) {
		testGroupByOperations(t, db)
	})

	t.Run("ComplexExpressions", func(t *testing.T) {
		testComplexExpressions(t, db)
	})

	t.Run("StringFunctions", func(t *testing.T) {
		testStringFunctions(t, db)
	})

	t.Run("DateTimeFunctions", func(t *testing.T) {
		testDateTimeFunctions(t, db)
	})

	t.Run("CastOperations", func(t *testing.T) {
		testCastOperations(t, db)
	})

	t.Run("NullHandling", func(t *testing.T) {
		testNullHandling(t, db)
	})

	t.Run("TransactionSupport", func(t *testing.T) {
		testTransactionSupport(t, db)
	})

	t.Run("SimpleJoins", func(t *testing.T) {
		testSimpleJoins(t, db)
	})
}

func testBasicAggregations(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE sales_data (
		id INT PRIMARY KEY,
		product TEXT,
		category TEXT,
		amount FLOAT,
		quantity INT,
		sale_date TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO sales_data VALUES
		(1, 'Laptop', 'Electronics', 1200.50, 2, '2024-01-15 10:00:00'),
		(2, 'Mouse', 'Electronics', 25.99, 5, '2024-01-15 11:00:00'),
		(3, 'Desk', 'Furniture', 350.00, 1, '2024-01-16 09:00:00'),
		(4, 'Chair', 'Furniture', 150.00, 4, '2024-01-16 10:00:00'),
		(5, 'Keyboard', 'Electronics', 79.99, 3, '2024-01-17 14:00:00')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test COUNT
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sales_data").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute COUNT: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected COUNT(*) = 5, got %d", count)
	}

	// Test SUM
	var total float64
	err = db.QueryRow("SELECT SUM(amount) FROM sales_data").Scan(&total)
	if err != nil {
		t.Fatalf("Failed to execute SUM: %v", err)
	}
	if total != 1806.48 {
		t.Errorf("Expected SUM(amount) = 1806.48, got %f", total)
	}

	// Test AVG
	var avg float64
	err = db.QueryRow("SELECT AVG(amount) FROM sales_data").Scan(&avg)
	if err != nil {
		t.Fatalf("Failed to execute AVG: %v", err)
	}
	if avg != 361.296 {
		t.Errorf("Expected AVG(amount) = 361.296, got %f", avg)
	}

	// Test MIN/MAX
	var min, max float64
	err = db.QueryRow("SELECT MIN(amount), MAX(amount) FROM sales_data").Scan(&min, &max)
	if err != nil {
		t.Fatalf("Failed to execute MIN/MAX: %v", err)
	}
	if min != 25.99 || max != 1200.50 {
		t.Errorf("Expected MIN=25.99, MAX=1200.50, got MIN=%f, MAX=%f", min, max)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE sales_data")
}

func testGroupByOperations(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE product_sales (
		id INT PRIMARY KEY,
		category TEXT,
		product TEXT,
		amount FLOAT,
		region TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO product_sales VALUES
		(1, 'Electronics', 'Laptop', 1200, 'North'),
		(2, 'Electronics', 'Phone', 800, 'North'),
		(3, 'Electronics', 'Tablet', 600, 'South'),
		(4, 'Furniture', 'Desk', 400, 'North'),
		(5, 'Furniture', 'Chair', 200, 'South'),
		(6, 'Furniture', 'Shelf', 150, 'South')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test GROUP BY with aggregation
	rows, err := db.Query(`
		SELECT category, COUNT(*) as cnt, SUM(amount) as total
		FROM product_sales
		GROUP BY category
		ORDER BY category`)
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY query: %v", err)
	}
	defer rows.Close()

	type result struct {
		category string
		count    int
		total    float64
	}
	var results []result

	for rows.Next() {
		var r result
		err := rows.Scan(&r.category, &r.count, &r.total)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}

	// Verify Electronics group
	if results[0].category != "Electronics" || results[0].count != 3 || results[0].total != 2600 {
		t.Errorf("Unexpected Electronics group: %+v", results[0])
	}

	// Verify Furniture group
	if results[1].category != "Furniture" || results[1].count != 3 || results[1].total != 750 {
		t.Errorf("Unexpected Furniture group: %+v", results[1])
	}

	// Test HAVING clause
	rows, err = db.Query(`
		SELECT category, SUM(amount) as total
		FROM product_sales
		GROUP BY category
		HAVING SUM(amount) > 1000`)
	if err != nil {
		t.Fatalf("Failed to execute HAVING query: %v", err)
	}
	defer rows.Close()

	var category string
	var total float64
	if rows.Next() {
		err = rows.Scan(&category, &total)
		if err != nil {
			t.Fatalf("Failed to scan HAVING result: %v", err)
		}
		if category != "Electronics" || total != 2600 {
			t.Errorf("Expected Electronics with total 2600, got %s with %f", category, total)
		}
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE product_sales")
}

func testComplexExpressions(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE expr_test (
		id INT PRIMARY KEY,
		a INT,
		b INT,
		c FLOAT,
		d TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO expr_test VALUES
		(1, 10, 5, 2.5, 'hello'),
		(2, 20, 8, 3.5, 'world'),
		(3, 15, 3, 4.5, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test arithmetic expressions with alias
	var result sql.NullFloat64
	err = db.QueryRow("SELECT (a + b) AS result FROM expr_test WHERE id = 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute arithmetic expression: %v", err)
	}
	if !result.Valid || result.Float64 != 15 { // (10 + 5)
		t.Errorf("Expected 15, got %v", result)
	}

	// Test complex WHERE with AND/OR
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM expr_test WHERE (a > 10 AND b < 10) OR c > 4.0").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute complex WHERE: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE expr_test")
}

func testStringFunctions(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE string_test (
		id INT PRIMARY KEY,
		text1 TEXT,
		text2 TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO string_test VALUES
		(1, 'Hello', 'World'),
		(2, 'STOOLAP', 'Database'),
		(3, 'Test', 'String')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test UPPER/LOWER
	var upper, lower string
	err = db.QueryRow("SELECT UPPER(text1), LOWER(text2) FROM string_test WHERE id = 1").Scan(&upper, &lower)
	if err != nil {
		t.Fatalf("Failed to execute UPPER/LOWER: %v", err)
	}
	if upper != "HELLO" || lower != "world" {
		t.Errorf("Expected HELLO/world, got %s/%s", upper, lower)
	}

	// Test LENGTH
	var length int
	err = db.QueryRow("SELECT LENGTH(text1) FROM string_test WHERE id = 2").Scan(&length)
	if err != nil {
		t.Fatalf("Failed to execute LENGTH: %v", err)
	}
	if length != 7 {
		t.Errorf("Expected length 7, got %d", length)
	}

	// Test CONCAT
	var concat string
	err = db.QueryRow("SELECT CONCAT(text1, ' ', text2) FROM string_test WHERE id = 1").Scan(&concat)
	if err != nil {
		t.Fatalf("Failed to execute CONCAT: %v", err)
	}
	if concat != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", concat)
	}

	// Test SUBSTRING
	var substr string
	err = db.QueryRow("SELECT SUBSTRING(text1, 1, 3) FROM string_test WHERE id = 2").Scan(&substr)
	if err != nil {
		t.Fatalf("Failed to execute SUBSTRING: %v", err)
	}
	if substr != "STO" {
		t.Errorf("Expected 'STO', got '%s'", substr)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE string_test")
}

func testDateTimeFunctions(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE datetime_test (
		id INT PRIMARY KEY,
		event_time TIMESTAMP,
		event_date DATE
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO datetime_test VALUES
		(1, '2024-01-15 14:30:45', '2024-01-15'),
		(2, '2024-02-20 09:15:30', '2024-02-20'),
		(3, '2024-03-25 18:45:00', '2024-03-25')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test NOW() function
	var now string
	err = db.QueryRow("SELECT NOW()").Scan(&now)
	if err != nil {
		t.Fatalf("Failed to execute NOW(): %v", err)
	}
	if now == "" {
		t.Error("NOW() returned empty string")
	}

	// Test DATE_TRUNC
	var truncated string
	err = db.QueryRow("SELECT DATE_TRUNC('day', event_time) FROM datetime_test WHERE id = 1").Scan(&truncated)
	if err != nil {
		t.Fatalf("Failed to execute DATE_TRUNC: %v", err)
	}
	// Just verify we got a result
	if truncated == "" {
		t.Error("DATE_TRUNC returned empty string")
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE datetime_test")
}

func testCastOperations(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE cast_test (
		id INT PRIMARY KEY,
		int_val INT,
		float_val FLOAT,
		text_val TEXT,
		bool_val BOOLEAN
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO cast_test VALUES
		(1, 42, 3.14, '123', true),
		(2, 100, 2.718, '456.78', false),
		(3, 0, 0.0, 'abc', true)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test INT to FLOAT cast
	var floatResult float64
	err = db.QueryRow("SELECT CAST(int_val AS FLOAT) FROM cast_test WHERE id = 1").Scan(&floatResult)
	if err != nil {
		t.Fatalf("Failed to execute INT to FLOAT cast: %v", err)
	}
	if floatResult != 42.0 {
		t.Errorf("Expected 42.0, got %f", floatResult)
	}

	// Test TEXT to INT cast
	var intResult int
	err = db.QueryRow("SELECT CAST(text_val AS INTEGER) FROM cast_test WHERE id = 1").Scan(&intResult)
	if err != nil {
		t.Fatalf("Failed to execute TEXT to INT cast: %v", err)
	}
	if intResult != 123 {
		t.Errorf("Expected 123, got %d", intResult)
	}

	// Test FLOAT to TEXT cast
	var textResult string
	err = db.QueryRow("SELECT CAST(float_val AS TEXT) FROM cast_test WHERE id = 1").Scan(&textResult)
	if err != nil {
		t.Fatalf("Failed to execute FLOAT to TEXT cast: %v", err)
	}
	if textResult != "3.14" && textResult != "3.140000" {
		t.Errorf("Expected '3.14' or '3.140000', got '%s'", textResult)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE cast_test")
}

func testNullHandling(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE null_test (
		id INT PRIMARY KEY,
		value INT,
		text_val TEXT,
		float_val FLOAT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with NULLs
	_, err = db.Exec(`INSERT INTO null_test VALUES
		(1, 100, 'test', 3.14),
		(2, NULL, NULL, NULL),
		(3, 200, 'value', NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test IS NULL
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM null_test WHERE value IS NULL").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute IS NULL: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 NULL value, got %d", count)
	}

	// Test IS NOT NULL
	err = db.QueryRow("SELECT COUNT(*) FROM null_test WHERE text_val IS NOT NULL").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute IS NOT NULL: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 non-NULL text values, got %d", count)
	}

	// Test COALESCE
	var result int
	err = db.QueryRow("SELECT COALESCE(value, 999) FROM null_test WHERE id = 2").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute COALESCE: %v", err)
	}
	if result != 999 {
		t.Errorf("Expected 999 from COALESCE, got %d", result)
	}

	// Test aggregates with NULL
	var avg sql.NullFloat64
	err = db.QueryRow("SELECT AVG(float_val) FROM null_test").Scan(&avg)
	if err != nil {
		t.Fatalf("Failed to execute AVG with NULL: %v", err)
	}
	if !avg.Valid || avg.Float64 != 3.14 {
		t.Errorf("Expected AVG=3.14, got %v", avg)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE null_test")
}

func testTransactionSupport(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`CREATE TABLE tx_test (
		id INT PRIMARY KEY,
		value INT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test successful transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO tx_test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO tx_test VALUES (2, 200)")
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data was committed
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count after commit: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows after commit, got %d", count)
	}

	// Test rollback
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO tx_test VALUES (3, 300)")
	if err != nil {
		t.Fatalf("Failed to insert in second transaction: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify rollback worked
	err = db.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count after rollback: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows after rollback, got %d", count)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE tx_test")
}

func testSimpleJoins(t *testing.T, db *sql.DB) {
	// Create tables
	_, err := db.Exec(`CREATE TABLE departments (
		id INT PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create departments table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE employees (
		id INT PRIMARY KEY,
		name TEXT,
		dept_id INT
	)`)
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO departments VALUES
		(1, 'Engineering'),
		(2, 'Sales'),
		(3, 'Marketing')`)
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}

	_, err = db.Exec(`INSERT INTO employees VALUES
		(1, 'Alice', 1),
		(2, 'Bob', 1),
		(3, 'Charlie', 2),
		(4, 'David', NULL)`)
	if err != nil {
		t.Fatalf("Failed to insert employees: %v", err)
	}

	// Test INNER JOIN
	rows, err := db.Query(`
		SELECT e.name, d.name
		FROM employees e
		INNER JOIN departments d ON e.dept_id = d.id
		ORDER BY e.name`)
	if err != nil {
		t.Fatalf("Failed to execute INNER JOIN: %v", err)
	}
	defer rows.Close()

	innerJoinCount := 0
	for rows.Next() {
		var empName, deptName string
		err := rows.Scan(&empName, &deptName)
		if err != nil {
			t.Fatalf("Failed to scan INNER JOIN result: %v", err)
		}
		innerJoinCount++
	}

	if innerJoinCount != 3 {
		t.Errorf("Expected 3 INNER JOIN results, got %d", innerJoinCount)
	}

	// Test LEFT JOIN
	rows, err = db.Query(`
		SELECT e.name, d.name
		FROM employees e
		LEFT JOIN departments d ON e.dept_id = d.id
		ORDER BY e.name`)
	if err != nil {
		t.Fatalf("Failed to execute LEFT JOIN: %v", err)
	}
	defer rows.Close()

	leftJoinCount := 0
	nullDeptCount := 0
	for rows.Next() {
		var empName string
		var deptName sql.NullString
		err := rows.Scan(&empName, &deptName)
		if err != nil {
			t.Fatalf("Failed to scan LEFT JOIN result: %v", err)
		}
		leftJoinCount++
		if !deptName.Valid {
			nullDeptCount++
		}
	}

	if leftJoinCount != 4 {
		t.Errorf("Expected 4 LEFT JOIN results, got %d", leftJoinCount)
	}
	if nullDeptCount != 1 {
		t.Errorf("Expected 1 NULL department, got %d", nullDeptCount)
	}

	// Clean up
	_, _ = db.Exec("DROP TABLE employees")
	_, _ = db.Exec("DROP TABLE departments")
}
