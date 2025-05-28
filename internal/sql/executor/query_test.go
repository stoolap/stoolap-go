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

package executor

import (
	"context"
	"testing"
)

func TestExecuteSelect(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)",
		"INSERT INTO users VALUES (1, 'Alice', 30, 'New York')",
		"INSERT INTO users VALUES (2, 'Bob', 25, 'London')",
		"INSERT INTO users VALUES (3, 'Charlie', 35, 'New York')",
		"INSERT INTO users VALUES (4, 'Diana', 28, 'Paris')",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "SelectAll",
			query:    "SELECT * FROM users",
			expected: 4,
		},
		{
			name:     "SelectWithWhere",
			query:    "SELECT * FROM users WHERE age > 28",
			expected: 2,
		},
		{
			name:     "SelectWithMultipleConditions",
			query:    "SELECT * FROM users WHERE age > 25 AND city = 'New York'",
			expected: 2,
		},
		{
			name:     "SelectWithOR",
			query:    "SELECT * FROM users WHERE city = 'London' OR city = 'Paris'",
			expected: 2,
		},
		{
			name:     "SelectSpecificColumns",
			query:    "SELECT name, age FROM users WHERE id = 1",
			expected: 1,
		},
		{
			name:     "SelectWithLike",
			query:    "SELECT * FROM users WHERE name LIKE 'C%'",
			expected: 1,
		},
		{
			name:     "SelectWithIN",
			query:    "SELECT * FROM users WHERE id IN (1, 3)",
			expected: 2,
		},
		{
			name:     "SelectWithBetween",
			query:    "SELECT * FROM users WHERE age BETWEEN 25 AND 30",
			expected: 3,
		},
		{
			name:     "SelectWithOrderBy",
			query:    "SELECT * FROM users ORDER BY age DESC",
			expected: 4,
		},
		{
			name:     "SelectWithLimit",
			query:    "SELECT * FROM users ORDER BY age LIMIT 2",
			expected: 2,
		},
		{
			name:     "SelectWithOffset",
			query:    "SELECT * FROM users ORDER BY id LIMIT 2 OFFSET 1",
			expected: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, nil, test.query)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()

			count := 0
			for result.Next() {
				count++
			}

			if count != test.expected {
				t.Errorf("Expected %d rows, got %d", test.expected, count)
			}
		})
	}
}

func TestSelectWithNulls(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data with NULLs
	setupSQL := []string{
		"CREATE TABLE nullable_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
		"INSERT INTO nullable_table VALUES (1, 'Alice', 100)",
		"INSERT INTO nullable_table VALUES (2, 'Bob', NULL)",
		"INSERT INTO nullable_table VALUES (3, NULL, 200)",
		"INSERT INTO nullable_table VALUES (4, NULL, NULL)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "SelectWhereNull",
			query:    "SELECT * FROM nullable_table WHERE value IS NULL",
			expected: 2,
		},
		{
			name:     "SelectWhereNotNull",
			query:    "SELECT * FROM nullable_table WHERE value IS NOT NULL",
			expected: 2,
		},
		{
			name:     "SelectWhereNameNull",
			query:    "SELECT * FROM nullable_table WHERE name IS NULL",
			expected: 2,
		},
		{
			name:     "SelectWhereNameAndValueNotNull",
			query:    "SELECT * FROM nullable_table WHERE name IS NOT NULL AND value IS NOT NULL",
			expected: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, nil, test.query)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()

			count := 0
			for result.Next() {
				count++
			}

			if count != test.expected {
				t.Errorf("Expected %d rows, got %d", test.expected, count)
			}
		})
	}
}

func TestSelectWithExpressions(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE numbers (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
		"INSERT INTO numbers VALUES (1, 10, 5)",
		"INSERT INTO numbers VALUES (2, 20, 8)",
		"INSERT INTO numbers VALUES (3, 15, 15)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test arithmetic expressions
	t.Run("ArithmeticExpressions", func(t *testing.T) {
		query := "SELECT id, a + b AS sum, a - b AS diff, a * b AS product, a / b AS quotient FROM numbers WHERE id = 1"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		// First check how many columns we actually get
		cols := result.Columns()
		if len(cols) != 5 {
			t.Fatalf("Expected 5 columns, got %d: %v", len(cols), cols)
		}

		if result.Next() {
			var id, sum, diff, product int
			var quotient float64 // Division might return float
			err := result.Scan(&id, &sum, &diff, &product, &quotient)
			if err != nil {
				t.Fatal(err)
			}

			if id != 1 || sum != 15 || diff != 5 || product != 50 || int(quotient) != 2 {
				t.Errorf("Arithmetic expressions failed: id=%d, sum=%d, diff=%d, product=%d, quotient=%f",
					id, sum, diff, product, quotient)
			}
		} else {
			t.Error("Expected one row")
		}
	})

	// Test comparison in WHERE with expressions
	t.Run("WhereWithExpressions", func(t *testing.T) {
		query := "SELECT * FROM numbers WHERE a + b > 20"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}

		if count != 2 { // id=2 (20+8=28) and id=3 (15+15=30)
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})
}

func TestSelectDistinct(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data with duplicates
	setupSQL := []string{
		"CREATE TABLE cities (id INTEGER PRIMARY KEY, name TEXT, country TEXT)",
		"INSERT INTO cities VALUES (1, 'New York', 'USA')",
		"INSERT INTO cities VALUES (2, 'London', 'UK')",
		"INSERT INTO cities VALUES (3, 'New York', 'USA')",
		"INSERT INTO cities VALUES (4, 'Paris', 'France')",
		"INSERT INTO cities VALUES (5, 'London', 'UK')",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("DistinctSingleColumn", func(t *testing.T) {
		query := "SELECT DISTINCT country FROM cities"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		countries := make(map[string]bool)
		for result.Next() {
			var country string
			if err := result.Scan(&country); err != nil {
				t.Fatal(err)
			}
			countries[country] = true
		}

		if len(countries) != 3 {
			t.Errorf("Expected 3 distinct countries, got %d", len(countries))
		}
	})

	t.Run("DistinctMultipleColumns", func(t *testing.T) {
		query := "SELECT DISTINCT name, country FROM cities"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}

		if count != 3 { // (New York, USA), (London, UK), (Paris, France)
			t.Errorf("Expected 3 distinct city-country pairs, got %d", count)
		}
	})
}

func TestSelectGroupBy(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, category TEXT, amount FLOAT, quantity INTEGER)",
		"INSERT INTO sales VALUES (1, 'Laptop', 'Electronics', 1000.00, 2)",
		"INSERT INTO sales VALUES (2, 'Mouse', 'Electronics', 25.00, 5)",
		"INSERT INTO sales VALUES (3, 'Desk', 'Furniture', 300.00, 1)",
		"INSERT INTO sales VALUES (4, 'Chair', 'Furniture', 150.00, 4)",
		"INSERT INTO sales VALUES (5, 'Monitor', 'Electronics', 400.00, 3)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("GroupByWithCount", func(t *testing.T) {
		query := "SELECT category, COUNT(*) as count FROM sales GROUP BY category"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		categories := make(map[string]int)
		for result.Next() {
			var category string
			var count int
			if err := result.Scan(&category, &count); err != nil {
				t.Fatal(err)
			}
			categories[category] = count
		}

		if categories["Electronics"] != 3 || categories["Furniture"] != 2 {
			t.Errorf("Unexpected counts: Electronics=%d, Furniture=%d",
				categories["Electronics"], categories["Furniture"])
		}
	})

	t.Run("GroupByWithSum", func(t *testing.T) {
		query := "SELECT category, SUM(amount) as total FROM sales GROUP BY category"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		totals := make(map[string]float64)
		for result.Next() {
			var category string
			var total float64
			if err := result.Scan(&category, &total); err != nil {
				t.Fatal(err)
			}
			totals[category] = total
		}

		expectedElectronics := 1425.00
		expectedFurniture := 450.00

		if totals["Electronics"] != expectedElectronics {
			t.Errorf("Expected Electronics total %.2f, got %.2f", expectedElectronics, totals["Electronics"])
		}
		if totals["Furniture"] != expectedFurniture {
			t.Errorf("Expected Furniture total %.2f, got %.2f", expectedFurniture, totals["Furniture"])
		}
	})

	t.Run("GroupByWithHaving", func(t *testing.T) {
		query := "SELECT category, SUM(amount) as total FROM sales GROUP BY category HAVING total > 500"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		for result.Next() {
			var category string
			var total float64
			if err := result.Scan(&category, &total); err != nil {
				t.Fatal(err)
			}
			if category != "Electronics" {
				t.Errorf("Expected only Electronics category, got %s", category)
			}
			count++
		}

		if count != 1 {
			t.Errorf("Expected 1 category with total > 500, got %d", count)
		}
	})
}

func TestSelectSubqueries(t *testing.T) {
	t.Skip("Subqueries are not yet supported in the current version of Stoolap")
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department_id INTEGER, salary FLOAT)",
		"CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO departments VALUES (1, 'Engineering')",
		"INSERT INTO departments VALUES (2, 'Sales')",
		"INSERT INTO employees VALUES (1, 'Alice', 1, 80000)",
		"INSERT INTO employees VALUES (2, 'Bob', 1, 75000)",
		"INSERT INTO employees VALUES (3, 'Charlie', 2, 65000)",
		"INSERT INTO employees VALUES (4, 'Diana', 2, 70000)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("SubqueryInWhere", func(t *testing.T) {
		query := `SELECT name FROM employees 
		          WHERE salary > (SELECT AVG(salary) FROM employees)`
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}

		if count != 2 { // Alice and Bob have salaries above average
			t.Errorf("Expected 2 employees above average salary, got %d", count)
		}
	})

	t.Run("SubqueryInFrom", func(t *testing.T) {
		query := `SELECT dept_avg.name, dept_avg.avg_salary 
		          FROM (
		              SELECT d.name, AVG(e.salary) as avg_salary 
		              FROM departments d 
		              JOIN employees e ON d.id = e.department_id 
		              GROUP BY d.name
		          ) AS dept_avg 
		          WHERE dept_avg.avg_salary > 70000`
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		found := false
		for result.Next() {
			var name string
			var avgSalary float64
			if err := result.Scan(&name, &avgSalary); err != nil {
				t.Fatal(err)
			}
			if name == "Engineering" {
				found = true
				if avgSalary != 77500 {
					t.Errorf("Expected Engineering avg salary 77500, got %.2f", avgSalary)
				}
			}
		}

		if !found {
			t.Error("Expected to find Engineering department")
		}
	})
}

func TestCaseSensitivity(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create table with mixed case
	setupSQL := []string{
		"CREATE TABLE TestTable (ID INTEGER PRIMARY KEY, Name TEXT)",
		"INSERT INTO TestTable VALUES (1, 'Alice')",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	// Test case-insensitive table and column names
	queries := []string{
		"SELECT * FROM testtable",
		"SELECT * FROM TESTTABLE",
		"SELECT id, name FROM TestTable",
		"SELECT ID, NAME FROM testtable",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			result, err := executor.Execute(ctx, nil, query)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()

			if !result.Next() {
				t.Error("Expected one row")
			}
		})
	}
}

func TestSelectWithCast(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE mixed_types (id INTEGER PRIMARY KEY, str_value TEXT, int_value INTEGER, float_value FLOAT)",
		"INSERT INTO mixed_types VALUES (1, '123', 456, 78.9)",
		"INSERT INTO mixed_types VALUES (2, '45.67', 89, 12.34)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("CastStringToInt", func(t *testing.T) {
		query := "SELECT CAST(str_value AS INTEGER) as int_val FROM mixed_types WHERE id = 1"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var intVal int
			if err := result.Scan(&intVal); err != nil {
				t.Fatal(err)
			}
			if intVal != 123 {
				t.Errorf("Expected 123, got %d", intVal)
			}
		}
	})

	t.Run("CastIntToString", func(t *testing.T) {
		query := "SELECT CAST(int_value AS TEXT) as str_val FROM mixed_types WHERE id = 1"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var strVal string
			if err := result.Scan(&strVal); err != nil {
				t.Fatal(err)
			}
			if strVal != "456" {
				t.Errorf("Expected '456', got '%s'", strVal)
			}
		}
	})
}

func TestSelectColumnAliases(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)",
		"INSERT INTO users VALUES (1, 'John', 'Doe')",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("ColumnAlias", func(t *testing.T) {
		query := "SELECT first_name AS fname, last_name AS lname FROM users"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		columns := result.Columns()
		if len(columns) != 2 || columns[0] != "fname" || columns[1] != "lname" {
			t.Errorf("Expected columns [fname, lname], got %v", columns)
		}
	})

	t.Run("ExpressionAlias", func(t *testing.T) {
		query := "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var fullName string
			if err := result.Scan(&fullName); err != nil {
				t.Fatal(err)
			}
			if fullName != "John Doe" {
				t.Errorf("Expected 'John Doe', got '%s'", fullName)
			}
		}
	})
}

func TestSelectEmptyResults(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Create empty table
	result, err := executor.Execute(ctx, nil, "CREATE TABLE empty_table (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	result.Close()

	t.Run("SelectFromEmptyTable", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, "SELECT * FROM empty_table")
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			t.Error("Expected no rows from empty table")
		}
	})

	t.Run("SelectWithImpossibleCondition", func(t *testing.T) {
		// First insert some data
		result, err := executor.Execute(ctx, nil, "INSERT INTO empty_table VALUES (1, 'test')")
		if err != nil {
			t.Fatal(err)
		}
		result.Close()

		result, err = executor.Execute(ctx, nil, "SELECT * FROM empty_table WHERE 1 = 0")
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			t.Error("Expected no rows with impossible condition")
		}
	})

	t.Run("AggregateOnEmptyResult", func(t *testing.T) {
		result, err := executor.Execute(ctx, nil, "SELECT COUNT(*), SUM(id), AVG(id) FROM empty_table WHERE id > 100")
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var count int
			var sum, avg interface{}
			if err := result.Scan(&count, &sum, &avg); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Errorf("Expected COUNT=0, got %d", count)
			}
			if sum != nil {
				t.Error("Expected NULL SUM")
			}
			if avg != nil {
				t.Error("Expected NULL AVG")
			}
		} else {
			t.Error("Expected one row with aggregate results")
		}
	})
}

func TestSelectWithFunctions(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE string_data (id INTEGER PRIMARY KEY, text_value TEXT, num_value INTEGER)",
		"INSERT INTO string_data VALUES (1, 'Hello World', 42)",
		"INSERT INTO string_data VALUES (2, 'UPPER CASE', -10)",
		"INSERT INTO string_data VALUES (3, 'lower case', 0)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("StringFunctions", func(t *testing.T) {
		query := "SELECT UPPER(text_value), LOWER(text_value), LENGTH(text_value) FROM string_data WHERE id = 1"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var upper, lower string
			var length int
			if err := result.Scan(&upper, &lower, &length); err != nil {
				t.Fatal(err)
			}
			if upper != "HELLO WORLD" || lower != "hello world" || length != 11 {
				t.Errorf("String functions failed: upper=%s, lower=%s, length=%d", upper, lower, length)
			}
		}
	})

	t.Run("MathFunctions", func(t *testing.T) {
		query := "SELECT ABS(num_value), ROUND((-10/3.0), 2) FROM string_data WHERE id = 2"
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var abs int
			var round float64
			if err := result.Scan(&abs, &round); err != nil {
				t.Fatal(err)
			}
			if abs != 10 {
				t.Errorf("Expected ABS(-10) = 10, got %d", abs)
			}
			expectedRound := -3.33
			if round < expectedRound-0.01 || round > expectedRound+0.01 {
				t.Errorf("Expected ROUND(-10/3.0, 2) ≈ %.2f, got %.2f", expectedRound, round)
			}
		}
	})

	t.Run("CoalesceFunction", func(t *testing.T) {
		// Insert NULL values
		insertSQL := "INSERT INTO string_data VALUES (4, NULL, 100)"
		result, err := executor.Execute(ctx, nil, insertSQL)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()

		query := "SELECT COALESCE(text_value, 'default') FROM string_data WHERE id = 4"
		result, err = executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		if result.Next() {
			var value string
			if err := result.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != "default" {
				t.Errorf("Expected COALESCE to return 'default', got '%s'", value)
			}
		}
	})
}

func TestSelectJoins(t *testing.T) {
	engine, executor := setupTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Setup test data
	setupSQL := []string{
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total FLOAT)",
		"CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, country TEXT)",
		"INSERT INTO customers VALUES (1, 'Alice', 'USA')",
		"INSERT INTO customers VALUES (2, 'Bob', 'UK')",
		"INSERT INTO customers VALUES (3, 'Charlie', 'Canada')",
		"INSERT INTO orders VALUES (1, 1, 100.00)",
		"INSERT INTO orders VALUES (2, 1, 150.00)",
		"INSERT INTO orders VALUES (3, 2, 200.00)",
	}

	for _, sql := range setupSQL {
		result, err := executor.Execute(ctx, nil, sql)
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
	}

	t.Run("InnerJoin", func(t *testing.T) {
		query := `SELECT c.name, o.total 
		          FROM customers c 
		          INNER JOIN orders o ON c.id = o.customer_id 
		          ORDER BY o.id`
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}

		if count != 3 {
			t.Errorf("Expected 3 rows from inner join, got %d", count)
		}
	})

	t.Run("LeftJoin", func(t *testing.T) {
		query := `SELECT c.name, o.total 
		          FROM customers c 
		          LEFT JOIN orders o ON c.id = o.customer_id 
		          ORDER BY c.id`
		result, err := executor.Execute(ctx, nil, query)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()

		count := 0
		hasNull := false
		for result.Next() {
			var name string
			var total interface{}
			if err := result.Scan(&name, &total); err != nil {
				t.Fatal(err)
			}
			if name == "Charlie" && total == nil {
				hasNull = true
			}
			count++
		}

		if count != 4 { // 3 orders + 1 customer without orders
			t.Errorf("Expected 4 rows from left join, got %d", count)
		}
		if !hasNull {
			t.Error("Expected NULL total for Charlie")
		}
	})
}
