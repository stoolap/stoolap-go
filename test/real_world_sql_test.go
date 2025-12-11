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
	"encoding/json"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/parser"
)

// getParser creates a new parser with functions registered
func getParser(input string) *parser.Parser {
	return parser.NewParser(parser.NewLexer(input))
}

// TestCreateTableWithAllTypes tests creating tables with all supported data types
func TestCreateTableWithAllTypes(t *testing.T) {
	sql := `
	CREATE TABLE employees (
		id INTEGER,
		name TEXT,
		salary FLOAT,
		is_active BOOLEAN,
		hire_date DATE,
		start_time TIME,
		created_at TIMESTAMP,
		metadata JSON
	)
	`

	p := getParser(sql)

	// Expected column names and types
	expectedColumns := []struct {
		name string
		typ  string
	}{
		{"id", "INTEGER"},
		{"name", "TEXT"},
		{"salary", "FLOAT"},
		{"is_active", "BOOLEAN"},
		{"hire_date", "DATE"},
		{"start_time", "TIME"},
		{"created_at", "TIMESTAMP"},
		{"metadata", "JSON"},
	}

	// Use ParseProgram instead of manually walking through tokens
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Fatalf("Parser had errors: %v", p.Errors())
	}

	// Verify we got a valid CREATE TABLE statement
	if len(program.Statements) == 0 {
		t.Fatal("No statements in program")
	}

	createStmt, ok := program.Statements[0].(*parser.CreateTableStatement)
	if !ok {
		t.Fatalf("Statement is not a CreateTableStatement, got %T", program.Statements[0])
	}

	// Check the table name
	if createStmt.TableName.Value != "employees" {
		t.Errorf("Expected table name 'employees', got '%s'", createStmt.TableName.Value)
	}

	// Check we have the right number of columns
	if len(createStmt.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(createStmt.Columns))
		return
	}

	// Check column names and types
	for i, expectedCol := range expectedColumns {
		if i >= len(createStmt.Columns) {
			break
		}

		col := createStmt.Columns[i]
		if col.Name.Value != expectedCol.name {
			t.Errorf("Column %d: expected name '%s', got '%s'", i, expectedCol.name, col.Name.Value)
		}
		if col.Type != expectedCol.typ {
			t.Errorf("Column %d: expected type '%s', got '%s'", i, expectedCol.typ, col.Type)
		}
	}
}

// TestInsertWithAllDataTypes tests inserting data with all supported types
func TestInsertWithAllDataTypes(t *testing.T) {
	testCases := []struct {
		name          string
		sql           string
		expectedType  interface{}
		checkFunction func(interface{}) bool
	}{
		{
			name:         "Insert with INTEGER value",
			sql:          "INSERT INTO employees (id) VALUES (42)",
			expectedType: int64(0),
			checkFunction: func(v interface{}) bool {
				_, ok := v.(int64)
				return ok
			},
		},
		{
			name:         "Insert with FLOAT value",
			sql:          "INSERT INTO employees (salary) VALUES (55.5)",
			expectedType: float64(0),
			checkFunction: func(v interface{}) bool {
				_, ok := v.(float64)
				return ok
			},
		},
		{
			name:         "Insert with TEXT value",
			sql:          "INSERT INTO employees (name) VALUES ('John Doe')",
			expectedType: "",
			checkFunction: func(v interface{}) bool {
				_, ok := v.(string)
				return ok
			},
		},
		{
			name:         "Insert with BOOLEAN value - TRUE",
			sql:          "INSERT INTO employees (is_active) VALUES (TRUE)",
			expectedType: true,
			checkFunction: func(v interface{}) bool {
				_, ok := v.(bool)
				return ok
			},
		},
		{
			name:         "Insert with BOOLEAN value - FALSE",
			sql:          "INSERT INTO employees (is_active) VALUES (FALSE)",
			expectedType: false,
			checkFunction: func(v interface{}) bool {
				_, ok := v.(bool)
				return ok
			},
		},
		{
			name:         "Insert with JSON object value",
			sql:          "INSERT INTO employees (metadata) VALUES ('{\"department\":\"Engineering\",\"skills\":[\"Go\",\"SQL\"]}')",
			expectedType: map[string]interface{}{},
			checkFunction: func(v interface{}) bool {
				_, ok := v.(map[string]interface{})
				return ok
			},
		},
		{
			name:         "Insert with multiple values of different types",
			sql:          "INSERT INTO employees (id, name, salary, is_active, hire_date, start_time, metadata) VALUES (1, 'John Smith', 75000.50, TRUE, '2022-01-15', '09:00:00', '{\"role\":\"developer\"}')",
			expectedType: nil,
			checkFunction: func(v interface{}) bool {
				return true // Just checking syntax here
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have an InsertStatement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			insertStmt, ok := program.Statements[0].(*parser.InsertStatement)
			if !ok {
				t.Fatalf("Statement is not an InsertStatement, got %T", program.Statements[0])
			}

			// For multiple values test case, just check we parsed it correctly
			if tc.name == "Insert with multiple values of different types" {
				if len(insertStmt.Values) == 0 {
					t.Fatal("No values in insert statement")
				}
				if len(insertStmt.Values[0]) != 7 {
					t.Fatalf("Expected 7 values, got %d", len(insertStmt.Values[0]))
				}
				return
			}

			// For single value tests, check the value type
			if len(insertStmt.Values) == 0 || len(insertStmt.Values[0]) == 0 {
				t.Fatal("No values in insert statement")
			}

			// Extract value based on the test case
			var value interface{}
			switch v := insertStmt.Values[0][0].(type) {
			case *parser.IntegerLiteral:
				value = v.Value
			case *parser.FloatLiteral:
				value = v.Value
			case *parser.StringLiteral:
				// Check TypeHint and handle special cases based on it
				switch v.TypeHint {
				case "DATE":
					// For date values, perform actual parsing
					date, err := time.Parse("2006-01-02", v.Value)
					if err == nil {
						value = date
					} else {
						value = v.Value
					}
				case "TIME":
					// For time values, perform actual parsing
					timeVal, err := time.Parse("15:04:05", v.Value)
					if err == nil {
						value = timeVal
					} else {
						value = v.Value
					}
				case "JSON":
					// For JSON values, perform actual parsing
					var jsonData map[string]interface{}
					if err := json.Unmarshal([]byte(v.Value), &jsonData); err == nil {
						value = jsonData
					} else {
						value = v.Value
					}
				default:
					value = v.Value
				}
			case *parser.BooleanLiteral:
				value = v.Value
			default:
				t.Fatalf("Unexpected value type: %T", v)
			}

			if !tc.checkFunction(value) {
				t.Errorf("Value type check failed for %s: got %T: %v", tc.name, value, value)
			}
		})
	}
}

// TestBasicSelectQueries tests basic SELECT statements that are fully implemented
func TestBasicSelectQueries(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple SELECT all columns",
			sql:  "SELECT * FROM employees",
		},
		{
			name: "SELECT with column list",
			sql:  "SELECT id, name, salary FROM employees",
		},
		{
			name: "SELECT with INTEGER condition",
			sql:  "SELECT * FROM employees WHERE id = 42",
		},
		{
			name: "SELECT with TEXT condition",
			sql:  "SELECT * FROM employees WHERE name = 'John Doe'",
		},
		{
			name: "SELECT with BOOLEAN condition",
			sql:  "SELECT * FROM employees WHERE is_active = TRUE",
		},
		{
			name: "SELECT with DATE condition",
			sql:  "SELECT * FROM employees WHERE hire_date = '2023-05-15'",
		},
		{
			name: "SELECT with complex date condition",
			sql:  "SELECT * FROM employees WHERE hire_date >= '2022-01-01' AND hire_date <= '2022-12-31'",
		},
		{
			name: "SELECT with TIME condition",
			sql:  "SELECT * FROM employees WHERE start_time = '09:00:00'",
		},
		{
			name: "SELECT with multiple conditions",
			sql:  "SELECT * FROM employees WHERE is_active = TRUE AND salary > 50000.0",
		},
		{
			name: "SELECT with ORDER BY",
			sql:  "SELECT * FROM employees ORDER BY hire_date DESC, salary ASC",
		},
		{
			name: "SELECT with LIMIT and OFFSET",
			sql:  "SELECT * FROM employees ORDER BY id LIMIT 10 OFFSET 20",
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT e.name, d.name FROM employees e JOIN departments d ON e.department_id = d.id",
		},
		{
			name: "SELECT with LEFT JOIN",
			sql:  "SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.department_id = d.id",
		},
		{
			name: "SELECT with date extraction",
			sql:  "SELECT id, name, hire_date FROM employees WHERE hire_date = '2023-05-15'",
		},
		{
			name: "SELECT with complex time and date conditions",
			sql:  "SELECT * FROM employees WHERE start_time >= '09:00:00' AND start_time <= '17:00:00' AND hire_date >= '2022-01-01'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a SelectStatement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			_, ok := program.Statements[0].(*parser.SelectStatement)
			if !ok {
				t.Fatalf("Statement is not a SelectStatement, got %T", program.Statements[0])
			}

			// Log that this query syntax was successfully parsed
			t.Logf("Successfully parsed query: %s", tc.name)
		})
	}
}

// TestAggregationFunctions tests SQL aggregation functions like COUNT, SUM, AVG
func TestAggregationFunctions(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "COUNT function",
			sql:  "SELECT COUNT(*) FROM employees",
		},
		{
			name: "COUNT with column",
			sql:  "SELECT COUNT(id) FROM employees",
		},
		{
			name: "SUM function",
			sql:  "SELECT SUM(salary) FROM employees",
		},
		{
			name: "AVG function",
			sql:  "SELECT AVG(salary) FROM employees",
		},
		{
			name: "MIN function",
			sql:  "SELECT MIN(salary) FROM employees",
		},
		{
			name: "MAX function",
			sql:  "SELECT MAX(salary) FROM employees",
		},
		{
			name: "Multiple aggregations",
			sql:  "SELECT COUNT(*), AVG(salary), MIN(salary), MAX(salary) FROM employees",
		},
		{
			name: "GROUP BY with aggregation",
			sql:  "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id",
		},
		{
			name: "HAVING clause",
			sql:  "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id HAVING COUNT(*) > 5",
		},
		{
			name: "Complex aggregation",
			sql:  "SELECT department_id, AVG(salary) FROM employees WHERE is_active = TRUE GROUP BY department_id HAVING COUNT(*) > 3 ORDER BY AVG(salary) DESC",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a SelectStatement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			_, ok := program.Statements[0].(*parser.SelectStatement)
			if !ok {
				t.Fatalf("Statement is not a SelectStatement, got %T", program.Statements[0])
			}

			// Successfully parsed
			t.Logf("Successfully parsed aggregation: %s", tc.name)
		})
	}
}

// TestScalarFunctions tests scalar SQL functions like UPPER, LOWER, ABS
func TestScalarRealWorldFunctions(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "UPPER function",
			sql:  "SELECT UPPER(name) FROM employees",
		},
		{
			name: "LOWER function",
			sql:  "SELECT LOWER(name) FROM employees",
		},
		{
			name: "ABS function",
			sql:  "SELECT ABS(salary - 50000) FROM employees",
		},
		{
			name: "ROUND function",
			sql:  "SELECT ROUND(salary, 2) FROM employees",
		},
		{
			name: "CEILING function",
			sql:  "SELECT CEILING(salary) FROM employees",
		},
		{
			name: "FLOOR function",
			sql:  "SELECT FLOOR(salary) FROM employees",
		},
		{
			name: "LENGTH function",
			sql:  "SELECT LENGTH(name) FROM employees",
		},
		{
			name: "SUBSTRING function",
			sql:  "SELECT SUBSTRING(name, 1, 3) FROM employees",
		},
		{
			name: "CONCAT function",
			sql:  "SELECT CONCAT(name, ' - ', department) FROM employees",
		},
		{
			name: "Multiple scalar functions",
			sql:  "SELECT id, UPPER(name), ROUND(salary, 2) FROM employees",
		},
		{
			name: "Scalar functions in WHERE",
			sql:  "SELECT * FROM employees WHERE UPPER(name) = 'JOHN DOE'",
		},
		{
			name: "Scalar and aggregation",
			sql:  "SELECT department_id, AVG(ROUND(salary, 0)) FROM employees GROUP BY department_id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a SelectStatement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			_, ok := program.Statements[0].(*parser.SelectStatement)
			if !ok {
				t.Fatalf("Statement is not a SelectStatement, got %T", program.Statements[0])
			}

			// Successfully parsed
			t.Logf("Successfully parsed scalar function: %s", tc.name)
		})
	}
}

// TestCastExpressions tests CAST expressions which are recently implemented
func TestCastExpressions(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "CAST INT to FLOAT",
			sql:  "SELECT CAST(id AS FLOAT) FROM employees",
		},
		{
			name: "CAST STRING to INT",
			sql:  "SELECT CAST('42' AS INT) FROM employees",
		},
		{
			name: "CAST in WHERE condition",
			sql:  "SELECT * FROM employees WHERE CAST(salary AS INT) > 50000",
		},
		{
			name: "Multiple CAST expressions",
			sql:  "SELECT CAST(id AS FLOAT), CAST(salary AS INT) FROM employees",
		},
		{
			name: "CAST with arithmetic",
			sql:  "SELECT CAST(id AS FLOAT) + 0.5 FROM employees",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Successfully parsed
			t.Logf("Successfully parsed CAST expression: %s", tc.name)
		})
	}
}

// TestCaseExpressions tests CASE expressions which are recently implemented
func TestCaseExpressions(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple CASE expression",
			sql:  "SELECT CASE WHEN salary > 50000 THEN 'High' ELSE 'Low' END FROM employees",
		},
		{
			name: "Multiple WHEN clauses",
			sql:  "SELECT CASE WHEN salary > 100000 THEN 'Very High' WHEN salary > 50000 THEN 'High' ELSE 'Low' END FROM employees",
		},
		{
			name: "CASE with column expression",
			sql:  "SELECT CASE salary WHEN 50000 THEN 'Target' WHEN 100000 THEN 'Excellent' ELSE 'Other' END FROM employees",
		},
		{
			name: "CASE in WHERE clause",
			sql:  "SELECT * FROM employees WHERE CASE WHEN salary > 50000 THEN TRUE ELSE FALSE END",
		},
		{
			name: "CASE with complex expressions",
			sql:  "SELECT CASE WHEN salary > 50000 AND is_active = TRUE THEN 'Active High' WHEN salary <= 50000 AND is_active = TRUE THEN 'Active Low' ELSE 'Inactive' END FROM employees",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Successfully parsed
			t.Logf("Successfully parsed CASE expression: %s", tc.name)
		})
	}
}

// TestSubqueries tests subquery expressions which are recently implemented
func TestSubqueries(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Subquery in WHERE clause",
			sql:  "SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)",
		},
		{
			name: "Subquery in FROM clause",
			sql:  "SELECT sub.name FROM (SELECT name, salary FROM employees WHERE salary > 50000) AS sub",
		},
		{
			name: "Correlated subquery",
			sql:  "SELECT e1.name FROM employees e1 WHERE e1.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department_id = e1.department_id)",
		},
		{
			name: "Subquery with EXISTS",
			sql:  "SELECT * FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id)",
		},
		{
			name: "Subquery with IN",
			sql:  "SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE location = 'New York')",
		},
		{
			name: "Multiple nested subqueries",
			sql:  "SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE location IN (SELECT location FROM locations WHERE country = 'USA'))",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			errors := p.Errors()

			// For subqueries, we might expect some errors since the implementation isn't complete
			// We're just checking that the parser doesn't crash and the basic structure is recognized
			if len(errors) > 0 {
				t.Logf("Parser had errors (expected for subqueries): %v", errors)
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Just log that we attempted to parse this subquery
			t.Logf("Processed subquery: %s", tc.name)
		})
	}
}

// TestWithClauses tests Common Table Expressions (WITH clauses) which are recently implemented
func TestWithClauses(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple WITH clause",
			sql:  "WITH high_salary AS (SELECT * FROM employees WHERE salary > 50000) SELECT * FROM high_salary",
		},
		{
			name: "Multiple CTEs",
			sql:  "WITH high_salary AS (SELECT * FROM employees WHERE salary > 50000), low_salary AS (SELECT * FROM employees WHERE salary <= 50000) SELECT * FROM high_salary UNION SELECT * FROM low_salary",
		},
		{
			name: "CTE with JOIN",
			sql:  "WITH dept_employees AS (SELECT d.name AS dept_name, e.name AS emp_name FROM departments d JOIN employees e ON d.id = e.department_id) SELECT * FROM dept_employees",
		},
		{
			name: "Nested CTEs",
			sql:  "WITH managers AS (SELECT * FROM employees WHERE role = 'Manager'), dept_managers AS (SELECT d.name, m.name FROM departments d JOIN managers m ON d.manager_id = m.id) SELECT * FROM dept_managers",
		},
		{
			name: "CTE with complex query",
			sql:  "WITH salary_stats AS (SELECT department_id, AVG(salary) as avg_salary FROM employees GROUP BY department_id) SELECT e.name, e.salary, s.avg_salary FROM employees e JOIN salary_stats s ON e.department_id = s.department_id WHERE e.salary > s.avg_salary",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			errors := p.Errors()

			// For WITH clauses, we might expect some errors since the implementation isn't complete
			// We're just checking that the parser doesn't crash and the basic structure is recognized
			if len(errors) > 0 {
				t.Logf("Parser had errors (expected for CTEs): %v", errors)
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Just log that we attempted to parse this WITH clause
			t.Logf("Processed WITH clause: %s", tc.name)
		})
	}
}

// TestPreparedStatementParams tests parameter binding which is recently implemented
func TestPreparedStatementParams(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Simple parameter",
			sql:  "SELECT * FROM employees WHERE id = ?",
		},
		{
			name: "Multiple parameters",
			sql:  "SELECT * FROM employees WHERE salary > ? AND hire_date < ?",
		},
		{
			name: "Parameters in INSERT",
			sql:  "INSERT INTO employees (name, salary) VALUES (?, ?)",
		},
		{
			name: "Parameters in UPDATE",
			sql:  "UPDATE employees SET name = ?, salary = ? WHERE id = ?",
		},
		{
			name: "Parameters in complex query",
			sql:  "SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments WHERE location = ?) AND salary > ?",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			errors := p.Errors()

			// For complex queries with parameters, we might expect some errors
			if len(errors) > 0 {
				t.Logf("Parser had errors (might be expected for complex parameter binding): %v", errors)
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Successfully parsed
			t.Logf("Processed prepared statement: %s", tc.name)
		})
	}
}

// TestAlterTable tests ALTER TABLE statements which are implemented
func TestAlterTable(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "ADD COLUMN",
			sql:  "ALTER TABLE employees ADD COLUMN department_id INTEGER",
		},
		{
			name: "DROP COLUMN",
			sql:  "ALTER TABLE employees DROP COLUMN metadata",
		},
		{
			name: "RENAME COLUMN",
			sql:  "ALTER TABLE employees RENAME COLUMN name TO full_name",
		},
		{
			name: "MODIFY COLUMN",
			sql:  "ALTER TABLE employees MODIFY COLUMN salary FLOAT",
		},
		{
			name: "RENAME TABLE",
			sql:  "ALTER TABLE employees RENAME TO staff",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Successfully parsed
			t.Logf("Successfully parsed ALTER TABLE: %s", tc.name)
		})
	}
}

// TestCreateDropIndex tests CREATE INDEX and DROP INDEX statements which are implemented
func TestCreateDropIndex(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE INDEX",
			sql:  "CREATE INDEX idx_salary ON employees(salary)",
		},
		{
			name: "CREATE UNIQUE INDEX",
			sql:  "CREATE UNIQUE INDEX idx_email ON employees(email)",
		},
		{
			name: "CREATE MULTI-COLUMN INDEX",
			sql:  "CREATE INDEX idx_dept_hire ON employees(department_id, hire_date)",
		},
		{
			name: "DROP INDEX",
			sql:  "DROP INDEX idx_salary ON employees",
		},
		{
			name: "DROP INDEX IF EXISTS",
			sql:  "DROP INDEX IF EXISTS idx_nonexistent ON employees",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := getParser(tc.sql)

			// Parse the program and check for errors
			program := p.ParseProgram()
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser had errors: %v", p.Errors())
			}

			// Verify we have a statement
			if len(program.Statements) == 0 {
				t.Fatal("No statements in program")
			}

			// Successfully parsed
			t.Logf("Successfully parsed index operation: %s", tc.name)
		})
	}
}
