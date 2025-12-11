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

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestVerySimpleJoins tests basic JOIN functionality with minimal setup
func TestVerySimpleJoins(t *testing.T) {
	// Connect to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Set up minimal test tables
	setupMinimalTables(t, db)

	// Test INNER JOIN
	t.Run("INNER JOIN Basic Test", func(t *testing.T) {
		testSimpleInnerJoin(t, db)
	})

	// Test LEFT JOIN
	t.Run("LEFT JOIN Basic Test", func(t *testing.T) {
		testSimpleLeftJoin(t, db)
	})
}

// setupMinimalTables creates simple test tables
func setupMinimalTables(t *testing.T, db *sql.DB) {
	// Create departments_simple table (with ID, name)
	_, err := db.Exec(`CREATE TABLE departments_simple (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create departments_simple table: %v", err)
	}

	// Create employees_simple table (with ID, name, department ID reference)
	_, err = db.Exec(`CREATE TABLE employees_simple (id INTEGER, name TEXT, dept_id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create employees_simple table: %v", err)
	}

	// Insert some departments_simple
	_, err = db.Exec(`
		INSERT INTO departments_simple (id, name) VALUES 
		(1, 'Engineering'),
		(2, 'Sales'),
		(3, 'Marketing')
	`)
	if err != nil {
		t.Fatalf("Failed to insert departments_simple: %v", err)
	}

	// Insert some employees_simple (with references to departments_simple)
	_, err = db.Exec(`
		INSERT INTO employees_simple (id, name, dept_id) VALUES 
		(101, 'John', 1),
		(102, 'Jane', 1),
		(103, 'Bob', 2),
		(104, 'Alice', NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert employees_simple: %v", err)
	}

	t.Log("Test tables created successfully")
}

// testSimpleInnerJoin performs a basic INNER JOIN test
func testSimpleInnerJoin(t *testing.T, db *sql.DB) {
	// Execute a simple INNER JOIN
	rows, err := db.Query(`
		SELECT e.id, e.name, d.name
		FROM employees_simple e
		INNER JOIN departments_simple d ON e.dept_id = d.id
	`)
	if err != nil {
		t.Fatalf("Failed to execute INNER JOIN: %v", err)
	}
	defer rows.Close()

	// Verify we get results and all columns
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("INNER JOIN returned columns: %v", cols)

	// Access all columns returned by the JOIN
	expectedResults := []struct {
		empID    int
		empName  string
		deptName string
	}{
		{101, "John", "Engineering"},
		{102, "Jane", "Engineering"},
		{103, "Bob", "Sales"},
	}

	count := 0
	for rows.Next() {
		// Need to handle all columns returned by the join
		scanValues := make([]interface{}, len(cols))
		for i := range scanValues {
			var val interface{}
			scanValues[i] = &val
		}

		if err := rows.Scan(scanValues...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		empID := 0
		if val, ok := (*(scanValues[0].(*interface{}))).(int64); ok {
			empID = int(val)
		}

		empName := ""
		if val, ok := (*(scanValues[1].(*interface{}))).(string); ok {
			empName = val
		}

		deptName := ""
		if val, ok := (*(scanValues[len(cols)-1].(*interface{}))).(string); ok {
			deptName = val
		}

		t.Logf("INNER JOIN row: Employee ID=%d, Name=%s, Department=%s",
			empID, empName, deptName)

		// Compare to expected results
		if count < len(expectedResults) {
			expected := expectedResults[count]
			if empID != expected.empID || empName != expected.empName || deptName != expected.deptName {
				t.Errorf("Row %d: expected (%d, %s, %s), got (%d, %s, %s)",
					count, expected.empID, expected.empName, expected.deptName,
					empID, empName, deptName)
			}
		}

		count++
	}

	// Verify count matches expected
	if count != len(expectedResults) {
		t.Errorf("Expected %d rows from INNER JOIN, got %d", len(expectedResults), count)
	} else {
		t.Logf("INNER JOIN test passed: Got %d rows as expected", count)
	}
}

// testSimpleLeftJoin performs a basic LEFT JOIN test
func testSimpleLeftJoin(t *testing.T, db *sql.DB) {
	// Execute a simple LEFT JOIN
	rows, err := db.Query(`
		SELECT e.id, e.name, d.name
		FROM employees_simple e
		LEFT JOIN departments_simple d ON e.dept_id = d.id
	`)
	if err != nil {
		t.Fatalf("Failed to execute LEFT JOIN: %v", err)
	}
	defer rows.Close()

	// Verify we get results and columns
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("LEFT JOIN returned columns: %v", cols)

	// Expected results, including employee with NULL department
	expectedCount := 4 // Including the one with no department

	count := 0
	for rows.Next() {
		// Handle all columns returned by the join
		scanValues := make([]interface{}, len(cols))
		for i := range scanValues {
			var val interface{}
			scanValues[i] = &val
		}

		if err := rows.Scan(scanValues...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		empID := 0
		if val, ok := (*(scanValues[0].(*interface{}))).(int64); ok {
			empID = int(val)
		}

		empName := ""
		if val, ok := (*(scanValues[1].(*interface{}))).(string); ok {
			empName = val
		}

		deptName := "NULL"
		deptVal := *(scanValues[len(cols)-1].(*interface{}))
		if deptVal != nil {
			if val, ok := deptVal.(string); ok {
				deptName = val
			}
		}

		t.Logf("LEFT JOIN row: Employee ID=%d, Name=%s, Department=%s",
			empID, empName, deptName)

		count++
	}

	// Verify count matches expected
	if count != expectedCount {
		t.Errorf("Expected %d rows from LEFT JOIN, got %d", expectedCount, count)
	} else {
		t.Logf("LEFT JOIN test passed: Got %d rows as expected", count)
	}
}
