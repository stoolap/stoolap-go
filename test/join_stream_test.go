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

func TestStreamingJoin(t *testing.T) {
	// Connect to the in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Set up test data with a larger dataset for measuring performance differences
	setupJoinTestTables(t, db, 1000, 100)

	// Execute a join query
	rows, err := db.Query(`
		SELECT e.id, e.name, d.name as dept_name
		FROM employees e
		INNER JOIN departments d ON e.dept_id = d.id
	`)
	if err != nil {
		t.Fatalf("Failed to execute join query: %v", err)
	}
	defer rows.Close()

	// Get the column count
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
	t.Logf("Join result has %d columns: %v", len(cols), cols)

	// Count the number of results
	count := 0
	for rows.Next() {
		// Create scan targets for all columns
		scanVals := make([]interface{}, len(cols))
		for i := range scanVals {
			var val interface{}
			scanVals[i] = &val
		}

		if err := rows.Scan(scanVals...); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	// Since we're now using the regular join implementation and not streaming join,
	// let's just make sure we have results. The regular join implementation is
	// tested in other tests.
	if count < 1000 {
		t.Fatalf("Expected at least 1000 rows, got %d", count)
	}
	t.Logf("Successfully executed join query with %d result rows", count)
}

// setupJoinTestTables creates tables and data for join tests
func setupJoinTestTables(t testing.TB, db *sql.DB, numEmployees, numDepartments int) {
	// Drop tables if they exist
	db.Exec("DROP TABLE IF EXISTS employees")
	db.Exec("DROP TABLE IF EXISTS departments")

	// Create departments table
	_, err := db.Exec(`CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create departments table: %v", err)
	}

	// Create employees table
	_, err = db.Exec(`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	// Start a transaction for bulk inserts
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Prepare statement for departments
	deptStmt, err := tx.Prepare("INSERT INTO departments (id, name) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare department insert: %v", err)
	}
	defer deptStmt.Close()

	// Insert departments
	for i := 1; i <= numDepartments; i++ {
		_, err = deptStmt.Exec(i, fmt.Sprintf("Department-%d", i))
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert department %d: %v", i, err)
		}
	}

	// Prepare statement for employees
	empStmt, err := tx.Prepare("INSERT INTO employees (id, name, dept_id) VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to prepare employee insert: %v", err)
	}
	defer empStmt.Close()

	// Insert employees
	for i := 1; i <= numEmployees; i++ {
		deptID := (i % numDepartments) + 1
		_, err = empStmt.Exec(i, fmt.Sprintf("Employee-%d", i), deptID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert employee %d: %v", i, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Logf("Created test database with %d employees across %d departments", numEmployees, numDepartments)
}
