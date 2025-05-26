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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stoolap/stoolap/pkg"
)

func TestDriverBasicOperations(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test table creation
	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test insert
	result, err := db.Exec(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Test query
	rows, err := db.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columns := rows.Columns()
	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}

	var id int
	var name string
	var age int

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	err = rows.Scan(&id, &name, &age)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	if id != 1 || name != "Alice" || age != 30 {
		t.Errorf("Unexpected values: id=%d, name=%s, age=%d", id, name, age)
	}
}

func TestDriverQueryRow(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE test (
		id INTEGER PRIMARY KEY,
		value TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(ctx, "INSERT INTO test (id, value) VALUES (1, 'test1'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test QueryRow
	var value string
	row := db.QueryRow(ctx, "SELECT value FROM test WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	err = row.Scan(&value)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	if value != "test1" {
		t.Errorf("Expected 'test1', got '%s'", value)
	}

	// Test QueryRow with no results
	row = db.QueryRow(ctx, "SELECT value FROM test WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 999})
	err = row.Scan(&value)
	if err != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, got %v", err)
	}
}

func TestDriverTransactions(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE accounts (
		id INT PRIMARY KEY,
		balance INT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 50)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test successful transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	if err != nil {
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 2})
	if err != nil {
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify balances
	var balance int
	row := db.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	err = row.Scan(&balance)
	if err != nil {
		t.Fatalf("Failed to query balance: %v", err)
	}
	if balance != 90 {
		t.Errorf("Expected balance 90, got %d", balance)
	}

	// Test rollback
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = 0 WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	if err != nil {
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify balance is unchanged
	row = db.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	err = row.Scan(&balance)
	if err != nil {
		t.Fatalf("Failed to query balance: %v", err)
	}
	if balance != 90 {
		t.Errorf("Expected balance 90 after rollback, got %d", balance)
	}
}

func TestDriverPreparedStatements(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price FLOAT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare insert statement
	stmt, err := db.Prepare("INSERT INTO products (id, name, price) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute prepared statement multiple times
	testData := []struct {
		id    int
		name  string
		price float64
	}{
		{1, "Product A", 10.99},
		{2, "Product B", 20.50},
		{3, "Product C", 15.75},
	}

	for _, data := range testData {
		_, err = stmt.ExecContext(ctx,
			driver.NamedValue{Ordinal: 1, Value: data.id},
			driver.NamedValue{Ordinal: 2, Value: data.name},
			driver.NamedValue{Ordinal: 3, Value: data.price},
		)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
	}

	// Prepare query statement
	queryStmt, err := db.Prepare("SELECT name, price FROM products WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare query statement: %v", err)
	}
	defer queryStmt.Close()

	// Execute prepared query
	rows, err := queryStmt.QueryContext(ctx, driver.NamedValue{Ordinal: 1, Value: 2})
	if err != nil {
		t.Fatalf("Failed to execute prepared query: %v", err)
	}
	defer rows.Close()

	var name string
	var price float64
	if !rows.Next() {
		t.Fatal("Expected a row")
	}

	err = rows.Scan(&name, &price)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if name != "Product B" || price != 20.50 {
		t.Errorf("Unexpected values: name=%s, price=%f", name, price)
	}
}

func TestDriverConcurrency(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE counter (
		id INT PRIMARY KEY,
		value INT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO counter (id, value) VALUES (1, 0)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, "SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Run concurrent updates
	var wg sync.WaitGroup
	numGoroutines := 10
	incrementsPerGoroutine := 100
	var successCount, conflictCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				// Use transaction with snapshot isolation
				tx, err := db.BeginTx(ctx, &sql.TxOptions{
					Isolation: sql.LevelSnapshot,
				})
				if err != nil {
					continue
				}

				// Use UPDATE with increment directly (read-modify-write in one statement)
				_, err = tx.ExecContext(ctx, "UPDATE counter SET value = value + 1 WHERE id = 1")
				if err != nil {
					tx.Rollback()
					continue
				}

				// Try to commit
				err = tx.Commit()
				if err != nil {
					if err.Error() == "transaction aborted due to write-write conflict" {
						atomic.AddInt32(&conflictCount, 1)
					}
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	// Check final value
	var finalValue int
	row := db.QueryRow(ctx, "SELECT value FROM counter WHERE id = 1")
	err = row.Scan(&finalValue)
	if err != nil {
		t.Fatalf("Failed to query final value: %v", err)
	}

	t.Logf("Final value: %d", finalValue)
	t.Logf("Successful commits: %d", successCount)
	t.Logf("Conflicts detected: %d", conflictCount)
	t.Logf("Total attempts: %d", numGoroutines*incrementsPerGoroutine)

	// With proper SNAPSHOT isolation and write-write conflict detection:
	// The final value should equal the number of successful commits
	if int32(finalValue) != successCount {
		t.Errorf("Inconsistency detected: final value %d != successful commits %d", finalValue, successCount)
	}

	// All attempts should be accounted for
	if successCount+conflictCount != int32(numGoroutines*incrementsPerGoroutine) {
		t.Errorf("Lost transactions: %d success + %d conflicts != %d total attempts",
			successCount, conflictCount, numGoroutines*incrementsPerGoroutine)
	}
}

func TestDriverEngineRegistry(t *testing.T) {
	// Test that the same DSN returns the same DB instance
	db1, err := pkg.Open("memory://test1")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db2, err := pkg.Open("memory://test1")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if db1 != db2 {
		t.Error("Expected same DB instance for same DSN")
	}

	// Test that different DSNs return different instances
	db3, err := pkg.Open("memory://test2")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if db1 == db3 {
		t.Error("Expected different DB instances for different DSNs")
	}

	// Clean up
	db1.Close()
	db3.Close()

	// After closing, opening again should create a new instance
	db4, err := pkg.Open("memory://test1")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db4.Close()

	if db1 == db4 {
		t.Error("Expected new DB instance after closing")
	}
}

func TestDriverExecutorReuse(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get executor reference
	executor1 := db.Executor()
	executor2 := db.Executor()

	// Verify same executor instance is returned
	if executor1 != executor2 {
		t.Error("Expected same executor instance")
	}

	// Create table and run queries to ensure executor works
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple times to test query caching
	for i := 0; i < 5; i++ {
		_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO test VALUES (%d)", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Verify executor is still the same
	executor3 := db.Executor()
	if executor1 != executor3 {
		t.Error("Expected same executor instance after operations")
	}
}

func TestDriverParameterBinding(t *testing.T) {
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		created_at TIMESTAMP
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test various parameter types
	now := time.Now()
	params := []driver.NamedValue{
		{Ordinal: 1, Value: 1},
		{Ordinal: 2, Value: "John Doe"},
		{Ordinal: 3, Value: "john@example.com"},
		{Ordinal: 4, Value: now},
	}

	_, err = db.ExecContext(ctx, "INSERT INTO users VALUES (?, ?, ?, ?)", params...)
	if err != nil {
		t.Fatalf("Failed to insert with parameters: %v", err)
	}

	// Query back with parameters
	rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE id = ?", driver.NamedValue{Ordinal: 1, Value: 1})
	if err != nil {
		t.Fatalf("Failed to query with parameters: %v", err)
	}
	defer rows.Close()

	var id int
	var name, email string
	var createdAt time.Time

	if !rows.Next() {
		t.Fatal("Expected a row")
	}

	err = rows.Scan(&id, &name, &email, &createdAt)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if id != 1 || name != "John Doe" || email != "john@example.com" {
		t.Errorf("Unexpected values: id=%d, name=%s, email=%s", id, name, email)
	}
}

func TestDriverErrorHandling(t *testing.T) {
	// Test invalid DSN
	_, err := pkg.Open("invalid://")
	if err == nil {
		t.Error("Expected error for invalid DSN scheme")
	}

	_, err = pkg.Open("invalid-format")
	if err == nil {
		t.Error("Expected error for invalid DSN format")
	}

	// Test SQL errors
	db, err := pkg.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test invalid SQL
	_, err = db.Exec(ctx, "INVALID SQL")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}

	// Test query on non-existent table
	_, err = db.Query(ctx, "SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}
