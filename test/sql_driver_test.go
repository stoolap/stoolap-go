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
	"fmt"
	"sync"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

func TestSQLDriverBasicOperations(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test table creation
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test insert
	result, err := db.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
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
	rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}
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

func TestSQLDriverQueryRow(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (id, value) VALUES (1, 'test value')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var value string
	err = db.QueryRow("SELECT value FROM test WHERE id = ?", 1).Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query row: %v", err)
	}

	if value != "test value" {
		t.Errorf("Expected 'test value', got '%s'", value)
	}

	// Test no rows
	err = db.QueryRow("SELECT value FROM test WHERE id = ?", 999).Scan(&value)
	if err != sql.ErrNoRows {
		t.Errorf("Expected sql.ErrNoRows, got %v", err)
	}
}

func TestSQLDriverPreparedStatements(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE numbers (n INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare insert statement
	stmt, err := db.Prepare("INSERT INTO numbers (n) VALUES (?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times
	for i := 1; i <= 5; i++ {
		_, err = stmt.Exec(i)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM numbers").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

func TestSQLDriverTransactions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE accounts (
		id INTEGER PRIMARY KEY,
		balance INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec("INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 50)")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test successful transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE accounts SET balance = balance - 10 WHERE id = 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE accounts SET balance = balance + 10 WHERE id = 2")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify results
	var balance1, balance2 int
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance1)
	if err != nil {
		t.Fatalf("Failed to query balance: %v", err)
	}
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = 2").Scan(&balance2)
	if err != nil {
		t.Fatalf("Failed to query balance: %v", err)
	}

	if balance1 != 90 || balance2 != 60 {
		t.Errorf("Unexpected balances: account1=%d, account2=%d", balance1, balance2)
	}

	// Test rollback
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE accounts SET balance = 0 WHERE id = 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	// Rollback instead of commit
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify rollback worked
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance1)
	if err != nil {
		t.Fatalf("Failed to query balance: %v", err)
	}
	if balance1 != 90 {
		t.Errorf("Expected balance to remain 90 after rollback, got %d", balance1)
	}
}

func TestSQLDriverConcurrency(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial value
	_, err = db.Exec("INSERT INTO counter (id, value) VALUES (1, 0)")
	if err != nil {
		t.Fatalf("Failed to insert initial value: %v", err)
	}

	// Run concurrent updates with SNAPSHOT isolation
	const numGoroutines = 10
	const incrementsPerGoroutine = 100
	var wg sync.WaitGroup
	var successCount, conflictCount int
	var countMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				// Use a transaction with SNAPSHOT isolation
				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
					Isolation: sql.LevelSnapshot,
				})
				if err != nil {
					fmt.Printf("Begin error: %v\n", err)
					continue
				}

				_, err = tx.Exec("UPDATE counter SET value = value + 1 WHERE id = 1")
				if err != nil {
					tx.Rollback()
					fmt.Printf("Update error: %v\n", err)
					continue
				}

				err = tx.Commit()
				countMu.Lock()
				if err != nil {
					if err.Error() == "transaction aborted due to write-write conflict" {
						conflictCount++
					} else {
						fmt.Printf("Commit error: %v\n", err)
					}
				} else {
					successCount++
				}
				countMu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Check final value
	var finalValue int
	err = db.QueryRow("SELECT value FROM counter WHERE id = 1").Scan(&finalValue)
	if err != nil {
		t.Fatalf("Failed to get final value: %v", err)
	}

	fmt.Printf("Final value: %d\n", finalValue)
	fmt.Printf("Successful commits: %d\n", successCount)
	fmt.Printf("Conflicts detected: %d\n", conflictCount)
	fmt.Printf("Total attempts: %d\n", numGoroutines*incrementsPerGoroutine)

	// With proper SNAPSHOT isolation and write-write conflict detection:
	// - The final value should equal the number of successful commits
	// - Conflicts + successes should equal total attempts
	if finalValue != successCount {
		t.Errorf("Inconsistency detected: final value %d != successful commits %d", finalValue, successCount)
	}

	if successCount+conflictCount != numGoroutines*incrementsPerGoroutine {
		t.Errorf("Lost transactions: %d successes + %d conflicts != %d total attempts",
			successCount, conflictCount, numGoroutines*incrementsPerGoroutine)
	}
}
