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
	"fmt"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go"
)

// TestAsOfTransaction tests AS OF TRANSACTION queries
func TestAsOfTransaction(t *testing.T) {
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a test table
	_, err = db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Track transaction IDs for temporal queries
	var txnID1, txnID2, txnID3 int64

	// Transaction 1: Insert initial data
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}
	txnID1 = tx1.ID()

	_, err = tx1.ExecContext(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("Failed to insert in tx1: %v", err)
	}
	_, err = tx1.ExecContext(ctx, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatalf("Failed to insert in tx1: %v", err)
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit tx1: %v", err)
	}

	// Transaction 2: Update Alice's age
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx2: %v", err)
	}
	txnID2 = tx2.ID()

	_, err = tx2.ExecContext(ctx, "UPDATE users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update in tx2: %v", err)
	}
	err = tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit tx2: %v", err)
	}

	// Transaction 3: Delete Bob
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx3: %v", err)
	}
	txnID3 = tx3.ID()

	_, err = tx3.ExecContext(ctx, "DELETE FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete in tx3: %v", err)
	}
	err = tx3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit tx3: %v", err)
	}

	// Test AS OF TRANSACTION queries
	tests := []struct {
		name         string
		query        string
		expectedRows []struct {
			id   int
			name string
			age  int
		}
	}{
		{
			name:  "AS OF before any transactions",
			query: fmt.Sprintf("SELECT id, name, age FROM users AS OF TRANSACTION %d ORDER BY id", txnID1-1),
			expectedRows: []struct {
				id   int
				name string
				age  int
			}{},
		},
		{
			name:  "AS OF after tx1 (initial inserts)",
			query: fmt.Sprintf("SELECT id, name, age FROM users AS OF TRANSACTION %d ORDER BY id", txnID1),
			expectedRows: []struct {
				id   int
				name string
				age  int
			}{
				{1, "Alice", 30},
				{2, "Bob", 25},
			},
		},
		{
			name:  "AS OF after tx2 (Alice updated)",
			query: fmt.Sprintf("SELECT id, name, age FROM users AS OF TRANSACTION %d ORDER BY id", txnID2),
			expectedRows: []struct {
				id   int
				name string
				age  int
			}{
				{1, "Alice", 31},
				{2, "Bob", 25},
			},
		},
		{
			name:  "AS OF after tx3 (Bob deleted)",
			query: fmt.Sprintf("SELECT id, name, age FROM users AS OF TRANSACTION %d ORDER BY id", txnID3),
			expectedRows: []struct {
				id   int
				name string
				age  int
			}{
				{1, "Alice", 31},
			},
		},
		{
			name:  "AS OF with WHERE clause",
			query: fmt.Sprintf("SELECT id, name, age FROM users AS OF TRANSACTION %d WHERE id = 1", txnID1),
			expectedRows: []struct {
				id   int
				name string
				age  int
			}{
				{1, "Alice", 30},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			var results []struct {
				id   int
				name string
				age  int
			}

			for rows.Next() {
				var id, age int
				var name string
				err := rows.Scan(&id, &name, &age)
				if err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}
				results = append(results, struct {
					id   int
					name string
					age  int
				}{id, name, age})
			}

			if len(results) != len(tt.expectedRows) {
				t.Errorf("Expected %d rows, got %d", len(tt.expectedRows), len(results))
				// Debug: print what we got
				for i, r := range results {
					t.Logf("  Row %d: %+v", i, r)
				}
				return
			}

			for i, expected := range tt.expectedRows {
				if i >= len(results) {
					break
				}
				if results[i] != expected {
					t.Errorf("Row %d: expected %+v, got %+v", i, expected, results[i])
				}
			}
		})
	}
}

// TestAsOfTimestamp tests AS OF TIMESTAMP queries
func TestAsOfTimestamp(t *testing.T) {
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Clean up any existing table first
	db.Exec(ctx, "DROP TABLE IF EXISTS events")

	// Create a test table
	_, err = db.Exec(ctx, `CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		event TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Capture timestamps before and after operations
	// Use UTC to match GetFastTimestamp() which uses UTC internally
	time0 := time.Now().UTC()
	t.Logf("time0: %v (%d ns)", time0, time0.UnixNano())
	time.Sleep(100 * time.Millisecond) // Ensure time difference

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO events (id, event, status) VALUES (1, 'Event A', 'pending')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO events (id, event, status) VALUES (2, 'Event B', 'pending')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	time1 := time.Now().UTC()
	t.Logf("time1: %v (%d ns)", time1, time1.UnixNano())
	time.Sleep(50 * time.Millisecond)

	// Update Event A status
	_, err = db.Exec(ctx, "UPDATE events SET status = 'completed' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	time2 := time.Now().UTC()
	time.Sleep(50 * time.Millisecond)

	// Delete Event B
	_, err = db.Exec(ctx, "DELETE FROM events WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	time3 := time.Now().UTC()

	// Test AS OF TIMESTAMP queries
	tests := []struct {
		name         string
		timestamp    time.Time
		expectedRows []struct {
			id     int
			event  string
			status string
		}
	}{
		{
			name:      "AS OF before any data",
			timestamp: time0,
			expectedRows: []struct {
				id     int
				event  string
				status string
			}{},
		},
		{
			name:      "AS OF after initial inserts",
			timestamp: time1,
			expectedRows: []struct {
				id     int
				event  string
				status string
			}{
				{1, "Event A", "pending"},
				{2, "Event B", "pending"},
			},
		},
		{
			name:      "AS OF after update",
			timestamp: time2,
			expectedRows: []struct {
				id     int
				event  string
				status string
			}{
				{1, "Event A", "completed"},
				{2, "Event B", "pending"},
			},
		},
		{
			name:      "AS OF after delete",
			timestamp: time3,
			expectedRows: []struct {
				id     int
				event  string
				status string
			}{
				{1, "Event A", "completed"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Format timestamp for SQL
			timestampStr := tt.timestamp.Format("2006-01-02 15:04:05.999999999")
			query := fmt.Sprintf("SELECT id, event, status FROM events AS OF TIMESTAMP '%s' ORDER BY id", timestampStr)
			t.Logf("Query: %s", query)
			t.Logf("Timestamp: %v (%d ns)", tt.timestamp, tt.timestamp.UnixNano())

			rows, err := db.Query(ctx, query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			var results []struct {
				id     int
				event  string
				status string
			}

			for rows.Next() {
				var id int
				var event, status string
				err := rows.Scan(&id, &event, &status)
				if err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}
				results = append(results, struct {
					id     int
					event  string
					status string
				}{id, event, status})
			}

			if len(results) != len(tt.expectedRows) {
				t.Errorf("Expected %d rows, got %d", len(tt.expectedRows), len(results))
				// Debug: print what we got
				for i, r := range results {
					t.Logf("  Got row %d: %+v", i, r)
				}
				return
			}

			for i, expected := range tt.expectedRows {
				if i >= len(results) {
					break
				}
				if results[i] != expected {
					t.Errorf("Row %d: expected %+v, got %+v", i, expected, results[i])
				}
			}
		})
	}
}

// TestAsOfIsolation tests that AS OF queries provide consistent snapshots
func TestAsOfIsolation(t *testing.T) {
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Set to SNAPSHOT isolation for predictable behavior
	_, err = db.Exec(ctx, "SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Create a test table
	_, err = db.Exec(ctx, `CREATE TABLE accounts (
		id INTEGER PRIMARY KEY,
		balance INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 1000)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Get current transaction ID for AS OF query
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	snapshotTxnID := tx.ID() - 1 // Previous transaction that did the insert
	tx.Rollback()

	// Start a concurrent transaction that modifies data
	txUpdate, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin update transaction: %v", err)
	}

	_, err = txUpdate.ExecContext(ctx, "UPDATE accounts SET balance = 2000 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Query AS OF should see old value even though update is in progress
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT balance FROM accounts AS OF TRANSACTION %d WHERE id = 1", snapshotTxnID))
	if err != nil {
		t.Fatalf("AS OF query failed: %v", err)
	}
	defer rows.Close()

	var balance int
	if rows.Next() {
		err = rows.Scan(&balance)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		if balance != 1000 {
			t.Errorf("AS OF query should see original value 1000, got %d", balance)
		}
	} else {
		t.Error("AS OF query returned no rows")
	}

	// Commit the update
	err = txUpdate.Commit()
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	// AS OF query should still see old value
	rows, err = db.Query(ctx, fmt.Sprintf("SELECT balance FROM accounts AS OF TRANSACTION %d WHERE id = 1", snapshotTxnID))
	if err != nil {
		t.Fatalf("AS OF query failed after commit: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&balance)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		if balance != 1000 {
			t.Errorf("AS OF query should still see original value 1000, got %d", balance)
		}
	} else {
		t.Error("AS OF query returned no rows after commit")
	}

	// Current query should see new value
	rows, err = db.Query(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("Current query failed: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&balance)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		if balance != 2000 {
			t.Errorf("Current query should see new value 2000, got %d", balance)
		}
	} else {
		t.Error("Current query returned no rows")
	}
}

// TestAsOfErrors tests error cases for AS OF queries
func TestAsOfErrors(t *testing.T) {
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a test table
	_, err = db.Exec(ctx, `CREATE TABLE test_errors (
		id INTEGER PRIMARY KEY,
		value TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test invalid timestamp format
	_, err = db.Query(ctx, "SELECT * FROM test_errors AS OF TIMESTAMP 'invalid-date'")
	if err == nil {
		t.Error("Expected error for invalid timestamp format")
	} else if !containsAny(err.Error(), []string{"invalid timestamp", "cannot parse", "parsing time"}) {
		t.Errorf("Unexpected error message: %v", err)
	}

	// Test non-existent table
	_, err = db.Query(ctx, "SELECT * FROM non_existent AS OF TRANSACTION 1")
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else if !containsAny(err.Error(), []string{"table not found", "does not exist", "unknown table"}) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestAsOfWithJoins tests AS OF with JOIN operations
// TODO: Enable this test when JOIN support is added for AS OF queries
func TestAsOfWithJoins(t *testing.T) {
	t.Skip("AS OF with JOINs not yet supported")
	// Create an in-memory database
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test tables
	_, err = db.Exec(ctx, `CREATE TABLE users_join (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE orders_join (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		amount INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO users_join (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert users: %v", err)
	}

	// Get transaction ID after initial data
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	txnIDAfterUsers := tx.ID() - 1
	tx.Rollback()

	// Insert orders
	_, err = db.Exec(ctx, "INSERT INTO orders_join (id, user_id, amount) VALUES (1, 1, 100), (2, 1, 200)")
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Test JOIN with AS OF on one table
	query := fmt.Sprintf(`
		SELECT u.name, COUNT(o.id) as order_count
		FROM users_join AS OF TRANSACTION %d u
		LEFT JOIN orders_join o ON u.id = o.user_id
		GROUP BY u.name
		ORDER BY u.name
	`, txnIDAfterUsers)

	rows, err := db.Query(ctx, query)
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name       string
		orderCount int
	}{
		{"Alice", 2},
		{"Bob", 0},
	}

	var results []struct {
		name       string
		orderCount int
	}

	for rows.Next() {
		var name string
		var orderCount int
		err := rows.Scan(&name, &orderCount)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		results = append(results, struct {
			name       string
			orderCount int
		}{name, orderCount})
	}

	if len(results) != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), len(results))
		return
	}

	for i, exp := range expected {
		if i >= len(results) {
			break
		}
		if results[i] != exp {
			t.Errorf("Row %d: expected %+v, got %+v", i, exp, results[i])
		}
	}
}
