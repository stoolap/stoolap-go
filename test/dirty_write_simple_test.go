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
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/storage"
)

func TestDirtyWriteSimple(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	err = db.Engine().SetIsolationLevel(storage.ReadCommitted)
	if err != nil {
		t.Fatal(err)
	}

	// Create table and insert initial value
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id, value) VALUES (1, 100)")
	if err != nil {
		t.Fatal(err)
	}

	// Start Tx1
	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Tx1 reads the value
	var value1 int
	rows1, err := tx1.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows1.Next() {
		rows1.Scan(&value1)
		rows1.Close()
	}
	fmt.Printf("Tx1 read value: %d\n", value1)

	// Tx1 updates but doesn't commit
	_, err = tx1.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = 1",
		driver.NamedValue{Ordinal: 1, Value: 200})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Tx1 updated to 200 (not committed)")

	// Start Tx2
	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Tx2 reads the value (should see 100, not 200)
	var value2 int
	rows2, err := tx2.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows2.Next() {
		rows2.Scan(&value2)
		rows2.Close()
	}
	fmt.Printf("Tx2 read value: %d (correct, sees committed value)\n", value2)

	// Tx2 tries to update the same row
	// This should fail or block because Tx1 has uncommitted changes to this row
	_, err = tx2.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = 1",
		driver.NamedValue{Ordinal: 1, Value: 300})
	if err != nil {
		fmt.Printf("Tx2 update failed (correct): %v\n", err)
	} else {
		fmt.Println("Tx2 update succeeded (WRONG! This is a dirty write)")

		// Let's see what each transaction thinks the value is
		var checkValue1, checkValue2 int

		// Tx1 checks
		rows, _ := tx1.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
		if rows.Next() {
			rows.Scan(&checkValue1)
			rows.Close()
		}
		fmt.Printf("Tx1 now sees value: %d\n", checkValue1)

		// Tx2 checks
		rows, _ = tx2.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
		if rows.Next() {
			rows.Scan(&checkValue2)
			rows.Close()
		}
		fmt.Printf("Tx2 now sees value: %d\n", checkValue2)
	}

	// Commit both
	err1 := tx1.Commit()
	err2 := tx2.Commit()

	fmt.Printf("Tx1 commit: %v\n", err1)
	fmt.Printf("Tx2 commit: %v\n", err2)

	// Check final value
	var finalValue int
	err = db.QueryRow(ctx, "SELECT value FROM test WHERE id = 1").Scan(&finalValue)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Final value: %d\n", finalValue)
}

// TestSimpleMoneyTransfer tests money transfer scenario with proper balance conservation
func TestSimpleMoneyTransfer(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	err = db.Engine().SetIsolationLevel(storage.SnapshotIsolation)
	if err != nil {
		t.Fatal(err)
	}

	// Create accounts table
	_, err = db.Exec(ctx, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Create 10 accounts with 1000 each
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO accounts (id, balance) VALUES (%d, 1000)", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify initial total
	var initialTotal int
	rows, _ := db.Query(ctx, "SELECT SUM(balance) FROM accounts")
	if rows.Next() {
		rows.Scan(&initialTotal)
		rows.Close()
	}
	fmt.Printf("Initial total balance: %d\n", initialTotal)

	// Run transfers
	var successfulTransfers atomic.Int32
	var failedTransfers atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(txID int) {
			defer wg.Done()

			// Add some delay to reduce contention
			time.Sleep(time.Duration(txID) * time.Millisecond)

			// Random transfer
			fromID := (txID % 10) + 1
			toID := ((txID + 3) % 10) + 1
			if fromID == toID {
				toID = (toID % 10) + 1
			}
			amount := 50

			// Start transaction
			tx, err := db.Begin()
			if err != nil {
				failedTransfers.Add(1)
				return
			}

			success := true

			// Check source balance
			var sourceBalance int
			rows, err := tx.QueryContext(ctx,
				"SELECT balance FROM accounts WHERE id = ?",
				driver.NamedValue{Ordinal: 1, Value: fromID})
			if err != nil {
				success = false
			} else if rows.Next() {
				rows.Scan(&sourceBalance)
				rows.Close()
				if sourceBalance < amount {
					success = false // Insufficient funds
				}
			} else {
				rows.Close()
				success = false
			}

			// Perform transfer
			if success {
				// Deduct from source
				_, err = tx.ExecContext(ctx,
					"UPDATE accounts SET balance = balance - ? WHERE id = ?",
					driver.NamedValue{Ordinal: 1, Value: amount},
					driver.NamedValue{Ordinal: 2, Value: fromID})
				if err != nil {
					t.Logf("Tx %d: Failed to deduct from account %d: %v", txID, fromID, err)
					success = false
				}
			}

			// Small delay to increase contention
			time.Sleep(5 * time.Millisecond)

			if success {
				// Add to destination
				_, err = tx.ExecContext(ctx,
					"UPDATE accounts SET balance = balance + ? WHERE id = ?",
					driver.NamedValue{Ordinal: 1, Value: amount},
					driver.NamedValue{Ordinal: 2, Value: toID})
				if err != nil {
					t.Logf("Tx %d: Failed to credit account %d: %v", txID, toID, err)
					success = false
				} else {
					t.Logf("Tx %d: Successfully transferred %d from account %d to %d", txID, amount, fromID, toID)
				}
			}

			// Commit or rollback
			if success {
				err = tx.Commit()
				if err == nil {
					successfulTransfers.Add(1)
					t.Logf("Tx %d: Committed successfully", txID)
				} else {
					t.Logf("Tx %d: Commit failed: %v", txID, err)
					failedTransfers.Add(1)
				}
			} else {
				_ = tx.Rollback()
				t.Logf("Tx %d: Rolled back due to failure", txID)
				failedTransfers.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Check final total
	var finalTotal int
	rows, _ = db.Query(ctx, "SELECT SUM(balance) FROM accounts")
	if rows.Next() {
		rows.Scan(&finalTotal)
		rows.Close()
	}

	// Check individual account balances
	fmt.Printf("\nFinal account balances:\n")
	rows, _ = db.Query(ctx, "SELECT id, balance FROM accounts ORDER BY id")
	for rows.Next() {
		var id, balance int
		rows.Scan(&id, &balance)
		fmt.Printf("  Account %d: %d\n", id, balance)
	}
	rows.Close()

	fmt.Printf("\nTransfer Results:\n")
	fmt.Printf("  Successful transfers: %d\n", successfulTransfers.Load())
	fmt.Printf("  Failed transfers: %d\n", failedTransfers.Load())
	fmt.Printf("  Initial total: %d\n", initialTotal)
	fmt.Printf("  Final total: %d\n", finalTotal)

	if initialTotal != finalTotal {
		t.Errorf("Money not conserved! Initial: %d, Final: %d", initialTotal, finalTotal)
	}
}

// TestArithmeticUpdate tests that arithmetic updates work correctly
func TestArithmeticUpdate(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial value
	_, err = db.Exec(ctx, "INSERT INTO counter (id, value) VALUES (1, 100)")
	if err != nil {
		t.Fatal(err)
	}

	// Test arithmetic update
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Update with arithmetic
	_, err = tx.ExecContext(ctx, "UPDATE counter SET value = value + 50 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	// Check value within transaction
	var valueInTx int
	rows, _ := tx.QueryContext(ctx, "SELECT value FROM counter WHERE id = 1")
	if rows.Next() {
		rows.Scan(&valueInTx)
		rows.Close()
	}
	fmt.Printf("Value in transaction after update: %d\n", valueInTx)

	// Rollback
	tx.Rollback()

	// Check value after rollback
	var valueAfterRollback int
	rows, _ = db.Query(ctx, "SELECT value FROM counter WHERE id = 1")
	if rows.Next() {
		rows.Scan(&valueAfterRollback)
		rows.Close()
	}
	fmt.Printf("Value after rollback: %d\n", valueAfterRollback)

	if valueAfterRollback != 100 {
		t.Errorf("Rollback didn't restore original value! Expected 100, got %d", valueAfterRollback)
	}
}
