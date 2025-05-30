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
	"testing"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/storage"
)

// TestSnapshotLostUpdatePrevention tests that SNAPSHOT isolation prevents lost updates
func TestSnapshotLostUpdatePrevention(t *testing.T) {
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

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO accounts (id, balance) VALUES (1, 1000)")
	if err != nil {
		t.Fatal(err)
	}

	// Start two transactions
	tx1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()

	tx2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	// Both transactions read the same account
	var balance1, balance2 int
	rows1, err := tx1.QueryContext(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows1.Next() {
		rows1.Scan(&balance1)
		rows1.Close()
	}
	t.Logf("Tx1 read balance: %d", balance1)

	rows2, err := tx2.QueryContext(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if rows2.Next() {
		rows2.Scan(&balance2)
		rows2.Close()
	}
	t.Logf("Tx2 read balance: %d", balance2)

	// Both try to update based on what they read
	// Tx1 adds 100
	_, err = tx1.ExecContext(ctx,
		"UPDATE accounts SET balance = ? WHERE id = 1",
		driver.NamedValue{Ordinal: 1, Value: balance1 + 100})
	if err != nil {
		t.Fatalf("Tx1 update failed: %v", err)
	}
	t.Log("Tx1 updated balance to", balance1+100)

	// Tx1 commits first
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Tx1 commit failed: %v", err)
	}
	t.Log("Tx1 committed successfully")

	// Tx2 also tries to add 200 based on its read
	_, err = tx2.ExecContext(ctx,
		"UPDATE accounts SET balance = ? WHERE id = 1",
		driver.NamedValue{Ordinal: 1, Value: balance2 + 200})
	if err != nil {
		t.Fatalf("Tx2 update failed: %v", err)
	}
	t.Log("Tx2 updated balance to", balance2+200)

	// Tx2 commit should fail due to write-write conflict
	err = tx2.Commit()
	if err == nil {
		t.Error("Expected write-write conflict, but tx2 commit succeeded - LOST UPDATE!")
	} else {
		t.Logf("Tx2 commit correctly failed with: %v", err)
	}

	// Verify final balance
	var finalBalance int
	rows, _ := db.Query(ctx, "SELECT balance FROM accounts WHERE id = 1")
	if rows.Next() {
		rows.Scan(&finalBalance)
		rows.Close()
	}
	t.Logf("Final balance: %d (should be 1100, not 1200)", finalBalance)

	if finalBalance != 1100 {
		t.Errorf("Lost update detected! Final balance is %d, expected 1100", finalBalance)
	}
}

// TestConcurrentSnapshotUpdates tests multiple concurrent updates under SNAPSHOT isolation
func TestConcurrentSnapshotUpdates(t *testing.T) {
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

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO counters (id, value) VALUES (1, 0)")
	if err != nil {
		t.Fatal(err)
	}

	// Run concurrent increments
	const numWorkers = 10
	const incrementsPerWorker = 10

	var wg sync.WaitGroup
	successCount := 0
	conflictCount := 0
	var mu sync.Mutex

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < incrementsPerWorker; i++ {
				tx, err := db.Begin()
				if err != nil {
					continue
				}

				// Read current value
				var currentValue int
				rows, err := tx.QueryContext(ctx, "SELECT value FROM counters WHERE id = 1")
				if err != nil {
					tx.Rollback()
					continue
				}
				if rows.Next() {
					rows.Scan(&currentValue)
					rows.Close()
				} else {
					rows.Close()
					tx.Rollback()
					continue
				}

				// Update with increment
				_, err = tx.ExecContext(ctx,
					"UPDATE counters SET value = ? WHERE id = 1",
					driver.NamedValue{Ordinal: 1, Value: currentValue + 1})
				if err != nil {
					tx.Rollback()
					continue
				}

				// Try to commit
				err = tx.Commit()
				mu.Lock()
				if err == nil {
					successCount++
				} else {
					conflictCount++
				}
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()

	// Check final value
	var finalValue int
	rows, _ := db.Query(ctx, "SELECT value FROM counters WHERE id = 1")
	if rows.Next() {
		rows.Scan(&finalValue)
		rows.Close()
	}

	fmt.Printf("\nConcurrent SNAPSHOT Update Results:\n")
	fmt.Printf("  Successful commits: %d\n", successCount)
	fmt.Printf("  Conflicts detected: %d\n", conflictCount)
	fmt.Printf("  Total attempts: %d\n", successCount+conflictCount)
	fmt.Printf("  Final counter value: %d\n", finalValue)

	// The final value should equal the number of successful commits
	if finalValue != successCount {
		t.Errorf("Lost updates detected! Final value %d != successful commits %d",
			finalValue, successCount)
	}
}
