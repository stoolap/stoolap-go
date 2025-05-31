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
	"time"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestMVCCSumConsistency tests that SUM aggregation sees a consistent snapshot
func TestMVCCSumConsistency(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create simple table
	_, err = db.Exec(`
		CREATE TABLE test_sum (
			id INTEGER PRIMARY KEY,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO test_sum (id, value) VALUES (?, ?)", i, 100)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start a SNAPSHOT transaction that will read
	readTx, err := db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()

	// Get initial sum
	var sum1 int
	err = readTx.QueryRow("SELECT SUM(value) FROM test_sum").Scan(&sum1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Initial sum: %d", sum1)

	// Now have another transaction modify the data
	writeTx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Update all values
	_, err = writeTx.Exec("UPDATE test_sum SET value = 200")
	if err != nil {
		t.Fatal(err)
	}

	// Commit the write
	err = writeTx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Read sum again in the snapshot transaction - should see same value
	var sum2 int
	err = readTx.QueryRow("SELECT SUM(value) FROM test_sum").Scan(&sum2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Sum after concurrent update: %d", sum2)

	if sum1 != sum2 {
		t.Errorf("SNAPSHOT isolation violated: sum changed from %d to %d", sum1, sum2)
	}

	// Also check individual values to debug
	rows, err := readTx.Query("SELECT id, value FROM test_sum ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	t.Log("Individual values in snapshot:")
	for rows.Next() {
		var id, value int
		err = rows.Scan(&id, &value)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("  id=%d, value=%d", id, value)
	}
}

// TestMVCCTransferConsistency tests money transfer consistency
func TestMVCCTransferConsistency(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create accounts table
	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			balance INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert two accounts
	_, err = db.Exec("INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 1000)")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Start multiple concurrent transfers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
			if err != nil {
				errors <- err
				return
			}
			defer tx.Rollback()

			// Transfer 100 from account 1 to account 2
			_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
			if err != nil {
				errors <- err
				return
			}

			// Small delay to increase chance of conflicts
			time.Sleep(10 * time.Millisecond)

			_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
			if err != nil {
				errors <- err
				return
			}

			err = tx.Commit()
			if err != nil {
				// Write conflicts are expected
				t.Logf("Expected conflict: %v", err)
			}
		}()
	}

	// Concurrent reader checking invariant
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				var total int
				tx, _ := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
				defer tx.Rollback()

				err := tx.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&total)
				if err != nil {
					errors <- err
					continue
				}
				if total != 2000 {
					errors <- fmt.Errorf("Balance invariant violated: got %d, expected 2000", total)
				}
			}
		}
	}()

	wg.Wait()
	close(done)
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Log(err)
	}

	// Final check
	var finalTotal int
	err = db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&finalTotal)
	if err != nil {
		t.Fatal(err)
	}
	if finalTotal != 2000 {
		t.Errorf("Final balance check failed: got %d, expected 2000", finalTotal)
	}
}
