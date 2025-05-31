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
	"sync"
	"testing"
	"time"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestConcurrentSumIsolation tests SUM with concurrent modifications
func TestConcurrentSumIsolation(t *testing.T) {
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

	// Insert 10 accounts with 1000 each
	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO accounts (id, balance) VALUES (?, 1000)", i)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify initial sum
	var initialSum int
	err = db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&initialSum)
	if err != nil {
		t.Fatal(err)
	}
	if initialSum != 10000 {
		t.Fatalf("Initial sum wrong: expected 10000, got %d", initialSum)
	}

	// Start concurrent transfers
	var wg sync.WaitGroup
	transferCount := 20

	for i := 0; i < transferCount; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()

			// Each thread does a transfer
			tx, err := db.Begin()
			if err != nil {
				t.Logf("Thread %d: Failed to begin tx: %v", tid, err)
				return
			}
			defer tx.Rollback()

			// Transfer 100 from account 1 to account 2
			_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
			if err != nil {
				t.Logf("Thread %d: Failed to debit: %v", tid, err)
				return
			}

			// Small delay to increase chance of interleaving
			time.Sleep(time.Millisecond)

			_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
			if err != nil {
				t.Logf("Thread %d: Failed to credit: %v", tid, err)
				return
			}

			err = tx.Commit()
			if err != nil {
				// Conflicts are expected in SNAPSHOT isolation
				t.Logf("Thread %d: Commit failed (expected): %v", tid, err)
			} else {
				t.Logf("Thread %d: Commit succeeded", tid)
			}
		}(i)
	}

	// Concurrent reader checking sum
	readerDone := make(chan bool)
	go func() {
		for {
			select {
			case <-readerDone:
				return
			default:
				var sum int
				err := db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&sum)
				if err != nil {
					t.Errorf("Reader failed: %v", err)
					continue
				}
				if sum != 10000 {
					t.Errorf("Sum invariant violated: expected 10000, got %d", sum)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Wait for transfers to complete
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Let reader do a few more checks
	close(readerDone)

	// Final verification
	var finalSum int
	err = db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&finalSum)
	if err != nil {
		t.Fatal(err)
	}
	if finalSum != 10000 {
		t.Errorf("Final sum wrong: expected 10000, got %d", finalSum)
	}
}
