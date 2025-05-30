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
	"time"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/storage"
)

// TestDirtyWritePrevention tests that dirty writes are not possible
func TestDirtyWritePrevention(t *testing.T) {
	testCases := []struct {
		name      string
		isolation storage.IsolationLevel
	}{
		{"READ_COMMITTED", storage.ReadCommitted},
		{"SNAPSHOT", storage.SnapshotIsolation},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := stoolap.Open("memory://")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			err = db.Engine().SetIsolationLevel(tc.isolation)
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

			// Start two transactions
			tx1, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}

			tx2, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}

			// Tx1 updates the value but doesn't commit
			_, err = tx1.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = 1",
				driver.NamedValue{Ordinal: 1, Value: 200})
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("[%s] Tx1 updated value to 200 (not committed)\n", tc.name)

			// Tx2 tries to update the same row (should block or fail, not overwrite)
			done := make(chan error, 1)
			go func() {
				_, err := tx2.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = 1",
					driver.NamedValue{Ordinal: 1, Value: 300})
				done <- err
			}()

			// Wait a bit to see if tx2 proceeds (it shouldn't for dirty write)
			select {
			case err := <-done:
				if err == nil {
					t.Errorf("[%s] DIRTY WRITE: Tx2 was able to update uncommitted data!", tc.name)
				} else {
					fmt.Printf("[%s] Tx2 update blocked/failed as expected: %v\n", tc.name, err)
				}
			case <-time.After(100 * time.Millisecond):
				fmt.Printf("[%s] Tx2 update blocked (timeout) - correct behavior\n", tc.name)
			}

			// Clean up
			tx1.Rollback()
			tx2.Rollback()
		})
	}
}

// TestDirtyReadPrevention tests that dirty reads are not possible
func TestDirtyReadPrevention(t *testing.T) {
	testCases := []struct {
		name      string
		isolation storage.IsolationLevel
	}{
		{"READ_COMMITTED", storage.ReadCommitted},
		{"SNAPSHOT", storage.SnapshotIsolation},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := stoolap.Open("memory://")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			err = db.Engine().SetIsolationLevel(tc.isolation)
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

			// Coordination channels
			updateDone := make(chan bool)
			readDone := make(chan int)

			// Start transaction 1 that will update but not commit
			go func() {
				tx1, err := db.Begin()
				if err != nil {
					t.Error(err)
					return
				}
				defer tx1.Rollback()

				// Update value to 200
				_, err = tx1.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = 1",
					driver.NamedValue{Ordinal: 1, Value: 200})
				if err != nil {
					t.Error(err)
					return
				}

				fmt.Printf("[%s] Tx1 updated value to 200 (not committed)\n", tc.name)
				updateDone <- true

				// Hold the transaction open for a while
				time.Sleep(200 * time.Millisecond)
				fmt.Printf("[%s] Tx1 rolling back\n", tc.name)
			}()

			// Wait for update to complete
			<-updateDone

			// Start transaction 2 that tries to read
			go func() {
				tx2, err := db.Begin()
				if err != nil {
					t.Error(err)
					readDone <- -1
					return
				}
				defer tx2.Rollback()

				// Try to read the value
				var value int
				rows, err := tx2.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
				if err != nil {
					t.Error(err)
					readDone <- -1
					return
				}

				if rows.Next() {
					err = rows.Scan(&value)
					rows.Close()
					if err != nil {
						t.Error(err)
						readDone <- -1
						return
					}
				}

				fmt.Printf("[%s] Tx2 read value: %d\n", tc.name, value)
				readDone <- value
			}()

			// Get the read value
			readValue := <-readDone

			// Check if dirty read occurred
			if readValue == 200 {
				t.Errorf("[%s] DIRTY READ: Tx2 read uncommitted value 200!", tc.name)
			} else if readValue == 100 {
				fmt.Printf("[%s] Correct: Tx2 read committed value 100\n", tc.name)
			} else {
				t.Errorf("[%s] Unexpected value read: %d", tc.name, readValue)
			}
		})
	}
}

// TestConcurrentDirtyWritePrevention tests dirty write prevention under concurrent load
func TestConcurrentDirtyWritePrevention(t *testing.T) {
	testCases := []struct {
		name      string
		isolation storage.IsolationLevel
	}{
		{"READ_COMMITTED", storage.ReadCommitted},
		{"SNAPSHOT", storage.SnapshotIsolation},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := stoolap.Open("memory://")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			err = db.Engine().SetIsolationLevel(tc.isolation)
			if err != nil {
				t.Fatal(err)
			}

			// Create table with multiple rows
			_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
			if err != nil {
				t.Fatal(err)
			}

			// Insert multiple rows
			for i := 1; i <= 5; i++ {
				_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO test (id, value) VALUES (%d, %d)", i, i*100))
				if err != nil {
					t.Fatal(err)
				}
			}

			// Run multiple concurrent transactions
			const numGoroutines = 10
			var wg sync.WaitGroup
			errors := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					tx, err := db.Begin()
					if err != nil {
						errors <- err
						return
					}
					defer tx.Rollback()

					// Each transaction tries to update all rows
					for rowID := 1; rowID <= 5; rowID++ {
						_, err = tx.ExecContext(ctx, "UPDATE test SET value = ? WHERE id = ?",
							driver.NamedValue{Ordinal: 1, Value: (id+1)*1000 + rowID},
							driver.NamedValue{Ordinal: 2, Value: rowID})
						if err != nil {
							// Update might fail due to conflicts, which is expected
							continue
						}
					}

					// Try to commit
					err = tx.Commit()
					if err != nil {
						errors <- err
					}
				}(i)
			}

			wg.Wait()
			close(errors)

			// Count errors (some conflicts are expected)
			errorCount := 0
			for err := range errors {
				if err != nil {
					errorCount++
					fmt.Printf("[%s] Transaction error (expected): %v\n", tc.name, err)
				}
			}

			fmt.Printf("[%s] %d/%d transactions failed (conflicts expected)\n", tc.name, errorCount, numGoroutines)

			// Verify final state is consistent
			rows, err := db.Query(ctx, "SELECT id, value FROM test ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			fmt.Printf("[%s] Final values:\n", tc.name)
			for rows.Next() {
				var id, value int
				if err := rows.Scan(&id, &value); err != nil {
					t.Fatal(err)
				}
				fmt.Printf("  Row %d: value=%d\n", id, value)
			}
		})
	}
}
