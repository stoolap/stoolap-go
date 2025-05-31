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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/storage"
)

// TestHighConcurrencyDirtyWrite tests dirty write prevention under extreme concurrency
func TestHighConcurrencyDirtyWrite(t *testing.T) {
	t.Skip("This test is designed to run for a long time and stress the MVCC implementation. This can run if needed.")
	testCases := []struct {
		name         string
		isolation    storage.IsolationLevel
		numWriters   int
		numRows      int
		numOpsPerTxn int
	}{
		{"READ_COMMITTED_100_WRITERS", storage.ReadCommitted, 100, 50, 10},
		{"SNAPSHOT_100_WRITERS", storage.SnapshotIsolation, 100, 50, 10},
		{"READ_COMMITTED_EXTREME", storage.ReadCommitted, 200, 100, 20},
		{"SNAPSHOT_EXTREME", storage.SnapshotIsolation, 200, 100, 20},
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

			// Create table
			_, err = db.Exec(ctx, "CREATE TABLE stress_test (id INTEGER PRIMARY KEY, value INTEGER, version INTEGER)")
			if err != nil {
				t.Fatal(err)
			}

			// Insert initial rows
			for i := 1; i <= tc.numRows; i++ {
				_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO stress_test (id, value, version) VALUES (%d, %d, 0)", i, i*100))
				if err != nil {
					t.Fatal(err)
				}
			}

			// Tracking variables
			var dirtyWriteAttempts atomic.Int64
			var successfulWrites atomic.Int64
			var dirtyWritesPrevented atomic.Int64
			var otherErrors atomic.Int64

			// Run concurrent writers
			var wg sync.WaitGroup
			start := time.Now()

			for w := 0; w < tc.numWriters; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					// Each worker performs multiple operations
					for op := 0; op < tc.numOpsPerTxn; op++ {
						tx, err := db.Begin()
						if err != nil {
							otherErrors.Add(1)
							continue
						}

						// Randomly select rows to update
						rowsToUpdate := make([]int, 0, 3)
						for i := 0; i < 3; i++ {
							rowID := rand.Intn(tc.numRows) + 1
							rowsToUpdate = append(rowsToUpdate, rowID)
						}

						// Try to update multiple rows in a transaction
						updateSuccess := true
						for _, rowID := range rowsToUpdate {
							dirtyWriteAttempts.Add(1)

							// Read current value
							var currentValue, currentVersion int
							rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT value, version FROM stress_test WHERE id = %d", rowID))
							if err != nil {
								updateSuccess = false
								break
							}
							if rows.Next() {
								rows.Scan(&currentValue, &currentVersion)
								rows.Close()
							} else {
								rows.Close()
								updateSuccess = false
								break
							}

							// Try to update
							_, err = tx.ExecContext(ctx,
								"UPDATE stress_test SET value = ?, version = ? WHERE id = ?",
								driver.NamedValue{Ordinal: 1, Value: currentValue + workerID},
								driver.NamedValue{Ordinal: 2, Value: currentVersion + 1},
								driver.NamedValue{Ordinal: 3, Value: rowID})

							if err != nil {
								if err.Error() == "error executing UPDATE: row is being modified by another transaction" {
									dirtyWritesPrevented.Add(1)
								} else {
									otherErrors.Add(1)
								}
								updateSuccess = false
								break
							}
						}

						// Try to commit
						if updateSuccess {
							err = tx.Commit()
							if err != nil {
								if err.Error() == "error executing UPDATE: row is being modified by another transaction" {
									dirtyWritesPrevented.Add(1)
								} else {
									otherErrors.Add(1)
								}
							} else {
								successfulWrites.Add(1)
							}
						} else {
							tx.Rollback()
						}
					}
				}(w)
			}

			wg.Wait()
			duration := time.Since(start)

			// Verify data consistency
			rows, err := db.Query(ctx, "SELECT id, value, version FROM stress_test ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			maxVersion := 0
			inconsistencies := 0
			for rows.Next() {
				var id, value, version int
				rows.Scan(&id, &value, &version)
				if version > maxVersion {
					maxVersion = version
				}
				// Check if value and version are consistent
				// Value should have been incremented by various amounts
				// Version should match the number of successful updates
				if version < 0 || value < id*100 {
					inconsistencies++
					t.Logf("Inconsistent row %d: value=%d, version=%d", id, value, version)
				}
			}

			// Report results
			fmt.Printf("\n[%s] Results:\n", tc.name)
			fmt.Printf("  Duration: %v\n", duration)
			fmt.Printf("  Total write attempts: %d\n", dirtyWriteAttempts.Load())
			fmt.Printf("  Successful writes: %d\n", successfulWrites.Load())
			fmt.Printf("  Dirty writes prevented: %d\n", dirtyWritesPrevented.Load())
			fmt.Printf("  Other errors: %d\n", otherErrors.Load())
			fmt.Printf("  Max version seen: %d\n", maxVersion)
			fmt.Printf("  Data inconsistencies: %d\n", inconsistencies)
			fmt.Printf("  Prevention rate: %.2f%%\n",
				float64(dirtyWritesPrevented.Load())/float64(dirtyWriteAttempts.Load())*100)

			// Verify no data corruption
			if inconsistencies > 0 {
				t.Errorf("Data corruption detected: %d inconsistent rows", inconsistencies)
			}
		})
	}
}

// TestHighConcurrencyDirtyRead tests dirty read prevention under extreme concurrency
func TestHighConcurrencyDirtyRead(t *testing.T) {
	t.Skip("This test is designed to run for a long time and stress the MVCC implementation. This can run if needed.")
	testCases := []struct {
		name       string
		isolation  storage.IsolationLevel
		numReaders int
		numWriters int
	}{
		{"READ_COMMITTED_HEAVY_READ", storage.ReadCommitted, 100, 20},
		{"SNAPSHOT_HEAVY_READ", storage.SnapshotIsolation, 100, 20},
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

			// Create table with a flag column
			_, err = db.Exec(ctx, "CREATE TABLE read_test (id INTEGER PRIMARY KEY, value INTEGER, is_committed INTEGER)")
			if err != nil {
				t.Fatal(err)
			}

			// Insert initial rows (1 = committed, 0 = uncommitted)
			for i := 1; i <= 10; i++ {
				_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO read_test (id, value, is_committed) VALUES (%d, %d, 1)", i, i*1000))
				if err != nil {
					t.Fatal(err)
				}
			}

			// Tracking variables
			var dirtyReadsDetected atomic.Int64
			var cleanReads atomic.Int64
			var totalReads atomic.Int64

			// Channel to coordinate
			stopCh := make(chan struct{})
			var wg sync.WaitGroup

			// Start writers that create uncommitted changes
			for w := 0; w < tc.numWriters; w++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()

					for {
						select {
						case <-stopCh:
							return
						default:
							tx, err := db.Begin()
							if err != nil {
								continue
							}

							// Update a random row with uncommitted marker
							rowID := rand.Intn(10) + 1
							newValue := writerID*10000 + rowID

							_, err = tx.ExecContext(ctx,
								"UPDATE read_test SET value = ?, is_committed = ? WHERE id = ?",
								driver.NamedValue{Ordinal: 1, Value: newValue},
								driver.NamedValue{Ordinal: 2, Value: 0}, // Mark as uncommitted (0)
								driver.NamedValue{Ordinal: 3, Value: rowID})

							if err != nil {
								tx.Rollback()
								continue
							}

							// Hold transaction open for a bit
							time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))

							// Randomly commit or rollback
							if rand.Float32() < 0.5 {
								// Before commit, mark as committed
								_, err = tx.ExecContext(ctx,
									"UPDATE read_test SET is_committed = ? WHERE id = ?",
									driver.NamedValue{Ordinal: 1, Value: 1}, // Mark as committed (1)
									driver.NamedValue{Ordinal: 2, Value: rowID})
								if err != nil {
									tx.Rollback()
									continue
								}
								tx.Commit()
							} else {
								tx.Rollback()
							}
						}
					}
				}(w)
			}

			// Start readers
			for r := 0; r < tc.numReaders; r++ {
				wg.Add(1)
				go func(readerID int) {
					defer wg.Done()

					for {
						select {
						case <-stopCh:
							return
						default:
							totalReads.Add(1)

							tx, err := db.Begin()
							if err != nil {
								continue
							}

							// Read all rows
							rows, err := tx.QueryContext(ctx, "SELECT id, value, is_committed FROM read_test ORDER BY id")
							if err != nil {
								tx.Rollback()
								continue
							}

							dirtyReadFound := false
							for rows.Next() {
								var id, value, isCommitted int
								rows.Scan(&id, &value, &isCommitted)

								// If we see is_committed=0, it means we read uncommitted data
								if isCommitted == 0 {
									dirtyReadFound = true
									dirtyReadsDetected.Add(1)
									t.Logf("DIRTY READ: Reader %d saw uncommitted value %d for row %d",
										readerID, value, id)
									break
								}
							}
							rows.Close()

							if !dirtyReadFound {
								cleanReads.Add(1)
							}

							tx.Rollback()
						}
					}
				}(r)
			}

			// Run for a fixed duration
			time.Sleep(5 * time.Second)
			close(stopCh)
			wg.Wait()

			// Report results
			fmt.Printf("\n[%s] Dirty Read Test Results:\n", tc.name)
			fmt.Printf("  Total reads: %d\n", totalReads.Load())
			fmt.Printf("  Clean reads: %d\n", cleanReads.Load())
			fmt.Printf("  Dirty reads detected: %d\n", dirtyReadsDetected.Load())
			fmt.Printf("  Dirty read rate: %.4f%%\n",
				float64(dirtyReadsDetected.Load())/float64(totalReads.Load())*100)

			// Verify no dirty reads
			if dirtyReadsDetected.Load() > 0 {
				t.Errorf("Dirty reads detected: %d out of %d reads",
					dirtyReadsDetected.Load(), totalReads.Load())
			}
		})
	}
}

// TestMixedWorkloadStress tests both reads and writes under extreme concurrency
func TestMixedWorkloadStress(t *testing.T) {
	t.Skip("This test is designed to run for a long time and stress the MVCC implementation. This can run if needed.")
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
	_, err = db.Exec(ctx, "CREATE TABLE mixed_test (id INTEGER PRIMARY KEY, balance INTEGER, txn_count INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial rows - simulate bank accounts
	const numAccounts = 100
	const initialBalance = 1000
	for i := 1; i <= numAccounts; i++ {
		_, err = db.Exec(ctx, fmt.Sprintf("INSERT INTO mixed_test (id, balance, txn_count) VALUES (%d, %d, 0)", i, initialBalance))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Tracking
	var totalTransfers atomic.Int64
	var successfulTransfers atomic.Int64
	var dirtyWritesPrevented atomic.Int64
	var balanceErrors atomic.Int64

	// Run mixed workload
	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	start := time.Now()

	// Transfer workers - simulate money transfers
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-stopCh:
					return
				default:
					totalTransfers.Add(1)

					// Random transfer between accounts
					fromAccount := rand.Intn(numAccounts) + 1
					toAccount := rand.Intn(numAccounts) + 1
					if fromAccount == toAccount {
						continue
					}
					amount := rand.Intn(100) + 1

					tx, err := db.Begin()
					if err != nil {
						continue
					}

					success := true

					// Read balances
					var fromBalance, toBalance int
					rows, err := tx.QueryContext(ctx,
						fmt.Sprintf("SELECT balance FROM mixed_test WHERE id = %d", fromAccount))
					if err != nil {
						success = false
					} else if rows.Next() {
						rows.Scan(&fromBalance)
						rows.Close()
					} else {
						rows.Close()
						success = false
					}

					if success {
						rows, err = tx.QueryContext(ctx,
							fmt.Sprintf("SELECT balance FROM mixed_test WHERE id = %d", toAccount))
						if err != nil {
							success = false
						} else if rows.Next() {
							rows.Scan(&toBalance)
							rows.Close()
						} else {
							rows.Close()
							success = false
						}
					}

					// Check sufficient balance
					if success && fromBalance < amount {
						success = false
						balanceErrors.Add(1)
					}

					// Perform transfer
					if success {
						// Deduct from source
						_, err = tx.ExecContext(ctx,
							"UPDATE mixed_test SET balance = balance - ?, txn_count = txn_count + 1 WHERE id = ?",
							driver.NamedValue{Ordinal: 1, Value: amount},
							driver.NamedValue{Ordinal: 2, Value: fromAccount})
						if err != nil {
							if err.Error() == "error executing UPDATE: row is being modified by another transaction" {
								dirtyWritesPrevented.Add(1)
							}
							success = false
						}
					}

					if success {
						// Add to destination
						_, err = tx.ExecContext(ctx,
							"UPDATE mixed_test SET balance = balance + ?, txn_count = txn_count + 1 WHERE id = ?",
							driver.NamedValue{Ordinal: 1, Value: amount},
							driver.NamedValue{Ordinal: 2, Value: toAccount})
						if err != nil {
							if err.Error() == "error executing UPDATE: row is being modified by another transaction" {
								dirtyWritesPrevented.Add(1)
							}
							success = false
						}
					}

					// Commit or rollback
					if success {
						err = tx.Commit()
						if err == nil {
							successfulTransfers.Add(1)
						} else {
							// Check for write-write conflict on commit
							if strings.Contains(err.Error(), "write-write conflict") {
								dirtyWritesPrevented.Add(1)
							}
						}
					} else {
						tx.Rollback()
					}
				}
			}
		}(w)
	}

	// Balance checker - continuously verify total balance
	var balanceInconsistencies atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-stopCh:
				return
			default:
				// Calculate total balance
				var totalBalance int
				rows, err := db.Query(ctx, "SELECT SUM(balance) FROM mixed_test")
				if err == nil && rows.Next() {
					rows.Scan(&totalBalance)
					rows.Close()

					expectedTotal := numAccounts * initialBalance
					if totalBalance != expectedTotal {
						balanceInconsistencies.Add(1)
						// Only log first few occurrences to avoid spam
						if balanceInconsistencies.Load() <= 5 {
							t.Logf("Balance inconsistency #%d: Expected %d, got %d (diff: %+d)",
								balanceInconsistencies.Load(), expectedTotal, totalBalance, totalBalance-expectedTotal)
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Run for fixed duration
	time.Sleep(10 * time.Second)
	close(stopCh)
	wg.Wait()
	duration := time.Since(start)

	// Final verification
	// Note: There appears to be a bug in handling multiple SUM() functions in one query,
	// so we'll use separate queries
	var totalBalance int
	err = db.QueryRow(ctx, "SELECT SUM(balance) FROM mixed_test").Scan(&totalBalance)
	if err != nil {
		t.Fatalf("Failed to get total balance: %v", err)
	}

	var totalTxnCount int
	err = db.QueryRow(ctx, "SELECT SUM(txn_count) FROM mixed_test").Scan(&totalTxnCount)
	if err != nil {
		t.Fatalf("Failed to get total txn count: %v", err)
	}

	// Report results
	fmt.Printf("\nMixed Workload Stress Test Results:\n")
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf("  Total transfer attempts: %d\n", totalTransfers.Load())
	fmt.Printf("  Successful transfers: %d\n", successfulTransfers.Load())
	fmt.Printf("  Dirty writes prevented: %d\n", dirtyWritesPrevented.Load())
	fmt.Printf("  Insufficient balance errors: %d\n", balanceErrors.Load())
	fmt.Printf("  Balance inconsistencies during run: %d\n", balanceInconsistencies.Load())
	fmt.Printf("  Total transactions recorded: %d\n", totalTxnCount/2) // Divided by 2 as each transfer touches 2 accounts
	fmt.Printf("  Throughput: %.2f transfers/sec\n", float64(successfulTransfers.Load())/duration.Seconds())

	// Verify conservation of money
	expectedTotal := numAccounts * initialBalance
	fmt.Printf("  Expected total balance: %d\n", expectedTotal)
	fmt.Printf("  Actual total balance: %d\n", totalBalance)

	if totalBalance != expectedTotal {
		t.Errorf("Money not conserved! Expected %d, got %d", expectedTotal, totalBalance)
	}

	// If balance doesn't match, let's see what happened to individual accounts
	if totalBalance != expectedTotal {
		// Check a sample of accounts
		rows, err := db.Query(ctx, "SELECT id, balance FROM mixed_test ORDER BY balance DESC LIMIT 10")
		if err == nil {
			fmt.Println("Top 10 accounts by balance:")
			for rows.Next() {
				var id, balance int
				rows.Scan(&id, &balance)
				fmt.Printf("  Account %d: balance=%d (expected around %d)\n", id, balance, initialBalance)
			}
			rows.Close()
		}

		// Check for negative balances
		var negCount int
		err = db.QueryRow(ctx, "SELECT COUNT(*) FROM mixed_test WHERE balance < 0").Scan(&negCount)
		if err == nil && negCount > 0 {
			fmt.Printf("WARNING: %d accounts have negative balance!\n", negCount)
		}
	}
}
