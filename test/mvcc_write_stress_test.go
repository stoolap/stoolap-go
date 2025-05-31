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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestMVCCWriteHeavyStress tests the true MVCC implementation with heavy concurrent writes
func TestMVCCWriteHeavyStress(t *testing.T) {
	t.Skip("This test is designed to run for a long time and stress the MVCC implementation. This can run if needed.")
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			balance INTEGER NOT NULL,
			last_update TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Initialize accounts
	numAccounts := 100
	initialBalance := 1000
	for i := 1; i <= numAccounts; i++ {
		_, err = db.Exec("INSERT INTO accounts (id, balance, last_update) VALUES (?, ?, NOW())",
			i, initialBalance)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test parameters - reduce for easier debugging
	numWriters := 10 // Number of concurrent writers
	numReaders := 5  // Number of concurrent readers
	duration := 2 * time.Second

	// Metrics
	var totalWrites int64
	var totalReads int64
	var writeConflicts int64
	var successfulCommits int64

	// Synchronization
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Writer goroutines - Heavy concurrent writes
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Random money transfer between accounts
					from := rand.Intn(numAccounts) + 1
					to := rand.Intn(numAccounts) + 1
					if from == to {
						continue
					}
					amount := rand.Intn(100) + 1

					// Use SNAPSHOT isolation for consistency
					tx, err := db.BeginTx(ctx, &sql.TxOptions{
						Isolation: sql.LevelSnapshot,
					})
					if err != nil {
						continue
					}

					// Read both balances
					var fromBalance, toBalance int
					err = tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", from).Scan(&fromBalance)
					if err != nil {
						tx.Rollback()
						continue
					}

					err = tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", to).Scan(&toBalance)
					if err != nil {
						tx.Rollback()
						continue
					}

					// Check sufficient balance
					if fromBalance < amount {
						tx.Rollback()
						continue
					}

					// Perform transfer
					_, err = tx.Exec("UPDATE accounts SET balance = balance - ?, last_update = NOW() WHERE id = ?",
						amount, from)
					if err != nil {
						tx.Rollback()
						continue
					}

					_, err = tx.Exec("UPDATE accounts SET balance = balance + ?, last_update = NOW() WHERE id = ?",
						amount, to)
					if err != nil {
						tx.Rollback()
						continue
					}

					// Commit
					err = tx.Commit()
					atomic.AddInt64(&totalWrites, 2) // Two updates per transaction

					if err != nil {
						// This is likely a write-write conflict
						atomic.AddInt64(&writeConflicts, 1)
					} else {
						atomic.AddInt64(&successfulCommits, 1)
					}
				}
			}
		}(w)
	}

	// Reader goroutines - Concurrent reads to stress visibility checks
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Use SNAPSHOT isolation for consistent reads
					tx, err := db.BeginTx(ctx, &sql.TxOptions{
						Isolation: sql.LevelSnapshot,
					})
					if err != nil {
						continue
					}

					// Read total balance - should always be constant
					var totalBalance sql.NullInt64
					err = tx.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&totalBalance)
					if err != nil {
						tx.Rollback()
						continue
					}

					// Count this read
					currentRead := atomic.AddInt64(&totalReads, 1)

					// Also read individual accounts to debug
					if totalBalance.Valid && totalBalance.Int64 != int64(numAccounts*initialBalance) {
						// Get ALL account balances to manually verify the sum
						rows, err := tx.Query("SELECT id, balance FROM accounts ORDER BY id")
						if err == nil {
							var manualSum int64 = 0
							var accountDetails []string
							accountCount := 0
							accountMap := make(map[int]int) // Track duplicates

							for rows.Next() {
								var id, balance int
								if err := rows.Scan(&id, &balance); err == nil {
									manualSum += int64(balance)
									accountCount++

									// Check for duplicate IDs
									if prev, exists := accountMap[id]; exists {
										t.Logf("DUPLICATE ID %d: prev balance=%d, new balance=%d", id, prev, balance)
									}
									accountMap[id] = balance

									if accountCount <= 10 { // Show first 10 accounts
										accountDetails = append(accountDetails, fmt.Sprintf("id%d=%d", id, balance))
									}
								}
							}
							rows.Close()

							// Log first few occurrences only
							if currentRead <= 5 && totalBalance.Int64 != manualSum {
								t.Logf("DEBUG[read %d]: SUM()=%d, manualSum=%d, accountCount=%d (expected %d), first accounts: %s",
									currentRead, totalBalance.Int64, manualSum, accountCount, numAccounts, strings.Join(accountDetails, ", "))
							}
						}
					}

					// Must commit the transaction to properly release resources
					err = tx.Commit()
					if err != nil {
						continue
					}

					// Verify invariant: total balance should remain constant
					if !totalBalance.Valid {
						t.Errorf("SUM returned NULL")
						continue
					}

					expectedTotal := int64(numAccounts * initialBalance)
					if totalBalance.Int64 != expectedTotal {
						t.Errorf("Balance invariant violated: expected %d, got %d",
							expectedTotal, totalBalance.Int64)
					}
				}
			}
		}(r)
	}

	// Wait for all goroutines
	wg.Wait()

	elapsed := time.Since(startTime)

	// Calculate metrics
	writesPerSec := float64(atomic.LoadInt64(&totalWrites)) / elapsed.Seconds()
	readsPerSec := float64(atomic.LoadInt64(&totalReads)) / elapsed.Seconds()
	conflictRate := float64(atomic.LoadInt64(&writeConflicts)) / float64(atomic.LoadInt64(&successfulCommits)+atomic.LoadInt64(&writeConflicts)) * 100

	// Print results
	fmt.Printf("\n=== MVCC Write-Heavy Stress Test Results ===\n")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Concurrent Writers: %d\n", numWriters)
	fmt.Printf("Concurrent Readers: %d\n", numReaders)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("  Total Writes: %d\n", atomic.LoadInt64(&totalWrites))
	fmt.Printf("  Writes/sec: %.2f\n", writesPerSec)
	fmt.Printf("  Total Reads: %d\n", atomic.LoadInt64(&totalReads))
	fmt.Printf("  Reads/sec: %.2f\n", readsPerSec)
	fmt.Printf("\nTransaction Metrics:\n")
	fmt.Printf("  Successful Commits: %d\n", atomic.LoadInt64(&successfulCommits))
	fmt.Printf("  Write Conflicts: %d\n", atomic.LoadInt64(&writeConflicts))
	fmt.Printf("  Conflict Rate: %.2f%%\n", conflictRate)

	// Final verification
	var finalTotal int64
	err = db.QueryRow("SELECT SUM(balance) FROM accounts").Scan(&finalTotal)
	if err != nil {
		t.Fatal(err)
	}

	expectedTotal := int64(numAccounts * initialBalance)
	if finalTotal != expectedTotal {
		t.Errorf("Final balance check failed: expected %d, got %d", expectedTotal, finalTotal)
	} else {
		fmt.Printf("\n✓ Balance Invariant Maintained: %d\n", finalTotal)
	}
}

// TestMVCCHighContentionHotspot tests MVCC with high contention on specific rows
func TestMVCCHighContentionHotspot(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table with a "hot" row that everyone wants to update
	_, err = db.Exec(`
		CREATE TABLE hotspot (
			id INTEGER PRIMARY KEY,
			counter INTEGER NOT NULL,
			version INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Initialize the hot row
	_, err = db.Exec("INSERT INTO hotspot (id, counter, version) VALUES (1, 0, 0)")
	if err != nil {
		t.Fatal(err)
	}

	// Test parameters
	numWorkers := 100
	incrementsPerWorker := 100

	var successfulIncrements int64
	var conflicts int64
	var maxVersionSeen int64

	start := time.Now()
	var wg sync.WaitGroup

	// All workers try to increment the same counter
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < incrementsPerWorker; i++ {
				// Use SNAPSHOT isolation to test conflict detection
				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
					Isolation: sql.LevelSnapshot,
				})
				if err != nil {
					continue
				}

				var counter, version int
				err = tx.QueryRow("SELECT counter, version FROM hotspot WHERE id = 1").Scan(&counter, &version)
				if err != nil {
					tx.Rollback()
					continue
				}

				// Track max version seen
				if int64(version) > atomic.LoadInt64(&maxVersionSeen) {
					atomic.StoreInt64(&maxVersionSeen, int64(version))
				}

				// Try to increment
				_, err = tx.Exec("UPDATE hotspot SET counter = ?, version = ? WHERE id = 1",
					counter+1, version+1)
				if err != nil {
					tx.Rollback()
					continue
				}

				err = tx.Commit()
				if err != nil {
					atomic.AddInt64(&conflicts, 1)
				} else {
					atomic.AddInt64(&successfulIncrements, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify final counter value
	var finalCounter, finalVersion int
	err = db.QueryRow("SELECT counter, version FROM hotspot WHERE id = 1").Scan(&finalCounter, &finalVersion)
	if err != nil {
		t.Fatal(err)
	}

	totalAttempts := int64(numWorkers * incrementsPerWorker)

	fmt.Printf("\n=== MVCC High Contention Hotspot Test Results ===\n")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Workers: %d\n", numWorkers)
	fmt.Printf("Increments per Worker: %d\n", incrementsPerWorker)
	fmt.Printf("\nResults:\n")
	fmt.Printf("  Total Attempts: %d\n", totalAttempts)
	fmt.Printf("  Successful Increments: %d\n", atomic.LoadInt64(&successfulIncrements))
	fmt.Printf("  Conflicts: %d\n", atomic.LoadInt64(&conflicts))
	fmt.Printf("  Conflict Rate: %.2f%%\n", float64(atomic.LoadInt64(&conflicts))/float64(totalAttempts)*100)
	fmt.Printf("  Final Counter: %d (expected: %d)\n", finalCounter, atomic.LoadInt64(&successfulIncrements))
	fmt.Printf("  Max Version Seen: %d\n", atomic.LoadInt64(&maxVersionSeen))
	fmt.Printf("  Throughput: %.2f increments/sec\n", float64(atomic.LoadInt64(&successfulIncrements))/elapsed.Seconds())

	if int64(finalCounter) != atomic.LoadInt64(&successfulIncrements) {
		t.Errorf("Counter mismatch: final=%d, successful=%d", finalCounter, atomic.LoadInt64(&successfulIncrements))
	}
}

// TestMVCCVersionChainDepth tests how deep version chains can grow under heavy updates
func TestMVCCVersionChainDepth(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE chain_test (
			id INTEGER PRIMARY KEY,
			value INTEGER NOT NULL,
			update_count INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial row
	_, err = db.Exec("INSERT INTO chain_test (id, value, update_count) VALUES (1, 0, 0)")
	if err != nil {
		t.Fatal(err)
	}

	// Parameters
	numUpdaters := 20
	updatesPerWorker := 50

	var totalUpdates int64
	start := time.Now()
	var wg sync.WaitGroup

	// Multiple updaters constantly updating the same row
	for u := 0; u < numUpdaters; u++ {
		wg.Add(1)
		go func(updaterID int) {
			defer wg.Done()

			for i := 0; i < updatesPerWorker; i++ {
				// Use READ COMMITTED for maximum concurrency
				_, err := db.Exec(`
					UPDATE chain_test 
					SET value = value + 1, update_count = update_count + 1 
					WHERE id = 1
				`)
				if err == nil {
					atomic.AddInt64(&totalUpdates, 1)
				}
			}
		}(u)
	}

	// Concurrent readers to keep old versions alive
	ctx, cancel := context.WithCancel(context.Background())
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Long-running transaction to prevent version cleanup
			tx, err := db.BeginTx(ctx, &sql.TxOptions{
				Isolation: sql.LevelSnapshot,
			})
			if err != nil {
				return
			}
			defer tx.Rollback()

			// Keep reading to maintain the transaction
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					var value int
					tx.QueryRow("SELECT value FROM chain_test WHERE id = 1").Scan(&value)
				}
			}
		}(r)
	}

	// Let updaters finish first
	time.Sleep(500 * time.Millisecond)
	cancel() // Stop readers
	wg.Wait()

	elapsed := time.Since(start)

	// Check final state
	var finalValue, finalUpdateCount int
	err = db.QueryRow("SELECT value, update_count FROM chain_test WHERE id = 1").Scan(&finalValue, &finalUpdateCount)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("\n=== MVCC Version Chain Depth Test Results ===\n")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Updaters: %d\n", numUpdaters)
	fmt.Printf("Updates per Worker: %d\n", updatesPerWorker)
	fmt.Printf("\nResults:\n")
	fmt.Printf("  Total Updates: %d\n", atomic.LoadInt64(&totalUpdates))
	fmt.Printf("  Final Value: %d\n", finalValue)
	fmt.Printf("  Final Update Count: %d\n", finalUpdateCount)
	fmt.Printf("  Updates/sec: %.2f\n", float64(atomic.LoadInt64(&totalUpdates))/elapsed.Seconds())
	fmt.Printf("\nNote: Version chains were maintained for concurrent readers\n")
}
