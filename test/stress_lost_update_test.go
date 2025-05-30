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

// TestStressLostUpdate aggressively tests for lost updates
func TestStressLostUpdate(t *testing.T) {
	for round := 0; round < 10; round++ {
		db, err := stoolap.Open("memory://")
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		err = db.Engine().SetIsolationLevel(storage.SnapshotIsolation)
		if err != nil {
			t.Fatal(err)
		}

		// Create table
		_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
		if err != nil {
			t.Fatal(err)
		}

		// Insert initial data
		_, err = db.Exec(ctx, "INSERT INTO test (id, value) VALUES (1, 0)")
		if err != nil {
			t.Fatal(err)
		}

		// Run 100 concurrent increments
		const numWorkers = 100
		var wg sync.WaitGroup
		successCount := 0
		var mu sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				tx, err := db.Begin()
				if err != nil {
					return
				}

				// Read current value
				var currentValue int
				rows, err := tx.QueryContext(ctx, "SELECT value FROM test WHERE id = 1")
				if err != nil {
					tx.Rollback()
					return
				}
				if rows.Next() {
					rows.Scan(&currentValue)
					rows.Close()
				} else {
					rows.Close()
					tx.Rollback()
					return
				}

				// Update with increment
				_, err = tx.ExecContext(ctx,
					"UPDATE test SET value = ? WHERE id = 1",
					driver.NamedValue{Ordinal: 1, Value: currentValue + 1})
				if err != nil {
					tx.Rollback()
					return
				}

				// Try to commit
				err = tx.Commit()
				if err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		// Check final value
		var finalValue int
		rows, _ := db.Query(ctx, "SELECT value FROM test WHERE id = 1")
		if rows.Next() {
			rows.Scan(&finalValue)
			rows.Close()
		}

		fmt.Printf("Round %d: %d successful commits, final value = %d",
			round+1, successCount, finalValue)

		if finalValue != successCount {
			fmt.Printf(" - LOST %d UPDATES!\n", successCount-finalValue)
			t.Errorf("Round %d: Lost updates detected! Final value %d != successful commits %d",
				round+1, finalValue, successCount)
		} else {
			fmt.Printf(" - OK\n")
		}

		db.Close()
	}
}
