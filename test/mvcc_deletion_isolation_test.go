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

// TestMVCCDeletionReadCommitted tests deletion visibility in READ COMMITTED isolation
func TestMVCCDeletionReadCommitted(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test_delete_rc (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = tx1.Exec("INSERT INTO test_delete_rc (id, name, value) VALUES (?, ?, ?)",
			i, fmt.Sprintf("item_%d", i), i*10)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit insert transaction: %v", err)
	}

	// Start a reader transaction BEFORE deletion
	txReader1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin reader transaction: %v", err)
	}

	// Count rows before deletion
	var count1 int
	err = txReader1.QueryRow("SELECT COUNT(*) FROM test_delete_rc").Scan(&count1)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count1 != 5 {
		t.Fatalf("Expected 5 rows before deletion, got %d", count1)
	}

	// Delete rows in another transaction
	txDeleter, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}

	// Delete rows where id IN (2, 4)
	result, err := txDeleter.Exec("DELETE FROM test_delete_rc WHERE id IN (2, 4)")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("Expected to delete 2 rows, deleted %d", deleted)
	}

	// Commit the deletion
	err = txDeleter.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}

	// In READ COMMITTED, the reader should immediately see the deletion
	// even though it started before the deletion
	var count2 int
	err = txReader1.QueryRow("SELECT COUNT(*) FROM test_delete_rc").Scan(&count2)
	if err != nil {
		t.Fatalf("Failed to count rows after deletion: %v", err)
	}
	if count2 != 3 {
		t.Errorf("READ COMMITTED: Expected 3 rows after deletion (should see committed changes), got %d", count2)
	}

	// Verify which rows remain
	rows, err := txReader1.Query("SELECT id FROM test_delete_rc ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query remaining rows: %v", err)
	}
	defer rows.Close()

	expectedIDs := []int{1, 3, 5}
	i := 0
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("Failed to scan id: %v", err)
		}
		if i >= len(expectedIDs) {
			t.Errorf("Too many rows returned")
			break
		}
		if id != expectedIDs[i] {
			t.Errorf("Expected id %d, got %d", expectedIDs[i], id)
		}
		i++
	}
	if i < len(expectedIDs) {
		t.Errorf("Expected %d rows, got %d", len(expectedIDs), i)
	}

	txReader1.Commit()

	// Start a new reader after deletion
	txReader2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin reader transaction 2: %v", err)
	}

	var count3 int
	err = txReader2.QueryRow("SELECT COUNT(*) FROM test_delete_rc").Scan(&count3)
	if err != nil {
		t.Fatalf("Failed to count rows in new transaction: %v", err)
	}
	if count3 != 3 {
		t.Errorf("New transaction: Expected 3 rows, got %d", count3)
	}

	txReader2.Commit()
}

// TestMVCCDeletionSnapshot tests deletion visibility in SNAPSHOT isolation
func TestMVCCDeletionSnapshot(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test_delete_snapshot (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = tx1.Exec("INSERT INTO test_delete_snapshot (id, name, value) VALUES (?, ?, ?)",
			i, fmt.Sprintf("item_%d", i), i*10)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit insert transaction: %v", err)
	}

	// Start a reader transaction BEFORE deletion
	txReader1, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin reader transaction: %v", err)
	}

	// Count rows before deletion
	var count1 int
	err = txReader1.QueryRow("SELECT COUNT(*) FROM test_delete_snapshot").Scan(&count1)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count1 != 5 {
		t.Fatalf("Expected 5 rows before deletion, got %d", count1)
	}

	// Delete rows in another transaction
	txDeleter, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}

	// Delete rows where id IN (2, 4)
	result, err := txDeleter.Exec("DELETE FROM test_delete_snapshot WHERE id IN (2, 4)")
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("Expected to delete 2 rows, deleted %d", deleted)
	}

	// Commit the deletion
	err = txDeleter.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}

	// In SNAPSHOT isolation, the reader should NOT see the deletion
	// because it started before the deletion
	var count2 int
	err = txReader1.QueryRow("SELECT COUNT(*) FROM test_delete_snapshot").Scan(&count2)
	if err != nil {
		t.Fatalf("Failed to count rows after deletion: %v", err)
	}
	if count2 != 5 {
		// Let's check which rows are visible
		rows2, err := txReader1.Query("SELECT id FROM test_delete_snapshot ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query rows for debug: %v", err)
		}
		var visibleIDs []int
		for rows2.Next() {
			var id int
			rows2.Scan(&id)
			visibleIDs = append(visibleIDs, id)
		}
		rows2.Close()
		t.Errorf("SNAPSHOT: Expected 5 rows (should NOT see deletion), got %d. Visible IDs: %v", count2, visibleIDs)
	}

	// Verify all original rows are still visible
	rows, err := txReader1.Query("SELECT id FROM test_delete_snapshot ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query rows: %v", err)
	}
	defer rows.Close()

	expectedIDs := []int{1, 2, 3, 4, 5}
	i := 0
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("Failed to scan id: %v", err)
		}
		if i >= len(expectedIDs) {
			t.Errorf("Too many rows returned")
			break
		}
		if id != expectedIDs[i] {
			t.Errorf("Expected id %d, got %d", expectedIDs[i], id)
		}
		i++
	}
	if i < len(expectedIDs) {
		t.Errorf("SNAPSHOT: Expected %d rows, got %d", len(expectedIDs), i)
	}

	txReader1.Commit()

	// Start a new reader after deletion
	txReader2, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin reader transaction 2: %v", err)
	}

	var count3 int
	err = txReader2.QueryRow("SELECT COUNT(*) FROM test_delete_snapshot").Scan(&count3)
	if err != nil {
		t.Fatalf("Failed to count rows in new transaction: %v", err)
	}
	if count3 != 3 {
		t.Errorf("New transaction: Expected 3 rows (should see deletion), got %d", count3)
	}

	// Verify which rows remain for new transaction
	rows2, err := txReader2.Query("SELECT id FROM test_delete_snapshot ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query remaining rows: %v", err)
	}
	defer rows2.Close()

	expectedRemainingIDs := []int{1, 3, 5}
	j := 0
	for rows2.Next() {
		var id int
		err = rows2.Scan(&id)
		if err != nil {
			t.Fatalf("Failed to scan id: %v", err)
		}
		if j >= len(expectedRemainingIDs) {
			t.Errorf("Too many rows returned")
			break
		}
		if id != expectedRemainingIDs[j] {
			t.Errorf("Expected id %d, got %d", expectedRemainingIDs[j], id)
		}
		j++
	}
	if j < len(expectedRemainingIDs) {
		t.Errorf("Expected %d rows, got %d", len(expectedRemainingIDs), j)
	}

	txReader2.Commit()
}

// TestMVCCDeletionConcurrent tests concurrent deletion scenarios
func TestMVCCDeletionConcurrent(t *testing.T) {
	testCases := []struct {
		name      string
		isolation sql.IsolationLevel
	}{
		{"ReadCommitted", sql.LevelReadCommitted},
		{"Snapshot", sql.LevelSnapshot},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Create table
			tableName := fmt.Sprintf("test_concurrent_%s", tc.name)
			_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, grp TEXT, value INTEGER)", tableName))
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Insert test data
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tc.isolation})
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Insert 10 rows in 2 groups
			for i := 1; i <= 10; i++ {
				group := "A"
				if i%2 == 0 {
					group = "B"
				}
				_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (id, grp, value) VALUES (?, ?, ?)", tableName),
					i, group, i*10)
				if err != nil {
					t.Fatalf("Failed to insert row %d: %v", i, err)
				}
			}
			err = tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Run concurrent operations
			var wg sync.WaitGroup
			results := make(chan string, 10)

			// Reader that counts group A rows continuously
			wg.Add(1)
			go func() {
				defer wg.Done()

				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tc.isolation})
				if err != nil {
					results <- fmt.Sprintf("Reader error starting transaction: %v", err)
					return
				}
				defer tx.Commit()

				// Count group A rows multiple times
				for i := 0; i < 5; i++ {
					var count int
					err = tx.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE grp = 'A'", tableName)).Scan(&count)
					if err != nil {
						results <- fmt.Sprintf("Reader error: %v", err)
						return
					}

					results <- fmt.Sprintf("Reader iteration %d: Group A has %d rows", i+1, count)
					time.Sleep(10 * time.Millisecond)
				}
			}()

			// Deleter that removes group B rows
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Wait a bit to ensure reader has started
				time.Sleep(20 * time.Millisecond)

				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tc.isolation})
				if err != nil {
					results <- fmt.Sprintf("Deleter error starting transaction: %v", err)
					return
				}

				result, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE grp = 'B'", tableName))
				if err != nil {
					results <- fmt.Sprintf("Deleter error: %v", err)
					tx.Rollback()
					return
				}

				deleted, _ := result.RowsAffected()
				results <- fmt.Sprintf("Deleter: Deleted %d rows from group B", deleted)

				err = tx.Commit()
				if err != nil {
					results <- fmt.Sprintf("Deleter commit error: %v", err)
					return
				}
				results <- "Deleter: Committed"
			}()

			// Wait for all goroutines
			wg.Wait()
			close(results)

			// Collect and verify results
			t.Logf("\n=== Results for %s ===", tc.name)
			for result := range results {
				t.Log(result)
			}

			// Final verification
			tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tc.isolation})
			if err != nil {
				t.Fatalf("Failed to begin verification transaction: %v", err)
			}

			// Count remaining rows
			var totalCount int
			err = tx2.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&totalCount)
			if err != nil {
				t.Fatalf("Failed to count total rows: %v", err)
			}
			t.Logf("Final total count: %d rows", totalCount)

			// Group A should still have 5 rows
			var groupACount int
			err = tx2.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE grp = 'A'", tableName)).Scan(&groupACount)
			if err != nil {
				t.Fatalf("Failed to count group A: %v", err)
			}
			if groupACount != 5 {
				t.Errorf("Expected 5 rows in group A, got %d", groupACount)
			}

			// Group B should have 0 rows
			var groupBCount int
			err = tx2.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE grp = 'B'", tableName)).Scan(&groupBCount)
			if err != nil {
				t.Fatalf("Failed to count group B: %v", err)
			}
			if groupBCount != 0 {
				t.Errorf("Expected 0 rows in group B, got %d", groupBCount)
			}

			tx2.Commit()
		})
	}
}

// TestMVCCDeletionRollback tests rollback behavior with deletions
func TestMVCCDeletionRollback(t *testing.T) {
	testCases := []struct {
		name      string
		isolation string
	}{
		{"ReadCommitted", "READ COMMITTED"},
		{"Snapshot", "SNAPSHOT"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("stoolap", "memory://")
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Set isolation level
			_, err = db.Exec(fmt.Sprintf("SET ISOLATIONLEVEL = '%s'", tc.isolation))
			if err != nil {
				t.Fatalf("Failed to set isolation level: %v", err)
			}

			// Create table
			tableName := fmt.Sprintf("test_rollback_%s", tc.name)
			_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, data TEXT)", tableName))
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Insert test data
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			for i := 1; i <= 3; i++ {
				_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES (?, ?)", tableName),
					i, fmt.Sprintf("data_%d", i))
				if err != nil {
					t.Fatalf("Failed to insert row %d: %v", i, err)
				}
			}
			err = tx.Commit()
			if err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Start a transaction that will delete and rollback
			txDelete, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin delete transaction: %v", err)
			}

			// Delete row with id=2
			result, err := txDelete.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))
			if err != nil {
				t.Fatalf("Failed to delete: %v", err)
			}
			deleted, _ := result.RowsAffected()
			if deleted != 1 {
				t.Fatalf("Expected to delete 1 row, deleted %d", deleted)
			}

			// Within the same transaction, the row should not be visible
			var count int
			err = txDelete.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count rows: %v", err)
			}
			if count != 2 {
				t.Errorf("Within deleting transaction: Expected 2 rows, got %d", count)
			}

			// Start another transaction while delete is uncommitted
			txReader, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin reader transaction: %v", err)
			}

			// The reader should see all 3 rows (delete not committed)
			var readerCount int
			err = txReader.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&readerCount)
			if err != nil {
				t.Fatalf("Failed to count rows in reader: %v", err)
			}
			if readerCount != 3 {
				t.Errorf("Reader during uncommitted delete: Expected 3 rows, got %d", readerCount)
			}

			// Rollback the deletion
			err = txDelete.Rollback()
			if err != nil {
				t.Fatalf("Failed to rollback: %v", err)
			}

			// After rollback, reader should still see all 3 rows
			var readerCount2 int
			err = txReader.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&readerCount2)
			if err != nil {
				t.Fatalf("Failed to count rows after rollback: %v", err)
			}
			if readerCount2 != 3 {
				t.Errorf("Reader after rollback: Expected 3 rows, got %d", readerCount2)
			}

			txReader.Commit()

			// New transaction should see all 3 rows
			txFinal, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin final transaction: %v", err)
			}

			var finalCount int
			err = txFinal.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&finalCount)
			if err != nil {
				t.Fatalf("Failed to count final rows: %v", err)
			}
			if finalCount != 3 {
				t.Errorf("After rollback: Expected 3 rows, got %d", finalCount)
			}

			// Verify row 2 is still there
			var data string
			err = txFinal.QueryRow(fmt.Sprintf("SELECT data FROM %s WHERE id = 2", tableName)).Scan(&data)
			if err != nil {
				t.Errorf("Failed to find row 2 after rollback: %v", err)
			} else if data != "data_2" {
				t.Errorf("Row 2 has incorrect data: %s", data)
			}

			txFinal.Commit()
		})
	}
}

// TestMVCCDeletionWithUpdate tests deletion visibility when combined with updates
func TestMVCCDeletionWithUpdate(t *testing.T) {
	t.Skip("Skipping test for now, the MVCC only keep the single version of the row")
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test_delete_update (id INTEGER PRIMARY KEY, version INTEGER, status TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx1.Exec("INSERT INTO test_delete_update (id, version, status) VALUES (1, 1, 'active')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Start long-running reader
	txReader, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin reader transaction: %v", err)
	}

	// Verify initial state
	rows, err := txReader.Query("SELECT id, version, status FROM test_delete_update")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	count := 0
	for rows.Next() {
		var id, version int
		var status string
		err = rows.Scan(&id, &version, &status)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		t.Logf("Reader sees: id=%d, version=%d, status=%s", id, version, status)
		count++
	}
	rows.Close()
	if count != 1 {
		t.Fatalf("Expected 1 row initially, got %d", count)
	}

	// Update the row in another transaction
	txUpdate, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin update transaction: %v", err)
	}

	result, err := txUpdate.Exec("UPDATE test_delete_update SET version = 2, status = 'updated' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		t.Fatalf("Expected to update 1 row, updated %d", updated)
	}
	err = txUpdate.Commit()
	if err != nil {
		t.Fatalf("Failed to commit update: %v", err)
	}

	// Delete the row in yet another transaction
	txDelete, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}

	result, err = txDelete.Exec("DELETE FROM test_delete_update WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	deleted, _ := result.RowsAffected()
	if deleted != 1 {
		t.Fatalf("Expected to delete 1 row, deleted %d", deleted)
	}
	err = txDelete.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete: %v", err)
	}

	// The long-running reader should still see the original version
	rows2, err := txReader.Query("SELECT id, version, status FROM test_delete_update")
	if err != nil {
		t.Fatalf("Failed to query again: %v", err)
	}
	count2 := 0
	for rows2.Next() {
		var id, version int
		var status string
		err = rows2.Scan(&id, &version, &status)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		t.Logf("Reader still sees: id=%d, version=%d, status=%s", id, version, status)

		// Should see the original values
		if version != 1 {
			t.Errorf("Expected version=1, got %d", version)
		}
		if status != "active" {
			t.Errorf("Expected status=active, got %s", status)
		}
		count2++
	}
	rows2.Close()

	if count2 != 1 {
		t.Errorf("SNAPSHOT: Long-running reader should still see 1 row, got %d", count2)
	}

	txReader.Commit()

	// New transaction should see no rows (deleted)
	txFinal, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin final transaction: %v", err)
	}

	var finalCount int
	err = txFinal.QueryRow("SELECT COUNT(*) FROM test_delete_update").Scan(&finalCount)
	if err != nil {
		t.Fatalf("Failed to count final rows: %v", err)
	}
	if finalCount != 0 {
		t.Errorf("New transaction should see 0 rows (deleted), got %d", finalCount)
	}

	txFinal.Commit()
}
