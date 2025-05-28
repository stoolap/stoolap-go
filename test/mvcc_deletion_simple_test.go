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
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestSimpleDeletionVisibility tests basic deletion visibility across isolation levels
func TestSimpleDeletionVisibility(t *testing.T) {
	t.Run("ReadCommitted", func(t *testing.T) {
		db, err := sql.Open("stoolap", "memory://")
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create and populate table
		_, err = db.Exec("CREATE TABLE test_rc (id INTEGER PRIMARY KEY, value TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		_, err = db.Exec("INSERT INTO test_rc VALUES (1, 'one'), (2, 'two'), (3, 'three')")
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Start transaction T1 (reader)
		tx1, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin tx1: %v", err)
		}
		defer tx1.Rollback()

		// T1: Count rows before deletion
		var countBefore int
		err = tx1.QueryRow("SELECT COUNT(*) FROM test_rc").Scan(&countBefore)
		if err != nil {
			t.Fatalf("Failed to count before: %v", err)
		}
		t.Logf("T1: Before deletion, sees %d rows", countBefore)

		// Start transaction T2 (deleter)
		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin tx2: %v", err)
		}

		// T2: Delete row with id=2
		_, err = tx2.Exec("DELETE FROM test_rc WHERE id = 2")
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		// T2: Commit the deletion
		err = tx2.Commit()
		if err != nil {
			t.Fatalf("Failed to commit tx2: %v", err)
		}
		t.Log("T2: Committed deletion of id=2")

		// T1: Count rows after deletion (should see the deletion in READ COMMITTED)
		var countAfter int
		err = tx1.QueryRow("SELECT COUNT(*) FROM test_rc").Scan(&countAfter)
		if err != nil {
			t.Fatalf("Failed to count after: %v", err)
		}
		t.Logf("T1: After deletion, sees %d rows", countAfter)

		if countAfter != 2 {
			t.Errorf("READ COMMITTED: Expected T1 to see 2 rows after commit, got %d", countAfter)
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		db, err := sql.Open("stoolap", "memory://")
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create and populate table
		_, err = db.Exec("CREATE TABLE test_snap (id INTEGER PRIMARY KEY, value TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		_, err = db.Exec("INSERT INTO test_snap VALUES (1, 'one'), (2, 'two'), (3, 'three')")
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Start transaction T1 (reader)
		tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
		if err != nil {
			t.Fatalf("Failed to begin tx1: %v", err)
		}
		defer tx1.Rollback()

		// Get transaction ID for debugging
		var tx1ID int
		err = tx1.QueryRow("SELECT 1").Scan(&tx1ID) // This forces the transaction to start
		if err != nil {
			t.Fatalf("Failed to get tx1 ID: %v", err)
		}

		// T1: Count rows before deletion
		var countBefore int
		err = tx1.QueryRow("SELECT COUNT(*) FROM test_snap").Scan(&countBefore)
		if err != nil {
			t.Fatalf("Failed to count before: %v", err)
		}
		t.Logf("T1: Before deletion, sees %d rows", countBefore)

		// Start transaction T2 (deleter)
		tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
		if err != nil {
			t.Fatalf("Failed to begin tx2: %v", err)
		}

		// T2: Delete row with id=2
		_, err = tx2.Exec("DELETE FROM test_snap WHERE id = 2")
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		// T2: Commit the deletion
		err = tx2.Commit()
		if err != nil {
			t.Fatalf("Failed to commit tx2: %v", err)
		}
		t.Log("T2: Committed deletion of id=2")

		// T1: Count rows after deletion (should NOT see the deletion in SNAPSHOT)
		var countAfter int
		err = tx1.QueryRow("SELECT COUNT(*) FROM test_snap").Scan(&countAfter)
		if err != nil {
			t.Fatalf("Failed to count after: %v", err)
		}
		t.Logf("T1: After deletion, sees %d rows", countAfter)

		// Also check which rows are visible
		rows, err := tx1.Query("SELECT id FROM test_snap ORDER BY id")
		if err != nil {
			t.Fatalf("Failed to query rows: %v", err)
		}
		var visibleIDs []int
		for rows.Next() {
			var id int
			rows.Scan(&id)
			visibleIDs = append(visibleIDs, id)
		}
		rows.Close()
		t.Logf("T1: Visible IDs: %v", visibleIDs)

		if countAfter != 3 {
			t.Errorf("SNAPSHOT: Expected T1 to still see 3 rows after commit, got %d", countAfter)
		}

		// T1: Verify we can still see the deleted row
		var value string
		err = tx1.QueryRow("SELECT value FROM test_snap WHERE id = 2").Scan(&value)
		if err == sql.ErrNoRows {
			t.Error("SNAPSHOT: T1 should still see the deleted row with id=2")

			// Let's also try without using the primary key
			var count int
			err2 := tx1.QueryRow("SELECT COUNT(*) FROM test_snap WHERE value = 'two'").Scan(&count)
			if err2 != nil {
				t.Fatalf("Failed to count by value: %v", err2)
			}
			t.Logf("T1: COUNT(*) WHERE value='two' returns %d", count)

			// Try different queries to understand the issue
			rows2, err2 := tx1.Query("SELECT id, value FROM test_snap WHERE id >= 2 AND id <= 2")
			if err2 != nil {
				t.Fatalf("Failed to query with range: %v", err2)
			}
			var foundRows []string
			for rows2.Next() {
				var id int
				var val string
				rows2.Scan(&id, &val)
				foundRows = append(foundRows, fmt.Sprintf("id=%d,value=%s", id, val))
			}
			rows2.Close()
			t.Logf("T1: WHERE id >= 2 AND id <= 2 returns: %v", foundRows)
		} else if err != nil {
			t.Fatalf("Failed to query deleted row: %v", err)
		} else {
			t.Logf("T1: Can still see deleted row with value='%s'", value)
		}
	})
}

// TestDeletionVisibilityDebug provides detailed visibility information
func TestDeletionVisibilityDebug(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set SNAPSHOT isolation
	_, err = db.Exec("SET ISOLATIONLEVEL = 'SNAPSHOT'")
	if err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test_debug (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_debug VALUES (1, 'one'), (2, 'two')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Start T1
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}
	defer tx1.Rollback()

	// T1: Read initial state
	rows, err := tx1.Query("SELECT id, value FROM test_debug ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	t.Log("T1 initial view:")
	for rows.Next() {
		var id int
		var value string
		rows.Scan(&id, &value)
		t.Logf("  id=%d, value=%s", id, value)
	}
	rows.Close()

	// Delete in separate transaction
	_, err = db.Exec("DELETE FROM test_debug WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	t.Log("Deleted id=2 in separate transaction")

	// T1: Read again
	rows, err = tx1.Query("SELECT id, value FROM test_debug ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	t.Log("T1 view after deletion:")
	count := 0
	for rows.Next() {
		var id int
		var value string
		rows.Scan(&id, &value)
		t.Logf("  id=%d, value=%s", id, value)
		count++
	}
	rows.Close()

	if count != 2 {
		t.Errorf("SNAPSHOT: T1 should still see 2 rows, but sees %d", count)
	}

	// New transaction should see deletion
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin tx3: %v", err)
	}
	defer tx3.Rollback()

	var count3 int
	err = tx3.QueryRow("SELECT COUNT(*) FROM test_debug").Scan(&count3)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	t.Logf("T3 (new transaction) sees %d rows", count3)

	if count3 != 1 {
		t.Errorf("New transaction should see 1 row after deletion, got %d", count3)
	}
}
