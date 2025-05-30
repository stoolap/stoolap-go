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
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestMVCCDeletionByPrimaryKey specifically tests the bug where deleted rows
// were not visible when queried by primary key in SNAPSHOT isolation
func TestMVCCDeletionByPrimaryKey(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test_pk_deletion (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test_pk_deletion VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Start T1 with SNAPSHOT isolation
	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}
	defer tx1.Rollback()

	// T1: Verify initial state
	var count int
	err = tx1.QueryRow("SELECT COUNT(*) FROM test_pk_deletion").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 3 {
		t.Fatalf("Expected 3 rows initially, got %d", count)
	}

	// Delete Bob in a separate transaction
	tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin tx2: %v", err)
	}

	_, err = tx2.Exec("DELETE FROM test_pk_deletion WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	err = tx2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit deletion: %v", err)
	}

	// T1: Should still see all 3 rows
	err = tx1.QueryRow("SELECT COUNT(*) FROM test_pk_deletion").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count after deletion: %v", err)
	}
	if count != 3 {
		t.Errorf("SNAPSHOT: Expected 3 rows, got %d", count)
	}

	// T1: Query by primary key - this was the failing case
	var name string
	err = tx1.QueryRow("SELECT name FROM test_pk_deletion WHERE id = 2").Scan(&name)
	if err == sql.ErrNoRows {
		t.Error("SNAPSHOT: Should be able to see deleted row when querying by primary key")
	} else if err != nil {
		t.Fatalf("Failed to query by primary key: %v", err)
	} else if name != "Bob" {
		t.Errorf("Expected name 'Bob', got '%s'", name)
	} else {
		t.Log("SUCCESS: Can see deleted row 'Bob' when querying by primary key")
	}

	// T1: Also verify with other query patterns
	rows, err := tx1.Query("SELECT id, name FROM test_pk_deletion ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query all rows: %v", err)
	}
	defer rows.Close()

	expectedRows := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(expectedRows) {
			t.Error("Too many rows returned")
			break
		}

		if id != expectedRows[i].id || name != expectedRows[i].name {
			t.Errorf("Row %d: expected (%d, %s), got (%d, %s)",
				i, expectedRows[i].id, expectedRows[i].name, id, name)
		}
		i++
	}

	if i != len(expectedRows) {
		t.Errorf("Expected %d rows, got %d", len(expectedRows), i)
	}

	tx1.Commit()

	// New transaction should NOT see Bob
	tx3, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatalf("Failed to begin tx3: %v", err)
	}
	defer tx3.Rollback()

	err = tx3.QueryRow("SELECT name FROM test_pk_deletion WHERE id = 2").Scan(&name)
	if err != sql.ErrNoRows {
		t.Error("New transaction should NOT see deleted row")
	}

	var count3 int
	err = tx3.QueryRow("SELECT COUNT(*) FROM test_pk_deletion").Scan(&count3)
	if err != nil {
		t.Fatalf("Failed to count in new transaction: %v", err)
	}
	if count3 != 2 {
		t.Errorf("New transaction should see 2 rows, got %d", count3)
	}
}
