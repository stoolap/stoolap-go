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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stoolap/stoolap"
)

// TestOldSnapshotWithWAL tests the scenario where:
// 1. User has an old snapshot with 100 rows
// 2. User deletes all rows (recorded in WAL)
// 3. No new snapshot is created before exit
// 4. On restart, system loads old snapshot + replays WAL deletions
func TestOldSnapshotWithWAL(t *testing.T) {
	ctx := context.Background()

	// Use a temporary directory for the test database
	tempDir := t.TempDir()

	// Don't use snapshot interval, we'll manually trigger snapshots
	dbPath := fmt.Sprintf("file://%s", tempDir)

	// Session 1: Create data and wait for snapshot
	t.Run("Session1_CreateSnapshot", func(t *testing.T) {
		db, err := stoolap.Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		// Create table
		_, err = db.Exec(ctx, `CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
              )`)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert 100 users
		t.Logf("Inserting 100 users...")
		for i := 1; i <= 100; i++ {
			_, err = db.Exec(ctx, fmt.Sprintf(
				"INSERT INTO users (id, name, email) VALUES (%d, 'User %d', 'user%d@example.com')",
				i, i, i,
			))
			if err != nil {
				t.Fatalf("Failed to insert user %d: %v", i, err)
			}
		}

		// Manually create snapshot
		t.Logf("Creating snapshot...")
		if err := db.Engine().CreateSnapshot(); err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		// Verify snapshot exists
		snapshots, _ := filepath.Glob(filepath.Join(tempDir, "users", "snapshot-*.bin"))
		if len(snapshots) == 0 {
			t.Fatalf("No snapshot created")
		}
		t.Logf("Snapshot created: %d snapshot(s)", len(snapshots))

		// Close database
		db.Close()

		// Clear WAL to simulate clean state
		walDir := filepath.Join(tempDir, "wal")
		os.RemoveAll(walDir)
		t.Logf("Cleared WAL directory to simulate clean state")
	})

	// Session 2: Delete data WITHOUT creating new snapshot
	t.Run("Session2_DeleteWithoutSnapshot", func(t *testing.T) {
		// Use default snapshot interval (5 minutes) so no new snapshot
		dbPath = fmt.Sprintf("file://%s", tempDir)

		db, err := stoolap.Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		// Verify initial state (from snapshot)
		var count int
		row := db.QueryRow(ctx, "SELECT COUNT(*) FROM users")
		if err := row.Scan(&count); err != nil {
			t.Fatalf("Failed to count: %v", err)
		}
		t.Logf("Starting with %d users from snapshot", count)

		// Delete all users
		t.Logf("Deleting all users...")
		result, err := db.Exec(ctx, "DELETE FROM users")
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()
		t.Logf("Deleted %d rows", rowsAffected)

		// Verify deletion in current session
		row = db.QueryRow(ctx, "SELECT COUNT(*) FROM users")
		if err := row.Scan(&count); err != nil {
			t.Fatalf("Failed to count after delete: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 users after delete, got %d", count)
		}

		// Close WITHOUT waiting for new snapshot
		t.Logf("Closing database (no new snapshot should be created)...")
		db.Close()

		// Verify we still have old snapshot and new WAL
		snapshots, _ := filepath.Glob(filepath.Join(tempDir, "users", "snapshot-*.bin"))
		walFiles, _ := filepath.Glob(filepath.Join(tempDir, "wal", "wal-*.log"))
		t.Logf("After close - Snapshots: %d, WAL files: %d", len(snapshots), len(walFiles))

		if len(snapshots) != 1 {
			t.Errorf("Expected 1 old snapshot, found %d", len(snapshots))
		}

		if len(walFiles) == 0 {
			t.Errorf("Expected WAL file with deletions, found none")
		}

		// Show file sizes
		for _, s := range snapshots {
			info, _ := os.Stat(s)
			t.Logf("  Snapshot: %s (size: %d bytes)", filepath.Base(s), info.Size())
		}
		for _, w := range walFiles {
			info, _ := os.Stat(w)
			t.Logf("  WAL: %s (size: %d bytes)", filepath.Base(w), info.Size())
		}
	})

	// Session 3: Reopen and check if deletions persist
	t.Run("Session3_LoadSnapshotAndWAL", func(t *testing.T) {
		t.Logf("Reopening database - should load old snapshot + replay WAL deletions...")

		dbPath = fmt.Sprintf("file://%s", tempDir)
		db, err := stoolap.Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Count all rows
		var count int
		row := db.QueryRow(ctx, "SELECT COUNT(*) FROM users")
		if err := row.Scan(&count); err != nil {
			t.Fatalf("Failed to count: %v", err)
		}

		// Should be 0 (snapshot has 100, but WAL has DELETE operations)
		if count != 0 {
			t.Errorf("BUG: Expected 0 users, got %d - snapshot+WAL replay failed!", count)

			// Show which rows exist
			rows, err := db.Query(ctx, "SELECT id, name FROM users ORDER BY id LIMIT 10")
			if err == nil {
				defer rows.Close()
				t.Logf("Rows that should not exist:")
				for rows.Next() {
					var id int
					var name string
					rows.Scan(&id, &name)
					t.Logf("  id=%d, name=%s (SHOULD BE DELETED!)", id, name)
				}
			}
		} else {
			t.Logf("Success: Found %d users (WAL deletions correctly applied over snapshot)", count)
		}
	})
}
