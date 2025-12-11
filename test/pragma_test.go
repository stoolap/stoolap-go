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
	"testing"

	// Import stoolap driver
	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestPragmaCommands tests the PRAGMA statement functionality
func TestPragmaCommands(t *testing.T) {
	// Create an in-memory database for testing
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test cases for different PRAGMA commands
	testCases := []struct {
		name     string
		query    string
		validate func(t *testing.T, rows *sql.Rows)
	}{
		{
			name:  "Read snapshot_interval",
			query: "PRAGMA snapshot_interval",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var snapshotInterval int64
				if err := rows.Scan(&snapshotInterval); err != nil {
					t.Fatalf("Failed to scan snapshot_interval: %v", err)
				}

				t.Logf("Default snapshot_interval: %d", snapshotInterval)
				// Just verify we can read it, actual value may vary
			},
		},
		{
			name:  "Set snapshot_interval",
			query: "PRAGMA snapshot_interval = 60",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var snapshotInterval int64
				if err := rows.Scan(&snapshotInterval); err != nil {
					t.Fatalf("Failed to scan snapshot_interval: %v", err)
				}

				if snapshotInterval != 60 {
					t.Fatalf("Expected snapshot_interval to be 60, got %d", snapshotInterval)
				}
			},
		},
		{
			name:  "Read snapshot_interval after setting",
			query: "PRAGMA snapshot_interval",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var snapshotInterval int64
				if err := rows.Scan(&snapshotInterval); err != nil {
					t.Fatalf("Failed to scan snapshot_interval: %v", err)
				}

				if snapshotInterval != 60 {
					t.Fatalf("Expected snapshot_interval to be 60, got %d", snapshotInterval)
				}
			},
		},
		{
			name:  "Set keep_snapshots",
			query: "PRAGMA keep_snapshots = 10",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var keepSnapshots int64
				if err := rows.Scan(&keepSnapshots); err != nil {
					t.Fatalf("Failed to scan keep_snapshots: %v", err)
				}

				if keepSnapshots != 10 {
					t.Fatalf("Expected keep_snapshots to be 10, got %d", keepSnapshots)
				}
			},
		},
		{
			name:  "Read keep_snapshots",
			query: "PRAGMA keep_snapshots",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var keepSnapshots int64
				if err := rows.Scan(&keepSnapshots); err != nil {
					t.Fatalf("Failed to scan keep_snapshots: %v", err)
				}

				if keepSnapshots != 10 {
					t.Fatalf("Expected keep_snapshots to be 10, got %d", keepSnapshots)
				}
			},
		},
		{
			name:  "Set sync_mode",
			query: "PRAGMA sync_mode = 2",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var syncMode int64
				if err := rows.Scan(&syncMode); err != nil {
					t.Fatalf("Failed to scan sync_mode: %v", err)
				}

				if syncMode != 2 {
					t.Fatalf("Expected sync_mode to be 2, got %d", syncMode)
				}
			},
		},
		{
			name:  "Read sync_mode",
			query: "PRAGMA sync_mode",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var syncMode int64
				if err := rows.Scan(&syncMode); err != nil {
					t.Fatalf("Failed to scan sync_mode: %v", err)
				}

				if syncMode != 2 {
					t.Fatalf("Expected sync_mode to be 2, got %d", syncMode)
				}
			},
		},
		{
			name:  "Set wal_flush_trigger",
			query: "PRAGMA wal_flush_trigger = 1000",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var walFlushTrigger int64
				if err := rows.Scan(&walFlushTrigger); err != nil {
					t.Fatalf("Failed to scan wal_flush_trigger: %v", err)
				}

				if walFlushTrigger != 1000 {
					t.Fatalf("Expected wal_flush_trigger to be 1000, got %d", walFlushTrigger)
				}
			},
		},
		{
			name:  "Read wal_flush_trigger",
			query: "PRAGMA wal_flush_trigger",
			validate: func(t *testing.T, rows *sql.Rows) {
				if !rows.Next() {
					t.Fatalf("Expected at least one row in result")
				}

				var walFlushTrigger int64
				if err := rows.Scan(&walFlushTrigger); err != nil {
					t.Fatalf("Failed to scan wal_flush_trigger: %v", err)
				}

				if walFlushTrigger != 1000 {
					t.Fatalf("Expected wal_flush_trigger to be 1000, got %d", walFlushTrigger)
				}
			},
		},
	}

	// Execute each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.query)
			if err != nil {
				t.Fatalf("Error executing query '%s': %v", tc.query, err)
			}
			defer rows.Close()

			// Validate the result
			tc.validate(t, rows)
		})
	}

	// Test PRAGMA interaction with transactions
	t.Run("PRAGMA and transactions", func(t *testing.T) {
		// First, set a baseline value
		_, err = db.Exec("PRAGMA snapshot_interval = 60")
		if err != nil {
			t.Fatalf("Failed to set baseline snapshot_interval: %v", err)
		}

		// Verify the baseline value
		var initialValue int64
		err = db.QueryRow("PRAGMA snapshot_interval").Scan(&initialValue)
		if err != nil {
			t.Fatalf("Failed to get initial snapshot_interval: %v", err)
		}
		if initialValue != 60 {
			t.Fatalf("Expected initial snapshot_interval to be 60, got %d", initialValue)
		}

		// Start a transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Run regular SQL in the transaction (not PRAGMA)
		_, err = tx.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create table in transaction: %v", err)
		}

		// Commit the transaction
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Now change a PRAGMA setting outside of a transaction
		_, err = db.Exec("PRAGMA snapshot_interval = 90")
		if err != nil {
			t.Fatalf("Failed to set snapshot_interval: %v", err)
		}

		// Verify the change was applied
		var newValue int64
		err = db.QueryRow("PRAGMA snapshot_interval").Scan(&newValue)
		if err != nil {
			t.Fatalf("Failed to read snapshot_interval after setting: %v", err)
		}
		if newValue != 90 {
			t.Fatalf("Expected snapshot_interval to be 90, got %d", newValue)
		}

		// Test that PRAGMA works after transactions
		_, err = db.Exec("PRAGMA snapshot_interval = 120")
		if err != nil {
			t.Fatalf("Failed to set snapshot_interval after transaction: %v", err)
		}

		var finalValue int64
		err = db.QueryRow("PRAGMA snapshot_interval").Scan(&finalValue)
		if err != nil {
			t.Fatalf("Failed to read final snapshot_interval: %v", err)
		}
		if finalValue != 120 {
			t.Fatalf("Expected final snapshot_interval to be 120, got %d", finalValue)
		}
	})
}

// TestPragmaWithPersistentDB tests PRAGMA commands with a persistent database
func TestPragmaWithPersistentDB(t *testing.T) {
	// Skip this test in automated environments since it requires a writable filesystem
	t.Skip("Skip persistent DB test - remove this line to run the test locally")

	// Use the file driver for persistent storage
	db, err := sql.Open("stoolap", "db:///tmp/stoolap_pragma_test")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set a PRAGMA value
	_, err = db.Exec("PRAGMA snapshot_interval = 120")
	if err != nil {
		t.Fatalf("Failed to set PRAGMA snapshot_interval: %v", err)
	}

	// Verify the setting
	var snapshotInterval int64
	err = db.QueryRow("PRAGMA snapshot_interval").Scan(&snapshotInterval)
	if err != nil {
		t.Fatalf("Failed to query PRAGMA snapshot_interval: %v", err)
	}
	if snapshotInterval != 120 {
		t.Errorf("Expected snapshot_interval to be 120, got %d", snapshotInterval)
	}

	// Close the database
	db.Close()

	// Reopen the database and verify the setting was persisted
	db, err = sql.Open("stoolap", "db:///tmp/stoolap_pragma_test")
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Check if the PRAGMA value was persisted
	err = db.QueryRow("PRAGMA snapshot_interval").Scan(&snapshotInterval)
	if err != nil {
		t.Fatalf("Failed to query PRAGMA snapshot_interval after reopen: %v", err)
	}
	if snapshotInterval != 120 {
		t.Errorf("Expected snapshot_interval to be 120 after reopen, got %d", snapshotInterval)
	}

	// Clean up
	// Note: In a real test, we'd clean up the database file, but we're skipping this test by default
}
