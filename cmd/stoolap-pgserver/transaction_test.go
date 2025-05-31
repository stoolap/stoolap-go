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
package main

import (
	"database/sql"
	"testing"

	"github.com/stoolap/stoolap"
)

func TestConnectionStateInit(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Test initial state
	if cs.db != db {
		t.Error("DB not set correctly")
	}

	if cs.currentTx != nil {
		t.Error("Should not have transaction initially")
	}

	if cs.inTransaction {
		t.Error("Should not be in transaction initially")
	}

	if cs.isolationLevel != sql.LevelDefault {
		t.Error("Isolation level should be default")
	}

	if cs.readOnly {
		t.Error("Should not be read-only initially")
	}
}

func TestBeginTransaction(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	tests := []struct {
		name     string
		opts     *sql.TxOptions
		wantErr  bool
		checkISO sql.IsolationLevel
		checkRO  bool
	}{
		{
			name:     "nil options",
			opts:     nil,
			wantErr:  false,
			checkISO: sql.LevelDefault,
			checkRO:  false,
		},
		{
			name: "read committed",
			opts: &sql.TxOptions{
				Isolation: sql.LevelReadCommitted,
			},
			wantErr:  false,
			checkISO: sql.LevelReadCommitted,
			checkRO:  false,
		},
		{
			name: "snapshot",
			opts: &sql.TxOptions{
				Isolation: sql.LevelSnapshot,
			},
			wantErr:  false,
			checkISO: sql.LevelSnapshot,
			checkRO:  false,
		},
		{
			name: "read only",
			opts: &sql.TxOptions{
				ReadOnly: true,
			},
			wantErr:  false,
			checkISO: sql.LevelDefault,
			checkRO:  true,
		},
		{
			name: "snapshot read only",
			opts: &sql.TxOptions{
				Isolation: sql.LevelSnapshot,
				ReadOnly:  true,
			},
			wantErr:  false,
			checkISO: sql.LevelSnapshot,
			checkRO:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean state
			if cs.inTransaction {
				cs.rollbackTransaction()
			}

			err := cs.beginTransaction(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("beginTransaction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if !cs.inTransaction {
					t.Error("Should be in transaction")
				}

				if cs.currentTx == nil {
					t.Error("Should have current transaction")
				}

				if tt.opts != nil {
					if cs.isolationLevel != tt.checkISO {
						t.Errorf("Isolation level = %v, want %v", cs.isolationLevel, tt.checkISO)
					}

					if cs.readOnly != tt.checkRO {
						t.Errorf("ReadOnly = %v, want %v", cs.readOnly, tt.checkRO)
					}
				}

				// Clean up
				cs.rollbackTransaction()
			}
		})
	}
}

func TestNestedBeginTransaction(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Begin first transaction
	err = cs.beginTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Try to begin another transaction - should be allowed (PostgreSQL behavior)
	err = cs.beginTransaction(nil)
	if err != nil {
		t.Error("Nested BEGIN should be allowed")
	}

	// Clean up
	cs.rollbackTransaction()
}

func TestCommitTransaction(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Test commit without transaction
	err = cs.commitTransaction()
	if err != nil {
		t.Error("COMMIT outside transaction should be allowed")
	}

	// Begin transaction
	err = cs.beginTransaction(&sql.TxOptions{
		Isolation: sql.LevelSnapshot,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Commit
	err = cs.commitTransaction()
	if err != nil {
		t.Error("COMMIT should succeed")
	}

	// Check state after commit
	if cs.inTransaction {
		t.Error("Should not be in transaction after commit")
	}

	if cs.currentTx != nil {
		t.Error("Should not have current transaction after commit")
	}

	if cs.readOnly {
		t.Error("ReadOnly should be reset after commit")
	}
}

func TestRollbackTransaction(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Test rollback without transaction
	err = cs.rollbackTransaction()
	if err != nil {
		t.Error("ROLLBACK outside transaction should be allowed")
	}

	// Begin transaction
	err = cs.beginTransaction(&sql.TxOptions{
		Isolation: sql.LevelSnapshot,
		ReadOnly:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rollback
	err = cs.rollbackTransaction()
	if err != nil {
		t.Error("ROLLBACK should succeed")
	}

	// Check state after rollback
	if cs.inTransaction {
		t.Error("Should not be in transaction after rollback")
	}

	if cs.currentTx != nil {
		t.Error("Should not have current transaction after rollback")
	}

	if cs.readOnly {
		t.Error("ReadOnly should be reset after rollback")
	}
}

func TestGetTxStatus(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Test idle status
	if status := cs.getTxStatus(); status != 'I' {
		t.Errorf("getTxStatus() = %c, want 'I'", status)
	}

	// Begin transaction
	err = cs.beginTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Test in transaction status
	if status := cs.getTxStatus(); status != 'T' {
		t.Errorf("getTxStatus() = %c, want 'T'", status)
	}

	// Commit
	cs.commitTransaction()

	// Test idle status again
	if status := cs.getTxStatus(); status != 'I' {
		t.Errorf("getTxStatus() = %c, want 'I'", status)
	}
}

func TestIsInTransaction(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Initially not in transaction
	if cs.isInTransaction() {
		t.Error("Should not be in transaction initially")
	}

	// Begin transaction
	err = cs.beginTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Should be in transaction
	if !cs.isInTransaction() {
		t.Error("Should be in transaction after BEGIN")
	}

	// Rollback
	cs.rollbackTransaction()

	// Should not be in transaction
	if cs.isInTransaction() {
		t.Error("Should not be in transaction after ROLLBACK")
	}
}

func TestParseIsolationLevelFunction(t *testing.T) {
	tests := []struct {
		input    string
		expected sql.IsolationLevel
	}{
		{"READ UNCOMMITTED", sql.LevelReadUncommitted},
		{"READ_UNCOMMITTED", sql.LevelReadUncommitted},
		{"READ COMMITTED", sql.LevelReadCommitted},
		{"READ_COMMITTED", sql.LevelReadCommitted},
		{"REPEATABLE READ", sql.LevelRepeatableRead},
		{"REPEATABLE_READ", sql.LevelRepeatableRead},
		{"SERIALIZABLE", sql.LevelSerializable},
		{"SNAPSHOT", sql.LevelSnapshot},
		{"", sql.LevelDefault},
		{"INVALID", sql.LevelDefault},
		{"read committed", sql.LevelDefault}, // Case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseIsolationLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseIsolationLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestConcurrentTransactions tests concurrent transaction operations
func TestConcurrentTransactions(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)
	done := make(chan bool, 3)

	// Concurrent begin
	go func() {
		err := cs.beginTransaction(nil)
		if err != nil {
			t.Errorf("Concurrent begin error: %v", err)
		}
		done <- true
	}()

	// Concurrent status check
	go func() {
		_ = cs.getTxStatus()
		_ = cs.isInTransaction()
		done <- true
	}()

	// Concurrent rollback
	go func() {
		// Small delay to let begin happen first
		err := cs.rollbackTransaction()
		if err != nil {
			t.Errorf("Concurrent rollback error: %v", err)
		}
		done <- true
	}()

	// Wait for all operations
	for i := 0; i < 3; i++ {
		<-done
	}
}
