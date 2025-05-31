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
	"sync"

	"github.com/stoolap/stoolap"
)

// connectionState tracks the state of a PostgreSQL connection
type connectionState struct {
	mu             sync.RWMutex
	db             *stoolap.DB
	currentTx      stoolap.Tx
	inTransaction  bool
	isolationLevel sql.IsolationLevel
	readOnly       bool
}

// newConnectionState creates a new connection state
func newConnectionState(db *stoolap.DB) *connectionState {
	return &connectionState{
		db:             db,
		isolationLevel: sql.LevelDefault,
	}
}

// beginTransaction starts a new transaction
func (cs *connectionState) beginTransaction(opts *sql.TxOptions) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.inTransaction {
		// PostgreSQL allows nested BEGIN - just warn and continue
		return nil
	}

	tx, err := cs.db.BeginTx(nil, opts)
	if err != nil {
		return err
	}

	cs.currentTx = tx
	cs.inTransaction = true
	if opts != nil {
		cs.isolationLevel = opts.Isolation
		cs.readOnly = opts.ReadOnly
	}

	return nil
}

// commitTransaction commits the current transaction
func (cs *connectionState) commitTransaction() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !cs.inTransaction {
		return nil // PostgreSQL allows COMMIT outside transaction
	}

	err := cs.currentTx.Commit()
	cs.currentTx = nil
	cs.inTransaction = false
	cs.readOnly = false

	return err
}

// rollbackTransaction rolls back the current transaction
func (cs *connectionState) rollbackTransaction() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !cs.inTransaction {
		return nil // PostgreSQL allows ROLLBACK outside transaction
	}

	err := cs.currentTx.Rollback()
	cs.currentTx = nil
	cs.inTransaction = false
	cs.readOnly = false

	return err
}

// getTxStatus returns the PostgreSQL transaction status character
func (cs *connectionState) getTxStatus() byte {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.inTransaction {
		return 'T' // In transaction
	}
	return 'I' // Idle
}

// isInTransaction returns whether we're in a transaction
func (cs *connectionState) isInTransaction() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.inTransaction
}

// parseIsolationLevel converts PostgreSQL isolation level string to sql.IsolationLevel
func parseIsolationLevel(level string) sql.IsolationLevel {
	switch level {
	case "READ UNCOMMITTED", "READ_UNCOMMITTED":
		return sql.LevelReadUncommitted
	case "READ COMMITTED", "READ_COMMITTED":
		return sql.LevelReadCommitted
	case "REPEATABLE READ", "REPEATABLE_READ":
		return sql.LevelRepeatableRead
	case "SERIALIZABLE":
		return sql.LevelSerializable
	case "SNAPSHOT":
		return sql.LevelSnapshot
	default:
		return sql.LevelDefault
	}
}
