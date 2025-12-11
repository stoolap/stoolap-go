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
package driver

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Tx represents a transaction
type Tx struct {
	conn     *Conn
	ctx      context.Context
	txObj    interface{} // The actual transaction object from the engine
	readOnly bool
	closed   bool // Track if the transaction has been committed or rolled back
}

// Commit commits the transaction
func (t *Tx) Commit() error {
	if t.conn.tx != t {
		return errors.New("transaction not in progress")
	}

	if t.closed {
		return errors.New("transaction already closed")
	}

	// Commit the underlying transaction
	if engineTx, ok := t.txObj.(interface{ Commit() error }); ok {
		err := engineTx.Commit()
		t.conn.tx = nil
		t.closed = true
		return err
	}

	// Fallback if the transaction object doesn't implement Commit
	t.conn.tx = nil
	t.closed = true
	return nil
}

// Rollback rolls back the transaction
func (t *Tx) Rollback() error {
	if t.conn.tx != t {
		return errors.New("transaction not in progress")
	}

	if t.closed {
		return errors.New("transaction already closed")
	}

	// Rollback the underlying transaction
	if engineTx, ok := t.txObj.(interface{ Rollback() error }); ok {
		err := engineTx.Rollback()
		t.conn.tx = nil
		t.closed = true
		return err
	}

	// Fallback if the transaction object doesn't implement Rollback
	t.conn.tx = nil
	t.closed = true
	return nil
}

// Exec executes a query that doesn't return rows within the transaction
// Deprecated: Use ExecContext instead
func (t *Tx) Exec(query string, args []driver.Value) (driver.Result, error) {
	return t.ExecContext(t.ctx, query, convertValues(args))
}

// GetStorageTransaction returns the underlying storage transaction object
// This is a helper for getting a strongly typed storage.Transaction if available
func (t *Tx) GetStorageTransaction() storage.Transaction {
	if txObj := t.txObj; txObj != nil {
		if storageTx, ok := txObj.(storage.Transaction); ok {
			return storageTx
		}
	}
	return nil
}

// ExecContext executes a query that doesn't return rows within the transaction, with context
func (t *Tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if t.closed {
		return nil, errors.New("transaction already closed")
	}

	// Create a statement
	stmt := &Stmt{conn: t.conn, query: query}
	defer stmt.Close()

	// Execute the statement with args
	return stmt.ExecContext(ctx, args)
}

// Query executes a query that returns rows within the transaction
// Deprecated: Use QueryContext instead
func (t *Tx) Query(query string, args []driver.Value) (driver.Rows, error) {
	return t.QueryContext(t.ctx, query, convertValues(args))
}

// QueryContext executes a query that returns rows within the transaction, with context
func (t *Tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if t.closed {
		return nil, errors.New("transaction already closed")
	}

	// Create a statement
	stmt := &Stmt{conn: t.conn, query: query}
	defer stmt.Close()

	// Execute the statement with args
	return stmt.QueryContext(ctx, args)
}

// Prepare prepares a statement for execution within the transaction
func (t *Tx) Prepare(query string) (driver.Stmt, error) {
	return t.PrepareContext(t.ctx, query)
}

// PrepareContext prepares a statement for execution within the transaction, with context
func (t *Tx) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if t.closed {
		return nil, errors.New("transaction already closed")
	}

	// Create a transaction-specific statement
	return &txStmt{
		tx:    t,
		stmt:  &Stmt{conn: t.conn, query: query},
		query: query,
		ctx:   ctx,
	}, nil
}

// txStmt is a transaction-specific statement
type txStmt struct {
	tx    *Tx
	stmt  *Stmt
	query string
	ctx   context.Context
}

// Close closes the statement
func (s *txStmt) Close() error {
	return s.stmt.Close()
}

// NumInput returns the number of placeholder parameters
func (s *txStmt) NumInput() int {
	return s.stmt.NumInput()
}

// Exec executes a query that doesn't return rows
func (s *txStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.tx.closed {
		return nil, errors.New("transaction already closed")
	}
	return s.stmt.Exec(args)
}

// ExecContext executes a query that doesn't return rows, with context
func (s *txStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.tx.closed {
		return nil, errors.New("transaction already closed")
	}
	return s.stmt.ExecContext(ctx, args)
}

// Query executes a query that returns rows
func (s *txStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.tx.closed {
		return nil, errors.New("transaction already closed")
	}
	return s.stmt.Query(args)
}

// QueryContext executes a query that returns rows, with context
func (s *txStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.tx.closed {
		return nil, errors.New("transaction already closed")
	}
	return s.stmt.QueryContext(ctx, args)
}

// convertValues converts driver.Value to driver.NamedValue
func convertValues(vals []driver.Value) []driver.NamedValue {
	result := make([]driver.NamedValue, len(vals))
	for i, val := range vals {
		result[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   val,
		}
	}
	return result
}
