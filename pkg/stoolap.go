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
package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"

	sqlexecutor "github.com/stoolap/stoolap/internal/sql/executor"
	"github.com/stoolap/stoolap/internal/storage"

	// Import the storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// Global engine registry to ensure only one engine instance per DSN
var (
	// engineRegistry maps DSN strings to engine instances
	engineRegistry = make(map[string]*DB)
	// engineMutex protects access to the engine registry
	engineMutex sync.RWMutex
)

// DB represents a stoolap database
type DB struct {
	engine   storage.Engine
	executor Executor
	mu       sync.RWMutex // Protects executor access
}

// Storage engine constants
const (
	// MemoryScheme is the in-memory storage engine URI scheme
	MemoryScheme = "memory"
	// FileScheme is the persistent file storage engine URI scheme
	FileScheme = "file"
)

// Open opens a database connection
// STRICT RULE: Only ONE engine instance can exist per DSN for the entire application lifetime
// Any attempt to open the same DSN again will return the existing engine instance
func Open(dsn string) (*DB, error) {
	// First check if we already have an engine for this DSN in our registry
	engineMutex.RLock()
	db, exists := engineRegistry[dsn]
	engineMutex.RUnlock()

	// If we found an existing engine, return it immediately
	if exists {
		return db, nil
	}

	// Acquire write lock for creating a new engine
	engineMutex.Lock()
	defer engineMutex.Unlock()

	// Double-check after acquiring the lock
	if db, exists := engineRegistry[dsn]; exists {
		return db, nil
	}

	// Not found in registry, create a new engine

	// Simple scheme validation without full URL parsing
	var scheme string
	if idx := strings.Index(dsn, "://"); idx != -1 {
		scheme = dsn[:idx]
	} else {
		return nil, errors.New("invalid connection string format: expected scheme://path")
	}

	// Validate scheme
	switch scheme {
	case MemoryScheme:
		// Memory scheme is valid
	case FileScheme:
		// File scheme is valid - factory will handle path validation
		path := dsn[len("file://"):]
		// Remove query parameters for basic validation
		if idx := strings.Index(path, "?"); idx != -1 {
			path = path[:idx]
		}
		if path == "" {
			return nil, errors.New("file:// scheme requires a non-empty path")
		}
	default:
		return nil, errors.New("unsupported connection string format: use 'memory://' for in-memory or 'file://path' for persistent storage")
	}

	// Use the storage engine factory to create the engine
	factory := storage.GetEngineFactory("mvcc")
	if factory == nil {
		return nil, errors.New("database storage engine factory not found")
	}

	// Create the engine with the validated connection string
	engine, err := factory.Create(dsn)
	if err != nil {
		return nil, err
	}

	if err := engine.Open(); err != nil {
		return nil, err
	}

	// Create new DB instance with a single executor
	db = &DB{
		engine:   engine,
		executor: sqlexecutor.NewExecutor(engine),
	}

	// Add to registry
	engineRegistry[dsn] = db

	return db, nil
}

// Close closes the database connection and releases resources
// This will actually close the engine and remove it from the registry
func (db *DB) Close() error {
	engineMutex.Lock()
	defer engineMutex.Unlock()

	// Find and remove this engine from the registry
	for dsn, registeredDB := range engineRegistry {
		if registeredDB == db {
			delete(engineRegistry, dsn)
			break
		}
	}

	// Actually close the engine
	return db.engine.Close()
}

// Engine returns the underlying storage engine
func (db *DB) Engine() storage.Engine {
	return db.engine
}

// Executor represents a SQL query executor
type Executor interface {
	Execute(ctx context.Context, tx storage.Transaction, query string) (storage.Result, error)
	ExecuteWithParams(ctx context.Context, tx storage.Transaction, query string, params []driver.NamedValue) (storage.Result, error)
	EnableVectorizedMode()
	DisableVectorizedMode()
	IsVectorizedModeEnabled() bool
	GetDefaultIsolationLevel() storage.IsolationLevel
}

// Executor returns the SQL executor for the database
func (db *DB) Executor() Executor {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.executor
}

// Exec executes a query without returning any rows
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	// Execute the query with nil transaction - Execute will handle it
	result, err := db.executor.Execute(ctx, nil, query)
	if err != nil {
		return nil, err
	}

	// Check if we got a Result or not
	if result != nil {
		defer result.Close()

		// Create and return result
		return &execResult{
			rowsAffected: result.RowsAffected(),
			lastInsertID: result.LastInsertID(),
		}, nil
	}

	// No result, return empty result
	return &execResult{}, nil
}

// execResult implements sql.Result
type execResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Now let's add interfaces for results

// Rows is an iterator over query results
type Rows interface {
	// Next advances to the next row
	Next() bool
	// Scan copies columns into provided destinations
	Scan(dest ...interface{}) error
	// Close closes the rows
	Close() error
	// Columns returns column names
	Columns() []string
}

// Row represents a single row result
type Row interface {
	// Scan copies columns into provided destinations
	Scan(dest ...interface{}) error
}

// Stmt represents a prepared statement
type Stmt interface {
	// ExecContext executes the statement
	ExecContext(ctx context.Context, args ...driver.NamedValue) (sql.Result, error)
	// QueryContext executes a query
	QueryContext(ctx context.Context, args ...driver.NamedValue) (Rows, error)
	// Close closes the statement
	Close() error
}

// Tx represents a database transaction
type Tx interface {
	// Commit commits the transaction
	Commit() error
	// Rollback aborts the transaction
	Rollback() error
	// ExecContext executes a query in the transaction
	ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error)
	// QueryContext executes a query in the transaction
	QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error)
	// Prepare creates a prepared statement for the transaction
	Prepare(query string) (Stmt, error)
}

// Implementation types

type rows struct {
	result          storage.Result
	tx              storage.Transaction
	ctx             context.Context
	closed          bool
	ownsTransaction bool // true if rows should commit/rollback tx on close
}

func (r *rows) Next() bool {
	if r.closed {
		return false
	}
	return r.result.Next()
}

func (r *rows) Scan(dest ...interface{}) error {
	if r.closed {
		return errors.New("rows are closed")
	}
	return r.result.Scan(dest...)
}

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if err := r.result.Close(); err != nil {
		return err
	}
	// Only commit if we own the transaction and have a transaction
	if r.ownsTransaction && r.tx != nil {
		return r.tx.Commit()
	}
	return nil
}

func (r *rows) Columns() []string {
	return r.result.Columns()
}

type row struct {
	rows Rows
	err  error
}

func (r *row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()

	if !r.rows.Next() {
		return sql.ErrNoRows
	}

	return r.rows.Scan(dest...)
}

type transaction struct {
	tx  storage.Transaction
	db  *DB
	ctx context.Context
}

func (t *transaction) Commit() error {
	return t.tx.Commit()
}

func (t *transaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *transaction) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error) {
	result, err := t.db.executor.ExecuteWithParams(ctx, t.tx, query, args)
	if err != nil {
		return nil, err
	}

	defer result.Close()
	return &execResult{
		rowsAffected: result.RowsAffected(),
		lastInsertID: result.LastInsertID(),
	}, nil
}

func (t *transaction) QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error) {
	result, err := t.db.executor.ExecuteWithParams(ctx, t.tx, query, args)
	if err != nil {
		return nil, err
	}

	return &rows{
		result:          result,
		tx:              t.tx,
		ctx:             ctx,
		ownsTransaction: false, // Transaction query doesn't own the tx
	}, nil
}

func (t *transaction) Prepare(query string) (Stmt, error) {
	return &stmt{
		db:    t.db,
		query: query,
		ctx:   t.ctx,
		tx:    t,
	}, nil
}

type stmt struct {
	db    *DB
	query string
	ctx   context.Context
	tx    Tx
}

func (s *stmt) ExecContext(ctx context.Context, args ...driver.NamedValue) (sql.Result, error) {
	if s.tx != nil {
		return s.tx.ExecContext(ctx, s.query, args...)
	}
	return s.db.ExecContext(ctx, s.query, args...)
}

func (s *stmt) QueryContext(ctx context.Context, args ...driver.NamedValue) (Rows, error) {
	if s.tx != nil {
		return s.tx.QueryContext(ctx, s.query, args...)
	}
	return s.db.QueryContext(ctx, s.query, args...)
}

func (s *stmt) Close() error {
	return nil // No resources to release for now
}

// Query executes a query that returns rows
func (db *DB) Query(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error) {
	return db.QueryContext(ctx, query, args...)
}

// QueryContext executes a query that returns rows with context
func (db *DB) QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error) {
	// Execute the query with nil transaction - ExecuteWithParams will handle it
	result, err := db.executor.ExecuteWithParams(ctx, nil, query, args)
	if err != nil {
		return nil, err
	}

	// Wrap the result in a Rows interface
	return &rows{
		result:          result,
		tx:              nil, // No transaction to manage
		ctx:             ctx,
		ownsTransaction: false, // No transaction ownership
	}, nil
}

// QueryRow executes a query that is expected to return at most one row
func (db *DB) QueryRow(ctx context.Context, query string, args ...driver.NamedValue) Row {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &row{err: err}
	}
	return &row{rows: rows}
}

// ExecContext executes a query with context and parameters
func (db *DB) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error) {
	// Execute the query with nil transaction - ExecuteWithParams will handle it
	result, err := db.executor.ExecuteWithParams(ctx, nil, query, args)
	if err != nil {
		return nil, err
	}

	// Check if we got a Result or not
	if result != nil {
		defer result.Close()

		// Convert to sql.Result
		return &execResult{
			rowsAffected: result.RowsAffected(),
			lastInsertID: result.LastInsertID(),
		}, nil
	}

	// No result, return empty result
	return &execResult{}, nil
}

// Begin starts a new transaction
func (db *DB) Begin() (Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx starts a new transaction with options
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	isolationLevel := sql.LevelReadCommitted
	if opts != nil {
		isolationLevel = opts.Isolation
	}

	tx, err := db.engine.BeginTx(ctx, isolationLevel)
	if err != nil {
		return nil, err
	}

	return &transaction{
		tx:  tx,
		db:  db,
		ctx: ctx,
	}, nil
}

// Prepare creates a prepared statement
func (db *DB) Prepare(query string) (Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement with context
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	return &stmt{
		db:    db,
		query: query,
		ctx:   ctx,
	}, nil
}
