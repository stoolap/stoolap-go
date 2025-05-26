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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	sqlexecutor "github.com/stoolap/stoolap/internal/sql/executor"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/pkg"

	// Import database storage engine
	_ "github.com/stoolap/stoolap/internal/storage/mvcc"
)

// StmtKey is the key used for statement cache lookups
type StmtKey struct {
	DSN   string
	Query string
}

func init() {
	// Register driver with optimized configuration
	driver := &Driver{
		conns:         make(map[string]*connEntry),
		stmtCacheSize: 20, // Cache up to 20 statements per query by default
	}
	sql.Register("stoolap", driver)
}

// Driver is a database/sql driver for stoolap
type Driver struct {
	connsMu       sync.RWMutex // Separate mutex for connections map to reduce contention
	conns         map[string]*connEntry
	stmtCacheSize int // Maximum number of statements to cache per query (default: 20)
}

// connEntry holds a connection and its reference count
type connEntry struct {
	db       *pkg.DB
	refCount int
	lastUsed time.Time
	createMu sync.Mutex // Mutex to synchronize concurrent creation attempts
}

// OpenConnector implements DriverContext for modern connection handling
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{
		driver: d,
		name:   name,
	}, nil
}

// Connector implements the driver.Connector interface for connection pooling
type Connector struct {
	driver *Driver
	name   string
}

// Connect creates a new connection to the database
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.name)
}

// Driver returns the underlying Driver
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Open opens a new connection to the database with optimized concurrency
func (d *Driver) Open(name string) (driver.Conn, error) {
	// First check if connection exists using read lock only
	d.connsMu.RLock()
	entry, ok := d.conns[name]
	d.connsMu.RUnlock()

	// If connection doesn't exist, we need to create it with proper locking
	if !ok {
		d.connsMu.Lock()
		// Check again after acquiring write lock (double-checked locking pattern)
		entry, ok = d.conns[name]
		if !ok {
			// Create new entry
			entry = &connEntry{
				refCount: 0,
				lastUsed: time.Now(),
			}
			d.conns[name] = entry
		}
		d.connsMu.Unlock()
	}

	// Use connection-specific mutex to synchronize access to this connection
	entry.createMu.Lock()
	defer entry.createMu.Unlock()

	// If database is not initialized, initialize it
	if entry.db == nil {
		var err error
		entry.db, err = pkg.Open(name)
		if err != nil {
			return nil, err
		}
	}

	// Update usage statistics
	entry.refCount++
	entry.lastUsed = time.Now()

	// Create a new connection with proper initialization
	conn := &Conn{
		db:      entry.db,
		driver:  d,
		dsn:     name,
		mu:      &sync.Mutex{},   // Each connection gets its own mutex
		stmtsMu: &sync.RWMutex{}, // Each connection gets its own statements mutex
		stmts:   make(map[string]*Stmt),
	}

	// Create a new executor instance for this connection
	// Each connection needs its own executor to maintain independent isolation levels
	conn.executor = sqlexecutor.NewExecutor(entry.db.Engine())

	return conn, nil
}

// Conn represents a connection to a stoolap database with optimized concurrency
type Conn struct {
	db       *pkg.DB
	driver   *Driver
	dsn      string
	tx       driver.Tx
	closed   bool             // Track if the connection has been closed
	executor pkg.Executor     // Cached executor instance to reduce memory usage
	mu       *sync.Mutex      // Connection-specific mutex for thread safety
	stmtsMu  *sync.RWMutex    // Mutex for statements map
	stmts    map[string]*Stmt // Cache of prepared statements
}

// Prepare prepares a query for execution
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext prepares a query for execution with context and statement caching
// Optimized version to reduce allocations and lock contention
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Quick context check without allocation
	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Fast path: For most statements in transaction or batch execution loops,
	// we skip the caching overhead to improve performance
	// This approach is based on benchmarks showing that caching is most beneficial
	// for repeated prepared statements across connections, not for high-frequency
	// statements used in transaction batches

	// We'll create a new statement without mutex overhead for
	// statements used in transaction loops (common in benchmarks)
	if c.tx != nil {
		// For transaction statements, bypass the cache
		return &Stmt{
			conn:  c,
			query: query,
			ctx:   ctx,
		}, nil
	}

	// Fast local cache check
	c.stmtsMu.RLock()
	stmt, exists := c.stmts[query]
	if exists && !stmt.closed {
		c.stmtsMu.RUnlock()
		return stmt, nil
	}
	c.stmtsMu.RUnlock()

	// Create new statement with minimal overhead
	stmt = &Stmt{
		conn:  c,
		query: query,
		ctx:   ctx,
	}

	// Only cache if it's a common query pattern (contains SELECT/INSERT/UPDATE/DELETE)
	// This avoids caching one-time DDL statements
	if isCacheableQuery(query) {
		// Store in connection cache
		c.stmtsMu.Lock()
		c.stmts[query] = stmt
		c.stmtsMu.Unlock()

		// We don't update the global cache for every statement
		// Only do so for statements that are likely to be reused
		// across connections to reduce contention on the sync.Map
	}

	return stmt, nil
}

// isCacheableQuery determines if a query should be cached
// We only want to cache queries that are likely to be executed repeatedly
func isCacheableQuery(query string) bool {
	if len(query) < 6 {
		return false
	}

	// Fast check for common query prefixes using a single character
	// This avoids expensive string operations
	c := query[0]
	//FIXME : This is a very naive check, we should use a more robust parser
	if c == 'S' || c == 's' || // SELECT
		c == 'I' || c == 'i' || // INSERT
		c == 'U' || c == 'u' || // UPDATE
		c == 'D' || c == 'd' { // DELETE
		return true
	}

	return false
}

// Close closes the connection with optimized reference counting
func (c *Conn) Close() error {
	// Use our connection-specific mutex
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// Close all cached statements
	c.stmtsMu.Lock()
	for _, stmt := range c.stmts {
		stmt.closed = true
	}
	// Clear the map to help garbage collection
	c.stmts = nil
	c.stmtsMu.Unlock()

	// Make sure any active transaction is rolled back
	if c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
	}

	// Use a read lock first to check the connection
	c.driver.connsMu.RLock()
	entry, ok := c.driver.conns[c.dsn]
	c.driver.connsMu.RUnlock()

	if !ok {
		// Connection entry already gone, nothing more to do
		return nil
	}

	// Use a connection-specific lock for modifying reference count
	entry.createMu.Lock()
	defer entry.createMu.Unlock()

	// Decrement reference count
	entry.refCount--
	entry.lastUsed = time.Now()

	if entry.refCount <= 0 {
		entry.refCount = 0 // Reset to zero (not negative)

		err := entry.db.Close()
		if err != nil {
			log.Printf("Error: closing database: %v\n", err)
		}

		c.driver.connsMu.Lock()
		delete(c.driver.conns, c.dsn)
		c.driver.connsMu.Unlock()
	}

	return nil
}

// Begin starts a transaction
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// Exec executes a query without returning any rows
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, convertValues(args))
}

// ExecContext executes a query with the provided context
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(*Stmt).ExecContext(ctx, args)
}

// Query executes a query that returns rows
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, convertValues(args))
}

// QueryContext executes a query that returns rows with optimized buffer allocation
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Prepare the statement with our optimized statement cache
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// We're using a cached statement, no need to close it immediately
	// The statement will be closed when the connection is closed
	// or when the statement cache is pruned

	// Execute the query using our optimized statement
	rows, err := stmt.(*Stmt).QueryContext(ctx, args)
	if err != nil {
		stmt.Close() // Only close on error
		return nil, err
	}

	return rows, nil
}

// BeginTx starts a transaction with the given context and options
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("transaction already in progress")
	}

	// Add isolation level to context if specified
	txCtx := ctx
	isolationLevel := sql.LevelReadCommitted

	if opts.Isolation != driver.IsolationLevel(0) {
		// Use the explicitly specified isolation level
		isolationLevel = sql.IsolationLevel(opts.Isolation)
	} else {
		// Use the executor's default isolation level
		executorLevel := c.executor.GetDefaultIsolationLevel()
		if executorLevel == storage.SnapshotIsolation {
			isolationLevel = sql.LevelSnapshot
		}
	}

	// Begin a transaction in the underlying engine with isolation level in context
	engineTx, err := c.db.Engine().BeginTx(txCtx, isolationLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Activate the transaction using the Begin method of the storage.Transaction interface
	if storageTransaction, ok := engineTx.(interface{ Begin() error }); ok {
		if err := storageTransaction.Begin(); err != nil {
			return nil, fmt.Errorf("failed to activate transaction: %w", err)
		}
	}

	tx := &Tx{
		conn:     c,
		ctx:      txCtx,
		txObj:    engineTx,
		readOnly: opts.ReadOnly,
	}
	c.tx = tx
	return tx, nil
}

// Ping verifies a connection to the database is still alive
// This implements the driver.Pinger interface
func (c *Conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue
	}

	// Use the cached executor to check that the database is responsive
	_, err := c.executor.Execute(ctx, nil, "SELECT 1")
	if err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// ResetSession resets the session state
// This implements the driver.SessionResetter interface
func (c *Conn) ResetSession(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	// Roll back any active transaction
	if c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
	}

	return nil
	// Ping the connection to verify it's still valid
	// return c.Ping(ctx)
}

// IsValid checks if the connection is still valid
// This implements the driver.Validator interface
func (c *Conn) IsValid() bool {
	if c.closed {
		return false
	}

	return true

	// Quick check by pinging the database
	// return c.Ping(context.Background()) == nil
}
