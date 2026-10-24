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
package wasm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
)

// wasmEngine is the global engine set by SetWASM().
var (
	globalEngine   *Engine
	globalEngineMu sync.Mutex
)

// SetWASM initializes the global WASM engine from the given binary.
// Must be called before sql.Open("stoolap-wasm", ...).
func SetWASM(ctx context.Context, wasmBytes []byte) error {
	globalEngineMu.Lock()
	defer globalEngineMu.Unlock()

	e, err := NewEngine(ctx, wasmBytes)
	if err != nil {
		return err
	}
	globalEngine = e
	return nil
}

func init() {
	sql.Register("stoolap-wasm", &Driver{})
}

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Open implements driver.Driver.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	globalEngineMu.Lock()
	e := globalEngine
	globalEngineMu.Unlock()

	if e == nil {
		return nil, errors.New("stoolap-wasm: engine not initialized, call wasm.SetWASM() first")
	}

	ctx := context.Background()
	db, err := e.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &driverConnector{engine: e, db: db, dsn: dsn}, nil
}

// driverConnector implements driver.Connector.
type driverConnector struct {
	engine *Engine
	db     *DB
	dsn    string
	mu     sync.Mutex
}

// Connect implements driver.Connector.
func (c *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.db == nil {
		return nil, errors.New("stoolap-wasm: connector is closed")
	}
	clone, err := c.db.Clone(ctx)
	if err != nil {
		return nil, err
	}
	return &driverConn{db: clone, engine: c.engine}, nil
}

// Driver implements driver.Connector.
func (c *driverConnector) Driver() driver.Driver {
	return &Driver{}
}

// ─── Conn ───────────────────────────────────────────────────────────────────

type driverConn struct {
	db     *DB
	engine *Engine
	tx     *Tx
	closed bool
}

var (
	_ driver.Conn               = (*driverConn)(nil)
	_ driver.ConnBeginTx        = (*driverConn)(nil)
	_ driver.ExecerContext      = (*driverConn)(nil)
	_ driver.QueryerContext     = (*driverConn)(nil)
	_ driver.ConnPrepareContext = (*driverConn)(nil)
	_ driver.Pinger             = (*driverConn)(nil)
	_ driver.SessionResetter    = (*driverConn)(nil)
	_ driver.Validator          = (*driverConn)(nil)
)

func (c *driverConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *driverConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	s, err := c.db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return &driverStmt{stmt: s, conn: c}, nil
}

func (c *driverConn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.db.Close()
}

func (c *driverConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *driverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if opts.ReadOnly {
		return nil, errors.New("stoolap-wasm: read-only transactions are not supported")
	}

	sqlOpts := &sql.TxOptions{Isolation: sql.IsolationLevel(opts.Isolation)}
	tx, err := c.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &driverTx{tx: tx, conn: c}, nil
}

func (c *driverConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if hasNamed(args) {
		named, err := toNamedArgs(args)
		if err != nil {
			return nil, err
		}
		if c.tx != nil {
			return c.tx.ExecNamed(ctx, query, named)
		}
		return c.db.ExecNamed(ctx, query, named)
	}
	goArgs := namedToAny(args)

	if c.tx != nil {
		if len(goArgs) == 0 {
			return c.tx.Exec(ctx, query)
		}
		return c.tx.ExecParams(ctx, query, goArgs)
	}
	if len(goArgs) == 0 {
		return c.db.Exec(ctx, query)
	}
	return c.db.ExecParams(ctx, query, goArgs)
}

func (c *driverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	tx, err := c.txPtr()
	if err != nil {
		return nil, err
	}
	var cols []string
	var rows [][]any
	if hasNamed(args) {
		named, err := toNamedArgs(args)
		if err != nil {
			return nil, err
		}
		cols, rows, err = c.db.queryAllPacked(ctx, tx, query, nil, named)
	} else {
		cols, rows, err = c.db.queryAllPacked(ctx, tx, query, namedToAny(args), nil)
	}
	if err != nil {
		return nil, err
	}
	return &driverRows{cols: cols, rows: rows}, nil
}

// txPtr returns the open transaction of the connection, or 0.
func (c *driverConn) txPtr() (uint32, error) {
	if c.tx == nil {
		return 0, nil
	}
	if c.tx.ptr == 0 {
		return 0, errTxDone
	}
	return c.tx.ptr, nil
}

func (c *driverConn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	r, err := c.db.Query(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	return r.Close()
}

func (c *driverConn) ResetSession(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	c.tx = nil
	return nil
}

func (c *driverConn) IsValid() bool {
	return !c.closed
}

// ─── Tx ─────────────────────────────────────────────────────────────────────

type driverTx struct {
	tx   *Tx
	conn *driverConn
}

func (t *driverTx) Commit() error {
	t.conn.tx = nil
	return t.tx.Commit()
}

func (t *driverTx) Rollback() error {
	t.conn.tx = nil
	return t.tx.Rollback()
}

// ─── Stmt ───────────────────────────────────────────────────────────────────

type driverStmt struct {
	stmt *Stmt
	conn *driverConn
}

var (
	_ driver.Stmt             = (*driverStmt)(nil)
	_ driver.StmtExecContext  = (*driverStmt)(nil)
	_ driver.StmtQueryContext = (*driverStmt)(nil)
)

func (s *driverStmt) Close() error {
	return s.stmt.Close()
}

func (s *driverStmt) NumInput() int {
	return -1
}

func (s *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *driverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	tx, err := s.conn.txPtr()
	if err != nil {
		return nil, err
	}
	if hasNamed(args) {
		named, err := toNamedArgs(args)
		if err != nil {
			return nil, err
		}
		return s.stmt.execIn(ctx, tx, nil, named)
	}
	return s.stmt.execIn(ctx, tx, namedToAny(args), nil)
}

func (s *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	tx, err := s.conn.txPtr()
	if err != nil {
		return nil, err
	}
	var cols []string
	var rows [][]any
	if hasNamed(args) {
		named, err := toNamedArgs(args)
		if err != nil {
			return nil, err
		}
		cols, rows, err = s.stmt.queryAllPacked(ctx, tx, nil, named)
	} else {
		cols, rows, err = s.stmt.queryAllPacked(ctx, tx, namedToAny(args), nil)
	}
	if err != nil {
		return nil, err
	}
	return &driverRows{cols: cols, rows: rows}, nil
}

// ─── Driver Rows ────────────────────────────────────────────────────────────

// driverRows serves a result that one WASM call decoded in full. The engine
// hands every row back packed, so there is no handle to iterate or close.
type driverRows struct {
	cols   []string
	rows   [][]any
	cursor int
}

func (r *driverRows) Columns() []string {
	return r.cols
}

func (r *driverRows) Close() error {
	r.rows = nil
	return nil
}

func (r *driverRows) Next(dest []driver.Value) error {
	if r.cursor >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.cursor]
	r.cursor++
	for i := range dest {
		dest[i] = row[i]
	}
	return nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

var errMixedArgs = errors.New("stoolap-wasm: cannot mix named and positional arguments")

func hasNamed(args []driver.NamedValue) bool {
	for i := range args {
		if args[i].Name != "" {
			return true
		}
	}
	return false
}

// toNamedArgs converts database/sql named values. Every argument must carry a name.
func toNamedArgs(args []driver.NamedValue) ([]sql.NamedArg, error) {
	out := make([]sql.NamedArg, len(args))
	for i, a := range args {
		if a.Name == "" {
			return nil, errMixedArgs
		}
		out[i] = sql.NamedArg{Name: a.Name, Value: a.Value}
	}
	return out, nil
}

func namedToAny(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
