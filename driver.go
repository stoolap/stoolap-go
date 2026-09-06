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
package stoolap

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
)

func init() {
	sql.Register("stoolap", &Driver{})
}

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

// Open implements driver.Driver.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	conn, connErr := connector.Connect(context.Background())
	connector.(*driverConnector).Close()
	if connErr != nil {
		return nil, connErr
	}
	return conn, nil
}

// OpenConnector implements driver.DriverContext.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	db, err := Open(dsn)
	if err != nil {
		return nil, err
	}
	return &driverConnector{db: db, dsn: dsn}, nil
}

// driverConnector implements driver.Connector and io.Closer.
type driverConnector struct {
	mu  sync.Mutex
	db  *DB
	dsn string
}

// Connect implements driver.Connector. Each connection gets a cloned handle.
func (c *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil, errors.New("stoolap: connector is closed")
	}
	clone, err := c.db.Clone()
	if err != nil {
		return nil, err
	}
	return &driverConn{db: clone}, nil
}

// Driver implements driver.Connector.
func (c *driverConnector) Driver() driver.Driver {
	return &Driver{}
}

// Close closes the connector's underlying database handle.
func (c *driverConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

// ─── Conn ───────────────────────────────────────────────────────────────────

type driverConn struct {
	db     *DB
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
	if c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
	}
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
		return nil, errors.New("stoolap: read-only transactions are not supported")
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
	goArgs := namedToAny(args)

	var r *Rows
	var err error
	if hasNamed(args) {
		var named []sql.NamedArg
		named, err = toNamedArgs(args)
		if err != nil {
			return nil, err
		}
		if c.tx != nil {
			r, err = c.tx.QueryNamed(ctx, query, named)
		} else {
			r, err = c.db.QueryNamed(ctx, query, named)
		}
	} else if c.tx != nil {
		if len(goArgs) == 0 {
			r, err = c.tx.Query(ctx, query)
		} else {
			r, err = c.tx.QueryParams(ctx, query, goArgs)
		}
	} else {
		if len(goArgs) == 0 {
			r, err = c.db.Query(ctx, query)
		} else {
			r, err = c.db.QueryParams(ctx, query, goArgs)
		}
	}
	if err != nil {
		return nil, err
	}
	return newDriverRows(r), nil
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
	if c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
	}
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
	if hasNamed(args) {
		return nil, errNamedStmtUnsupported
	}
	return s.stmt.ExecContext(ctx, namedToAny(args))
}

func (s *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if hasNamed(args) {
		return nil, errNamedStmtUnsupported
	}
	r, err := s.stmt.QueryContext(ctx, namedToAny(args))
	if err != nil {
		return nil, err
	}
	return newDriverRows(r), nil
}

// ─── Driver Rows ────────────────────────────────────────────────────────────

// driverRows fetches all rows in a single native call on first Next(),
// then serves them from Go memory. This minimizes FFI round-trips for
// maximum throughput. QueryRow/LIMIT queries return small result sets,
// so FetchAll is equally efficient for those cases.
type driverRows struct {
	rows     *Rows
	allRows  [][]any
	cursor   int
	colCount int
	fetched  bool
}

var driverRowsPool = sync.Pool{New: func() any { return &driverRows{} }}

func newDriverRows(r *Rows) *driverRows {
	dr := driverRowsPool.Get().(*driverRows)
	dr.rows = r
	dr.colCount = r.colCount
	dr.allRows = nil
	dr.cursor = 0
	dr.fetched = false
	return dr
}

func (r *driverRows) Columns() []string {
	return r.rows.Columns()
}

func (r *driverRows) Close() error {
	r.allRows = nil
	err := r.rows.Close()
	r.rows = nil
	driverRowsPool.Put(r)
	return err
}

func (r *driverRows) Next(dest []driver.Value) error {
	if !r.fetched {
		r.fetched = true
		allRows, err := r.rows.FetchAll()
		if err != nil {
			return err
		}
		r.allRows = allRows
		r.cursor = 0
	}

	if r.cursor >= len(r.allRows) {
		return io.EOF
	}

	row := r.allRows[r.cursor]
	r.cursor++

	for i := 0; i < r.colCount && i < len(row); i++ {
		dest[i] = row[i]
	}
	return nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

var errMixedArgs = errors.New("stoolap: cannot mix named and positional arguments")
var errNamedStmtUnsupported = errors.New("stoolap: named arguments are not supported on prepared statements")

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
