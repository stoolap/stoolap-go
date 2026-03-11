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
	"strconv"
	"time"

	"github.com/stoolap/stoolap-go/internal/cs"
)

// Pre-allocated errors to avoid allocations on error paths.
var (
	errClosed          = errors.New("stoolap: database is closed")
	errRowsClosed      = errors.New("stoolap: rows are closed")
	errColumnMismatch  = errors.New("stoolap: column count mismatch")
	errUnsupportedScan = errors.New("stoolap: unsupported scan destination type")
)

// DB represents a stoolap database connection.
type DB struct {
	handle *cs.DB
}

// Rows is an iterator over query results.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Columns() []string
	// FetchAll fetches all remaining rows into a packed binary buffer.
	// This is useful for bulk data transfer, avoiding per-row overhead.
	// The caller should still call Close() after FetchAll.
	FetchAll() ([]byte, error)
}

// Row represents a single row result.
type Row interface {
	Scan(dest ...any) error
}

// Stmt represents a prepared statement.
type Stmt interface {
	ExecContext(ctx context.Context, args ...driver.NamedValue) (sql.Result, error)
	QueryContext(ctx context.Context, args ...driver.NamedValue) (Rows, error)
	SQL() string
	Close() error
}

// Tx represents a database transaction.
type Tx interface {
	Commit() error
	Rollback() error
	ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error)
	Prepare(query string) (Stmt, error)
	ID() int64
}

// Version returns the stoolap library version string.
func Version() string {
	return cs.Version()
}

// Open opens a database connection with the given DSN.
func Open(dsn string) (*DB, error) {
	h, err := cs.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &DB{handle: h}, nil
}

// Close closes the database connection and releases resources.
func (db *DB) Close() error {
	if db.handle == nil {
		return nil
	}
	err := db.handle.Close()
	db.handle = nil
	return err
}

// Clone creates a cloned handle for multi-threaded use.
// The clone shares the underlying engine but has its own executor and error state.
func (db *DB) Clone() (*DB, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	clone, err := db.handle.Clone()
	if err != nil {
		return nil, err
	}
	return &DB{handle: clone}, nil
}

// Handle returns the underlying FFI handle (for internal/driver use).
func (db *DB) Handle() *cs.DB {
	return db.handle
}

// Exec executes a query without returning any rows.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	affected, err := db.handle.Exec(query)
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

// ExecContext executes a query with context and parameters.
func (db *DB) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return db.Exec(ctx, query)
	}
	vb, verr := acquireDirectValues(args)
	if verr != nil {
		return nil, verr
	}
	affected, err := db.handle.ExecParams(query, vb.Values)
	vb.Release()
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (Rows, error) {
	return db.QueryContext(ctx, query)
}

// QueryContext executes a query that returns rows with parameters.
func (db *DB) QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var r *cs.Rows
	var err error

	if len(args) == 0 {
		r, err = db.handle.Query(query)
	} else {
		vb, verr := acquireDirectValues(args)
		if verr != nil {
			return nil, verr
		}
		r, err = db.handle.QueryParams(query, vb.Values)
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return newRows(r), nil
}

// QueryRow executes a query that is expected to return at most one row.
func (db *DB) QueryRow(ctx context.Context, query string, args ...driver.NamedValue) Row {
	r, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &singleRow{err: err}
	}
	return &singleRow{rows: r}
}

// Begin starts a new transaction with default isolation level.
func (db *DB) Begin() (Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx starts a new transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var t *cs.Tx
	var err error

	if opts != nil && opts.Isolation != sql.LevelDefault && opts.Isolation != sql.LevelReadCommitted {
		if opts.Isolation == sql.LevelSnapshot || opts.Isolation == sql.LevelRepeatableRead {
			t, err = db.handle.BeginWithIsolation(cs.IsolationSnapshot)
		} else {
			return nil, errors.New("stoolap: unsupported isolation level")
		}
	} else {
		t, err = db.handle.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &transaction{tx: t, db: db}, nil
}

// Prepare creates a prepared statement.
func (db *DB) Prepare(query string) (Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement with context.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	if db.handle == nil {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := db.handle.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &preparedStmt{stmt: s, db: db}, nil
}

// acquireDirectValues converts direct API args to a pooled ValueBuf.
func acquireDirectValues(args []driver.NamedValue) (*cs.ValueBuf, error) {
	vb := cs.AcquireValueBuf(len(args))
	for i, arg := range args {
		v, err := cs.GoValueToFFI(arg.Value)
		if err != nil {
			vb.Release()
			return nil, err
		}
		vb.Values[i] = v
	}
	return vb, nil
}

// ---- execResult implements sql.Result ----

type execResult int64

func (r execResult) LastInsertId() (int64, error) { return 0, nil }
func (r execResult) RowsAffected() (int64, error) { return int64(r), nil }

// ---- rows implementation ----

type resultRows struct {
	cRows    *cs.Rows
	cols     []string
	colsOK   bool
	colCount int
	err      error
	closed   bool
}

func newRows(r *cs.Rows) *resultRows {
	return &resultRows{
		cRows:    r,
		colCount: r.ColumnCount(),
	}
}

func (r *resultRows) Columns() []string {
	if !r.colsOK {
		r.cols = make([]string, r.colCount)
		for i := range r.cols {
			r.cols[i] = r.cRows.ColumnName(i)
		}
		r.colsOK = true
	}
	return r.cols
}

func (r *resultRows) Next() bool {
	if r.closed || r.cRows == nil {
		return false
	}
	hasRow, err := r.cRows.Next()
	if err != nil {
		r.err = err
		return false
	}
	return hasRow
}

func (r *resultRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.closed || r.cRows == nil {
		return errRowsClosed
	}
	if len(dest) != r.colCount {
		return errColumnMismatch
	}

	for i := 0; i < r.colCount; i++ {
		colType := r.cRows.ColumnType(i)
		if colType == cs.TypeNull {
			scanNull(dest[i])
			continue
		}
		if err := scanColumn(r.cRows, i, colType, dest[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *resultRows) FetchAll() ([]byte, error) {
	if r.closed || r.cRows == nil {
		return nil, errRowsClosed
	}
	return r.cRows.FetchAll()
}

func (r *resultRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.cRows.Close()
}

// ---- singleRow implementation ----

type singleRow struct {
	rows Rows
	err  error
}

func (r *singleRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close() //nolint:errcheck // best-effort; scan result takes precedence
	if !r.rows.Next() {
		// Check if Next() failed due to an engine error, not just empty result.
		if rr, ok := r.rows.(*resultRows); ok && rr.err != nil {
			return rr.err
		}
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// ---- transaction implementation ----

type transaction struct {
	tx *cs.Tx
	db *DB
}

func (t *transaction) Commit() error {
	return t.tx.Commit()
}

func (t *transaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *transaction) ID() int64 {
	return 0
}

func (t *transaction) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var affected int64
	var err error

	if len(args) == 0 {
		affected, err = t.tx.Exec(query)
	} else {
		vb, verr := acquireDirectValues(args)
		if verr != nil {
			return nil, verr
		}
		affected, err = t.tx.ExecParams(query, vb.Values)
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

func (t *transaction) QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var r *cs.Rows
	var err error

	if len(args) == 0 {
		r, err = t.tx.Query(query)
	} else {
		vb, verr := acquireDirectValues(args)
		if verr != nil {
			return nil, verr
		}
		r, err = t.tx.QueryParams(query, vb.Values)
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return newRows(r), nil
}

func (t *transaction) Prepare(query string) (Stmt, error) {
	if t.db.handle == nil {
		return nil, errClosed
	}
	s, err := t.db.handle.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &preparedStmt{stmt: s, db: t.db, tx: t.tx}, nil
}

// ---- preparedStmt implementation ----

type preparedStmt struct {
	stmt *cs.Stmt
	db   *DB
	tx   *cs.Tx
}

var errStmtClosed = errors.New("stoolap: statement is closed")

func (s *preparedStmt) ExecContext(ctx context.Context, args ...driver.NamedValue) (sql.Result, error) {
	if s.stmt == nil {
		return nil, errStmtClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params []cs.Value
	var vb *cs.ValueBuf
	if len(args) > 0 {
		var verr error
		vb, verr = acquireDirectValues(args)
		if verr != nil {
			return nil, verr
		}
		params = vb.Values
	}

	var affected int64
	var err error

	if s.tx != nil {
		affected, err = s.tx.StmtExec(s.stmt, params)
	} else {
		affected, err = s.stmt.Exec(params)
	}
	if vb != nil {
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

func (s *preparedStmt) QueryContext(ctx context.Context, args ...driver.NamedValue) (Rows, error) {
	if s.stmt == nil {
		return nil, errStmtClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params []cs.Value
	var vb *cs.ValueBuf
	if len(args) > 0 {
		var verr error
		vb, verr = acquireDirectValues(args)
		if verr != nil {
			return nil, verr
		}
		params = vb.Values
	}

	var r *cs.Rows
	var err error

	if s.tx != nil {
		r, err = s.tx.StmtQuery(s.stmt, params)
	} else {
		r, err = s.stmt.Query(params)
	}
	if vb != nil {
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return newRows(r), nil
}

func (s *preparedStmt) SQL() string {
	if s.stmt == nil {
		return ""
	}
	return s.stmt.SQL()
}

func (s *preparedStmt) Close() error {
	if s.stmt != nil {
		s.stmt.Finalize()
		s.stmt = nil
	}
	return nil
}

// ---- helpers ----

// scanNull assigns nil/zero to the destination for NULL columns.
func scanNull(dest any) {
	switch d := dest.(type) {
	case *any:
		*d = nil
	case *string:
		*d = ""
	case *int64:
		*d = 0
	case *float64:
		*d = 0
	case *float32:
		*d = 0
	case *bool:
		*d = false
	case *time.Time:
		*d = time.Time{}
	case *[]byte:
		*d = nil
	case *int:
		*d = 0
	case *int32:
		*d = 0
	case *sql.NullString:
		d.String = ""
		d.Valid = false
	case *sql.NullInt64:
		d.Int64 = 0
		d.Valid = false
	case *sql.NullFloat64:
		d.Float64 = 0
		d.Valid = false
	case *sql.NullBool:
		d.Bool = false
		d.Valid = false
	case *sql.NullTime:
		d.Time = time.Time{}
		d.Valid = false
	}
}

// scanColumn reads a column value from FFI rows into a destination.
func scanColumn(r *cs.Rows, index int, colType int32, dest any) error {
	switch d := dest.(type) {
	case *any:
		*d = columnToAny(r, index, colType)
		return nil
	case *string:
		switch colType {
		case cs.TypeText, cs.TypeJSON:
			*d = r.ColumnText(index)
		case cs.TypeInteger:
			*d = strconv.FormatInt(r.ColumnInt64(index), 10)
		case cs.TypeFloat:
			*d = strconv.FormatFloat(r.ColumnDouble(index), 'g', -1, 64)
		case cs.TypeBoolean:
			if r.ColumnBool(index) {
				*d = "true"
			} else {
				*d = "false"
			}
		case cs.TypeTimestamp:
			*d = r.ColumnTimestamp(index).Format(time.RFC3339Nano)
		default:
			*d = r.ColumnText(index)
		}
		return nil
	case *int64:
		switch colType {
		case cs.TypeInteger:
			*d = r.ColumnInt64(index)
		case cs.TypeFloat:
			*d = int64(r.ColumnDouble(index))
		case cs.TypeBoolean:
			if r.ColumnBool(index) {
				*d = 1
			} else {
				*d = 0
			}
		default:
			*d = r.ColumnInt64(index)
		}
		return nil
	case *int:
		switch colType {
		case cs.TypeInteger:
			*d = int(r.ColumnInt64(index))
		case cs.TypeFloat:
			*d = int(r.ColumnDouble(index))
		case cs.TypeBoolean:
			if r.ColumnBool(index) {
				*d = 1
			} else {
				*d = 0
			}
		default:
			*d = int(r.ColumnInt64(index))
		}
		return nil
	case *int32:
		*d = int32(r.ColumnInt64(index))
		return nil
	case *float64:
		switch colType {
		case cs.TypeFloat:
			*d = r.ColumnDouble(index)
		case cs.TypeInteger:
			*d = float64(r.ColumnInt64(index))
		default:
			*d = r.ColumnDouble(index)
		}
		return nil
	case *float32:
		*d = float32(r.ColumnDouble(index))
		return nil
	case *bool:
		switch colType {
		case cs.TypeBoolean:
			*d = r.ColumnBool(index)
		case cs.TypeInteger:
			*d = r.ColumnInt64(index) != 0
		default:
			*d = r.ColumnBool(index)
		}
		return nil
	case *time.Time:
		*d = r.ColumnTimestamp(index)
		return nil
	case *[]byte:
		switch colType {
		case cs.TypeBlob:
			*d = r.ColumnBlob(index)
		case cs.TypeText, cs.TypeJSON:
			*d = []byte(r.ColumnText(index))
		default:
			*d = r.ColumnBlob(index)
		}
		return nil
	case *sql.NullString:
		d.Valid = true
		d.String = r.ColumnText(index)
		return nil
	case *sql.NullInt64:
		d.Valid = true
		d.Int64 = r.ColumnInt64(index)
		return nil
	case *sql.NullFloat64:
		d.Valid = true
		d.Float64 = r.ColumnDouble(index)
		return nil
	case *sql.NullBool:
		d.Valid = true
		d.Bool = r.ColumnBool(index)
		return nil
	case *sql.NullTime:
		d.Valid = true
		d.Time = r.ColumnTimestamp(index)
		return nil
	}
	return errUnsupportedScan
}

// columnToAny extracts a column value into an any.
func columnToAny(r *cs.Rows, index int, colType int32) any {
	switch colType {
	case cs.TypeInteger:
		return r.ColumnInt64(index)
	case cs.TypeFloat:
		return r.ColumnDouble(index)
	case cs.TypeText:
		return r.ColumnText(index)
	case cs.TypeBoolean:
		return r.ColumnBool(index)
	case cs.TypeTimestamp:
		return r.ColumnTimestamp(index)
	case cs.TypeJSON:
		return r.ColumnText(index)
	case cs.TypeBlob:
		return r.ColumnBlob(index)
	default:
		return nil
	}
}
