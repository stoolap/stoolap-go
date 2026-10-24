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
	"errors"

	"github.com/tetratelabs/wazero/api"
)

var (
	errDBClosed   = errors.New("stoolap-wasm: database is closed")
	errStmtClosed = errors.New("stoolap-wasm: statement is closed")
	errTxDone     = errors.New("stoolap-wasm: transaction has already been committed or rolled back")
)

// DB represents a stoolap database connection over WASM.
type DB struct {
	engine *Engine
	ptr    uint32
}

// Open opens a database with the given DSN on this engine.
func (e *Engine) Open(ctx context.Context, dsn string) (*DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sqlPtr, _, err := e.stage(ctx, dsn, nil, nil)
	if err != nil {
		return nil, err
	}
	rc := e.call2(ctx, e.fnOpen, uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, 0)
	}
	return &DB{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// OpenMemory opens a new in-memory database.
func (e *Engine) OpenMemory(ctx context.Context) (*DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call1(ctx, e.fnOpenInMemory, uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, 0)
	}
	return &DB{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	if db.ptr == 0 {
		return nil
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callVoid1(context.Background(), e.fnClose, uint64(db.ptr))
	db.ptr = 0
	return nil
}

// Clone creates a cloned handle that shares the underlying engine.
func (db *DB) Clone(ctx context.Context) (*DB, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call2(ctx, e.fnClone, uint64(db.ptr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, db.ptr)
	}
	return &DB{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// sqlEntry names the three ad hoc entry points of a handle (a database or a
// transaction) for one operation: without parameters, with positional
// parameters and with named parameters, plus where its errors are read.
type sqlEntry struct {
	plain, positional, named, errmsg api.Function
}

// execOn runs an ad hoc statement on handle and returns the rows affected.
// The caller holds e.mu.
func (e *Engine) execOn(ctx context.Context, fn sqlEntry, handle uint32, query string, args []any, named []sql.NamedArg) (sql.Result, error) {
	sqlPtr, paramsPtr, err := e.stage(ctx, query, args, named)
	if err != nil {
		return nil, err
	}
	var rc uint64
	switch {
	case len(named) > 0:
		rc = e.call5(ctx, fn.named, uint64(handle), uint64(sqlPtr), uint64(paramsPtr), uint64(len(named)), uint64(e.outSlotB))
	case len(args) > 0:
		rc = e.call5(ctx, fn.positional, uint64(handle), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotB))
	default:
		rc = e.call3(ctx, fn.plain, uint64(handle), uint64(sqlPtr), uint64(e.outSlotB))
	}
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, fn.errmsg, handle)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

// queryOn runs an ad hoc query on handle and returns a chunked Rows over its
// result. The caller holds e.mu.
func (e *Engine) queryOn(ctx context.Context, fn sqlEntry, handle uint32, query string, args []any, named []sql.NamedArg) (*Rows, error) {
	sqlPtr, paramsPtr, err := e.stage(ctx, query, args, named)
	if err != nil {
		return nil, err
	}
	var rc uint64
	switch {
	case len(named) > 0:
		rc = e.call5(ctx, fn.named, uint64(handle), uint64(sqlPtr), uint64(paramsPtr), uint64(len(named)), uint64(e.outSlotA))
	case len(args) > 0:
		rc = e.call5(ctx, fn.positional, uint64(handle), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotA))
	default:
		rc = e.call3(ctx, fn.plain, uint64(handle), uint64(sqlPtr), uint64(e.outSlotA))
	}
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, fn.errmsg, handle)
	}
	return newRows(e, ctx, e.readU32(e.outSlotA)), nil
}

func (db *DB) execEntry() sqlEntry {
	e := db.engine
	return sqlEntry{plain: e.fnExec, positional: e.fnExecParams, named: e.fnExecNamed, errmsg: e.fnErrmsg}
}

func (db *DB) queryEntry() sqlEntry {
	e := db.engine
	return sqlEntry{plain: e.fnQuery, positional: e.fnQueryParams, named: e.fnQueryNamed, errmsg: e.fnErrmsg}
}

// Exec executes a SQL statement without parameters.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	return db.ExecParams(ctx, query, nil)
}

// ExecParams executes a SQL statement with positional parameters ($1, ?).
func (db *DB) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execOn(ctx, db.execEntry(), db.ptr, query, args, nil)
}

// ExecNamed executes a SQL statement with named parameters (:name).
func (db *DB) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execOn(ctx, db.execEntry(), db.ptr, query, nil, args)
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (*Rows, error) {
	return db.QueryParams(ctx, query, nil)
}

// QueryParams executes a query with positional parameters ($1, ?).
func (db *DB) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryOn(ctx, db.queryEntry(), db.ptr, query, args, nil)
}

// QueryNamed executes a query with named parameters (:name).
func (db *DB) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryOn(ctx, db.queryEntry(), db.ptr, query, nil, args)
}

// QueryAll runs a query with positional parameters and returns every row in
// one WASM call. There is no cursor to iterate or close.
func (db *DB) QueryAll(ctx context.Context, query string, args []any) (*ResultSet, error) {
	cols, rows, err := db.queryAllPacked(ctx, 0, query, args, nil)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// QueryNamedAll runs a query with named parameters (:name) and returns every
// row in one WASM call.
func (db *DB) QueryNamedAll(ctx context.Context, query string, args []sql.NamedArg) (*ResultSet, error) {
	cols, rows, err := db.queryAllPacked(ctx, 0, query, nil, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// queryAllPacked runs stoolap_query_params_all, or the named variant, on db,
// inside tx when tx is not 0, and decodes the packed result.
func (db *DB) queryAllPacked(ctx context.Context, tx uint32, query string, args []any, named []sql.NamedArg) ([]string, [][]any, error) {
	if db.ptr == 0 {
		return nil, nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	sqlPtr, paramsPtr, err := e.stage(ctx, query, args, named)
	if err != nil {
		return nil, nil, err
	}
	fn, n := e.fnQueryParamsAll, len(args)
	if len(named) > 0 {
		fn, n = e.fnQueryNamedAll, len(named)
	}
	rc := e.call7(ctx, fn, uint64(db.ptr), uint64(tx), uint64(sqlPtr), uint64(paramsPtr), uint64(n),
		uint64(e.outSlotA), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, nil, e.errFrom(ctx, e.fnErrmsg, db.ptr)
	}
	return e.decodePacked(e.readU32(e.outSlotA), e.readI64(e.outSlotB))
}

// Prepare creates a prepared statement.
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	sqlPtr, _, err := e.stage(ctx, query, nil, nil)
	if err != nil {
		return nil, err
	}
	rc := e.call3(ctx, e.fnPrepare, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, db.ptr)
	}
	return &Stmt{engine: e, db: db, ptr: e.readU32(e.outSlotA)}, nil
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call2(ctx, e.fnBegin, uint64(db.ptr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, db.ptr)
	}
	return &Tx{engine: e, db: db, ptr: e.readU32(e.outSlotA)}, nil
}

// BeginTx starts a transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	isolation := uint64(isolationReadCommitted)
	if opts != nil {
		switch sql.IsolationLevel(opts.Isolation) {
		case sql.LevelDefault, sql.LevelReadCommitted:
		case sql.LevelSnapshot, sql.LevelRepeatableRead:
			isolation = uint64(isolationSnapshot)
		default:
			return nil, errors.New("stoolap-wasm: unsupported isolation level")
		}
	}

	rc := e.call3(ctx, e.fnBeginIsolation, uint64(db.ptr), isolation, uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, db.ptr)
	}
	return &Tx{engine: e, db: db, ptr: e.readU32(e.outSlotA)}, nil
}

// ─── Stmt ───────────────────────────────────────────────────────────────────

// Stmt is a prepared statement.
type Stmt struct {
	engine *Engine
	db     *DB
	ptr    uint32
}

// ExecContext executes the statement with positional parameters.
func (s *Stmt) ExecContext(ctx context.Context, args []any) (sql.Result, error) {
	return s.execIn(ctx, 0, args, nil)
}

// ExecNamed executes the statement with named parameters (:name).
func (s *Stmt) ExecNamed(ctx context.Context, args []sql.NamedArg) (sql.Result, error) {
	return s.execIn(ctx, 0, nil, args)
}

// execIn runs the statement, inside tx when tx is not 0.
func (s *Stmt) execIn(ctx context.Context, tx uint32, args []any, named []sql.NamedArg) (sql.Result, error) {
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	paramsPtr, err := e.stageParams(ctx, args, named)
	if err != nil {
		return nil, err
	}
	fn, n := e.fnStmtExec, len(args)
	if len(named) > 0 {
		fn, n = e.fnStmtExecNamed, len(named)
	}
	var rc uint64
	if tx != 0 {
		if fn == e.fnStmtExec {
			fn = e.fnTxStmtExec
		} else {
			fn = e.fnTxStmtExecNamed
		}
		rc = e.call5(ctx, fn, uint64(tx), uint64(s.ptr), uint64(paramsPtr), uint64(n), uint64(e.outSlotB))
	} else {
		rc = e.call4(ctx, fn, uint64(s.ptr), uint64(paramsPtr), uint64(n), uint64(e.outSlotB))
	}
	if int32(rc) != stoolapOK {
		return nil, s.errIn(ctx, tx)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

// errIn reads the error of the last call: from the transaction for the
// stoolap_tx_stmt_* entry points, otherwise from the statement.
func (s *Stmt) errIn(ctx context.Context, tx uint32) error {
	if tx != 0 {
		return s.engine.errFrom(ctx, s.engine.fnTxErrmsg, tx)
	}
	return s.engine.errFrom(ctx, s.engine.fnStmtErrmsg, s.ptr)
}

// ExecBatch executes a prepared statement once per row inside a single
// transaction. One WASM call replaces begin + N x exec + commit. Every row
// must have the same number of parameters. On error the transaction is
// rolled back and nothing is applied. The result reports total rows affected.
func (s *Stmt) ExecBatch(ctx context.Context, rows [][]any) (sql.Result, error) {
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	if s.db.ptr == 0 {
		return nil, errDBClosed
	}
	if len(rows) == 0 {
		return execResult(0), nil
	}
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	paramsPtr, ppr, err := e.stageBatch(ctx, rows)
	if err != nil {
		return nil, err
	}
	rc := e.call6(ctx, e.fnStmtExecBatch, uint64(s.db.ptr), uint64(s.ptr), uint64(paramsPtr),
		uint64(uint32(ppr)), uint64(uint32(len(rows))), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnErrmsg, s.db.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

// QueryContext executes the statement with positional parameters and returns
// a chunked Rows over its result.
func (s *Stmt) QueryContext(ctx context.Context, args []any) (*Rows, error) {
	return s.query(ctx, args, nil)
}

// QueryNamed executes the statement with named parameters (:name).
func (s *Stmt) QueryNamed(ctx context.Context, args []sql.NamedArg) (*Rows, error) {
	return s.query(ctx, nil, args)
}

func (s *Stmt) query(ctx context.Context, args []any, named []sql.NamedArg) (*Rows, error) {
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	paramsPtr, err := e.stageParams(ctx, args, named)
	if err != nil {
		return nil, err
	}
	fn, n := e.fnStmtQuery, len(args)
	if len(named) > 0 {
		fn, n = e.fnStmtQueryNamed, len(named)
	}
	rc := e.call4(ctx, fn, uint64(s.ptr), uint64(paramsPtr), uint64(n), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFrom(ctx, e.fnStmtErrmsg, s.ptr)
	}
	return newRows(e, ctx, e.readU32(e.outSlotA)), nil
}

// QueryAll runs the statement with positional parameters and returns every
// row in one WASM call.
func (s *Stmt) QueryAll(ctx context.Context, args []any) (*ResultSet, error) {
	cols, rows, err := s.queryAllPacked(ctx, 0, args, nil)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// QueryNamedAll runs the statement with named parameters (:name) and returns
// every row in one WASM call.
func (s *Stmt) QueryNamedAll(ctx context.Context, args []sql.NamedArg) (*ResultSet, error) {
	cols, rows, err := s.queryAllPacked(ctx, 0, nil, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// queryAllPacked runs stoolap_stmt_query_all, or the named variant, inside
// tx when tx is not 0, and decodes the packed result.
func (s *Stmt) queryAllPacked(ctx context.Context, tx uint32, args []any, named []sql.NamedArg) ([]string, [][]any, error) {
	if s.ptr == 0 {
		return nil, nil, errStmtClosed
	}
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	paramsPtr, err := e.stageParams(ctx, args, named)
	if err != nil {
		return nil, nil, err
	}
	fn, n := e.fnStmtQueryAll, len(args)
	if len(named) > 0 {
		fn, n = e.fnStmtQueryNamedAll, len(named)
	}
	rc := e.call6(ctx, fn, uint64(s.ptr), uint64(tx), uint64(paramsPtr), uint64(n), uint64(e.outSlotA), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, nil, e.errFrom(ctx, e.fnStmtErrmsg, s.ptr)
	}
	return e.decodePacked(e.readU32(e.outSlotA), e.readI64(e.outSlotB))
}

// Close destroys the prepared statement.
func (s *Stmt) Close() error {
	if s.ptr == 0 {
		return nil
	}
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callVoid1(context.Background(), e.fnStmtFinalize, uint64(s.ptr))
	s.ptr = 0
	return nil
}

// ─── Tx ─────────────────────────────────────────────────────────────────────

// Tx is a transaction. Commit and Rollback consume it.
type Tx struct {
	engine *Engine
	db     *DB
	ptr    uint32
}

func (tx *Tx) execEntry() sqlEntry {
	e := tx.engine
	return sqlEntry{plain: e.fnTxExec, positional: e.fnTxExecParams, named: e.fnTxExecNamed, errmsg: e.fnTxErrmsg}
}

func (tx *Tx) queryEntry() sqlEntry {
	e := tx.engine
	return sqlEntry{plain: e.fnTxQuery, positional: e.fnTxQueryParams, named: e.fnTxQueryNamed, errmsg: e.fnTxErrmsg}
}

// Exec executes a SQL statement within the transaction.
func (tx *Tx) Exec(ctx context.Context, query string) (sql.Result, error) {
	return tx.ExecParams(ctx, query, nil)
}

// ExecParams executes a SQL statement with positional parameters within the transaction.
func (tx *Tx) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execOn(ctx, tx.execEntry(), tx.ptr, query, args, nil)
}

// ExecNamed executes a SQL statement with named parameters (:name) within the transaction.
func (tx *Tx) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execOn(ctx, tx.execEntry(), tx.ptr, query, nil, args)
}

// Query executes a query within the transaction.
func (tx *Tx) Query(ctx context.Context, query string) (*Rows, error) {
	return tx.QueryParams(ctx, query, nil)
}

// QueryParams executes a query with positional parameters within the transaction.
func (tx *Tx) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryOn(ctx, tx.queryEntry(), tx.ptr, query, args, nil)
}

// QueryNamed executes a query with named parameters (:name) within the transaction.
func (tx *Tx) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryOn(ctx, tx.queryEntry(), tx.ptr, query, nil, args)
}

// QueryAll runs a query with positional parameters inside the transaction
// and returns every row in one WASM call.
func (tx *Tx) QueryAll(ctx context.Context, query string, args []any) (*ResultSet, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	cols, rows, err := tx.db.queryAllPacked(ctx, tx.ptr, query, args, nil)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// QueryNamedAll runs a query with named parameters (:name) inside the
// transaction and returns every row in one WASM call.
func (tx *Tx) QueryNamedAll(ctx context.Context, query string, args []sql.NamedArg) (*ResultSet, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	cols, rows, err := tx.db.queryAllPacked(ctx, tx.ptr, query, nil, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if tx.ptr == 0 {
		return errTxDone
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	ctx := context.Background()
	rc := e.call1(ctx, e.fnTxCommit, uint64(tx.ptr))
	if int32(rc) != stoolapOK {
		return e.errFrom(ctx, e.fnTxErrmsg, tx.ptr)
	}
	tx.ptr = 0
	return nil
}

// Rollback rolls the transaction back. Rolling back a finished transaction is a no-op.
func (tx *Tx) Rollback() error {
	if tx.ptr == 0 {
		return nil
	}
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callVoid1(context.Background(), e.fnTxRollback, uint64(tx.ptr))
	tx.ptr = 0
	return nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

type execResult int64

func (r execResult) LastInsertId() (int64, error) { return 0, nil }
func (r execResult) RowsAffected() (int64, error) { return int64(r), nil }
