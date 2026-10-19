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
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var errDBClosed = errors.New("stoolap: database is closed")
var errRowsClosed = errors.New("stoolap: rows are closed")
var errStmtClosed = errors.New("stoolap: statement is closed")
var errBatchRowLength = errors.New("stoolap: batch rows must all have the same number of parameters")
var errEmptyParamName = errors.New("stoolap: named parameter has an empty name")
var errTxDone = errors.New("stoolap: transaction has already been committed or rolled back")
var errColumnCount = errors.New("stoolap: scan destination count does not match column count")
var errUnsupportedDest = errors.New("stoolap: unsupported scan destination type")

// ErrorCode categorizes stoolap engine errors.
type ErrorCode int

const (
	// ErrGeneral is the default code for uncategorized errors.
	ErrGeneral ErrorCode = iota
	// ErrUniqueConstraint indicates a unique index constraint violation.
	ErrUniqueConstraint
	// ErrPrimaryKeyConstraint indicates a primary key constraint violation.
	ErrPrimaryKeyConstraint
	// ErrNotNullConstraint indicates a NOT NULL constraint violation.
	ErrNotNullConstraint
	// ErrCheckConstraint indicates a CHECK constraint violation.
	ErrCheckConstraint
	// ErrForeignKeyViolation indicates a foreign key constraint violation.
	ErrForeignKeyViolation
	// ErrTableNotFound indicates the referenced table does not exist.
	ErrTableNotFound
	// ErrTableExists indicates the table already exists.
	ErrTableExists
)

// Error is a stoolap engine error with a categorized error code.
// Use errors.As to extract it:
//
//	var stErr *stoolap.Error
//	if errors.As(err, &stErr) {
//	    switch stErr.Code() {
//	    case stoolap.ErrUniqueConstraint:
//	        // handle duplicate
//	    }
//	}
type Error struct {
	msg  string
	code ErrorCode
}

func (e *Error) Error() string   { return e.msg }
func (e *Error) Code() ErrorCode { return e.code }

// IsConstraintViolation reports whether the error is any kind of constraint violation
// (unique, primary key, not null, check, or foreign key).
func (e *Error) IsConstraintViolation() bool {
	return e.code >= ErrUniqueConstraint && e.code <= ErrForeignKeyViolation
}

func newError(msg string) error {
	return &Error{msg: msg, code: classifyError(msg)}
}

func classifyError(msg string) ErrorCode {
	if strings.HasPrefix(msg, "unique constraint failed") {
		return ErrUniqueConstraint
	}
	if strings.HasPrefix(msg, "primary key constraint failed") {
		return ErrPrimaryKeyConstraint
	}
	if strings.HasPrefix(msg, "not null constraint failed") {
		return ErrNotNullConstraint
	}
	if strings.HasPrefix(msg, "CHECK constraint failed") {
		return ErrCheckConstraint
	}
	if strings.HasPrefix(msg, "foreign key constraint violation") {
		return ErrForeignKeyViolation
	}
	if strings.HasPrefix(msg, "table '") || strings.HasPrefix(msg, "table or view '") {
		if strings.Contains(msg, "not found") {
			return ErrTableNotFound
		}
		if strings.Contains(msg, "already exists") {
			return ErrTableExists
		}
	}
	return ErrGeneral
}

// Version returns the stoolap engine version.
func Version() (string, error) {
	if err := loadLibrary(); err != nil {
		return "", err
	}
	return goString(unsafe.Pointer(abiCall1(sym.version, 0))), nil
}

// DB represents a stoolap database connection.
type DB struct {
	ptr uintptr
}

// errStr reads the error message from a handle (db, tx, stmt, or 0 for global).
func errStr(fn, handle uintptr) string {
	return goString(unsafe.Pointer(abiCall1(fn, handle)))
}

// Open opens a database with the given DSN.
func Open(dsn string) (*DB, error) {
	if err := loadLibrary(); err != nil {
		return nil, err
	}
	cs := newCStr(dsn)
	var dbPtr uintptr
	rc := abiCall2(sym.open, cs.ptr, uintptr(unsafe.Pointer(&dbPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, 0))
	}
	return &DB{ptr: dbPtr}, nil
}

// OpenMemory opens a new in-memory database.
func OpenMemory() (*DB, error) {
	if err := loadLibrary(); err != nil {
		return nil, err
	}
	var dbPtr uintptr
	rc := abiCall1(sym.openInMemory, uintptr(unsafe.Pointer(&dbPtr)))
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, 0))
	}
	return &DB{ptr: dbPtr}, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if db.ptr == 0 {
		return nil
	}
	abiCall1(sym.close, db.ptr)
	db.ptr = 0
	return nil
}

// Clone creates a cloned handle for concurrent use.
func (db *DB) Clone() (*DB, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	var clonePtr uintptr
	rc := abiCall2(sym.clone, db.ptr, uintptr(unsafe.Pointer(&clonePtr)))
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return &DB{ptr: clonePtr}, nil
}

// Exec executes a SQL statement.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var affected int64
	rc := abiCall3(sym.exec, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return execResult(affected), nil
}

// ExecParams executes a SQL statement with positional parameters.
func (db *DB) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return db.Exec(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeParams(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	rc := abiCall5(sym.execParams, db.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return execResult(affected), nil
}

// ExecNamed executes a SQL statement with named parameters (:name).
func (db *DB) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return db.Exec(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	rc := abiCall5(sym.execNamed, db.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return execResult(affected), nil
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (*Rows, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var rowsPtr uintptr
	rc := abiCall3(sym.query, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryParams executes a query with positional parameters.
func (db *DB) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return db.Query(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeParams(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall5(sym.queryParams, db.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryNamed executes a query with named parameters (:name).
func (db *DB) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return db.Query(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall5(sym.queryNamed, db.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryAll runs a query with positional parameters and returns every row in
// one native call. No rows handle is created, so a point lookup costs a single
// crossing. Use Query for results too large to hold in memory.
func (db *DB) QueryAll(ctx context.Context, query string, args []any) (*ResultSet, error) {
	cols, rows, err := db.queryAllPacked(ctx, 0, query, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// queryAllPacked runs stoolap_query_params_all on db, inside tx when tx is
// not 0, and decodes the packed result the engine hands back.
func (db *DB) queryAllPacked(ctx context.Context, tx uintptr, query string, args []any) ([]string, [][]any, error) {
	if db.ptr == 0 {
		return nil, nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	cs := newCStr(query)
	ep, err := encodeParams(args)
	if err != nil {
		return nil, nil, err
	}
	var bufPtr uintptr
	var bufLen int64
	rc := abiCall7(sym.queryParamsAll, db.ptr, tx, cs.ptr, ep.ptr, uintptr(int32(len(args))),
		uintptr(unsafe.Pointer(&bufPtr)), uintptr(unsafe.Pointer(&bufLen)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return decodePacked(bufPtr, bufLen)
}

// Prepare creates a prepared statement.
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var stmtPtr uintptr
	rc := abiCall3(sym.prepare, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&stmtPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return &Stmt{ptr: stmtPtr, db: db}, nil
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var txPtr uintptr
	rc := abiCall2(sym.begin, db.ptr, uintptr(unsafe.Pointer(&txPtr)))
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return &Tx{ptr: txPtr, db: db}, nil
}

// BeginTx starts a transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if db.ptr == 0 {
		return nil, errDBClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	isolation := int32(isolationReadCommitted)
	if opts != nil {
		switch sql.IsolationLevel(opts.Isolation) {
		case sql.LevelDefault, sql.LevelReadCommitted:
		case sql.LevelSnapshot, sql.LevelRepeatableRead:
			isolation = isolationSnapshot
		default:
			return nil, errors.New("stoolap: unsupported isolation level")
		}
	}
	var txPtr uintptr
	rc := abiCall3(sym.beginIso, db.ptr, uintptr(isolation), uintptr(unsafe.Pointer(&txPtr)))
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, db.ptr))
	}
	return &Tx{ptr: txPtr, db: db}, nil
}

// ─── Stmt ───────────────────────────────────────────────────────────────────

// Stmt is a prepared statement.
type Stmt struct {
	mu      sync.Mutex
	ptr     uintptr
	db      *DB // owning connection, used by ExecBatch
	scratch stmtParamScratch
}

// ExecContext executes a prepared statement with parameters.
func (s *Stmt) ExecContext(ctx context.Context, args []any) (sql.Result, error) {
	return s.execIn(ctx, 0, args)
}

// ExecNamed executes a prepared statement with named parameters (:name).
func (s *Stmt) ExecNamed(ctx context.Context, args []sql.NamedArg) (sql.Result, error) {
	return s.execNamedIn(ctx, 0, args)
}

// execIn runs the statement, inside tx when tx is not 0.
func (s *Stmt) execIn(ctx context.Context, tx uintptr, args []any) (sql.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	ep, err := s.scratch.encode(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	var rc uintptr
	if tx == 0 {
		rc = abiCall4(sym.stmtExec, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	} else {
		rc = abiCall5(sym.txStmtExec, tx, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	}
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, s.errIn(tx)
	}
	return execResult(affected), nil
}

// execNamedIn runs the statement with named parameters, inside tx when tx is not 0.
func (s *Stmt) execNamedIn(ctx context.Context, tx uintptr, args []sql.NamedArg) (sql.Result, error) {
	if len(args) == 0 {
		return s.execIn(ctx, tx, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	var rc uintptr
	if tx == 0 {
		rc = abiCall4(sym.stmtExecNamed, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	} else {
		rc = abiCall5(sym.txStmtExecNamed, tx, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	}
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, s.errIn(tx)
	}
	return execResult(affected), nil
}

// errIn reads the error of the last call: from the transaction for the
// stoolap_tx_stmt_* entry points, otherwise from the statement.
func (s *Stmt) errIn(tx uintptr) error {
	if tx != 0 {
		return newError(errStr(sym.txErrmsg, tx))
	}
	return newError(errStr(sym.stmtErrmsg, s.ptr))
}

// ExecBatch executes a prepared statement once per row inside a single
// transaction. One native call replaces begin + N x exec + commit. Every row
// must have the same number of parameters. On error the transaction is
// rolled back and nothing is applied. The result reports total rows affected.
func (s *Stmt) ExecBatch(ctx context.Context, rows [][]any) (sql.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	if s.db.ptr == 0 {
		return nil, errDBClosed
	}
	if len(rows) == 0 {
		return execResult(0), nil
	}
	ep, ppr, err := s.scratch.encodeBatch(rows)
	if err != nil {
		return nil, err
	}
	var affected int64
	rc := abiCall6(sym.stmtExecBatch, s.db.ptr, s.ptr, ep.ptr, uintptr(int32(ppr)), uintptr(int32(len(rows))), uintptr(unsafe.Pointer(&affected)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.errmsg, s.db.ptr))
	}
	return execResult(affected), nil
}

// QueryContext executes a prepared statement query with parameters.
func (s *Stmt) QueryContext(ctx context.Context, args []any) (*Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	ep, err := s.scratch.encode(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall4(sym.stmtQuery, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.stmtErrmsg, s.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryNamed executes a prepared statement query with named parameters (:name).
func (s *Stmt) QueryNamed(ctx context.Context, args []sql.NamedArg) (*Rows, error) {
	if len(args) == 0 {
		return s.QueryContext(ctx, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, errStmtClosed
	}
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall4(sym.stmtQueryNamed, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.stmtErrmsg, s.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryAll runs a prepared query and returns every row in one native call.
func (s *Stmt) QueryAll(ctx context.Context, args []any) (*ResultSet, error) {
	cols, rows, err := s.queryAllPacked(ctx, 0, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// QueryNamedAll runs a prepared query with named parameters (:name) and
// returns every row in one native call.
func (s *Stmt) QueryNamedAll(ctx context.Context, args []sql.NamedArg) (*ResultSet, error) {
	cols, rows, err := s.queryNamedAllPacked(ctx, 0, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// queryAllPacked runs stoolap_stmt_query_all, inside tx when tx is not 0,
// and decodes the packed result the engine hands back.
func (s *Stmt) queryAllPacked(ctx context.Context, tx uintptr, args []any) ([]string, [][]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, nil, errStmtClosed
	}
	ep, err := s.scratch.encode(args)
	if err != nil {
		return nil, nil, err
	}
	var bufPtr uintptr
	var bufLen int64
	rc := abiCall6(sym.stmtQueryAll, s.ptr, tx, ep.ptr, uintptr(int32(len(args))),
		uintptr(unsafe.Pointer(&bufPtr)), uintptr(unsafe.Pointer(&bufLen)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, nil, newError(errStr(sym.stmtErrmsg, s.ptr))
	}
	return decodePacked(bufPtr, bufLen)
}

// queryNamedAllPacked is queryAllPacked with named parameters.
func (s *Stmt) queryNamedAllPacked(ctx context.Context, tx uintptr, args []sql.NamedArg) ([]string, [][]any, error) {
	if len(args) == 0 {
		return s.queryAllPacked(ctx, tx, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil, nil, errStmtClosed
	}
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, nil, err
	}
	var bufPtr uintptr
	var bufLen int64
	rc := abiCall6(sym.stmtQueryNamedAll, s.ptr, tx, ep.ptr, uintptr(int32(len(args))),
		uintptr(unsafe.Pointer(&bufPtr)), uintptr(unsafe.Pointer(&bufLen)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, nil, newError(errStr(sym.stmtErrmsg, s.ptr))
	}
	return decodePacked(bufPtr, bufLen)
}

// Close destroys the prepared statement.
func (s *Stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == 0 {
		return nil
	}
	abiCallVoid1(sym.stmtFinalize, s.ptr)
	s.ptr = 0
	s.scratch.reset()
	return nil
}

// ─── Tx ─────────────────────────────────────────────────────────────────────

// Tx is a database transaction.
type Tx struct {
	ptr uintptr
	db  *DB
}

// Exec executes within the transaction.
func (tx *Tx) Exec(ctx context.Context, query string) (sql.Result, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var affected int64
	rc := abiCall3(sym.txExec, tx.ptr, cs.ptr, uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return execResult(affected), nil
}

// ExecParams executes with parameters within the transaction.
func (tx *Tx) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return tx.Exec(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeParams(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	rc := abiCall5(sym.txExecParams, tx.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return execResult(affected), nil
}

// ExecNamed executes with named parameters (:name) within the transaction.
func (tx *Tx) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return tx.Exec(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var affected int64
	rc := abiCall5(sym.txExecNamed, tx.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return execResult(affected), nil
}

// Query within the transaction.
func (tx *Tx) Query(ctx context.Context, query string) (*Rows, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var rowsPtr uintptr
	rc := abiCall3(sym.txQuery, tx.ptr, cs.ptr, uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryParams executes a query with parameters within the transaction.
func (tx *Tx) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return tx.Query(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeParams(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall5(sym.txQueryParams, tx.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryNamed executes a query with named parameters (:name) within the transaction.
func (tx *Tx) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return tx.Query(ctx, query)
	}
	cs := newCStr(query)
	ep, err := encodeNamedParams(args)
	if err != nil {
		return nil, err
	}
	var rowsPtr uintptr
	rc := abiCall5(sym.txQueryNamed, tx.ptr, cs.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, newError(errStr(sym.txErrmsg, tx.ptr))
	}
	return newRows(rowsPtr), nil
}

// Commit commits the transaction.
// The underlying transaction handle is freed whether commit succeeds or fails,
// so tx.ptr is always zeroed. On failure the error is retrieved from the
// process-global error slot (the handle is no longer valid).
func (tx *Tx) Commit() error {
	if tx.ptr == 0 {
		return nil
	}
	rc := abiCall1(sym.txCommit, tx.ptr)
	tx.ptr = 0
	if int32(rc) != stoolapOK {
		return newError(errStr(sym.errmsg, 0))
	}
	return nil
}

// Rollback rolls back the transaction.
func (tx *Tx) Rollback() error {
	if tx.ptr == 0 {
		return nil
	}
	abiCallVoid1(sym.txRollback, tx.ptr)
	tx.ptr = 0
	return nil
}

// QueryAll runs a query with positional parameters inside the transaction
// and returns every row in one native call.
func (tx *Tx) QueryAll(ctx context.Context, query string, args []any) (*ResultSet, error) {
	if tx.ptr == 0 {
		return nil, errTxDone
	}
	cols, rows, err := tx.db.queryAllPacked(ctx, tx.ptr, query, args)
	if err != nil {
		return nil, err
	}
	return &ResultSet{Columns: cols, Rows: rows}, nil
}

// ─── Parameter encoding ─────────────────────────────────────────────────────

// StoolapValue C layout (64-bit native):
//
//	[0:4]   int32  value_type
//	[4:8]   int32  _padding
//	[8:24]  union  (16 bytes)
//	  int64/float64/timestamp: 8 bytes at offset 8
//	  bool: int32 at offset 8
//	  text/blob: pointer(8 bytes) at offset 8 + length(8 bytes) at offset 16
const stoolapValueSize = 24
const maxPooledParamBufCap = stoolapValueSize * 64
const maxPooledParamDataCap = 16 << 10

var encodedParamsBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, stoolapValueSize*8)
		return &b
	},
}

var encodedParamsDataPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// encodedParams holds the encoded StoolapValue array and keeps all
// Go-allocated buffers alive until the FFI call completes.
type encodedParams struct {
	ptr    uintptr // pointer to buf[0]
	buf    []byte  // StoolapValue array
	data   []byte  // packed string/blob data
	bufpb  *[]byte
	datapb *[]byte
}

type stmtParamScratch struct {
	buf  []byte
	data []byte
}

func (ep *encodedParams) keepAlive() {
	runtime.KeepAlive(ep.buf)
	runtime.KeepAlive(ep.data)
	if ep.bufpb != nil {
		*ep.bufpb = ep.buf[:0]
		encodedParamsBufPool.Put(ep.bufpb)
	}
	if ep.datapb != nil {
		*ep.datapb = ep.data[:0]
		encodedParamsDataPool.Put(ep.datapb)
	}
}

func (ps *stmtParamScratch) encode(args []any) (encodedParams, error) {
	if len(args) == 0 {
		return encodedParams{}, nil
	}
	ps.buf = resizeAndClearBytes(ps.buf, len(args)*stoolapValueSize)
	ps.data = resizeBytes(ps.data, paramsDataSize(args))
	return encodeParamsBuffers(args, ps.buf, ps.data)
}

// encodeBatch packs rows into one flat row-major StoolapValue array and
// returns the params-per-row count. Reuses the statement scratch buffers.
func (ps *stmtParamScratch) encodeBatch(rows [][]any) (encodedParams, int, error) {
	ppr := len(rows[0])
	dataSize := 0
	for _, row := range rows {
		if len(row) != ppr {
			return encodedParams{}, 0, errBatchRowLength
		}
		dataSize += paramsDataSize(row)
	}
	if ppr == 0 {
		return encodedParams{}, 0, nil
	}
	ps.buf = resizeAndClearBytes(ps.buf, len(rows)*ppr*stoolapValueSize)
	ps.data = resizeBytes(ps.data, dataSize)
	bufOff, dataOff := 0, 0
	for _, row := range rows {
		if _, err := encodeParamsBuffers(row, ps.buf[bufOff:], ps.data[dataOff:]); err != nil {
			return encodedParams{}, 0, err
		}
		bufOff += ppr * stoolapValueSize
		dataOff += paramsDataSize(row)
	}
	return encodedParams{
		ptr:  uintptr(unsafe.Pointer(&ps.buf[0])),
		buf:  ps.buf,
		data: ps.data,
	}, ppr, nil
}

func (ps *stmtParamScratch) reset() {
	ps.buf = nil
	ps.data = nil
}

func encodeParams(args []any) (encodedParams, error) {
	if len(args) == 0 {
		return encodedParams{}, nil
	}

	size := len(args) * stoolapValueSize
	var buf []byte
	var bufpb *[]byte
	if size <= maxPooledParamBufCap {
		bufpb = encodedParamsBufPool.Get().(*[]byte)
		buf = *bufpb
		buf = resizeAndClearBytes(buf, size)
		*bufpb = buf
	} else {
		buf = resizeAndClearBytes(nil, size)
	}

	dataSize := paramsDataSize(args)
	var data []byte
	var datapb *[]byte
	if dataSize > 0 {
		if dataSize <= maxPooledParamDataCap {
			datapb = encodedParamsDataPool.Get().(*[]byte)
			data = *datapb
			data = resizeBytes(data, dataSize)
			*datapb = data
		} else {
			data = resizeBytes(nil, dataSize)
		}
	}
	ep, err := encodeParamsBuffers(args, buf, data)
	if err != nil {
		if bufpb != nil {
			encodedParamsBufPool.Put(bufpb)
		}
		if datapb != nil {
			encodedParamsDataPool.Put(datapb)
		}
		return encodedParams{}, err
	}
	ep.bufpb = bufpb
	ep.datapb = datapb
	return ep, nil
}

// putPtr writes a native pointer value into a byte slice.
func putPtr(b []byte, p uintptr) {
	switch unsafe.Sizeof(uintptr(0)) {
	case 8:
		binary.LittleEndian.PutUint64(b, uint64(p))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(p))
	}
}

func paramsDataSize(args []any) int {
	dataSize := 0
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			dataSize += len(v)
		case []byte:
			dataSize += len(v)
		}
	}
	return dataSize
}

func resizeAndClearBytes(buf []byte, size int) []byte {
	if size == 0 {
		return buf[:0]
	}
	if cap(buf) < size {
		return make([]byte, size)
	}
	buf = buf[:size]
	clear(buf)
	return buf
}

func resizeBytes(buf []byte, size int) []byte {
	if size == 0 {
		return buf[:0]
	}
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

var errUnsupportedParamType = errors.New("stoolap: unsupported parameter type")

func encodeParamsBuffers(args []any, buf, data []byte) (encodedParams, error) {
	if len(args) == 0 {
		return encodedParams{}, nil
	}

	dataOff := 0
	for i, arg := range args {
		off := i * stoolapValueSize
		switch v := arg.(type) {
		case nil:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeNull))
		case int64:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeInteger))
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(v))
		case int:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeInteger))
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(v))
		case int32:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeInteger))
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(v))
		case float64:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeFloat))
			binary.LittleEndian.PutUint64(buf[off+8:], math.Float64bits(v))
		case float32:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeFloat))
			binary.LittleEndian.PutUint64(buf[off+8:], math.Float64bits(float64(v)))
		case bool:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeBoolean))
			if v {
				binary.LittleEndian.PutUint32(buf[off+8:], 1)
			}
		case string:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeText))
			if len(v) > 0 {
				copy(data[dataOff:dataOff+len(v)], v)
				putPtr(buf[off+8:], uintptr(unsafe.Pointer(&data[dataOff])))
				dataOff += len(v)
			}
			binary.LittleEndian.PutUint64(buf[off+16:], uint64(len(v)))
		case []byte:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeBlob))
			if len(v) > 0 {
				copy(data[dataOff:dataOff+len(v)], v)
				putPtr(buf[off+8:], uintptr(unsafe.Pointer(&data[dataOff])))
				dataOff += len(v)
			}
			binary.LittleEndian.PutUint64(buf[off+16:], uint64(len(v)))
		case time.Time:
			binary.LittleEndian.PutUint32(buf[off:], uint32(typeTimestamp))
			nanos := v.UnixNano()
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(nanos))
		default:
			return encodedParams{}, errUnsupportedParamType
		}
	}

	return encodedParams{
		ptr:  uintptr(unsafe.Pointer(&buf[0])),
		buf:  buf,
		data: data,
	}, nil
}

// encodeValue writes one StoolapValue into buf[0:stoolapValueSize] for the
// named-parameter path. The positional path keeps its own inlined switch so
// the hot loop pays no call per value. String and blob payloads are copied
// into data starting at dataOff; the new offset is returned.
func encodeValue(buf, data []byte, dataOff int, arg any) (int, error) {
	switch v := arg.(type) {
	case nil:
		binary.LittleEndian.PutUint32(buf, uint32(typeNull))
	case int64:
		binary.LittleEndian.PutUint32(buf, uint32(typeInteger))
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case int:
		binary.LittleEndian.PutUint32(buf, uint32(typeInteger))
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case int32:
		binary.LittleEndian.PutUint32(buf, uint32(typeInteger))
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case float64:
		binary.LittleEndian.PutUint32(buf, uint32(typeFloat))
		binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(v))
	case float32:
		binary.LittleEndian.PutUint32(buf, uint32(typeFloat))
		binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(float64(v)))
	case bool:
		binary.LittleEndian.PutUint32(buf, uint32(typeBoolean))
		if v {
			binary.LittleEndian.PutUint32(buf[8:], 1)
		}
	case string:
		binary.LittleEndian.PutUint32(buf, uint32(typeText))
		if len(v) > 0 {
			copy(data[dataOff:dataOff+len(v)], v)
			putPtr(buf[8:], uintptr(unsafe.Pointer(&data[dataOff])))
			dataOff += len(v)
		}
		binary.LittleEndian.PutUint64(buf[16:], uint64(len(v)))
	case []byte:
		binary.LittleEndian.PutUint32(buf, uint32(typeBlob))
		if len(v) > 0 {
			copy(data[dataOff:dataOff+len(v)], v)
			putPtr(buf[8:], uintptr(unsafe.Pointer(&data[dataOff])))
			dataOff += len(v)
		}
		binary.LittleEndian.PutUint64(buf[16:], uint64(len(v)))
	case time.Time:
		binary.LittleEndian.PutUint32(buf, uint32(typeTimestamp))
		binary.LittleEndian.PutUint64(buf[8:], uint64(v.UnixNano()))
	default:
		return dataOff, errUnsupportedParamType
	}
	return dataOff, nil
}

// StoolapNamedParam C layout (64-bit native):
//
//	[0:8]   pointer name (not NUL-terminated)
//	[8:12]  int32   name_len
//	[12:16] int32   _padding
//	[16:40] StoolapValue value
const stoolapNamedParamSize = 40

// encodeNamedParams packs args into a StoolapNamedParam array. Names and
// string/blob payloads share one data buffer. Uses the same pools as
// encodeParams.
func encodeNamedParams(args []sql.NamedArg) (encodedParams, error) {
	size := len(args) * stoolapNamedParamSize
	dataSize := 0
	for _, a := range args {
		if a.Name == "" {
			return encodedParams{}, errEmptyParamName
		}
		dataSize += len(a.Name)
		switch v := a.Value.(type) {
		case string:
			dataSize += len(v)
		case []byte:
			dataSize += len(v)
		}
	}

	var buf []byte
	var bufpb *[]byte
	if size <= maxPooledParamBufCap {
		bufpb = encodedParamsBufPool.Get().(*[]byte)
		buf = resizeAndClearBytes(*bufpb, size)
		*bufpb = buf
	} else {
		buf = resizeAndClearBytes(nil, size)
	}
	var data []byte
	var datapb *[]byte
	if dataSize <= maxPooledParamDataCap {
		datapb = encodedParamsDataPool.Get().(*[]byte)
		data = resizeBytes(*datapb, dataSize)
		*datapb = data
	} else {
		data = resizeBytes(nil, dataSize)
	}

	dataOff := 0
	for i, a := range args {
		slot := buf[i*stoolapNamedParamSize:]
		copy(data[dataOff:dataOff+len(a.Name)], a.Name)
		putPtr(slot, uintptr(unsafe.Pointer(&data[dataOff])))
		binary.LittleEndian.PutUint32(slot[8:], uint32(len(a.Name)))
		dataOff += len(a.Name)
		var err error
		dataOff, err = encodeValue(slot[16:], data, dataOff, a.Value)
		if err != nil {
			if bufpb != nil {
				encodedParamsBufPool.Put(bufpb)
			}
			if datapb != nil {
				encodedParamsDataPool.Put(datapb)
			}
			return encodedParams{}, err
		}
	}

	return encodedParams{
		ptr:    uintptr(unsafe.Pointer(&buf[0])),
		buf:    buf,
		data:   data,
		bufpb:  bufpb,
		datapb: datapb,
	}, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

type execResult int64

func (r execResult) LastInsertId() (int64, error) { return 0, nil }
func (r execResult) RowsAffected() (int64, error) { return int64(r), nil }

// copyBlob copies len bytes from a C-allocated pointer into a Go []byte.
func copyBlob(ptr unsafe.Pointer, length int64) []byte {
	src := unsafe.Slice((*byte)(ptr), length)
	cp := make([]byte, length)
	copy(cp, src)
	return cp
}
