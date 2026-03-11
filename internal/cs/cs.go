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
package cs

/*
#include "stoolap.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"database/sql/driver"
	"errors"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// Status codes (matching STOOLAP_OK, STOOLAP_ERROR, STOOLAP_ROW, STOOLAP_DONE)
const (
	statusOK   = 0
	statusRow  = 100
	statusDone = 101
)

// Type codes (matching STOOLAP_TYPE_*)
const (
	TypeNull      int32 = 0
	TypeInteger   int32 = 1
	TypeFloat     int32 = 2
	TypeText      int32 = 3
	TypeBoolean   int32 = 4
	TypeTimestamp int32 = 5
	TypeJSON      int32 = 6
	TypeBlob      int32 = 7
)

// Isolation levels (matching STOOLAP_ISOLATION_*)
const (
	IsolationReadCommitted int32 = 0
	IsolationSnapshot      int32 = 1
)

// Pre-allocated errors to avoid allocations on hot paths
var (
	errNilDB            = errors.New("stoolap: nil database handle")
	errNilStmt          = errors.New("stoolap: nil statement handle")
	errNilTx            = errors.New("stoolap: nil transaction handle")
	errNilRows          = errors.New("stoolap: nil rows handle")
	errUnknown          = errors.New("stoolap: unknown error")
	errUnknownStmt      = errors.New("stoolap: unknown statement error")
	errUnknownTx        = errors.New("stoolap: unknown transaction error")
	errUnknownRows      = errors.New("stoolap: unknown rows error")
	errCloseFailed      = errors.New("stoolap: close failed")
	errUnsupportedParam = errors.New("stoolap: unsupported parameter type")
)

// DB wraps a StoolapDB* handle.
type DB struct {
	ptr *C.StoolapDB
}

// Stmt wraps a StoolapStmt* handle.
type Stmt struct {
	ptr *C.StoolapStmt
}

// Tx wraps a StoolapTx* handle.
type Tx struct {
	ptr *C.StoolapTx
}

// Rows wraps a StoolapRows* handle.
type Rows struct {
	ptr *C.StoolapRows
}

// errFromDB extracts the error message from a DB handle.
func errFromDB(db *C.StoolapDB) error {
	msg := C.stoolap_errmsg(db)
	if msg == nil {
		return errUnknown
	}
	s := C.GoString(msg)
	if s == "" {
		return errUnknown
	}
	return errors.New("stoolap: " + s)
}

// errFromGlobal extracts the global error message (for open failures).
func errFromGlobal() error {
	msg := C.stoolap_errmsg(nil)
	if msg == nil {
		return errUnknown
	}
	s := C.GoString(msg)
	if s == "" {
		return errUnknown
	}
	return errors.New("stoolap: " + s)
}

// errFromStmt extracts the error message from a Stmt handle.
func errFromStmt(stmt *C.StoolapStmt) error {
	msg := C.stoolap_stmt_errmsg(stmt)
	if msg == nil {
		return errUnknownStmt
	}
	s := C.GoString(msg)
	if s == "" {
		return errUnknownStmt
	}
	return errors.New("stoolap: " + s)
}

// errFromTx extracts the error message from a Tx handle.
func errFromTx(tx *C.StoolapTx) error {
	msg := C.stoolap_tx_errmsg(tx)
	if msg == nil {
		return errUnknownTx
	}
	s := C.GoString(msg)
	if s == "" {
		return errUnknownTx
	}
	return errors.New("stoolap: " + s)
}

// errFromRows extracts the error message from a Rows handle.
func errFromRows(rows *C.StoolapRows) error {
	msg := C.stoolap_rows_errmsg(rows)
	if msg == nil {
		return errUnknownRows
	}
	s := C.GoString(msg)
	if s == "" {
		return errUnknownRows
	}
	return errors.New("stoolap: " + s)
}

// ---- Database lifecycle ----

// Version returns the stoolap library version string.
func Version() string {
	return C.GoString(C.stoolap_version())
}

// Open opens a database connection with the given DSN.
func Open(dsn string) (*DB, error) {
	// Pin goroutine to OS thread: errFromGlobal reads thread-local error storage.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cdsn := C.CString(dsn)
	defer C.free(unsafe.Pointer(cdsn))

	var ptr *C.StoolapDB
	rc := C.stoolap_open(cdsn, &ptr)
	if rc != statusOK {
		return nil, errFromGlobal()
	}
	return &DB{ptr: ptr}, nil
}

// OpenInMemory opens a new in-memory database.
func OpenInMemory() (*DB, error) {
	// Pin goroutine to OS thread: errFromGlobal reads thread-local error storage.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var ptr *C.StoolapDB
	rc := C.stoolap_open_in_memory(&ptr)
	if rc != statusOK {
		return nil, errFromGlobal()
	}
	return &DB{ptr: ptr}, nil
}

// Clone creates a cloned handle for multi-threaded use.
// The clone shares the underlying engine but has its own executor and error state.
func (db *DB) Clone() (*DB, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	var ptr *C.StoolapDB
	rc := C.stoolap_clone(db.ptr, &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &DB{ptr: ptr}, nil
}

// Close closes the database handle and frees resources.
func (db *DB) Close() error {
	if db.ptr == nil {
		return nil
	}
	rc := C.stoolap_close(db.ptr)
	db.ptr = nil
	if rc != statusOK {
		return errCloseFailed
	}
	return nil
}

// ---- Execute ----

// Exec executes a SQL statement without parameters.
func (db *DB) Exec(sql string) (int64, error) {
	if db.ptr == nil {
		return 0, errNilDB
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var affected C.int64_t
	rc := C.stoolap_exec(db.ptr, csql, &affected)
	if rc != statusOK {
		return 0, errFromDB(db.ptr)
	}
	return int64(affected), nil
}

// ExecParams executes a SQL statement with parameters.
func (db *DB) ExecParams(sql string, params []Value) (int64, error) {
	if db.ptr == nil {
		return 0, errNilDB
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var affected C.int64_t
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_exec_params(db.ptr, csql, cparams, paramsLen, &affected)
	if rc != statusOK {
		return 0, errFromDB(db.ptr)
	}
	return int64(affected), nil
}

// ---- Query ----

// Query executes a query without parameters.
func (db *DB) Query(sql string) (*Rows, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var ptr *C.StoolapRows
	rc := C.stoolap_query(db.ptr, csql, &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// QueryParams executes a query with parameters.
func (db *DB) QueryParams(sql string, params []Value) (*Rows, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var ptr *C.StoolapRows
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_query_params(db.ptr, csql, cparams, paramsLen, &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// ---- Prepared statements ----

// Prepare creates a prepared statement.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var ptr *C.StoolapStmt
	rc := C.stoolap_prepare(db.ptr, csql, &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &Stmt{ptr: ptr}, nil
}

// Exec executes the prepared statement with parameters.
func (s *Stmt) Exec(params []Value) (int64, error) {
	if s.ptr == nil {
		return 0, errNilStmt
	}

	var affected C.int64_t
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_stmt_exec(s.ptr, cparams, paramsLen, &affected)
	if rc != statusOK {
		return 0, errFromStmt(s.ptr)
	}
	return int64(affected), nil
}

// Query executes the prepared statement as a query with parameters.
func (s *Stmt) Query(params []Value) (*Rows, error) {
	if s.ptr == nil {
		return nil, errNilStmt
	}

	var ptr *C.StoolapRows
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_stmt_query(s.ptr, cparams, paramsLen, &ptr)
	if rc != statusOK {
		return nil, errFromStmt(s.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// SQL returns the SQL text of the prepared statement.
func (s *Stmt) SQL() string {
	if s.ptr == nil {
		return ""
	}
	return C.GoString(C.stoolap_stmt_sql(s.ptr))
}

// Finalize destroys the prepared statement and frees resources.
func (s *Stmt) Finalize() {
	if s.ptr == nil {
		return
	}
	C.stoolap_stmt_finalize(s.ptr)
	s.ptr = nil
}

// ---- Transactions ----

// Begin starts a transaction with default isolation level (READ COMMITTED).
func (db *DB) Begin() (*Tx, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	var ptr *C.StoolapTx
	rc := C.stoolap_begin(db.ptr, &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &Tx{ptr: ptr}, nil
}

// BeginWithIsolation starts a transaction with a specific isolation level.
func (db *DB) BeginWithIsolation(isolation int32) (*Tx, error) {
	if db.ptr == nil {
		return nil, errNilDB
	}
	var ptr *C.StoolapTx
	rc := C.stoolap_begin_with_isolation(db.ptr, C.int32_t(isolation), &ptr)
	if rc != statusOK {
		return nil, errFromDB(db.ptr)
	}
	return &Tx{ptr: ptr}, nil
}

// Exec executes a SQL statement within the transaction without parameters.
func (tx *Tx) Exec(sql string) (int64, error) {
	if tx.ptr == nil {
		return 0, errNilTx
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var affected C.int64_t
	rc := C.stoolap_tx_exec(tx.ptr, csql, &affected)
	if rc != statusOK {
		return 0, errFromTx(tx.ptr)
	}
	return int64(affected), nil
}

// ExecParams executes a SQL statement within the transaction with parameters.
func (tx *Tx) ExecParams(sql string, params []Value) (int64, error) {
	if tx.ptr == nil {
		return 0, errNilTx
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var affected C.int64_t
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_tx_exec_params(tx.ptr, csql, cparams, paramsLen, &affected)
	if rc != statusOK {
		return 0, errFromTx(tx.ptr)
	}
	return int64(affected), nil
}

// Query executes a query within the transaction without parameters.
func (tx *Tx) Query(sql string) (*Rows, error) {
	if tx.ptr == nil {
		return nil, errNilTx
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var ptr *C.StoolapRows
	rc := C.stoolap_tx_query(tx.ptr, csql, &ptr)
	if rc != statusOK {
		return nil, errFromTx(tx.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// QueryParams executes a query within the transaction with parameters.
func (tx *Tx) QueryParams(sql string, params []Value) (*Rows, error) {
	if tx.ptr == nil {
		return nil, errNilTx
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var ptr *C.StoolapRows
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_tx_query_params(tx.ptr, csql, cparams, paramsLen, &ptr)
	if rc != statusOK {
		return nil, errFromTx(tx.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// StmtExec executes a prepared statement within the transaction.
func (tx *Tx) StmtExec(stmt *Stmt, params []Value) (int64, error) {
	if tx.ptr == nil {
		return 0, errNilTx
	}
	if stmt.ptr == nil {
		return 0, errNilStmt
	}

	var affected C.int64_t
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_tx_stmt_exec(tx.ptr, stmt.ptr, cparams, paramsLen, &affected)
	if rc != statusOK {
		return 0, errFromTx(tx.ptr)
	}
	return int64(affected), nil
}

// StmtQuery executes a prepared statement query within the transaction.
func (tx *Tx) StmtQuery(stmt *Stmt, params []Value) (*Rows, error) {
	if tx.ptr == nil {
		return nil, errNilTx
	}
	if stmt.ptr == nil {
		return nil, errNilStmt
	}

	var ptr *C.StoolapRows
	var cparams *C.StoolapValue
	var paramsLen C.int32_t

	if len(params) > 0 {
		pb := acquireParamBuf(len(params))
		fillParamArray(pb.cvals, params)
		cparams = &pb.cvals[0]
		paramsLen = C.int32_t(len(params))
		defer pb.release()
	}

	rc := C.stoolap_tx_stmt_query(tx.ptr, stmt.ptr, cparams, paramsLen, &ptr)
	if rc != statusOK {
		return nil, errFromTx(tx.ptr)
	}
	return &Rows{ptr: ptr}, nil
}

// Commit commits the transaction. The handle is consumed.
func (tx *Tx) Commit() error {
	if tx.ptr == nil {
		return errNilTx
	}
	// Pin goroutine to OS thread: errFromGlobal reads thread-local error storage.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ptr := tx.ptr
	tx.ptr = nil // consumed regardless of outcome
	rc := C.stoolap_tx_commit(ptr)
	if rc != statusOK {
		return errFromGlobal()
	}
	return nil
}

// Rollback rolls back the transaction. The handle is consumed.
func (tx *Tx) Rollback() error {
	if tx.ptr == nil {
		return nil // safe to rollback nil (already consumed)
	}
	// Pin goroutine to OS thread: errFromGlobal reads thread-local error storage.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ptr := tx.ptr
	tx.ptr = nil // consumed regardless of outcome
	rc := C.stoolap_tx_rollback(ptr)
	if rc != statusOK {
		return errFromGlobal()
	}
	return nil
}

// ---- Result set iteration ----

// Next advances to the next row.
// Returns true if a row is available, false when done or on error.
func (r *Rows) Next() (bool, error) {
	if r.ptr == nil {
		return false, errNilRows
	}
	rc := C.stoolap_rows_next(r.ptr)
	switch int32(rc) {
	case statusRow:
		return true, nil
	case statusDone:
		return false, nil
	default:
		return false, errFromRows(r.ptr)
	}
}

// ColumnCount returns the number of columns in the result set.
func (r *Rows) ColumnCount() int {
	if r.ptr == nil {
		return 0
	}
	return int(C.stoolap_rows_column_count(r.ptr))
}

// ColumnName returns the name of a column by 0-based index.
func (r *Rows) ColumnName(index int) string {
	if r.ptr == nil {
		return ""
	}
	cname := C.stoolap_rows_column_name(r.ptr, C.int32_t(index))
	if cname == nil {
		return ""
	}
	return C.GoString(cname)
}

// ColumnType returns the type code of a column value in the current row.
func (r *Rows) ColumnType(index int) int32 {
	if r.ptr == nil {
		return TypeNull
	}
	return int32(C.stoolap_rows_column_type(r.ptr, C.int32_t(index)))
}

// ColumnIsNull returns true if the column value is NULL.
func (r *Rows) ColumnIsNull(index int) bool {
	if r.ptr == nil {
		return true
	}
	return C.stoolap_rows_column_is_null(r.ptr, C.int32_t(index)) != 0
}

// ColumnInt64 returns an integer column value.
func (r *Rows) ColumnInt64(index int) int64 {
	if r.ptr == nil {
		return 0
	}
	return int64(C.stoolap_rows_column_int64(r.ptr, C.int32_t(index)))
}

// ColumnDouble returns a float column value.
func (r *Rows) ColumnDouble(index int) float64 {
	if r.ptr == nil {
		return 0
	}
	return float64(C.stoolap_rows_column_double(r.ptr, C.int32_t(index)))
}

// ColumnText returns a text column value.
func (r *Rows) ColumnText(index int) string {
	if r.ptr == nil {
		return ""
	}
	var outLen C.int64_t
	ctext := C.stoolap_rows_column_text(r.ptr, C.int32_t(index), &outLen)
	if ctext == nil {
		return ""
	}
	n := int(outLen)
	if n == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(unsafe.Pointer(ctext)), n))
}

// ColumnBool returns a boolean column value.
func (r *Rows) ColumnBool(index int) bool {
	if r.ptr == nil {
		return false
	}
	return C.stoolap_rows_column_bool(r.ptr, C.int32_t(index)) != 0
}

// ColumnTimestamp returns a timestamp column value as time.Time.
// Callers must check ColumnType/ColumnIsNull for NULL before calling this.
func (r *Rows) ColumnTimestamp(index int) time.Time {
	if r.ptr == nil {
		return time.Time{}
	}
	nanos := int64(C.stoolap_rows_column_timestamp(r.ptr, C.int32_t(index)))
	sec := nanos / 1_000_000_000
	nsec := nanos % 1_000_000_000
	return time.Unix(sec, nsec).UTC()
}

// ColumnBlob returns a blob/vector column value.
func (r *Rows) ColumnBlob(index int) []byte {
	if r.ptr == nil {
		return nil
	}
	var outLen C.int64_t
	cblob := C.stoolap_rows_column_blob(r.ptr, C.int32_t(index), &outLen)
	if cblob == nil || outLen == 0 {
		return nil
	}
	n := int(outLen)
	out := make([]byte, n)
	copy(out, unsafe.Slice((*byte)(unsafe.Pointer(cblob)), n))
	return out
}

// Affected returns the number of rows affected (for DML results).
func (r *Rows) Affected() int64 {
	if r.ptr == nil {
		return 0
	}
	return int64(C.stoolap_rows_affected(r.ptr))
}

// Close closes the result set and frees resources.
func (r *Rows) Close() error {
	if r.ptr == nil {
		return nil
	}
	C.stoolap_rows_close(r.ptr)
	r.ptr = nil
	return nil
}

// ---- Bulk fetch ----

// FetchAll fetches all remaining rows into a packed binary buffer.
func (r *Rows) FetchAll() ([]byte, error) {
	if r.ptr == nil {
		return nil, errNilRows
	}
	var buf *C.uint8_t
	var bufLen C.int64_t
	rc := C.stoolap_rows_fetch_all(r.ptr, &buf, &bufLen)
	if rc != statusOK {
		return nil, errFromRows(r.ptr)
	}
	if buf == nil || bufLen == 0 {
		return nil, nil
	}
	n := int(bufLen)
	goBytes := make([]byte, n)
	copy(goBytes, unsafe.Slice((*byte)(unsafe.Pointer(buf)), n))
	C.stoolap_buffer_free(buf, bufLen)
	return goBytes, nil
}

// ---- Pools for reducing allocations ----

// ValueBuf is a pooled buffer for Value slices. Acquire with AcquireValueBuf, return with Release.
type ValueBuf struct {
	Values []Value
}

var valueBufPool = sync.Pool{
	New: func() any {
		return &ValueBuf{Values: make([]Value, 0, 8)}
	},
}

// AcquireValueBuf returns a pooled Value slice of length n.
func AcquireValueBuf(n int) *ValueBuf {
	vb := valueBufPool.Get().(*ValueBuf)
	if cap(vb.Values) >= n {
		vb.Values = vb.Values[:n]
	} else {
		vb.Values = make([]Value, n)
	}
	return vb
}

// Release returns the buffer to the pool.
// Only clears pointer-containing fields (Text, Blob, Timestamp) to allow GC
// while avoiding zeroing scalar fields (Type, Integer, Float, Bool).
func (vb *ValueBuf) Release() {
	for i := range vb.Values {
		vb.Values[i].Text = ""
		vb.Values[i].Blob = nil
		vb.Values[i].Timestamp = time.Time{}
	}
	valueBufPool.Put(vb)
}

// paramBuf pools C.StoolapValue arrays to avoid per-query allocation.
type paramBuf struct {
	cvals []C.StoolapValue
}

var paramBufPool = sync.Pool{
	New: func() any {
		return &paramBuf{cvals: make([]C.StoolapValue, 0, 8)}
	},
}

func acquireParamBuf(n int) *paramBuf {
	pb := paramBufPool.Get().(*paramBuf)
	if cap(pb.cvals) >= n {
		pb.cvals = pb.cvals[:n]
	} else {
		pb.cvals = make([]C.StoolapValue, n)
	}
	return pb
}

func (pb *paramBuf) release() {
	freeParamArray(pb.cvals)
	for i := range pb.cvals {
		pb.cvals[i] = C.StoolapValue{}
	}
	paramBufPool.Put(pb)
}

// ---- Parameter value types ----

// Value represents a parameter value for SQL statements.
type Value struct {
	Type      int32
	Integer   int64
	Float     float64
	Bool      bool
	Text      string
	Blob      []byte
	Timestamp time.Time
}

// NullValue creates a NULL parameter value.
func NullValue() Value {
	return Value{Type: TypeNull}
}

// IntegerValue creates an INTEGER parameter value.
func IntegerValue(v int64) Value {
	return Value{Type: TypeInteger, Integer: v}
}

// FloatValue creates a FLOAT parameter value.
func FloatValue(v float64) Value {
	return Value{Type: TypeFloat, Float: v}
}

// TextValue creates a TEXT parameter value.
func TextValue(v string) Value {
	return Value{Type: TypeText, Text: v}
}

// BoolValue creates a BOOLEAN parameter value.
func BoolValue(v bool) Value {
	return Value{Type: TypeBoolean, Bool: v}
}

// TimestampValue creates a TIMESTAMP parameter value.
func TimestampValue(v time.Time) Value {
	return Value{Type: TypeTimestamp, Timestamp: v}
}

// JSONValue creates a JSON parameter value.
func JSONValue(v string) Value {
	return Value{Type: TypeJSON, Text: v}
}

// BlobValue creates a BLOB parameter value.
func BlobValue(v []byte) Value {
	return Value{Type: TypeBlob, Blob: v}
}

// GoValueToFFI converts a Go value (any) to a cs.Value.
// Supports primitive types, sql.Null* wrappers, and driver.Valuer.
// Returns an error for unsupported types.
func GoValueToFFI(v any) (Value, error) {
	if v == nil {
		return NullValue(), nil
	}
	switch val := v.(type) {
	case int64:
		return IntegerValue(val), nil
	case float64:
		return FloatValue(val), nil
	case bool:
		return BoolValue(val), nil
	case string:
		return TextValue(val), nil
	case []byte:
		return BlobValue(val), nil
	case time.Time:
		return TimestampValue(val), nil
	case int:
		return IntegerValue(int64(val)), nil
	case int32:
		return IntegerValue(int64(val)), nil
	case float32:
		return FloatValue(float64(val)), nil
	case driver.Valuer:
		inner, err := val.Value()
		if err != nil {
			return NullValue(), err
		}
		if inner == nil {
			return NullValue(), nil
		}
		return GoValueToFFI(inner)
	default:
		return NullValue(), errUnsupportedParam
	}
}

// fillParamArray fills a pre-allocated C StoolapValue array from Go Value slice.
// Uses index-based access to avoid copying the ~96-byte Value struct per element.
func fillParamArray(cvals []C.StoolapValue, params []Value) {
	for i := range params {
		cvals[i].value_type = C.int32_t(params[i].Type)
		switch params[i].Type {
		case TypeInteger:
			*(*C.int64_t)(unsafe.Pointer(&cvals[i].v)) = C.int64_t(params[i].Integer)
		case TypeFloat:
			*(*C.double)(unsafe.Pointer(&cvals[i].v)) = C.double(params[i].Float)
		case TypeBoolean:
			if params[i].Bool {
				*(*C.int32_t)(unsafe.Pointer(&cvals[i].v)) = 1
			} else {
				*(*C.int32_t)(unsafe.Pointer(&cvals[i].v)) = 0
			}
		case TypeText, TypeJSON:
			cs := C.CString(params[i].Text)
			*(*uintptr)(unsafe.Pointer(&cvals[i].v)) = uintptr(unsafe.Pointer(cs))
			*(*C.int64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(&cvals[i].v)) + unsafe.Sizeof(uintptr(0)))) = C.int64_t(len(params[i].Text))
		case TypeBlob:
			if len(params[i].Blob) > 0 {
				blobCopy := C.CBytes(params[i].Blob)
				*(*uintptr)(unsafe.Pointer(&cvals[i].v)) = uintptr(blobCopy)
				*(*C.int64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(&cvals[i].v)) + unsafe.Sizeof(uintptr(0)))) = C.int64_t(len(params[i].Blob))
			}
		case TypeTimestamp:
			nanos := params[i].Timestamp.UnixNano()
			*(*C.int64_t)(unsafe.Pointer(&cvals[i].v)) = C.int64_t(nanos)
		}
	}
}

// freeParamArray frees C strings allocated in makeParamArray.
func freeParamArray(cvals []C.StoolapValue) {
	for i := range cvals {
		vt := int32(cvals[i].value_type)
		if vt == TypeText || vt == TypeJSON || vt == TypeBlob {
			ptr := *(*unsafe.Pointer)(unsafe.Pointer(&cvals[i].v))
			if ptr != nil {
				C.free(ptr)
			}
		}
	}
}
