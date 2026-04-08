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
	"sync"
	"time"
	"unsafe"
)

var errRowsClosed = errors.New("stoolap: rows are closed")
var errStmtClosed = errors.New("stoolap: statement is closed")
var errColumnCount = errors.New("stoolap: scan destination count does not match column count")
var errUnsupportedDest = errors.New("stoolap: unsupported scan destination type")

const inlineColTypesCap = 8

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
		return nil, errors.New(errStr(sym.errmsg, 0))
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
		return nil, errors.New(errStr(sym.errmsg, 0))
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
	var clonePtr uintptr
	rc := abiCall2(sym.clone, db.ptr, uintptr(unsafe.Pointer(&clonePtr)))
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return &DB{ptr: clonePtr}, nil
}

// Exec executes a SQL statement.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var affected int64
	rc := abiCall3(sym.exec, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return execResult(affected), nil
}

// ExecParams executes a SQL statement with positional parameters.
func (db *DB) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
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
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return execResult(affected), nil
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (*Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var rowsPtr uintptr
	rc := abiCall3(sym.query, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryParams executes a query with positional parameters.
func (db *DB) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
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
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return newRows(rowsPtr), nil
}

// Prepare creates a prepared statement.
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var stmtPtr uintptr
	rc := abiCall3(sym.prepare, db.ptr, cs.ptr, uintptr(unsafe.Pointer(&stmtPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return &Stmt{ptr: stmtPtr}, nil
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var txPtr uintptr
	rc := abiCall2(sym.begin, db.ptr, uintptr(unsafe.Pointer(&txPtr)))
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return &Tx{ptr: txPtr}, nil
}

// BeginTx starts a transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
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
		return nil, errors.New(errStr(sym.errmsg, db.ptr))
	}
	return &Tx{ptr: txPtr}, nil
}

// ─── Rows ───────────────────────────────────────────────────────────────────

// Rows is an iterator over query results.
type Rows struct {
	ptr            uintptr
	cols           []string
	colTypes       []int32
	colCount       int
	closed         bool
	colTypesLoaded bool
	colTypesInline [inlineColTypesCap]int32
	textBuf        []byte // reusable buffer for batching text copies in Scan
	colNameBuf     []byte // reusable scratch for gathering column names in Columns
}

var rowsPool = sync.Pool{New: func() any { return &Rows{} }}

func newRows(ptr uintptr) *Rows {
	r := rowsPool.Get().(*Rows)
	r.reset(ptr)
	return r
}

// Columns returns the column names as a Go-owned slice, safe to hold after Close().
// First call per query: 2 allocations (one string for combined name data, one []string).
// Subsequent calls on the same Rows return the cached slice with no allocation.
func (r *Rows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}

	// Inline scratch for per-column end offsets; fits the vast majority of tables.
	var inlineEnds [32]int
	var ends []int
	if r.colCount <= len(inlineEnds) {
		ends = inlineEnds[:r.colCount]
	} else {
		ends = make([]int, r.colCount)
	}

	// Pass 1: gather all column-name bytes into a reusable scratch buffer,
	// recording where each name ends.
	buf := r.colNameBuf[:0]
	for i := 0; i < r.colCount; i++ {
		p := unsafe.Pointer(abiCall2(sym.rowsColName, r.ptr, uintptr(i)))
		if p != nil {
			n := 0
			for *(*byte)(unsafe.Add(p, n)) != 0 {
				n++
			}
			if n > 0 {
				buf = append(buf, unsafe.Slice((*byte)(p), n)...)
			}
		}
		ends[i] = len(buf)
	}
	r.colNameBuf = buf

	// One Go-owned allocation holding all the bytes.
	all := string(buf)
	cols := make([]string, r.colCount)
	prev := 0
	for i := 0; i < r.colCount; i++ {
		cols[i] = all[prev:ends[i]]
		prev = ends[i]
	}
	r.cols = cols
	return cols
}

// IsNull reports whether the current row has a NULL value at column i.
func (r *Rows) IsNull(i int) bool {
	return int32(abiCall2(sym.rowsColIsNull, r.ptr, uintptr(i))) == 1
}

// Int64 returns the current row's INTEGER value at column i.
func (r *Rows) Int64(i int) int64 {
	return int64(abiCall2(sym.rowsColInt64, r.ptr, uintptr(i)))
}

// Float64 returns the current row's FLOAT value at column i.
func (r *Rows) Float64(i int) float64 {
	return abiCallFloat2(sym.rowsColDouble, r.ptr, uintptr(i))
}

// Bool returns the current row's BOOLEAN value at column i.
func (r *Rows) Bool(i int) bool {
	return int32(abiCall2(sym.rowsColBool, r.ptr, uintptr(i))) != 0
}

// Timestamp returns the current row's TIMESTAMP value at column i.
func (r *Rows) Timestamp(i int) time.Time {
	nanos := int64(abiCall2(sym.rowsColTimestamp, r.ptr, uintptr(i)))
	return time.Unix(nanos/1e9, nanos%1e9).UTC()
}

// Next advances to the next row.
func (r *Rows) Next() bool {
	if r.closed {
		return false
	}
	if int32(abiCall1(sym.rowsNext, r.ptr)) != stoolapRow {
		return false
	}
	return true
}

// Scan reads the current row into dest.
// Text columns are batched into a single string allocation per row.
// The textBuf is reused across rows via Rows pooling.
func (r *Rows) Scan(dest ...any) error {
	if r.closed {
		return errRowsClosed
	}
	if len(dest) != r.colCount {
		return errColumnCount
	}
	rp := r.ptr
	r.textBuf = r.textBuf[:0]

	const maxTextRefs = 16
	type tref struct {
		destIdx int
		start   int
		len     int
	}
	var refs [maxTextRefs]tref
	nRefs := 0

	appendText := func(v uintptr, n int64, destIdx int) bool {
		if nRefs >= maxTextRefs {
			return false
		}
		start := len(r.textBuf)
		size := int(n)
		r.textBuf = append(r.textBuf, unsafe.Slice((*byte)(unsafe.Pointer(v)), size)...)
		refs[nRefs] = tref{destIdx: destIdx, start: start, len: size}
		nRefs++
		return true
	}

	for i, d := range dest {
		idx := uintptr(i)
		isNull := int32(abiCall2(sym.rowsColIsNull, rp, idx)) == 1

		switch p := d.(type) {
		case *int64:
			if isNull {
				*p = 0
			} else {
				*p = r.Int64(i)
			}
		case *float64:
			if isNull {
				*p = 0
			} else {
				*p = r.Float64(i)
			}
		case *string:
			if isNull {
				*p = ""
			} else {
				v, n := abiCallPtrLen(sym.rowsColText, rp, idx)
				if v == 0 || n <= 0 {
					*p = ""
				} else if !appendText(v, n, i) {
					*p = goStringN(unsafe.Pointer(v), int(n))
				}
			}
		case *bool:
			if isNull {
				*p = false
			} else {
				*p = r.Bool(i)
			}
		case *time.Time:
			if isNull {
				*p = time.Time{}
			} else {
				*p = r.Timestamp(i)
			}
		case *[]byte:
			if isNull {
				*p = nil
			} else {
				*p = r.readColBlob(i)
			}
		case *sql.NullString:
			if isNull {
				p.String = ""
				p.Valid = false
			} else {
				v, n := abiCallPtrLen(sym.rowsColText, rp, idx)
				if v == 0 {
					p.String = ""
					p.Valid = false
				} else if !appendText(v, n, i) {
					p.String = goStringN(unsafe.Pointer(v), int(n))
					p.Valid = true
				}
			}
		case *sql.NullInt64:
			if isNull {
				p.Int64 = 0
				p.Valid = false
			} else {
				p.Int64 = int64(abiCall2(sym.rowsColInt64, rp, idx))
				p.Valid = true
			}
		case *sql.NullFloat64:
			if isNull {
				p.Float64 = 0
				p.Valid = false
			} else {
				p.Float64 = r.Float64(i)
				p.Valid = true
			}
		case *sql.NullBool:
			if isNull {
				p.Bool = false
				p.Valid = false
			} else {
				p.Bool = r.Bool(i)
				p.Valid = true
			}
		case *sql.NullTime:
			if isNull {
				p.Time = time.Time{}
				p.Valid = false
			} else {
				p.Time = r.Timestamp(i)
				p.Valid = true
			}
		case *any:
			if isNull {
				*p = nil
			} else {
				r.ensureColTypes()
				switch r.colTypes[i] {
				case typeInteger:
					*p = r.Int64(i)
				case typeFloat:
					*p = r.Float64(i)
				case typeText, typeJSON:
					v, n := abiCallPtrLen(sym.rowsColText, rp, idx)
					if !appendText(v, n, i) {
						*p = goStringN(unsafe.Pointer(v), int(n))
					}
				case typeBoolean:
					*p = r.Bool(i)
				case typeTimestamp:
					*p = r.Timestamp(i)
				case typeBlob:
					*p = r.readColBlob(i)
				default:
					*p = nil
				}
			}
		default:
			return errUnsupportedDest
		}
	}

	// Batch-create all text strings from one allocation.
	// string(r.textBuf) copies once; substrings share the backing array.
	if nRefs > 0 {
		all := string(r.textBuf)
		for j := range nRefs {
			ref := refs[j]
			s := all[ref.start : ref.start+ref.len]
			switch p := dest[ref.destIdx].(type) {
			case *string:
				*p = s
			case *sql.NullString:
				p.String = s
				p.Valid = true
			case *any:
				*p = s
			}
		}
	}

	return nil
}

// FetchAll fetches all remaining rows in a single native call and returns them
// as a slice of []any rows. Dramatically faster for large result sets.
// After calling FetchAll, the Rows handle is consumed; call Close() afterward.
func (r *Rows) FetchAll() ([][]any, error) {
	if r.closed {
		return nil, errRowsClosed
	}
	var bufPtr uintptr
	var bufLen int64
	rc := abiCall3(sym.rowsFetchAll, r.ptr, uintptr(unsafe.Pointer(&bufPtr)), uintptr(unsafe.Pointer(&bufLen)))
	if int32(rc) != stoolapOK {
		return nil, errors.New("stoolap: fetch_all failed")
	}
	if bufPtr == 0 || bufLen == 0 {
		return nil, nil
	}

	// Copy the buffer from C into Go, then free the C buffer
	buf := copyBlob(unsafe.Pointer(bufPtr), bufLen)
	abiCall2(sym.bufferFree, bufPtr, uintptr(bufLen))

	return parseFetchAllBuffer(buf)
}

// Close closes the result set.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	abiCallVoid1(sym.rowsClose, r.ptr)
	r.ptr = 0
	// Release the columns slice so the next pooled use cannot share its
	// backing array with a caller that is still holding a previous result.
	// textBuf / colNameBuf stay — they hold no externally-visible data.
	r.cols = nil
	rowsPool.Put(r)
	return nil
}

// ─── Stmt ───────────────────────────────────────────────────────────────────

// Stmt is a prepared statement.
type Stmt struct {
	mu      sync.Mutex
	ptr     uintptr
	scratch stmtParamScratch
}

// ExecContext executes a prepared statement with parameters.
func (s *Stmt) ExecContext(ctx context.Context, args []any) (sql.Result, error) {
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
	rc := abiCall4(sym.stmtExec, s.ptr, ep.ptr, uintptr(int32(len(args))), uintptr(unsafe.Pointer(&affected)))
	ep.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.stmtErrmsg, s.ptr))
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
		return nil, errors.New(errStr(sym.stmtErrmsg, s.ptr))
	}
	return newRows(rowsPtr), nil
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
}

// Exec executes within the transaction.
func (tx *Tx) Exec(ctx context.Context, query string) (sql.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var affected int64
	rc := abiCall3(sym.txExec, tx.ptr, cs.ptr, uintptr(unsafe.Pointer(&affected)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.txErrmsg, tx.ptr))
	}
	return execResult(affected), nil
}

// ExecParams executes with parameters within the transaction.
func (tx *Tx) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
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
		return nil, errors.New(errStr(sym.txErrmsg, tx.ptr))
	}
	return execResult(affected), nil
}

// Query within the transaction.
func (tx *Tx) Query(ctx context.Context, query string) (*Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cs := newCStr(query)
	var rowsPtr uintptr
	rc := abiCall3(sym.txQuery, tx.ptr, cs.ptr, uintptr(unsafe.Pointer(&rowsPtr)))
	cs.keepAlive()
	if int32(rc) != stoolapOK {
		return nil, errors.New(errStr(sym.txErrmsg, tx.ptr))
	}
	return newRows(rowsPtr), nil
}

// QueryParams executes a query with parameters within the transaction.
func (tx *Tx) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
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
		return nil, errors.New(errStr(sym.txErrmsg, tx.ptr))
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
		return errors.New(errStr(sym.errmsg, 0))
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

// ─── FetchAll parser ────────────────────────────────────────────────────────

// parseFetchAllBuffer parses the packed binary format from stoolap_rows_fetch_all.
func parseFetchAllBuffer(buf []byte) ([][]any, error) {
	if len(buf) < 4 {
		return nil, nil
	}
	off := 0

	colCount := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4

	// Skip column names
	for range colCount {
		if off+2 > len(buf) {
			return nil, errors.New("stoolap: truncated column name")
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2 + nameLen
	}

	if off+4 > len(buf) {
		return nil, errors.New("stoolap: truncated row count")
	}
	rowCount := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4

	flat := make([]any, rowCount*colCount)
	rows := make([][]any, rowCount)
	for ri := range rowCount {
		row := flat[ri*colCount : (ri+1)*colCount : (ri+1)*colCount]
		rows[ri] = row
		for c := range colCount {
			if off >= len(buf) {
				return nil, errors.New("stoolap: truncated row data")
			}
			typeTag := buf[off]
			off++

			switch typeTag {
			case typeNull:
				row[c] = nil
			case typeInteger:
				row[c] = int64(binary.LittleEndian.Uint64(buf[off:]))
				off += 8
			case typeFloat:
				row[c] = math.Float64frombits(binary.LittleEndian.Uint64(buf[off:]))
				off += 8
			case typeText, typeJSON:
				sLen := int(binary.LittleEndian.Uint32(buf[off:]))
				off += 4
				if sLen == 0 {
					row[c] = ""
				} else {
					row[c] = unsafe.String(&buf[off], sLen)
				}
				off += sLen
			case typeBoolean:
				row[c] = buf[off] != 0
				off++
			case typeTimestamp:
				nanos := int64(binary.LittleEndian.Uint64(buf[off:]))
				off += 8
				row[c] = time.Unix(nanos/1e9, nanos%1e9).UTC()
			case typeBlob:
				bLen := int(binary.LittleEndian.Uint32(buf[off:]))
				off += 4
				blob := make([]byte, bLen)
				copy(blob, buf[off:off+bLen])
				off += bLen
				row[c] = blob
			default:
				row[c] = nil
			}
		}
	}
	return rows, nil
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

func (r *Rows) reset(ptr uintptr) {
	cc := int(int32(abiCall1(sym.rowsColCount, ptr)))
	r.ptr = ptr
	r.colCount = cc
	r.closed = false
	r.colTypesLoaded = false
	// Keep cols backing array but mark as stale (length 0).
	r.cols = r.cols[:0]
	if cc <= len(r.colTypesInline) {
		r.colTypes = r.colTypesInline[:cc]
	} else {
		if cap(r.colTypes) < cc {
			r.colTypes = make([]int32, cc)
		} else {
			r.colTypes = r.colTypes[:cc]
		}
	}
	// textBuf survives across pool reuse — pre-size once.
	if r.textBuf == nil {
		r.textBuf = make([]byte, 0, 256)
	}
}

func (r *Rows) ensureColTypes() {
	if r.colTypesLoaded {
		return
	}
	for i := range r.colCount {
		r.colTypes[i] = int32(abiCall2(sym.rowsColType, r.ptr, uintptr(i)))
	}
	r.colTypesLoaded = true
}

func (r *Rows) readColBlob(i int) []byte {
	v, n := abiCallPtrLen(sym.rowsColBlob, r.ptr, uintptr(i))
	if v == 0 {
		return nil
	}
	return copyBlob(unsafe.Pointer(v), n)
}
