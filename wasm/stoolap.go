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
	"encoding/binary"
	"errors"
	"math"
	"time"
)

var errRowsClosed = errors.New("stoolap-wasm: rows are closed")

// DB represents a stoolap database connection over WASM.
type DB struct {
	engine *Engine
	ptr    uint32
}

// Open opens a database with the given DSN on this engine.
func (e *Engine) Open(ctx context.Context, dsn string) (*DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, dsn)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write DSN")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(dsn)+1), 1)
		}
	}()

	rc := e.call2(ctx, e.fnOpen, uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, 0)
	}
	return &DB{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// OpenMemory opens a new in-memory database.
func (e *Engine) OpenMemory(ctx context.Context) (*DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call1(ctx, e.fnOpenInMemory, uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, 0)
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
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return &DB{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// Exec executes a SQL statement without parameters.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	rc := e.call3(ctx, e.fnExec, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

// ExecParams executes a SQL statement with parameters.
func (db *DB) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	if len(args) == 0 {
		rc := e.call3(ctx, e.fnExec, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotB))
		if int32(rc) != stoolapOK {
			return nil, e.errFromDB(ctx, db.ptr)
		}
		return execResult(e.readI64(e.outSlotB)), nil
	}

	paramsPtr, err := e.writeParams(ctx, args)
	if err != nil {
		return nil, err
	}

	rc := e.call5(ctx, e.fnExecParams, uint64(db.ptr), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (*Rows, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	rc := e.call3(ctx, e.fnQuery, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	rowsPtr := e.readU32(e.outSlotA)
	n := int32(e.call1(ctx, e.fnRowsColCount, uint64(rowsPtr)))
	return &Rows{engine: e, ptr: rowsPtr, ctx: ctx, colCount: int(n), colTypes: make([]int32, n)}, nil
}

// QueryParams executes a query with parameters.
func (db *DB) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	var rc uint64
	if len(args) == 0 {
		rc = e.call3(ctx, e.fnQuery, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotA))
	} else {
		paramsPtr, err := e.writeParams(ctx, args)
		if err != nil {
			return nil, err
		}
		rc = e.call5(ctx, e.fnQueryParams, uint64(db.ptr), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotA))
	}
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	rowsPtr := e.readU32(e.outSlotA)
	n := int32(e.call1(ctx, e.fnRowsColCount, uint64(rowsPtr)))
	return &Rows{engine: e, ptr: rowsPtr, ctx: ctx, colCount: int(n), colTypes: make([]int32, n)}, nil
}

// Prepare creates a prepared statement.
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	rc := e.call3(ctx, e.fnPrepare, uint64(db.ptr), uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return &Stmt{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// Begin starts a transaction.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	e := db.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call2(ctx, e.fnBegin, uint64(db.ptr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return &Tx{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// BeginTx starts a transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
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
		return nil, e.errFromDB(ctx, db.ptr)
	}
	return &Tx{engine: e, ptr: e.readU32(e.outSlotA)}, nil
}

// ─── Rows ───────────────────────────────────────────────────────────────────

type Rows struct {
	engine   *Engine
	ptr      uint32
	ctx      context.Context
	cols     []string
	colTypes []int32
	colCount int
	closed   bool
	firstRow bool
}

func (r *Rows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}
	e := r.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	r.cols = make([]string, r.colCount)
	for i := range r.cols {
		ptr := e.call2(r.ctx, e.fnRowsColName, uint64(r.ptr), uint64(i))
		r.cols[i] = e.readCString(uint32(ptr))
	}
	return r.cols
}

func (r *Rows) Next() bool {
	if r.closed {
		return false
	}
	e := r.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	rc := e.call1(r.ctx, e.fnRowsNext, uint64(r.ptr))
	if int32(rc) != stoolapRow {
		return false
	}
	if !r.firstRow {
		r.firstRow = true
		for i := range r.colCount {
			r.colTypes[i] = int32(e.call2(r.ctx, e.fnRowsColType, uint64(r.ptr), uint64(i)))
		}
	}
	return true
}

func (r *Rows) Scan(dest ...any) error {
	if r.closed {
		return errRowsClosed
	}
	e := r.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	ctx := r.ctx
	for i, d := range dest {
		isNull := int32(e.call2(ctx, e.fnRowsColIsNull, uint64(r.ptr), uint64(i))) == 1

		switch p := d.(type) {
		case *int64:
			if isNull {
				*p = 0
			} else {
				*p = int64(e.call2(ctx, e.fnRowsColInt64, uint64(r.ptr), uint64(i)))
			}
		case *float64:
			if isNull {
				*p = 0
			} else {
				*p = math.Float64frombits(e.call2(ctx, e.fnRowsColDouble, uint64(r.ptr), uint64(i)))
			}
		case *string:
			if isNull {
				*p = ""
			} else {
				ptr := e.call3(ctx, e.fnRowsColText, uint64(r.ptr), uint64(i), 0)
				*p = e.readCString(uint32(ptr))
			}
		case *bool:
			if isNull {
				*p = false
			} else {
				*p = int32(e.call2(ctx, e.fnRowsColBool, uint64(r.ptr), uint64(i))) != 0
			}
		case *time.Time:
			if isNull {
				*p = time.Time{}
			} else {
				nanos := int64(e.call2(ctx, e.fnRowsColTimestamp, uint64(r.ptr), uint64(i)))
				*p = time.Unix(nanos/1e9, nanos%1e9).UTC()
			}
		case *[]byte:
			if isNull {
				*p = nil
			} else {
				blobPtr := uint32(e.call3(ctx, e.fnRowsColBlob, uint64(r.ptr), uint64(i), uint64(e.outSlotB)))
				if blobPtr == 0 {
					*p = nil
				} else {
					blobLen := e.readI64(e.outSlotB)
					data, _ := e.mem.Read(blobPtr, uint32(blobLen))
					cp := make([]byte, len(data))
					copy(cp, data)
					*p = cp
				}
			}
		case *sql.NullString:
			if isNull {
				p.Valid = false
			} else {
				ptr := e.call3(ctx, e.fnRowsColText, uint64(r.ptr), uint64(i), 0)
				p.String = e.readCString(uint32(ptr))
				p.Valid = true
			}
		case *sql.NullInt64:
			if isNull {
				p.Valid = false
			} else {
				p.Int64 = int64(e.call2(ctx, e.fnRowsColInt64, uint64(r.ptr), uint64(i)))
				p.Valid = true
			}
		case *sql.NullFloat64:
			if isNull {
				p.Valid = false
			} else {
				p.Float64 = math.Float64frombits(e.call2(ctx, e.fnRowsColDouble, uint64(r.ptr), uint64(i)))
				p.Valid = true
			}
		case *sql.NullBool:
			if isNull {
				p.Valid = false
			} else {
				p.Bool = int32(e.call2(ctx, e.fnRowsColBool, uint64(r.ptr), uint64(i))) != 0
				p.Valid = true
			}
		case *sql.NullTime:
			if isNull {
				p.Valid = false
			} else {
				nanos := int64(e.call2(ctx, e.fnRowsColTimestamp, uint64(r.ptr), uint64(i)))
				p.Time = time.Unix(nanos/1e9, nanos%1e9).UTC()
				p.Valid = true
			}
		case *any:
			if isNull {
				*p = nil
			} else {
				*p = r.readAny(e, ctx, i)
			}
		}
	}
	return nil
}

func (r *Rows) readAny(e *Engine, ctx context.Context, i int) any {
	switch r.colTypes[i] {
	case typeInteger:
		return int64(e.call2(ctx, e.fnRowsColInt64, uint64(r.ptr), uint64(i)))
	case typeFloat:
		return math.Float64frombits(e.call2(ctx, e.fnRowsColDouble, uint64(r.ptr), uint64(i)))
	case typeText, typeJSON:
		ptr := e.call3(ctx, e.fnRowsColText, uint64(r.ptr), uint64(i), 0)
		return e.readCString(uint32(ptr))
	case typeBoolean:
		return int32(e.call2(ctx, e.fnRowsColBool, uint64(r.ptr), uint64(i))) != 0
	case typeTimestamp:
		nanos := int64(e.call2(ctx, e.fnRowsColTimestamp, uint64(r.ptr), uint64(i)))
		return time.Unix(nanos/1e9, nanos%1e9).UTC()
	case typeBlob:
		blobPtr := uint32(e.call3(ctx, e.fnRowsColBlob, uint64(r.ptr), uint64(i), uint64(e.outSlotB)))
		if blobPtr == 0 {
			return nil
		}
		blobLen := e.readI64(e.outSlotB)
		data, _ := e.mem.Read(blobPtr, uint32(blobLen))
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp
	default:
		return nil
	}
}

// FetchAll fetches all remaining rows in a single WASM call and returns them
// as a slice of []driver.Value rows. This is dramatically faster for large
// result sets because it reduces WASM boundary crossings from O(rows*cols) to O(1).
// After calling FetchAll, the Rows handle is consumed; call Close() afterward.
func (r *Rows) FetchAll() ([][]any, error) {
	if r.closed {
		return nil, errRowsClosed
	}
	e := r.engine
	e.mu.Lock()
	defer e.mu.Unlock()

	// stoolap_rows_fetch_all(rows, &out_buf, &out_len) -> rc
	// We need two output slots: out_buf (i32 ptr) and out_len (i64)
	rc := e.call3(r.ctx, e.fnRowsFetchAll, uint64(r.ptr), uint64(e.outSlotA), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, errors.New("stoolap-wasm: fetch_all failed")
	}

	bufPtr := e.readU32(e.outSlotA)
	bufLen := e.readI64(e.outSlotB)
	if bufPtr == 0 || bufLen == 0 {
		return nil, nil
	}

	// Read the entire buffer from WASM memory into Go (one copy)
	data, ok := e.mem.Read(bufPtr, uint32(bufLen))
	if !ok {
		return nil, errors.New("stoolap-wasm: failed to read fetch_all buffer")
	}
	// Copy before freeing WASM buffer
	buf := make([]byte, len(data))
	copy(buf, data)

	// Free the WASM-side buffer
	e.call2(r.ctx, e.fnBufferFree, uint64(bufPtr), uint64(bufLen))

	// Parse the packed binary format in pure Go (zero WASM calls)
	return parseFetchAllBuffer(buf)
}

// parseFetchAllBuffer parses the packed binary format from stoolap_rows_fetch_all.
func parseFetchAllBuffer(buf []byte) ([][]any, error) {
	if len(buf) < 4 {
		return nil, nil
	}
	off := 0

	// Column count
	colCount := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4

	// Skip column names
	for range colCount {
		if off+2 > len(buf) {
			return nil, errors.New("stoolap-wasm: truncated column name")
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2 + nameLen
	}

	// Row count
	if off+4 > len(buf) {
		return nil, errors.New("stoolap-wasm: truncated row count")
	}
	rowCount := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4

	rows := make([][]any, 0, rowCount)
	for range rowCount {
		row := make([]any, colCount)
		for c := range colCount {
			if off >= len(buf) {
				return nil, errors.New("stoolap-wasm: truncated row data")
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
				row[c] = string(buf[off : off+sLen])
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
		rows = append(rows, row)
	}
	return rows, nil
}

func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	e := r.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callVoid1(r.ctx, e.fnRowsClose, uint64(r.ptr))
	return nil
}

// ─── Stmt ───────────────────────────────────────────────────────────────────

type Stmt struct {
	engine *Engine
	ptr    uint32
}

func (s *Stmt) ExecContext(ctx context.Context, args []any) (sql.Result, error) {
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	var paramsPtr uint32
	if len(args) > 0 {
		var err error
		paramsPtr, err = e.writeParams(ctx, args)
		if err != nil {
			return nil, err
		}
	}

	rc := e.call4(ctx, e.fnStmtExec, uint64(s.ptr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFromStmt(ctx, s.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

func (s *Stmt) QueryContext(ctx context.Context, args []any) (*Rows, error) {
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	var paramsPtr uint32
	if len(args) > 0 {
		var err error
		paramsPtr, err = e.writeParams(ctx, args)
		if err != nil {
			return nil, err
		}
	}

	rc := e.call4(ctx, e.fnStmtQuery, uint64(s.ptr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromStmt(ctx, s.ptr)
	}
	rowsPtr := e.readU32(e.outSlotA)
	n := int32(e.call1(ctx, e.fnRowsColCount, uint64(rowsPtr)))
	return &Rows{engine: e, ptr: rowsPtr, ctx: ctx, colCount: int(n), colTypes: make([]int32, n)}, nil
}

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

type Tx struct {
	engine *Engine
	ptr    uint32
}

func (tx *Tx) Exec(ctx context.Context, query string) (sql.Result, error) {
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	rc := e.call3(ctx, e.fnTxExec, uint64(tx.ptr), uint64(sqlPtr), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFromTx(ctx, tx.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

func (tx *Tx) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error) {
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	if len(args) == 0 {
		rc := e.call3(ctx, e.fnTxExec, uint64(tx.ptr), uint64(sqlPtr), uint64(e.outSlotB))
		if int32(rc) != stoolapOK {
			return nil, e.errFromTx(ctx, tx.ptr)
		}
		return execResult(e.readI64(e.outSlotB)), nil
	}

	paramsPtr, err := e.writeParams(ctx, args)
	if err != nil {
		return nil, err
	}

	rc := e.call5(ctx, e.fnTxExecParams, uint64(tx.ptr), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		return nil, e.errFromTx(ctx, tx.ptr)
	}
	return execResult(e.readI64(e.outSlotB)), nil
}

func (tx *Tx) Query(ctx context.Context, query string) (*Rows, error) {
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	rc := e.call3(ctx, e.fnTxQuery, uint64(tx.ptr), uint64(sqlPtr), uint64(e.outSlotA))
	if int32(rc) != stoolapOK {
		return nil, e.errFromTx(ctx, tx.ptr)
	}
	rowsPtr := e.readU32(e.outSlotA)
	n := int32(e.call1(ctx, e.fnRowsColCount, uint64(rowsPtr)))
	return &Rows{engine: e, ptr: rowsPtr, ctx: ctx, colCount: int(n), colTypes: make([]int32, n)}, nil
}

// QueryParams executes a query with parameters within the transaction.
func (tx *Tx) QueryParams(ctx context.Context, query string, args []any) (*Rows, error) {
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	e.arenaReset()

	sqlPtr, mustFree := e.writeSQLFallback(ctx, query)
	if sqlPtr == 0 {
		return nil, errors.New("stoolap-wasm: failed to write SQL")
	}
	defer func() {
		if mustFree {
			e.free.Call(ctx, uint64(sqlPtr), uint64(len(query)+1), 1)
		}
	}()

	if len(args) == 0 {
		rc := e.call3(ctx, e.fnTxQuery, uint64(tx.ptr), uint64(sqlPtr), uint64(e.outSlotA))
		if int32(rc) != stoolapOK {
			return nil, e.errFromTx(ctx, tx.ptr)
		}
	} else {
		paramsPtr, err := e.writeParams(ctx, args)
		if err != nil {
			return nil, err
		}
		rc := e.call5(ctx, e.fnTxQueryParams, uint64(tx.ptr), uint64(sqlPtr), uint64(paramsPtr), uint64(len(args)), uint64(e.outSlotA))
		if int32(rc) != stoolapOK {
			return nil, e.errFromTx(ctx, tx.ptr)
		}
	}
	rowsPtr := e.readU32(e.outSlotA)
	n := int32(e.call1(ctx, e.fnRowsColCount, uint64(rowsPtr)))
	return &Rows{engine: e, ptr: rowsPtr, ctx: ctx, colCount: int(n), colTypes: make([]int32, n)}, nil
}

func (tx *Tx) Commit() error {
	e := tx.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	ctx := context.Background()
	rc := e.call1(ctx, e.fnTxCommit, uint64(tx.ptr))
	if int32(rc) != stoolapOK {
		return e.errFromTx(ctx, tx.ptr)
	}
	tx.ptr = 0
	return nil
}

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
