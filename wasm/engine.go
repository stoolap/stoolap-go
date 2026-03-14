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

// Package wasm provides a pure-Go stoolap driver using WebAssembly (wazero).
// No CGO or platform-specific shared libraries are required.
//
// Usage with database/sql:
//
//	import _ "github.com/stoolap/stoolap-go/wasm"
//	db, _ := sql.Open("stoolap-wasm", "memory://")
//
// Or with the direct API:
//
//	engine, _ := wasm.NewEngine(wasmBytes)
//	db, _ := engine.Open("memory://")
package wasm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Status codes from the C API.
const (
	stoolapOK   = 0
	stoolapRow  = 100
	stoolapDone = 101
)

// Type codes from the C API.
const (
	typeNull      = 0
	typeInteger   = 1
	typeFloat     = 2
	typeText      = 3
	typeBoolean   = 4
	typeTimestamp = 5
	typeJSON      = 6
	typeBlob      = 7
)

// Isolation levels.
const (
	isolationReadCommitted = 0
	isolationSnapshot      = 1
)

// StoolapValue size in wasm32 (ILP32).
// Layout: int32 type(4) + int32 pad(4) + union(16) = 24 bytes.
const stoolapValueSize = 24

// Arena size: 256KB pre-allocated in WASM memory. No malloc/free during operations.
const arenaSize = 256 * 1024

// Engine manages a wazero WASM runtime with an instantiated stoolap module.
// All WASM calls are serialized through a mutex since WASM is single-threaded.
//
// Memory strategy: A single arena is allocated at init in WASM linear memory.
// All per-operation data (SQL strings, parameter arrays, output slots) is written
// into this arena via a bump allocator. The arena is reset between operations.
// This eliminates all malloc/free WASM calls during normal operations.
type Engine struct {
	mu      sync.Mutex
	runtime wazero.Runtime
	mod     api.Module
	mem     api.Memory

	// Memory management (only used for arena init and overflow)
	malloc api.Function
	free   api.Function

	// Arena: one large pre-allocated region in WASM memory.
	arenaPtr uint32 // base address
	arenaCap uint32 // total capacity
	arenaOff uint32 // current bump offset

	// Fixed slots within arena (first 16 bytes)
	outSlotA uint32 // 8 bytes at arenaPtr+0 (for i32/i64 output params)
	outSlotB uint32 // 8 bytes at arenaPtr+8

	// Go-side reusable buffer for building StoolapValue arrays
	paramBuf []byte

	// Pre-allocated call stack for CallWithStack (avoids allocation per WASM call).
	// Max stoolap FFI function has 5 params + 1 result = 6 slots. Use 8 for safety.
	stack [8]uint64

	// FFI functions
	fnVersion          api.Function
	fnOpen             api.Function
	fnOpenInMemory     api.Function
	fnClone            api.Function
	fnClose            api.Function
	fnErrmsg           api.Function
	fnExec             api.Function
	fnExecParams       api.Function
	fnQuery            api.Function
	fnQueryParams      api.Function
	fnPrepare          api.Function
	fnStmtExec         api.Function
	fnStmtQuery        api.Function
	fnStmtFinalize     api.Function
	fnStmtErrmsg       api.Function
	fnBegin            api.Function
	fnBeginIsolation   api.Function
	fnTxExec           api.Function
	fnTxExecParams     api.Function
	fnTxQuery          api.Function
	fnTxQueryParams    api.Function
	fnTxStmtExec       api.Function
	fnTxStmtQuery      api.Function
	fnTxCommit         api.Function
	fnTxRollback       api.Function
	fnTxErrmsg         api.Function
	fnRowsNext         api.Function
	fnRowsColCount     api.Function
	fnRowsColName      api.Function
	fnRowsColType      api.Function
	fnRowsColInt64     api.Function
	fnRowsColDouble    api.Function
	fnRowsColText      api.Function
	fnRowsColBool      api.Function
	fnRowsColTimestamp api.Function
	fnRowsColBlob      api.Function
	fnRowsColIsNull    api.Function
	fnRowsAffected     api.Function
	fnRowsClose        api.Function
	fnRowsErrmsg       api.Function
	fnRowsFetchAll     api.Function
	fnBufferFree       api.Function
	fnStringFree       api.Function
}

// NewEngine creates a new WASM engine from the given stoolap WASM binary.
// For in-memory databases only. Use NewEngineWithFS for file-based databases.
func NewEngine(ctx context.Context, wasmBytes []byte) (*Engine, error) {
	return newEngine(ctx, wasmBytes, nil)
}

// NewEngineWithFS creates a new WASM engine with filesystem access for persistent databases.
// The rootDir is mounted as "/" inside the WASM sandbox.
func NewEngineWithFS(ctx context.Context, wasmBytes []byte, rootDir string) (*Engine, error) {
	return newEngine(ctx, wasmBytes, &rootDir)
}

func newEngine(ctx context.Context, wasmBytes []byte, rootDir *string) (*Engine, error) {
	cfg := wazero.NewRuntimeConfig().WithCompilationCache(wazero.NewCompilationCache())
	r := wazero.NewRuntimeWithConfig(ctx, cfg)
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	modCfg := wazero.NewModuleConfig().
		WithSysNanosleep().
		WithSysWalltime().
		WithSysNanotime()
	if rootDir != nil {
		modCfg = modCfg.WithFSConfig(
			wazero.NewFSConfig().WithDirMount(*rootDir, "/data"),
		)
	}

	mod, err := r.InstantiateWithConfig(ctx, wasmBytes, modCfg)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("stoolap-wasm: instantiate: %w", err)
	}

	e := &Engine{
		runtime:  r,
		mod:      mod,
		mem:      mod.Memory(),
		paramBuf: make([]byte, 16*stoolapValueSize), // pre-alloc for 16 params
	}

	e.malloc = mod.ExportedFunction("__wbindgen_malloc")
	e.free = mod.ExportedFunction("__wbindgen_free")
	if e.malloc == nil || e.free == nil {
		return nil, errors.New("stoolap-wasm: missing malloc/free exports")
	}

	// Resolve FFI functions
	resolve := func(name string) api.Function { return mod.ExportedFunction(name) }
	e.fnVersion = resolve("stoolap_version")
	e.fnOpen = resolve("stoolap_open")
	e.fnOpenInMemory = resolve("stoolap_open_in_memory")
	e.fnClone = resolve("stoolap_clone")
	e.fnClose = resolve("stoolap_close")
	e.fnErrmsg = resolve("stoolap_errmsg")
	e.fnExec = resolve("stoolap_exec")
	e.fnExecParams = resolve("stoolap_exec_params")
	e.fnQuery = resolve("stoolap_query")
	e.fnQueryParams = resolve("stoolap_query_params")
	e.fnPrepare = resolve("stoolap_prepare")
	e.fnStmtExec = resolve("stoolap_stmt_exec")
	e.fnStmtQuery = resolve("stoolap_stmt_query")
	e.fnStmtFinalize = resolve("stoolap_stmt_finalize")
	e.fnStmtErrmsg = resolve("stoolap_stmt_errmsg")
	e.fnBegin = resolve("stoolap_begin")
	e.fnBeginIsolation = resolve("stoolap_begin_with_isolation")
	e.fnTxExec = resolve("stoolap_tx_exec")
	e.fnTxExecParams = resolve("stoolap_tx_exec_params")
	e.fnTxQuery = resolve("stoolap_tx_query")
	e.fnTxQueryParams = resolve("stoolap_tx_query_params")
	e.fnTxStmtExec = resolve("stoolap_tx_stmt_exec")
	e.fnTxStmtQuery = resolve("stoolap_tx_stmt_query")
	e.fnTxCommit = resolve("stoolap_tx_commit")
	e.fnTxRollback = resolve("stoolap_tx_rollback")
	e.fnTxErrmsg = resolve("stoolap_tx_errmsg")
	e.fnRowsNext = resolve("stoolap_rows_next")
	e.fnRowsColCount = resolve("stoolap_rows_column_count")
	e.fnRowsColName = resolve("stoolap_rows_column_name")
	e.fnRowsColType = resolve("stoolap_rows_column_type")
	e.fnRowsColInt64 = resolve("stoolap_rows_column_int64")
	e.fnRowsColDouble = resolve("stoolap_rows_column_double")
	e.fnRowsColText = resolve("stoolap_rows_column_text")
	e.fnRowsColBool = resolve("stoolap_rows_column_bool")
	e.fnRowsColTimestamp = resolve("stoolap_rows_column_timestamp")
	e.fnRowsColBlob = resolve("stoolap_rows_column_blob")
	e.fnRowsColIsNull = resolve("stoolap_rows_column_is_null")
	e.fnRowsAffected = resolve("stoolap_rows_affected")
	e.fnRowsClose = resolve("stoolap_rows_close")
	e.fnRowsErrmsg = resolve("stoolap_rows_errmsg")
	e.fnRowsFetchAll = resolve("stoolap_rows_fetch_all")
	e.fnBufferFree = resolve("stoolap_buffer_free")
	e.fnStringFree = resolve("stoolap_string_free")

	// Allocate the arena — one malloc call, used for everything.
	res, err := e.malloc.Call(ctx, arenaSize, 8)
	if err != nil {
		return nil, fmt.Errorf("stoolap-wasm: alloc arena: %w", err)
	}
	e.arenaPtr = uint32(res[0])
	e.arenaCap = arenaSize

	// Reserve first 16 bytes for output parameter slots.
	e.outSlotA = e.arenaPtr
	e.outSlotB = e.arenaPtr + 8
	e.arenaOff = 16

	return e, nil
}

// Close releases all resources held by the engine, including the WASM runtime.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runtime != nil {
		err := e.runtime.Close(ctx)
		e.runtime = nil
		return err
	}
	return nil
}

// ─── Zero-alloc WASM call helpers using CallWithStack ───────────────────────
// These avoid the []uint64 allocation that Call() makes on every invocation.

// call0 calls a WASM function with no params, returns result[0].
func (e *Engine) call0(ctx context.Context, fn api.Function) uint64 {
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call1 calls with 1 param, returns result[0].
func (e *Engine) call1(ctx context.Context, fn api.Function, a uint64) uint64 {
	e.stack[0] = a
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call2 calls with 2 params, returns result[0].
func (e *Engine) call2(ctx context.Context, fn api.Function, a, b uint64) uint64 {
	e.stack[0], e.stack[1] = a, b
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call3 calls with 3 params, returns result[0].
func (e *Engine) call3(ctx context.Context, fn api.Function, a, b, c uint64) uint64 {
	e.stack[0], e.stack[1], e.stack[2] = a, b, c
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call4 calls with 4 params, returns result[0].
func (e *Engine) call4(ctx context.Context, fn api.Function, a, b, c, d uint64) uint64 {
	e.stack[0], e.stack[1], e.stack[2], e.stack[3] = a, b, c, d
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call5 calls with 5 params, returns result[0].
func (e *Engine) call5(ctx context.Context, fn api.Function, a, b, c, d, f uint64) uint64 {
	e.stack[0], e.stack[1], e.stack[2], e.stack[3], e.stack[4] = a, b, c, d, f
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// callVoid calls with 1 param, ignores result.
func (e *Engine) callVoid1(ctx context.Context, fn api.Function, a uint64) {
	e.stack[0] = a
	fn.CallWithStack(ctx, e.stack[:])
}

// Version returns the stoolap engine version string.
func (e *Engine) Version(ctx context.Context) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	ptr := e.call0(ctx, e.fnVersion)
	return e.readCString(uint32(ptr))
}

// ─── Arena bump allocator ───────────────────────────────────────────────────

// arenaReset resets the arena offset past the fixed output slots.
func (e *Engine) arenaReset() {
	e.arenaOff = 16
}

// arenaAlloc bumps the arena pointer by size (with alignment).
// Returns the WASM pointer. Panics if arena overflows (should never happen for normal queries).
func (e *Engine) arenaAlloc(size, align uint32) uint32 {
	// Align up
	off := (e.arenaOff + align - 1) &^ (align - 1)
	end := off + size
	if end > e.arenaCap {
		return 0 // overflow — caller should fall back to malloc
	}
	e.arenaOff = end
	return e.arenaPtr + off
}

// ─── WASM memory helpers (zero-alloc hot path) ─────────────────────────────

// writeSQL writes a NUL-terminated SQL string into the arena. Zero Go allocations.
func (e *Engine) writeSQL(s string) uint32 {
	n := uint32(len(s) + 1)
	ptr := e.arenaAlloc(n, 1)
	if ptr == 0 {
		return 0
	}
	// Write string bytes directly to WASM memory without Go []byte allocation
	dst, ok := e.mem.Read(ptr, n)
	if !ok {
		return 0
	}
	copy(dst, unsafe.Slice(unsafe.StringData(s), len(s)))
	dst[len(s)] = 0
	return ptr
}

// writeSQLFallback writes SQL to arena, falls back to malloc for oversized strings.
func (e *Engine) writeSQLFallback(ctx context.Context, s string) (ptr uint32, mustFree bool) {
	ptr = e.writeSQL(s)
	if ptr != 0 {
		return ptr, false
	}
	// Arena overflow — malloc (rare)
	n := uint64(len(s) + 1)
	res, err := e.malloc.Call(ctx, n, 1)
	if err != nil {
		return 0, false
	}
	ptr = uint32(res[0])
	dst, _ := e.mem.Read(ptr, uint32(n))
	copy(dst, unsafe.Slice(unsafe.StringData(s), len(s)))
	dst[len(s)] = 0
	return ptr, true
}

// readCString reads a NUL-terminated C string from WASM memory.
func (e *Engine) readCString(ptr uint32) string {
	if ptr == 0 {
		return ""
	}
	// Fast path: read 256 bytes, find NUL
	data, ok := e.mem.Read(ptr, 256)
	if !ok {
		return ""
	}
	for i, b := range data {
		if b == 0 {
			return string(data[:i])
		}
	}
	// Slow path: larger string
	for size := uint32(1024); size <= 1<<20; size *= 2 {
		data, ok = e.mem.Read(ptr, size)
		if !ok {
			return ""
		}
		for i, b := range data {
			if b == 0 {
				return string(data[:i])
			}
		}
	}
	return ""
}

// readU32 reads a little-endian uint32 from WASM memory.
func (e *Engine) readU32(ptr uint32) uint32 {
	data, _ := e.mem.Read(ptr, 4)
	return binary.LittleEndian.Uint32(data)
}

// readI64 reads a little-endian int64 from WASM memory.
func (e *Engine) readI64(ptr uint32) int64 {
	data, _ := e.mem.Read(ptr, 8)
	return int64(binary.LittleEndian.Uint64(data))
}

// ─── Error helpers ──────────────────────────────────────────────────────────

func (e *Engine) errFromDB(ctx context.Context, dbPtr uint32) error {
	res, _ := e.fnErrmsg.Call(ctx, uint64(dbPtr))
	msg := e.readCString(uint32(res[0]))
	if msg == "" {
		return errors.New("stoolap-wasm: unknown error")
	}
	return errors.New(msg)
}

func (e *Engine) errFromTx(ctx context.Context, txPtr uint32) error {
	res, _ := e.fnTxErrmsg.Call(ctx, uint64(txPtr))
	msg := e.readCString(uint32(res[0]))
	if msg == "" {
		return errors.New("stoolap-wasm: unknown transaction error")
	}
	return errors.New(msg)
}

func (e *Engine) errFromStmt(ctx context.Context, stmtPtr uint32) error {
	res, _ := e.fnStmtErrmsg.Call(ctx, uint64(stmtPtr))
	msg := e.readCString(uint32(res[0]))
	if msg == "" {
		return errors.New("stoolap-wasm: unknown statement error")
	}
	return errors.New(msg)
}

// ─── Parameter encoding (arena-based, zero malloc) ──────────────────────────

// writeParams encodes Go values into a WASM StoolapValue array in the arena.
// String/blob data is also written into the arena. No malloc calls.
// Returns the WASM pointer to the array. Arena is reset by caller.
func (e *Engine) writeParams(ctx context.Context, args []any) (uint32, error) {
	n := len(args)
	if n == 0 {
		return 0, nil
	}

	totalSize := uint32(n * stoolapValueSize)

	// Ensure Go-side buffer is large enough
	if cap(e.paramBuf) < int(totalSize) {
		e.paramBuf = make([]byte, totalSize)
	}
	buf := e.paramBuf[:totalSize]
	// Zero the buffer
	for i := range buf {
		buf[i] = 0
	}

	// First pass: compute total string/blob data needed
	var strDataSize uint32
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			strDataSize += uint32(len(v))
		case []byte:
			strDataSize += uint32(len(v))
		}
	}

	// Allocate array + string data in arena contiguously
	arrayPtr := e.arenaAlloc(totalSize, 8)
	if arrayPtr == 0 {
		return 0, errors.New("stoolap-wasm: arena overflow for params")
	}
	var strDataPtr uint32
	if strDataSize > 0 {
		strDataPtr = e.arenaAlloc(strDataSize, 1)
		if strDataPtr == 0 {
			return 0, errors.New("stoolap-wasm: arena overflow for param strings")
		}
	}
	strOff := strDataPtr

	// Second pass: encode values
	for i, arg := range args {
		off := i * stoolapValueSize
		if arg == nil {
			binary.LittleEndian.PutUint32(buf[off:], typeNull)
			continue
		}
		switch v := arg.(type) {
		case int64:
			binary.LittleEndian.PutUint32(buf[off:], typeInteger)
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(v))
		case float64:
			binary.LittleEndian.PutUint32(buf[off:], typeFloat)
			binary.LittleEndian.PutUint64(buf[off+8:], math.Float64bits(v))
		case bool:
			binary.LittleEndian.PutUint32(buf[off:], typeBoolean)
			if v {
				binary.LittleEndian.PutUint32(buf[off+8:], 1)
			}
		case string:
			binary.LittleEndian.PutUint32(buf[off:], typeText)
			binary.LittleEndian.PutUint32(buf[off+8:], strOff)
			binary.LittleEndian.PutUint64(buf[off+16:], uint64(len(v)))
			// Write string data directly to WASM memory
			dst, _ := e.mem.Read(strOff, uint32(len(v)))
			copy(dst, unsafe.Slice(unsafe.StringData(v), len(v)))
			strOff += uint32(len(v))
		case []byte:
			binary.LittleEndian.PutUint32(buf[off:], typeBlob)
			binary.LittleEndian.PutUint32(buf[off+8:], strOff)
			binary.LittleEndian.PutUint64(buf[off+16:], uint64(len(v)))
			dst, _ := e.mem.Read(strOff, uint32(len(v)))
			copy(dst, v)
			strOff += uint32(len(v))
		case time.Time:
			binary.LittleEndian.PutUint32(buf[off:], typeTimestamp)
			binary.LittleEndian.PutUint64(buf[off+8:], uint64(v.UnixNano()))
		default:
			return 0, fmt.Errorf("stoolap-wasm: unsupported parameter type %T", arg)
		}
	}

	// Write the encoded array to WASM memory
	e.mem.Write(arrayPtr, buf)
	return arrayPtr, nil
}
