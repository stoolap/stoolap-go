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
	"database/sql"
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

// Status code from the C API.
const stoolapOK = 0

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
// Text and blob payloads: pointer(4) at offset 8, int64 length at offset 16.
const stoolapValueSize = 24

// StoolapNamedParam size in wasm32: pointer name(4) + int32 name_len(4) +
// padding to the 8 byte alignment of the value(8) + StoolapValue(24).
const stoolapNamedParamSize = 40

// Arena: pre-allocated in WASM memory, reset before every operation and grown
// when an operation needs more. The first arenaSlots bytes are three 8 byte
// output slots for pointer, length and count results.
const (
	arenaInitSize = 256 * 1024
	arenaSlots    = 24
)

var (
	errArena                = errors.New("stoolap-wasm: arena overflow")
	errEmptyParamName       = errors.New("stoolap-wasm: named parameter has an empty name")
	errUnsupportedParamType = errors.New("stoolap-wasm: unsupported parameter type")
	errBatchRowLength       = errors.New("stoolap-wasm: batch rows must all have the same number of parameters")
)

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

	// Memory management (only used for the arena)
	malloc api.Function
	free   api.Function

	// Arena: one large pre-allocated region in WASM memory.
	arenaPtr uint32 // base address
	arenaCap uint32 // total capacity
	arenaOff uint32 // current bump offset

	// Fixed slots within arena (first arenaSlots bytes)
	outSlotA uint32 // 8 bytes at arenaPtr+0
	outSlotB uint32 // 8 bytes at arenaPtr+8
	outSlotC uint32 // 8 bytes at arenaPtr+16

	// Go-side reusable buffer for building StoolapValue arrays
	paramBuf []byte

	// Pre-allocated call stack for CallWithStack (avoids allocation per WASM call).
	// The widest stoolap FFI function has 7 params and 1 result.
	stack [8]uint64

	// FFI functions
	fnVersion           api.Function
	fnOpen              api.Function
	fnOpenInMemory      api.Function
	fnClone             api.Function
	fnClose             api.Function
	fnErrmsg            api.Function
	fnExec              api.Function
	fnExecParams        api.Function
	fnExecNamed         api.Function
	fnQuery             api.Function
	fnQueryParams       api.Function
	fnQueryNamed        api.Function
	fnQueryParamsAll    api.Function
	fnQueryNamedAll     api.Function
	fnPrepare           api.Function
	fnStmtExec          api.Function
	fnStmtExecNamed     api.Function
	fnStmtExecBatch     api.Function
	fnStmtQuery         api.Function
	fnStmtQueryNamed    api.Function
	fnStmtQueryAll      api.Function
	fnStmtQueryNamedAll api.Function
	fnStmtFinalize      api.Function
	fnStmtErrmsg        api.Function
	fnBegin             api.Function
	fnBeginIsolation    api.Function
	fnTxExec            api.Function
	fnTxExecParams      api.Function
	fnTxExecNamed       api.Function
	fnTxQuery           api.Function
	fnTxQueryParams     api.Function
	fnTxQueryNamed      api.Function
	fnTxStmtExec        api.Function
	fnTxStmtExecNamed   api.Function
	fnTxCommit          api.Function
	fnTxRollback        api.Function
	fnTxErrmsg          api.Function
	fnRowsFetchChunk    api.Function
	fnRowsFetchAll      api.Function
	fnRowsClose         api.Function
	fnRowsErrmsg        api.Function
	fnBufferFree        api.Function
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
		r.Close(ctx)
		return nil, errors.New("stoolap-wasm: missing malloc/free exports")
	}

	// Resolve FFI functions. Every one is required: a binary that lacks one
	// predates the entry points this driver is written against.
	for _, b := range []struct {
		name string
		fn   *api.Function
	}{
		{"stoolap_version", &e.fnVersion},
		{"stoolap_open", &e.fnOpen},
		{"stoolap_open_in_memory", &e.fnOpenInMemory},
		{"stoolap_clone", &e.fnClone},
		{"stoolap_close", &e.fnClose},
		{"stoolap_errmsg", &e.fnErrmsg},
		{"stoolap_exec", &e.fnExec},
		{"stoolap_exec_params", &e.fnExecParams},
		{"stoolap_exec_named", &e.fnExecNamed},
		{"stoolap_query", &e.fnQuery},
		{"stoolap_query_params", &e.fnQueryParams},
		{"stoolap_query_named", &e.fnQueryNamed},
		{"stoolap_query_params_all", &e.fnQueryParamsAll},
		{"stoolap_query_named_all", &e.fnQueryNamedAll},
		{"stoolap_prepare", &e.fnPrepare},
		{"stoolap_stmt_exec", &e.fnStmtExec},
		{"stoolap_stmt_exec_named", &e.fnStmtExecNamed},
		{"stoolap_stmt_exec_batch", &e.fnStmtExecBatch},
		{"stoolap_stmt_query", &e.fnStmtQuery},
		{"stoolap_stmt_query_named", &e.fnStmtQueryNamed},
		{"stoolap_stmt_query_all", &e.fnStmtQueryAll},
		{"stoolap_stmt_query_named_all", &e.fnStmtQueryNamedAll},
		{"stoolap_stmt_finalize", &e.fnStmtFinalize},
		{"stoolap_stmt_errmsg", &e.fnStmtErrmsg},
		{"stoolap_begin", &e.fnBegin},
		{"stoolap_begin_with_isolation", &e.fnBeginIsolation},
		{"stoolap_tx_exec", &e.fnTxExec},
		{"stoolap_tx_exec_params", &e.fnTxExecParams},
		{"stoolap_tx_exec_named", &e.fnTxExecNamed},
		{"stoolap_tx_query", &e.fnTxQuery},
		{"stoolap_tx_query_params", &e.fnTxQueryParams},
		{"stoolap_tx_query_named", &e.fnTxQueryNamed},
		{"stoolap_tx_stmt_exec", &e.fnTxStmtExec},
		{"stoolap_tx_stmt_exec_named", &e.fnTxStmtExecNamed},
		{"stoolap_tx_commit", &e.fnTxCommit},
		{"stoolap_tx_rollback", &e.fnTxRollback},
		{"stoolap_tx_errmsg", &e.fnTxErrmsg},
		{"stoolap_rows_fetch_chunk", &e.fnRowsFetchChunk},
		{"stoolap_rows_fetch_all", &e.fnRowsFetchAll},
		{"stoolap_rows_close", &e.fnRowsClose},
		{"stoolap_rows_errmsg", &e.fnRowsErrmsg},
		{"stoolap_buffer_free", &e.fnBufferFree},
	} {
		*b.fn = mod.ExportedFunction(b.name)
		if *b.fn == nil {
			r.Close(ctx)
			return nil, fmt.Errorf("stoolap-wasm: the WASM binary does not export %s; build it from stoolap 0.5.0 or newer", b.name)
		}
	}

	if err := e.arenaReserve(ctx, arenaInitSize-arenaSlots-16); err != nil {
		r.Close(ctx)
		return nil, err
	}
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

// call6 calls with 6 params, returns result[0].
func (e *Engine) call6(ctx context.Context, fn api.Function, a, b, c, d, f, g uint64) uint64 {
	e.stack[0], e.stack[1], e.stack[2], e.stack[3], e.stack[4], e.stack[5] = a, b, c, d, f, g
	fn.CallWithStack(ctx, e.stack[:])
	return e.stack[0]
}

// call7 calls with 7 params, returns result[0].
func (e *Engine) call7(ctx context.Context, fn api.Function, a, b, c, d, f, g, h uint64) uint64 {
	e.stack[0], e.stack[1], e.stack[2], e.stack[3], e.stack[4], e.stack[5], e.stack[6] = a, b, c, d, f, g, h
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
	e.arenaOff = arenaSlots
}

// arenaReserve makes sure the arena can hold n more bytes and grows it when
// it cannot. Growing moves the arena, and the output slots with it, so it
// runs before anything for the operation is written.
func (e *Engine) arenaReserve(ctx context.Context, n uint32) error {
	need := arenaSlots + n + 16 // alignment slack
	if need <= e.arenaCap {
		return nil
	}
	newCap := max(e.arenaCap, arenaInitSize)
	for newCap < need {
		newCap *= 2
	}
	res, err := e.malloc.Call(ctx, uint64(newCap), 8)
	if err != nil {
		return fmt.Errorf("stoolap-wasm: alloc arena: %w", err)
	}
	if e.arenaPtr != 0 {
		e.free.Call(ctx, uint64(e.arenaPtr), uint64(e.arenaCap), 8)
	}
	e.arenaPtr = uint32(res[0])
	e.arenaCap = newCap
	e.outSlotA = e.arenaPtr
	e.outSlotB = e.arenaPtr + 8
	e.outSlotC = e.arenaPtr + 16
	e.arenaOff = arenaSlots
	return nil
}

// arenaAlloc bumps the arena pointer by size (with alignment) and returns
// the WASM pointer, or 0 when the arena is full.
func (e *Engine) arenaAlloc(size, align uint32) uint32 {
	off := (e.arenaOff + align - 1) &^ (align - 1)
	end := off + size
	if end > e.arenaCap {
		return 0
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

// copyOut copies n bytes at ptr out of WASM memory into a Go slice.
func (e *Engine) copyOut(ptr uint32, n int64) ([]byte, bool) {
	if n < 0 || n > math.MaxUint32 {
		return nil, false
	}
	view, ok := e.mem.Read(ptr, uint32(n))
	if !ok {
		return nil, false
	}
	buf := make([]byte, len(view))
	copy(buf, view)
	return buf, true
}

// ─── Error helpers ──────────────────────────────────────────────────────────

// errFrom reads the last error of handle through one of the stoolap_*_errmsg
// entry points.
func (e *Engine) errFrom(ctx context.Context, errmsg api.Function, handle uint32) error {
	msg := e.readCString(uint32(e.call1(ctx, errmsg, uint64(handle))))
	if msg == "" {
		return errors.New("stoolap-wasm: unknown error")
	}
	return errors.New(msg)
}

// ─── Parameter encoding (arena-based, zero malloc) ──────────────────────────

// stage resets the arena and writes the SQL text and the parameters of one
// call into it. Named parameters are used when named is not empty, otherwise
// args. The caller holds e.mu.
func (e *Engine) stage(ctx context.Context, query string, args []any, named []sql.NamedArg) (sqlPtr, paramsPtr uint32, err error) {
	e.arenaReset()
	if err = e.arenaReserve(ctx, uint32(len(query))+1+paramsSize(args, named)); err != nil {
		return 0, 0, err
	}
	if sqlPtr = e.writeSQL(query); sqlPtr == 0 {
		return 0, 0, errArena
	}
	paramsPtr, err = e.writeParams(args, named)
	return sqlPtr, paramsPtr, err
}

// stageParams is stage for prepared statements, which carry no SQL text.
func (e *Engine) stageParams(ctx context.Context, args []any, named []sql.NamedArg) (uint32, error) {
	e.arenaReset()
	if err := e.arenaReserve(ctx, paramsSize(args, named)); err != nil {
		return 0, err
	}
	return e.writeParams(args, named)
}

// paramsSize returns the arena bytes the StoolapValue or StoolapNamedParam
// array for the parameters needs, payloads and alignment included.
func paramsSize(args []any, named []sql.NamedArg) uint32 {
	n := uint32(len(args)) * stoolapValueSize
	if len(named) > 0 {
		n = uint32(len(named)) * stoolapNamedParamSize
	}
	return n + payloadsSize(args, named) + 8
}

// payloadsSize returns the bytes the text and blob payloads, and the names,
// of the parameters take beside their array.
func payloadsSize(args []any, named []sql.NamedArg) uint32 {
	var n uint32
	if len(named) > 0 {
		for _, a := range named {
			n += uint32(len(a.Name)) + payloadSize(a.Value)
		}
		return n
	}
	for _, a := range args {
		n += payloadSize(a)
	}
	return n
}

// payloadSize returns the bytes a text or blob parameter writes beside its
// StoolapValue.
func payloadSize(arg any) uint32 {
	switch v := arg.(type) {
	case string:
		return uint32(len(v))
	case []byte:
		return uint32(len(v))
	}
	return 0
}

// staging returns the zeroed Go-side buffer the StoolapValue array is built in
// before one write into WASM memory.
func (e *Engine) staging(size int) []byte {
	if cap(e.paramBuf) < size {
		e.paramBuf = make([]byte, size)
	}
	buf := e.paramBuf[:size]
	clear(buf)
	return buf
}

// payloadWriter copies text and blob payloads into the arena behind the
// parameter array and hands out their WASM pointers. An empty payload points
// at empty: the engine reads pointer 0 as SQL NULL, so it must not be 0.
type payloadWriter struct {
	mem   api.Memory
	off   uint32
	empty uint32
}

func (w *payloadWriter) write(b []byte) uint32 {
	if len(b) == 0 {
		return w.empty
	}
	dst, _ := w.mem.Read(w.off, uint32(len(b)))
	copy(dst, b)
	ptr := w.off
	w.off += uint32(len(b))
	return ptr
}

// encodeValue writes one StoolapValue for arg into buf[0:stoolapValueSize].
func encodeValue(buf []byte, arg any, w *payloadWriter) error {
	switch v := arg.(type) {
	case nil:
		binary.LittleEndian.PutUint32(buf, typeNull)
	case int64:
		binary.LittleEndian.PutUint32(buf, typeInteger)
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case int:
		binary.LittleEndian.PutUint32(buf, typeInteger)
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case int32:
		binary.LittleEndian.PutUint32(buf, typeInteger)
		binary.LittleEndian.PutUint64(buf[8:], uint64(v))
	case float64:
		binary.LittleEndian.PutUint32(buf, typeFloat)
		binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(v))
	case float32:
		binary.LittleEndian.PutUint32(buf, typeFloat)
		binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(float64(v)))
	case bool:
		binary.LittleEndian.PutUint32(buf, typeBoolean)
		if v {
			binary.LittleEndian.PutUint32(buf[8:], 1)
		}
	case string:
		binary.LittleEndian.PutUint32(buf, typeText)
		binary.LittleEndian.PutUint32(buf[8:], w.write(unsafe.Slice(unsafe.StringData(v), len(v))))
		binary.LittleEndian.PutUint64(buf[16:], uint64(len(v)))
	case []byte:
		if v == nil {
			binary.LittleEndian.PutUint32(buf, typeNull)
			break
		}
		binary.LittleEndian.PutUint32(buf, typeBlob)
		binary.LittleEndian.PutUint32(buf[8:], w.write(v))
		binary.LittleEndian.PutUint64(buf[16:], uint64(len(v)))
	case time.Time:
		binary.LittleEndian.PutUint32(buf, typeTimestamp)
		binary.LittleEndian.PutUint64(buf[8:], uint64(v.UnixNano()))
	default:
		return errUnsupportedParamType
	}
	return nil
}

// writeParams encodes the parameters into a StoolapValue array, or a
// StoolapNamedParam array when named is not empty, in the arena. Payloads
// follow the array. Returns the WASM pointer of the array, 0 for no
// parameters. The arena must already hold paramsSize bytes.
func (e *Engine) writeParams(args []any, named []sql.NamedArg) (uint32, error) {
	n, slot := len(args), stoolapValueSize
	if len(named) > 0 {
		n, slot = len(named), stoolapNamedParamSize
	}
	if n == 0 {
		return 0, nil
	}
	buf := e.staging(n * slot)
	arrayPtr := e.arenaAlloc(uint32(n*slot), 8)
	if arrayPtr == 0 {
		return 0, errArena
	}
	w := payloadWriter{mem: e.mem, empty: arrayPtr}
	if dataSize := payloadsSize(args, named); dataSize > 0 {
		if w.off = e.arenaAlloc(dataSize, 1); w.off == 0 {
			return 0, errArena
		}
	}
	if len(named) > 0 {
		for i, a := range named {
			if a.Name == "" {
				return 0, errEmptyParamName
			}
			s := buf[i*slot:]
			binary.LittleEndian.PutUint32(s, w.write(unsafe.Slice(unsafe.StringData(a.Name), len(a.Name))))
			binary.LittleEndian.PutUint32(s[4:], uint32(len(a.Name)))
			if err := encodeValue(s[16:], a.Value, &w); err != nil {
				return 0, err
			}
		}
	} else {
		for i, a := range args {
			if err := encodeValue(buf[i*slot:], a, &w); err != nil {
				return 0, err
			}
		}
	}
	e.mem.Write(arrayPtr, buf)
	return arrayPtr, nil
}

// stageBatch resets the arena and writes rows as one flat row-major
// StoolapValue array for stoolap_stmt_exec_batch. It returns the array
// pointer and the parameters per row. The caller holds e.mu.
func (e *Engine) stageBatch(ctx context.Context, rows [][]any) (uint32, int, error) {
	ppr := len(rows[0])
	var dataSize uint32
	for _, row := range rows {
		if len(row) != ppr {
			return 0, 0, errBatchRowLength
		}
		for _, a := range row {
			dataSize += payloadSize(a)
		}
	}
	e.arenaReset()
	if ppr == 0 {
		return 0, 0, nil
	}
	total := len(rows) * ppr * stoolapValueSize
	if err := e.arenaReserve(ctx, uint32(total)+dataSize+8); err != nil {
		return 0, 0, err
	}
	buf := e.staging(total)
	arrayPtr := e.arenaAlloc(uint32(total), 8)
	if arrayPtr == 0 {
		return 0, 0, errArena
	}
	w := payloadWriter{mem: e.mem, empty: arrayPtr}
	if dataSize > 0 {
		if w.off = e.arenaAlloc(dataSize, 1); w.off == 0 {
			return 0, 0, errArena
		}
	}
	for ri, row := range rows {
		for ci, a := range row {
			if err := encodeValue(buf[(ri*ppr+ci)*stoolapValueSize:], a, &w); err != nil {
				return 0, 0, err
			}
		}
	}
	e.mem.Write(arrayPtr, buf)
	return arrayPtr, ppr, nil
}
