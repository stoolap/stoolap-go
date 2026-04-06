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

// Package stoolap provides a Go driver for the Stoolap embedded SQL database.
// Uses direct ABI calls to the native shared library. No C compiler needed.
// CGO_ENABLED=0 works.
//
// Usage with database/sql:
//
//	import _ "github.com/stoolap/stoolap-go"
//	db, _ := sql.Open("stoolap", "memory://")
//
// The shared library is loaded from the bundled lib/ submodule.
package stoolap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"
)

// Status codes
const (
	stoolapOK   = 0
	stoolapRow  = 100
	stoolapDone = 101
)

// Type codes
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

// Isolation levels
const (
	isolationReadCommitted = 0
	isolationSnapshot      = 1
)

// sym holds raw function pointers for direct ABI calls.
// Zero-dependency: loaded via our own dlopen/dlsym (no purego library).
// Called via abiCallN → runtime.asmcgocall → ARM64 trampoline.
var sym struct {
	// DB operations
	version      uintptr
	open         uintptr
	openInMemory uintptr
	close        uintptr
	clone        uintptr
	errmsg       uintptr
	exec         uintptr
	execParams   uintptr
	query        uintptr
	queryParams  uintptr
	prepare      uintptr
	begin        uintptr
	beginIso     uintptr

	// Stmt
	stmtExec     uintptr
	stmtQuery    uintptr
	stmtFinalize uintptr
	stmtErrmsg   uintptr

	// Tx
	txExec        uintptr
	txExecParams  uintptr
	txQuery       uintptr
	txQueryParams uintptr
	txCommit      uintptr
	txRollback    uintptr
	txErrmsg      uintptr

	// Rows
	rowsNext         uintptr
	rowsColCount     uintptr
	rowsColName      uintptr
	rowsColType      uintptr
	rowsColInt64     uintptr
	rowsColDouble    uintptr
	rowsColText      uintptr
	rowsColBool      uintptr
	rowsColTimestamp uintptr
	rowsColBlob      uintptr
	rowsColIsNull    uintptr
	rowsClose        uintptr
	rowsErrmsg       uintptr

	// Bulk
	rowsFetchAll uintptr
	bufferFree   uintptr
}

var libOnce sync.Once
var libErr error

const maxPooledCStringCap = 4 << 10

var cStrPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

func loadLibrary() error {
	libOnce.Do(func() {
		libPath, err := findLibrary()
		if err != nil {
			libErr = fmt.Errorf("stoolap: %w", err)
			return
		}

		handle, err := dlopen(libPath)
		if err != nil {
			libErr = fmt.Errorf("stoolap: %w", err)
			return
		}

		must := func(name string) uintptr {
			addr, e := dlsym(handle, name)
			if e != nil {
				libErr = errors.New("stoolap: missing symbol: " + name)
				return 0
			}
			return addr
		}

		sym.version = must("stoolap_version")
		sym.open = must("stoolap_open")
		sym.openInMemory = must("stoolap_open_in_memory")
		sym.close = must("stoolap_close")
		sym.clone = must("stoolap_clone")
		sym.errmsg = must("stoolap_errmsg")
		sym.exec = must("stoolap_exec")
		sym.execParams = must("stoolap_exec_params")
		sym.query = must("stoolap_query")
		sym.queryParams = must("stoolap_query_params")
		sym.prepare = must("stoolap_prepare")
		sym.begin = must("stoolap_begin")
		sym.beginIso = must("stoolap_begin_with_isolation")
		sym.stmtExec = must("stoolap_stmt_exec")
		sym.stmtQuery = must("stoolap_stmt_query")
		sym.stmtFinalize = must("stoolap_stmt_finalize")
		sym.stmtErrmsg = must("stoolap_stmt_errmsg")
		sym.txExec = must("stoolap_tx_exec")
		sym.txExecParams = must("stoolap_tx_exec_params")
		sym.txQuery = must("stoolap_tx_query")
		sym.txQueryParams = must("stoolap_tx_query_params")
		sym.txCommit = must("stoolap_tx_commit")
		sym.txRollback = must("stoolap_tx_rollback")
		sym.txErrmsg = must("stoolap_tx_errmsg")
		sym.rowsNext = must("stoolap_rows_next")
		sym.rowsColCount = must("stoolap_rows_column_count")
		sym.rowsColName = must("stoolap_rows_column_name")
		sym.rowsColType = must("stoolap_rows_column_type")
		sym.rowsColInt64 = must("stoolap_rows_column_int64")
		sym.rowsColDouble = must("stoolap_rows_column_double")
		sym.rowsColText = must("stoolap_rows_column_text")
		sym.rowsColBool = must("stoolap_rows_column_bool")
		sym.rowsColTimestamp = must("stoolap_rows_column_timestamp")
		sym.rowsColBlob = must("stoolap_rows_column_blob")
		sym.rowsColIsNull = must("stoolap_rows_column_is_null")
		sym.rowsClose = must("stoolap_rows_close")
		sym.rowsErrmsg = must("stoolap_rows_errmsg")
		sym.rowsFetchAll = must("stoolap_rows_fetch_all")
		sym.bufferFree = must("stoolap_buffer_free")
	})
	return libErr
}

func findLibrary() (string, error) {
	var libName string
	switch runtime.GOOS {
	case "darwin":
		libName = "libstoolap.dylib"
	case "linux":
		libName = "libstoolap.so"
	case "windows":
		libName = "stoolap.dll"
	default:
		return "", fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}

	if p := os.Getenv("STOOLAP_LIB"); p != "" {
		return p, nil
	}

	var subdir string
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/arm64":
		subdir = "darwin_arm64"
	case "linux/amd64":
		subdir = "linux_amd64"
	case "windows/amd64":
		subdir = "windows_amd64"
	default:
		return "", fmt.Errorf("no bundled library for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	if _, err := os.Stat(libName); err == nil {
		abs, _ := filepath.Abs(libName)
		return abs, nil
	}

	localLib := filepath.Join("lib", subdir, libName)
	if _, err := os.Stat(localLib); err == nil {
		abs, _ := filepath.Abs(localLib)
		return abs, nil
	}

	home, _ := os.UserHomeDir()
	patterns := []string{
		filepath.Join(home, "go", "pkg", "mod", "github.com", "stoolap", "stoolap-go", "lib", subdir+"@*", libName),
	}
	for _, pattern := range patterns {
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			return matches[len(matches)-1], nil
		}
	}

	return "", errors.New("library not found — set STOOLAP_LIB or place " + libName + " in current directory")
}

// ─── String helpers ─────────────────────────────────────────────────────────

type cStr struct {
	ptr uintptr
	buf []byte
	pb  *[]byte
}

func newCStr(s string) cStr {
	n := len(s) + 1
	if n <= maxPooledCStringCap {
		pb := cStrPool.Get().(*[]byte)
		b := *pb
		if cap(b) < n {
			b = make([]byte, n)
		} else {
			b = b[:n]
		}
		copy(b, s)
		b[len(s)] = 0
		*pb = b
		return cStr{ptr: uintptr(unsafe.Pointer(&b[0])), buf: b, pb: pb}
	}

	b := make([]byte, n)
	copy(b, s)
	return cStr{ptr: uintptr(unsafe.Pointer(&b[0])), buf: b}
}

func (c cStr) keepAlive() {
	runtime.KeepAlive(c.buf)
	if c.pb != nil {
		*c.pb = c.buf[:0]
		cStrPool.Put(c.pb)
	}
}

// goString reads a NUL-terminated C string. Single allocation.
func goString(ptr unsafe.Pointer) string {
	if ptr == nil {
		return ""
	}
	n := 0
	for *(*byte)(unsafe.Add(ptr, n)) != 0 {
		n++
	}
	if n == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(ptr), n))
}

// goStringN copies n bytes from a C pointer into a Go string.
// Skips the O(n) NUL-byte walk when length is known.
func goStringN(ptr unsafe.Pointer, n int) string {
	if ptr == nil || n <= 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(ptr), n))
}
