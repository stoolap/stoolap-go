//go:build linux && amd64

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
	"unsafe"
	_ "unsafe"
)

// On Linux, runtime.asmcgocall zeroes the FS register on the g0 system stack.
// Rust code needs FS for thread-local storage. runtime.cgocall preserves FS
// via entersyscall/exitsyscall. With CGO_ENABLED=0, fakecgo (imported via
// nocgo_unix.go) enables cgocall. With CGO_ENABLED=1, Go's native
// cgo runtime enables it.
//
// We use the same assembly trampolines as darwin/amd64 but dispatch through
// cgocall instead of asmcgocall.

// abiCallFrame carries up to 7 integer/pointer arguments to the trampoline and
// receives the integer return value in r1.
type abiCallFrame struct {
	fn uintptr
	a1 uintptr
	a2 uintptr
	a3 uintptr
	a4 uintptr
	a5 uintptr
	a6 uintptr
	a7 uintptr
	r1 uintptr
}

var abiCallABI0 uintptr

//go:linkname runtime_cgocall runtime.cgocall
func runtime_cgocall(fn, arg unsafe.Pointer) int32

// noescape hides a pointer from escape analysis. Safe here because
// cgocall runs synchronously on g0 -- frame is still on goroutine stack.
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

//go:nosplit
func abiCall1(fn, a1 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall2(fn, a1, a2 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall3(fn, a1, a2, a3 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall4(fn, a1, a2, a3, a4 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall5(fn, a1, a2, a3, a4, a5 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4, a5: a5}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall6(fn, a1, a2, a3, a4, a5, a6 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4, a5: a5, a6: a6}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall7(fn, a1, a2, a3, a4, a5, a6, a7 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4, a5: a5, a6: a6, a7: a7}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCallVoid1(fn, a1 uintptr) {
	frame := abiCallFrame{fn: fn, a1: a1}
	runtime_cgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
}
