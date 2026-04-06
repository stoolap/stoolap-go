//go:build darwin && amd64

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

// We issue raw System V AMD64 ABI calls from g0 via runtime.asmcgocall
// instead of placing foreign frames on the goroutine stack.
// That keeps GC stack scanning safe without paying runtime.cgocall's
// entersyscall/exitsyscall overhead.

type abiCallFrame struct {
	fn uintptr
	a1 uintptr
	a2 uintptr
	a3 uintptr
	a4 uintptr
	a5 uintptr
	r1 uintptr
}

// abiCallPtrLenFrame is used by abiCallPtrLen to pass &outLen as arg3 to
// foreign functions like column_text/column_blob that return (ptr, outLen).
// The out_len field lives in this struct (on goroutine stack), accessible from g0.
type abiCallPtrLenFrame struct {
	fn     uintptr // column_text symbol
	a1     uintptr // rows ptr
	a2     uintptr // column index
	outLen int64   // <- C writes length here (passed as arg3 = &outLen)
	r1     uintptr // return: char pointer
}

// abiCallFloatFrame captures a float64 return value from XMM0.
type abiCallFloatFrame struct {
	fn uintptr
	a1 uintptr
	a2 uintptr
	r1 float64
}

var abiCallABI0 uintptr
var abiCallPtrLenABI0 uintptr
var abiCallFloatABI0 uintptr

//go:linkname runtime_asmcgocall runtime.asmcgocall
func runtime_asmcgocall(fn, arg unsafe.Pointer) int32

// noescape hides a pointer from escape analysis. Safe here because
// asmcgocall runs synchronously on g0 -- frame is still on goroutine stack.
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

//go:nosplit
func abiCall1(fn, a1 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall2(fn, a1, a2 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall3(fn, a1, a2, a3 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall4(fn, a1, a2, a3, a4 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCall5(fn, a1, a2, a3, a4, a5 uintptr) uintptr {
	frame := abiCallFrame{fn: fn, a1: a1, a2: a2, a3: a3, a4: a4, a5: a5}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}

//go:nosplit
func abiCallVoid1(fn, a1 uintptr) {
	frame := abiCallFrame{fn: fn, a1: a1}
	runtime_asmcgocall(unsafe.Pointer(abiCallABI0), noescape(unsafe.Pointer(&frame)))
}

// abiCallPtrLen calls fn(a1, a2, &outLen) and returns (ptr, len).
// The outLen lives in the frame struct on the goroutine stack -- C writes to it
// from g0 via the trampoline that passes &frame.outLen as arg3.
// Eliminates the O(n) NUL-byte walk in goString for text/blob reads.
//
//go:nosplit
func abiCallPtrLen(fn, a1, a2 uintptr) (uintptr, int64) {
	frame := abiCallPtrLenFrame{fn: fn, a1: a1, a2: a2}
	runtime_asmcgocall(unsafe.Pointer(abiCallPtrLenABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1, frame.outLen
}

// abiCallFloat2 calls fn(a1, a2) and returns the float64 value from XMM0.
//
//go:nosplit
func abiCallFloat2(fn, a1, a2 uintptr) float64 {
	frame := abiCallFloatFrame{fn: fn, a1: a1, a2: a2}
	runtime_asmcgocall(unsafe.Pointer(abiCallFloatABI0), noescape(unsafe.Pointer(&frame)))
	return frame.r1
}
