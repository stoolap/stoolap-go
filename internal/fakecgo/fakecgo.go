//go:build linux && amd64 && !cgo

package fakecgo

// setg_trampoline calls the runtime-provided setg function with the supplied g.
func setg_trampoline(setg uintptr, g uintptr)

// call5 invokes a C function with up to 5 integer/pointer arguments.
func call5(fn, a1, a2, a3, a4, a5 uintptr) uintptr
