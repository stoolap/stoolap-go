//go:build linux && amd64 && !cgo

package fakecgo

import "unsafe"

//go:nosplit
//go:norace
func x_cgo_thread_start(arg *ThreadStart) {
	var ts *ThreadStart

	ts = (*ThreadStart)(malloc(unsafe.Sizeof(*ts)))
	if ts == nil {
		println("fakecgo: out of memory in thread_start")
		abort()
	}

	const ptrSize = unsafe.Sizeof(uintptr(0))
	dst := unsafe.Slice((*uintptr)(unsafe.Pointer(ts)), unsafe.Sizeof(*ts)/ptrSize)
	src := unsafe.Slice((*uintptr)(unsafe.Pointer(arg)), unsafe.Sizeof(*arg)/ptrSize)
	for i := range src {
		dst[i] = src[i]
	}

	_cgo_sys_thread_start(ts)
}
