//go:build linux && amd64 && !cgo

package fakecgo

import (
	"syscall"
	"unsafe"
)

var (
	pthread_g pthread_key_t

	runtime_init_cond = PTHREAD_COND_INITIALIZER
	runtime_init_mu   = PTHREAD_MUTEX_INITIALIZER
	runtime_init_done int
)

//go:nosplit
//go:norace
func x_cgo_notify_runtime_init_done() {
	pthread_mutex_lock(&runtime_init_mu)
	runtime_init_done = 1
	pthread_cond_broadcast(&runtime_init_cond)
	pthread_mutex_unlock(&runtime_init_mu)
}

//go:norace
func x_cgo_bindm(g unsafe.Pointer) {
	pthread_setspecific(pthread_g, g)
}

//go:nosplit
//go:norace
func _cgo_try_pthread_create(thread *pthread_t, attr *pthread_attr_t, pfn unsafe.Pointer, arg *ThreadStart) int {
	var ts syscall.Timespec
	tries := ts.Nsec

	for tries = 0; tries < 20; tries++ {
		err := int(call5(pthread_createABI0, uintptr(unsafe.Pointer(thread)), uintptr(unsafe.Pointer(attr)), uintptr(pfn), uintptr(unsafe.Pointer(arg)), 0))
		if err == 0 {
			call5(pthread_detachABI0, uintptr(*thread), 0, 0, 0, 0)
			return 0
		}
		if err != int(syscall.EAGAIN) {
			return err
		}

		ts.Sec = 0
		ts.Nsec = (tries + 1) * 1000 * 1000
		call5(nanosleepABI0, uintptr(unsafe.Pointer(&ts)), 0, 0, 0, 0)
	}

	return int(syscall.EAGAIN)
}
