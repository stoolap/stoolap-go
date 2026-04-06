//go:build linux && amd64 && !cgo

package fakecgo

import "unsafe"

//go:cgo_import_dynamic stoolap_fakecgo_malloc malloc "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_free free "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_setenv setenv "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_unsetenv unsetenv "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_sigfillset sigfillset "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_nanosleep nanosleep "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_abort abort "libc.so.6"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_attr_init pthread_attr_init "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_create pthread_create "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_detach pthread_detach "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_sigmask pthread_sigmask "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_mutex_lock pthread_mutex_lock "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_mutex_unlock pthread_mutex_unlock "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_cond_broadcast pthread_cond_broadcast "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_setspecific pthread_setspecific "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_attr_getstacksize pthread_attr_getstacksize "libpthread.so.0"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_attr_destroy pthread_attr_destroy "libpthread.so.0"

//go:nosplit
//go:norace
func pthread_attr_getstacksize(attr *pthread_attr_t, stacksize *size_t) int32 {
	return int32(call5(pthread_attr_getstacksizeABI0, uintptr(unsafe.Pointer(attr)), uintptr(unsafe.Pointer(stacksize)), 0, 0, 0))
}

//go:nosplit
//go:norace
func pthread_attr_destroy(attr *pthread_attr_t) int32 {
	return int32(call5(pthread_attr_destroyABI0, uintptr(unsafe.Pointer(attr)), 0, 0, 0, 0))
}

//go:linkname _pthread_attr_getstacksize _pthread_attr_getstacksize
var _pthread_attr_getstacksize uint8
var pthread_attr_getstacksizeABI0 = uintptr(unsafe.Pointer(&_pthread_attr_getstacksize))

//go:linkname _pthread_attr_destroy _pthread_attr_destroy
var _pthread_attr_destroy uint8
var pthread_attr_destroyABI0 = uintptr(unsafe.Pointer(&_pthread_attr_destroy))
