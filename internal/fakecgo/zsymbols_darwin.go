//go:build !cgo && darwin

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

package fakecgo

import "unsafe"

//go:cgo_import_dynamic stoolap_fakecgo_malloc malloc "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_free free "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_setenv setenv "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_unsetenv unsetenv "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_sigfillset sigfillset "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_nanosleep nanosleep "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_abort abort "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_attr_init pthread_attr_init "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_create pthread_create "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_detach pthread_detach "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_sigmask pthread_sigmask "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_mutex_lock pthread_mutex_lock "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_mutex_unlock pthread_mutex_unlock "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_cond_broadcast pthread_cond_broadcast "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_setspecific pthread_setspecific "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_self pthread_self "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_get_stacksize_np pthread_get_stacksize_np "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic stoolap_fakecgo_pthread_attr_setstacksize pthread_attr_setstacksize "/usr/lib/libSystem.B.dylib"

//go:nosplit
//go:norace
func pthread_self() pthread_t {
	return pthread_t(call5(pthread_selfABI0, 0, 0, 0, 0, 0))
}

//go:nosplit
//go:norace
func pthread_get_stacksize_np(thread pthread_t) size_t {
	return size_t(call5(pthread_get_stacksize_npABI0, uintptr(thread), 0, 0, 0, 0))
}

//go:nosplit
//go:norace
func pthread_attr_setstacksize(attr *pthread_attr_t, size size_t) int32 {
	return int32(call5(pthread_attr_setstacksizeABI0, uintptr(unsafe.Pointer(attr)), uintptr(size), 0, 0, 0))
}

//go:linkname _pthread_self _pthread_self
var _pthread_self uint8
var pthread_selfABI0 = uintptr(unsafe.Pointer(&_pthread_self))

//go:linkname _pthread_get_stacksize_np _pthread_get_stacksize_np
var _pthread_get_stacksize_np uint8
var pthread_get_stacksize_npABI0 = uintptr(unsafe.Pointer(&_pthread_get_stacksize_np))

//go:linkname _pthread_attr_setstacksize _pthread_attr_setstacksize
var _pthread_attr_setstacksize uint8
var pthread_attr_setstacksizeABI0 = uintptr(unsafe.Pointer(&_pthread_attr_setstacksize))
