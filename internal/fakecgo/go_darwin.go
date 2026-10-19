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

//go:nosplit
//go:norace
func _cgo_sys_thread_start(ts *ThreadStart) {
	var attr pthread_attr_t
	var ign, oset sigset_t
	var p pthread_t
	var size size_t
	var err int

	sigfillset(&ign)
	pthread_sigmask(SIG_SETMASK, &ign, &oset)

	size = pthread_get_stacksize_np(pthread_self())
	pthread_attr_init(&attr)
	pthread_attr_setstacksize(&attr, size)
	ts.g.stackhi = uintptr(size)

	err = _cgo_try_pthread_create(&p, &attr, unsafe.Pointer(threadentry_trampolineABI0), ts)

	pthread_sigmask(SIG_SETMASK, &oset, nil)

	if err != 0 {
		print("fakecgo: pthread_create failed: ")
		println(err)
		abort()
	}
}

//go:linkname x_threadentry_trampoline threadentry_trampoline
var x_threadentry_trampoline byte
var threadentry_trampolineABI0 = &x_threadentry_trampoline

//go:nosplit
//go:norace
func threadentry(v unsafe.Pointer) unsafe.Pointer {
	ts := *(*ThreadStart)(v)
	free(v)

	setg_trampoline(setg_func, uintptr(unsafe.Pointer(ts.g)))

	fn := uintptr(unsafe.Pointer(&ts.fn))
	(*(*func())(unsafe.Pointer(&fn)))()

	return nil
}

var setg_func uintptr

//go:nosplit
//go:norace
func x_cgo_init(g *G, setg uintptr) {
	var size size_t

	setg_func = setg
	size = pthread_get_stacksize_np(pthread_self())
	g.stacklo = uintptr(unsafe.Pointer(&size)) - uintptr(size) + 4096
}
