//go:build linux && amd64 && !cgo

package fakecgo

import _ "unsafe"

//go:linkname x_cgo_init_trampoline x_cgo_init_trampoline
//go:linkname _cgo_init _cgo_init
var x_cgo_init_trampoline byte
var _cgo_init = &x_cgo_init_trampoline

//go:linkname x_cgo_thread_start_trampoline x_cgo_thread_start_trampoline
//go:linkname _cgo_thread_start _cgo_thread_start
var x_cgo_thread_start_trampoline byte
var _cgo_thread_start = &x_cgo_thread_start_trampoline

//go:linkname x_cgo_notify_runtime_init_done_trampoline x_cgo_notify_runtime_init_done_trampoline
//go:linkname _cgo_notify_runtime_init_done _cgo_notify_runtime_init_done
var x_cgo_notify_runtime_init_done_trampoline byte
var _cgo_notify_runtime_init_done = &x_cgo_notify_runtime_init_done_trampoline

// The runtime requires this pointer to exist when iscgo=true. Leaving the
// pointed-to value at 0 disables pthread-key binding, which is sufficient for
// our Go->C-only use case.
//
//go:linkname _cgo_pthread_key_created _cgo_pthread_key_created
var x_cgo_pthread_key_created uintptr
var _cgo_pthread_key_created = &x_cgo_pthread_key_created

func set_crosscall2() {}

//go:linkname _set_crosscall2 runtime.set_crosscall2
var _set_crosscall2 = set_crosscall2

//go:linkname x_cgo_bindm_trampoline x_cgo_bindm_trampoline
//go:linkname _cgo_bindm _cgo_bindm
var x_cgo_bindm_trampoline byte
var _cgo_bindm = &x_cgo_bindm_trampoline

var (
	threadentry_call        = threadentry
	x_cgo_init_call         = x_cgo_init
	x_cgo_setenv_call       = x_cgo_setenv
	x_cgo_unsetenv_call     = x_cgo_unsetenv
	x_cgo_thread_start_call = x_cgo_thread_start
)
