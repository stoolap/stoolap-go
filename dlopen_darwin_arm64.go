//go:build darwin && arm64

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
	"errors"
	"unsafe"
)

// Import dlopen/dlsym/dlerror from libSystem at link time (CGO_ENABLED=0 safe).
// These are used via assembly trampolines defined in dlopen_darwin_arm64.s.

//go:cgo_import_dynamic libc_dlopen dlopen "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_dlsym dlsym "/usr/lib/libSystem.B.dylib"
//go:cgo_import_dynamic libc_dlerror dlerror "/usr/lib/libSystem.B.dylib"

// Trampoline addresses (filled by linker, defined in dlopen_darwin_arm64.s).
var libc_dlopen_trampoline_addr uintptr
var libc_dlsym_trampoline_addr uintptr
var libc_dlerror_trampoline_addr uintptr

const rtldNow = 0x2

func dlopen(path string) (uintptr, error) {
	cs := newCStr(path)
	handle := abiCall2(libc_dlopen_trampoline_addr, cs.ptr, uintptr(rtldNow))
	cs.keepAlive()
	if handle == 0 {
		return 0, errors.New("dlopen: " + dlerrorStr())
	}
	return handle, nil
}

func dlsym(handle uintptr, name string) (uintptr, error) {
	cs := newCStr(name)
	addr := abiCall2(libc_dlsym_trampoline_addr, handle, cs.ptr)
	cs.keepAlive()
	if addr == 0 {
		return 0, errors.New("dlsym: " + name + ": " + dlerrorStr())
	}
	return addr, nil
}

func dlerrorStr() string {
	// dlerror takes no args — pass 0 as dummy (ignored by the C function)
	ptr := abiCall1(libc_dlerror_trampoline_addr, 0)
	if ptr == 0 {
		return ""
	}
	return goString(unsafe.Pointer(ptr))
}
