//go:build linux && !cgo

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

// Import dlopen/dlsym/dlerror from libdl at link time (CGO_ENABLED=0 safe).
// These are used via assembly trampolines defined in dlopen_linux_{arch}.s.

//go:cgo_import_dynamic stoolap_dlopen_sym dlopen "libdl.so.2"
//go:cgo_import_dynamic stoolap_dlsym_sym dlsym "libdl.so.2"
//go:cgo_import_dynamic stoolap_dlerror_sym dlerror "libdl.so.2"
//go:cgo_import_dynamic _ _ "libdl.so.2"

// Imported symbol slots filled by the external linker.
var stoolap_dlopen_sym uintptr
var stoolap_dlsym_sym uintptr
var stoolap_dlerror_sym uintptr

//go:linkname libdlDlopen libdl_dlopen
var libdlDlopen uint8

//go:linkname libdlDlsym libdl_dlsym
var libdlDlsym uint8

//go:linkname libdlDlerror libdl_dlerror
var libdlDlerror uint8

var (
	libdlDlopenABI0  = uintptr(unsafe.Pointer(&libdlDlopen))
	libdlDlsymABI0   = uintptr(unsafe.Pointer(&libdlDlsym))
	libdlDlerrorABI0 = uintptr(unsafe.Pointer(&libdlDlerror))
)

const rtldNow = 0x2

func dlopen(path string) (uintptr, error) {
	cs := newCStr(path)
	handle := abiCall2(libdlDlopenABI0, cs.ptr, uintptr(rtldNow))
	cs.keepAlive()
	if handle == 0 {
		return 0, errors.New("dlopen: " + dlerrorStr())
	}
	return handle, nil
}

func dlsym(handle uintptr, name string) (uintptr, error) {
	cs := newCStr(name)
	addr := abiCall2(libdlDlsymABI0, handle, cs.ptr)
	cs.keepAlive()
	if addr == 0 {
		return 0, errors.New("dlsym: " + name + ": " + dlerrorStr())
	}
	return addr, nil
}

func dlerrorStr() string {
	// dlerror takes no args -- pass 0 as dummy (ignored by the C function)
	ptr := abiCall1(libdlDlerrorABI0, 0)
	if ptr == 0 {
		return ""
	}
	return goString(unsafe.Pointer(ptr))
}
