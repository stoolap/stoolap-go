//go:build windows

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
	"syscall"
)

// On Windows, use syscall.LoadLibrary/GetProcAddress which are available
// without CGO — they go through Go's internal Windows API wrappers.

const rtldNow = 0 // unused on Windows, kept for interface compatibility

func dlopen(path string) (uintptr, error) {
	handle, err := syscall.LoadLibrary(path)
	if err != nil {
		return 0, errors.New("LoadLibrary: " + path + ": " + err.Error())
	}
	return uintptr(handle), nil
}

func dlsym(handle uintptr, name string) (uintptr, error) {
	cs := newCStr(name)
	addr, err := syscall.GetProcAddress(syscall.Handle(handle), string(cs.buf[:len(name)]))
	cs.keepAlive()
	if err != nil {
		return 0, errors.New("GetProcAddress: " + name + ": " + err.Error())
	}
	return addr, nil
}

// dlerrorStr is unused on Windows but needed for interface compat.
func dlerrorStr() string {
	return ""
}
