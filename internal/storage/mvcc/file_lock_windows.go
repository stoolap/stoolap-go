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
package mvcc

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
	ERROR_LOCK_VIOLATION      = 33
)

type OVERLAPPED struct {
	Internal     uintptr
	InternalHigh uintptr
	Offset       uint32
	OffsetHigh   uint32
	HEvent       uintptr
}

// acquireLock tries to acquire an exclusive lock on the file (Windows implementation)
func acquireLock(file *os.File) error {
	handle := syscall.Handle(file.Fd())

	var overlapped OVERLAPPED

	ret, _, err := procLockFileEx.Call(
		uintptr(handle),
		uintptr(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)

	if ret == 0 {
		if err == syscall.Errno(ERROR_LOCK_VIOLATION) {
			return fmt.Errorf("database is locked by another process")
		}
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	return nil
}

// releaseFileHandles is called after closing files on Windows to ensure handles are released
func releaseFileHandles() {
	// Windows file handles may take a moment to be fully released
	// This small delay helps prevent "file in use" errors during cleanup
	time.Sleep(250 * time.Millisecond)
}
