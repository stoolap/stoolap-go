//go:build !windows

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
)

// acquireLock tries to acquire an exclusive lock on the file (Unix implementation)
func acquireLock(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if err == syscall.EWOULDBLOCK {
			return fmt.Errorf("database is locked by another process")
		}
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	return nil
}

// releaseFileHandles is a no-op on Unix systems
func releaseFileHandles() {
	// Unix systems don't need a delay after closing files
}
