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
	"path/filepath"
)

// FileLock represents an exclusive lock on a database directory
type FileLock struct {
	file *os.File
	path string
}

// AcquireFileLock attempts to acquire an exclusive lock on the database directory.
// It creates a lock file in the database directory and locks it using OS-level file locking.
// Returns an error if the lock cannot be acquired (typically because another process has it).
func AcquireFileLock(dbPath string) (*FileLock, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Lock file path
	lockFilePath := filepath.Join(dbPath, "db.lock")

	// Open the lock file (create if it doesn't exist)
	file, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	// Try to acquire an exclusive lock (platform-specific implementation)
	if err = acquireLock(file); err != nil {
		file.Close()
		return nil, err
	}

	// Write the current process ID to the lock file for debugging
	pid := os.Getpid()
	file.Truncate(0)
	file.Seek(0, 0)
	fmt.Fprintf(file, "%d", pid)

	return &FileLock{
		file: file,
		path: lockFilePath,
	}, nil
}

// Release releases the file lock
func (l *FileLock) Release() error {
	if l.file == nil {
		return nil
	}

	// Release the lock by closing the file
	err := l.file.Close()
	l.file = nil

	// We don't remove the lock file as it will be reused on next open
	// This helps preserve the lock file permissions between runs

	return err
}
