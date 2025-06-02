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

//go:build windows
// +build windows

package stoolap

import (
	"os"
	"time"
)

// releaseFileHandles adds a delay on Windows to ensure file handles are released
// before attempting to remove files. This is a Windows-specific issue where
// file handles are not immediately released after Close() is called.
func releaseFileHandles() {
	// Default delay
	delay := 250 * time.Millisecond
	
	// In CI environments, use a longer delay
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		delay = 1 * time.Second
	}
	
	time.Sleep(delay)
}