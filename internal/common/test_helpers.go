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

package common

import (
	"os"
	"runtime"
	"testing"
	"time"
)

// TempDir creates a temporary directory that is cleaned up when the test ends.
// Unlike common.TempDir(t), this ensures cleanup happens with proper timing for Windows.
// It uses t.Cleanup() to register cleanup that runs before common.TempDir(t) cleanup.
func TempDir(t *testing.T) string {
	t.Helper()

	// Create temp dir with test name for uniqueness
	prefix := "stoolap-" + t.Name() + "-"
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Register cleanup with retry logic
	t.Cleanup(func() {
		// On Windows, add a small delay before cleanup
		if runtime.GOOS == "windows" {
			time.Sleep(100 * time.Millisecond)
		}

		// Try to remove with retries for Windows
		maxRetries := 1
		if runtime.GOOS == "windows" {
			maxRetries = 5
		}

		var lastErr error
		for i := 0; i < maxRetries; i++ {
			err := os.RemoveAll(dir)
			if err == nil {
				return
			}
			lastErr = err

			// On Windows, wait before retry with exponential backoff
			if runtime.GOOS == "windows" && i < maxRetries-1 {
				time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
			}
		}

		// If we still can't remove it, log but don't fail the test
		// This is common on Windows due to antivirus or other processes
		if lastErr != nil {
			t.Logf("Warning: failed to remove temp dir %q after %d attempts: %v", dir, maxRetries, lastErr)
		}
	})

	return dir
}
