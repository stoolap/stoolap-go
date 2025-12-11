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
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// MVCCFactory creates mvcc storage engines
type MVCCFactory struct{}

// Create implements the EngineFactory interface
func (f *MVCCFactory) Create(dsn string) (storage.Engine, error) {
	// Handle different URL schemes for clarity
	var path string
	var persistenceEnabled bool

	// Parse DSN similar to SQLite driver handling style
	pos := strings.IndexRune(dsn, '?')
	queryParams := make(map[string]string)

	if pos >= 1 {
		// Parse query parameters using url.ParseQuery for proper handling
		params, err := url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid query parameters: %w", err)
		}

		// Convert url.Values to simple map[string]string
		for key, values := range params {
			if len(values) > 0 {
				queryParams[key] = values[0]
			}
		}

		// Remove query parameters from DSN
		dsn = dsn[:pos]
	}

	// Parse scheme
	var scheme string
	if idx := strings.Index(dsn, "://"); idx != -1 {
		scheme = dsn[:idx]
		path = dsn[idx+3:]
	} else {
		return nil, errors.New("invalid format: expected scheme://path")
	}

	switch scheme {
	case "memory":
		// In-memory mode - no persistence
		persistenceEnabled = false
		path = ""

	case "file":
		// File mode - always persistent
		persistenceEnabled = true

		// Path is already extracted, just validate it
		if path == "" {
			return nil, errors.New("file:// scheme requires a non-empty path")
		}

		// Handle home directory expansion
		if strings.HasPrefix(path, "~/") {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("failed to get home directory: %w", err)
			}
			path = filepath.Join(home, path[2:])
		}

		// Convert to absolute path - this handles:
		// - Relative paths (./db, ../db)
		// - Absolute paths (/var/db, C:\Users\db, C:/Users/db)
		// - Platform-specific separators
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("invalid file path: %w", err)
		}

		path = absPath

	default:
		return nil, errors.New("unsupported scheme: must use 'memory://' or 'file://'")
	}

	// Process query parameters for configuration

	// Configure the engine with default values
	config := &storage.Config{
		Path:        path,
		Persistence: storage.DefaultPersistenceConfig(),
	}

	// Set persistence based on the URL scheme - this is non-negotiable based on scheme
	config.Persistence.Enabled = persistenceEnabled

	// Parse sync mode
	if syncMode, ok := queryParams["sync_mode"]; ok && syncMode != "" {
		switch syncMode {
		case "none":
			config.Persistence.SyncMode = 0
		case "normal":
			config.Persistence.SyncMode = 1
		case "full":
			config.Persistence.SyncMode = 2
		default:
			// Try to parse as int
			if mode, err := strconv.Atoi(syncMode); err == nil && mode >= 0 && mode <= 2 {
				config.Persistence.SyncMode = mode
			}
		}
	}

	// Parse other configuration parameters
	// Parse snapshot interval
	if interval, ok := queryParams["snapshot_interval"]; ok && interval != "" {
		if val, err := strconv.Atoi(interval); err == nil && val > 0 {
			config.Persistence.SnapshotInterval = val
		}
	}

	// Parse keep snapshots
	if keep, ok := queryParams["keep_snapshots"]; ok && keep != "" {
		if val, err := strconv.Atoi(keep); err == nil && val > 0 {
			config.Persistence.KeepSnapshots = val
		}
	}

	// Parse WAL flush trigger
	if trigger, ok := queryParams["wal_flush_trigger"]; ok && trigger != "" {
		if val, err := strconv.Atoi(trigger); err == nil && val > 0 {
			config.Persistence.WALFlushTrigger = val
		}
	}

	// Parse WAL buffer size
	if bufSize, ok := queryParams["wal_buffer_size"]; ok && bufSize != "" {
		if val, err := strconv.Atoi(bufSize); err == nil && val > 0 {
			config.Persistence.WALBufferSize = val
		}
	}

	// Create and return the engine
	engine := NewMVCCEngine(config)
	return engine, nil
}

func init() {
	// Register the MVCC factory with the storage engine registry
	storage.RegisterEngineFactory("mvcc", &MVCCFactory{})
}
