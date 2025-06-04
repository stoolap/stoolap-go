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
package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// DatabaseMapper handles mapping database names to Stoolap DSNs
type DatabaseMapper struct {
	baseDir     string // Base directory for file-based databases
	defaultDSN  string // Default DSN from command line
	allowCreate bool   // Whether to create new databases on demand
}

// NewDatabaseMapper creates a new database mapper
func NewDatabaseMapper(defaultDSN string) *DatabaseMapper {
	mapper := &DatabaseMapper{
		defaultDSN:  defaultDSN,
		allowCreate: false, // Conservative default
	}

	// If the default DSN is file-based, extract the directory
	if strings.HasPrefix(defaultDSN, "file://") {
		path := strings.TrimPrefix(defaultDSN, "file://")
		mapper.baseDir = filepath.Dir(path)
		// Enable creation for file-based databases
		mapper.allowCreate = true
	}

	return mapper
}

// MapDatabase maps a database name to a Stoolap DSN
func (dm *DatabaseMapper) MapDatabase(dbName string) (string, error) {
	// Handle special cases
	switch dbName {
	case "", "postgres", "stoolap":
		// Use the default database for these
		return dm.defaultDSN, nil
	}

	// If we're using memory database, create a unique memory database per name
	if dm.defaultDSN == "memory://" {
		// For memory databases, we can't really have multiple named databases
		// So we either accept all names or reject non-default ones
		if dm.allowCreate {
			// In future, we could implement named memory databases
			return dm.defaultDSN, nil
		}
		return "", fmt.Errorf("database \"%s\" does not exist", dbName)
	}

	// If we have a base directory, map to a file in that directory
	if dm.baseDir != "" && dm.allowCreate {
		// Sanitize the database name to prevent path traversal
		safeName := sanitizeDatabaseName(dbName)
		if safeName == "" {
			return "", fmt.Errorf("invalid database name: %s", dbName)
		}

		// Create a path for this database
		dbPath := filepath.Join(dm.baseDir, safeName+".db")
		return fmt.Sprintf("file://%s", dbPath), nil
	}

	// Database doesn't exist and we can't create it
	return "", fmt.Errorf("database \"%s\" does not exist", dbName)
}

// sanitizeDatabaseName ensures the database name is safe for use as a filename
func sanitizeDatabaseName(name string) string {
	// Only allow alphanumeric, underscore, and dash
	var safe strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			safe.WriteRune(r)
		}
	}
	return safe.String()
}

// IsValidDatabaseName checks if a database name is valid
func IsValidDatabaseName(name string) bool {
	if name == "" {
		return false
	}

	// Check length
	if len(name) > 63 { // PostgreSQL limit
		return false
	}

	// Must start with letter or underscore
	first := rune(name[0])
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		return false
	}

	// Check all characters
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-') {
			return false
		}
	}

	return true
}
