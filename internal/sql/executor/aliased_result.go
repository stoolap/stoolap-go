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
package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/stoolap/stoolap/internal/storage"
)

// Object pools for map structures to reduce memory allocations
var (
	aliasMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]string, 32) // Pre-allocate with reasonable capacity
		},
	}
	columnMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[int]int, 32) // Pre-allocate with reasonable capacity
		},
	}
)

// AliasedResult is a wrapper for storage.Result that handles column aliases
// This specialized result set handles the translation between original column names
// and their aliases, so aliases work correctly at all levels of the system.
type AliasedResult struct {
	baseResult storage.Result

	// Map from alias to original column name
	aliases map[string]string

	// Map from original column name to alias
	reverseAliases map[string]string

	// Column names as they should be returned to the client
	displayColumns []string

	// Column names as they exist in the base result
	actualColumns []string

	// Column index mapping
	columnIndexMap map[int]int
}

// NewAliasedResult creates a new aliased result
func NewAliasedResult(base storage.Result, aliases map[string]string) *AliasedResult {
	// Get aliases map from the pool
	resultAliases := aliasMapPool.Get().(map[string]string)
	// Clear the map if it's not empty
	for k := range resultAliases {
		delete(resultAliases, k)
	}

	// Get reverse aliases map from the pool
	reverseAliases := aliasMapPool.Get().(map[string]string)
	// Clear the map if it's not empty
	for k := range reverseAliases {
		delete(reverseAliases, k)
	}

	// Get column index map from the pool
	columnIndexMap := columnMapPool.Get().(map[int]int)
	// Clear the map if it's not empty
	for k := range columnIndexMap {
		delete(columnIndexMap, k)
	}

	// If the aliases map is nil or empty, just use the base result
	actualColumns := base.Columns()
	displayColumns := make([]string, len(actualColumns))

	if len(aliases) == 0 {
		// Copy the columns as is for display
		copy(displayColumns, actualColumns)

		// Default mapping is 1:1
		for i := range displayColumns {
			columnIndexMap[i] = i
		}

		return &AliasedResult{
			baseResult:     base,
			aliases:        resultAliases,
			reverseAliases: reverseAliases,
			displayColumns: displayColumns,
			actualColumns:  actualColumns,
			columnIndexMap: columnIndexMap,
		}
	}

	// Copy aliases to our pooled map
	for alias, originalCol := range aliases {
		resultAliases[alias] = originalCol
		reverseAliases[originalCol] = alias
	}

	// Prepare the display columns (with aliases)
	for i, col := range actualColumns {
		if alias, exists := reverseAliases[col]; exists {
			displayColumns[i] = alias
		} else {
			displayColumns[i] = col
		}
	}

	// Create column index mapping for Scan
	for i := range displayColumns {
		columnIndexMap[i] = i // Default mapping is 1:1
	}

	result := &AliasedResult{
		baseResult:     base,
		aliases:        resultAliases,
		reverseAliases: reverseAliases,
		displayColumns: displayColumns,
		actualColumns:  actualColumns,
		columnIndexMap: columnIndexMap,
	}

	return result
}

// Columns returns the column names with aliases
func (a *AliasedResult) Columns() []string {
	// Use display columns (with aliases) instead of actual columns
	return a.displayColumns
}

// Next moves to the next row
func (a *AliasedResult) Next() bool {
	return a.baseResult.Next()
}

// Scan scans the values from the current row into the destination
func (a *AliasedResult) Scan(dest ...interface{}) error {
	// This is where we fix the issue - the destination slice should match
	// the column order as seen by the user, but we need to map it to the right
	// columns in the underlying result

	// First, make sure we have enough destinations
	if len(dest) != len(a.displayColumns) {
		return fmt.Errorf("expected %d destination arguments in Scan, got %d", len(a.displayColumns), len(dest))
	}

	// Get the current row directly using Row() instead of Scan()
	row := a.baseResult.Row()
	if row == nil {
		return fmt.Errorf("no row available")
	}

	// Now map the values to the destinations based on the alias mapping
	for i, alias := range a.displayColumns {
		// Find the index of this column (or its original name) in the actual columns
		actualIndex := -1
		originalColName := ""

		// If this is an alias, find the original column name
		if origCol, isAlias := a.aliases[alias]; isAlias {
			originalColName = origCol
		} else {
			originalColName = alias // Not an alias, use as is
		}

		// Find the index of the original column name
		for j, actualCol := range a.actualColumns {
			if actualCol == originalColName {
				actualIndex = j
				break
			}
		}

		// If we found the column, copy the value to the destination
		if actualIndex >= 0 && actualIndex < len(row) {
			// Get the ColumnValue directly from the row
			colVal := row[actualIndex]

			// Use the central utility function for consistent scanning
			if err := storage.ScanColumnValueToDestination(colVal, dest[i]); err != nil {
				return fmt.Errorf("column %s: %w", alias, err)
			}
		} else {
			return fmt.Errorf("column not found: %s", alias)
		}
	}

	return nil
}

// Close closes the result
func (a *AliasedResult) Close() error {
	// Return all maps to their pools
	if a.aliases != nil {
		// Clear the maps before returning them to the pool
		for k := range a.aliases {
			delete(a.aliases, k)
		}
		aliasMapPool.Put(a.aliases)
		a.aliases = nil
	}

	if a.reverseAliases != nil {
		// Clear the maps before returning them to the pool
		for k := range a.reverseAliases {
			delete(a.reverseAliases, k)
		}
		aliasMapPool.Put(a.reverseAliases)
		a.reverseAliases = nil
	}

	if a.columnIndexMap != nil {
		// Clear the maps before returning them to the pool
		for k := range a.columnIndexMap {
			delete(a.columnIndexMap, k)
		}
		columnMapPool.Put(a.columnIndexMap)
		a.columnIndexMap = nil
	}

	// Close the base result
	return a.baseResult.Close()
}

// RowsAffected returns the number of rows affected
func (a *AliasedResult) RowsAffected() int64 {
	return a.baseResult.RowsAffected()
}

// LastInsertID returns the last insert ID
func (a *AliasedResult) LastInsertID() int64 {
	return a.baseResult.LastInsertID()
}

// Context returns the result's context
func (a *AliasedResult) Context() context.Context {
	return a.baseResult.Context()
}

// WithAliases implements the storage.Result interface
func (a *AliasedResult) WithAliases(aliases map[string]string) storage.Result {
	// Get a combined aliases map from the pool
	combinedAliases := aliasMapPool.Get().(map[string]string)
	// Clear the map if it's not empty
	for k := range combinedAliases {
		delete(combinedAliases, k)
	}

	// First copy existing aliases
	for alias, origCol := range a.aliases {
		combinedAliases[alias] = origCol
	}

	// Then add new aliases (overriding if needed)
	for alias, origCol := range aliases {
		// If origCol is itself an alias, resolve it
		if actualCol, isAlias := a.aliases[origCol]; isAlias {
			combinedAliases[alias] = actualCol
		} else {
			combinedAliases[alias] = origCol
		}
	}

	// Create a new AliasedResult with the combined aliases
	result := NewAliasedResult(a.baseResult, combinedAliases)

	// Return the combined aliases map to the pool since NewAliasedResult makes its own copy
	aliasMapPool.Put(combinedAliases)

	return result
}

// Row implements the storage.Result interface
// It returns the current row directly from the base result
func (a *AliasedResult) Row() storage.Row {
	return a.baseResult.Row()
}
