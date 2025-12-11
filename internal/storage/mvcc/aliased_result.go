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
	"context"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// AliasedResult wraps a storage.Result and manages column aliases
type AliasedResult struct {
	baseResult storage.Result

	// Map from alias to original column name
	aliases map[string]string

	// Map from original column name to alias
	reverseAliases map[string]string

	// Original column names
	originalColumns []string

	// Aliased column names (what will be returned by Columns())
	aliasedColumns []string
}

// NewAliasedResult creates a new aliased result
func NewAliasedResult(base storage.Result, aliases map[string]string) *AliasedResult {
	// Get the original column names
	originalColumns := base.Columns()

	// Create reverse mapping for efficient lookups
	reverseAliases := make(map[string]string)
	for alias, original := range aliases {
		reverseAliases[original] = alias
	}

	// Create the aliased column names
	aliasedColumns := make([]string, len(originalColumns))
	for i, col := range originalColumns {
		if alias, exists := reverseAliases[col]; exists {
			// Use the alias for this column
			aliasedColumns[i] = alias
		} else {
			// No alias, use original name
			aliasedColumns[i] = col
		}
	}

	return &AliasedResult{
		baseResult:      base,
		aliases:         aliases,
		reverseAliases:  reverseAliases,
		originalColumns: originalColumns,
		aliasedColumns:  aliasedColumns,
	}
}

// Columns returns the column names (with aliases)
func (a *AliasedResult) Columns() []string {
	return a.aliasedColumns
}

// Next advances to the next row
func (a *AliasedResult) Next() bool {
	return a.baseResult.Next()
}

// Scan copies values from the current row into the destination variables
func (a *AliasedResult) Scan(dest ...interface{}) error {
	// Simply delegate to the base result
	// The column ordering is preserved, just the names are different
	return a.baseResult.Scan(dest...)
}

// Row returns the current row directly from the base result
func (a *AliasedResult) Row() storage.Row {
	// Simply delegate to the base result
	return a.baseResult.Row()
}

// Close closes the result set
func (a *AliasedResult) Close() error {
	return a.baseResult.Close()
}

// Context returns the result's context
func (a *AliasedResult) Context() context.Context {
	return a.baseResult.Context()
}

// RowsAffected returns the number of rows affected by the operation
func (a *AliasedResult) RowsAffected() int64 {
	return a.baseResult.RowsAffected()
}

// LastInsertID returns the last insert ID
func (a *AliasedResult) LastInsertID() int64 {
	return a.baseResult.LastInsertID()
}

// WithAliases sets column aliases for this result
// Returns self for method chaining
func (a *AliasedResult) WithAliases(aliases map[string]string) storage.Result {
	// Create a new aliased result with the combined aliases
	combinedAliases := make(map[string]string)

	// Copy existing aliases
	for k, v := range a.aliases {
		combinedAliases[k] = v
	}

	// Add new aliases (overriding existing ones if needed)
	for k, v := range aliases {
		combinedAliases[k] = v
	}

	// Return a new AliasedResult with the combined aliases
	return NewAliasedResult(a.baseResult, combinedAliases)
}
