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

	"github.com/stoolap/stoolap/internal/storage"
)

// QualifiedResult wraps a result to add table qualification to column names
type QualifiedResult struct {
	baseResult storage.Result
	tableAlias string
	columns    []string
}

// NewQualifiedResult creates a new qualified result
func NewQualifiedResult(baseResult storage.Result, tableAlias string) *QualifiedResult {
	baseColumns := baseResult.Columns()
	qualifiedColumns := make([]string, len(baseColumns))

	for i, col := range baseColumns {
		qualifiedColumns[i] = tableAlias + "." + col
	}

	return &QualifiedResult{
		baseResult: baseResult,
		tableAlias: tableAlias,
		columns:    qualifiedColumns,
	}
}

// Columns returns the qualified column names
func (r *QualifiedResult) Columns() []string {
	return r.columns
}

// Next advances to the next row
func (r *QualifiedResult) Next() bool {
	return r.baseResult.Next()
}

// Row returns the current row
func (r *QualifiedResult) Row() storage.Row {
	return r.baseResult.Row()
}

// Scan copies column values to destinations
func (r *QualifiedResult) Scan(dest ...interface{}) error {
	return r.baseResult.Scan(dest...)
}

// Close closes the result
func (r *QualifiedResult) Close() error {
	return r.baseResult.Close()
}

// RowsAffected returns rows affected
func (r *QualifiedResult) RowsAffected() int64 {
	return r.baseResult.RowsAffected()
}

// LastInsertID returns last insert ID
func (r *QualifiedResult) LastInsertID() int64 {
	return r.baseResult.LastInsertID()
}

// Context returns the context
func (r *QualifiedResult) Context() context.Context {
	return r.baseResult.Context()
}

// WithAliases returns self as columns are already qualified
func (r *QualifiedResult) WithAliases(aliases map[string]string) storage.Result {
	return r
}
