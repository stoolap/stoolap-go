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
// package executor provides SQL execution functionality
package sql

import (
	"context"
	"database/sql/driver"

	"github.com/stoolap/stoolap/internal/sql/executor"
	"github.com/stoolap/stoolap/internal/storage"
)

// NewExecutor creates a new SQL executor
func NewExecutor(engine storage.Engine) *Executor {
	// Create the real executor
	sqlExecutor := executor.NewExecutor(engine)

	return &Executor{
		sqlExecutor: sqlExecutor,
	}
}

// Executor executes SQL statements
type Executor struct {
	sqlExecutor *executor.Executor
}

// Execute executes a SQL statement
func (e *Executor) Execute(ctx context.Context, tx storage.Transaction, query string) (storage.Result, error) {
	return e.sqlExecutor.ExecuteWithParams(ctx, tx, query, nil)
}

// ExecuteWithParams executes a SQL statement with parameters
func (e *Executor) ExecuteWithParams(ctx context.Context, tx storage.Transaction, query string, params []driver.NamedValue) (storage.Result, error) {
	return e.sqlExecutor.ExecuteWithParams(ctx, tx, query, params)
}

// EnableVectorizedMode enables vectorized execution for appropriate query types
func (e *Executor) EnableVectorizedMode() {
	e.sqlExecutor.EnableVectorizedMode()
}

// DisableVectorizedMode disables vectorized execution
func (e *Executor) DisableVectorizedMode() {
	e.sqlExecutor.DisableVectorizedMode()
}

// IsVectorizedModeEnabled returns whether vectorized execution is enabled
func (e *Executor) IsVectorizedModeEnabled() bool {
	return e.sqlExecutor.IsVectorizedModeEnabled()
}
