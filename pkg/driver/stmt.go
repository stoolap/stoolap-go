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
package driver

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Stmt represents a prepared statement with concurrency optimizations
type Stmt struct {
	conn     *Conn
	query    string
	ctx      context.Context // Context for this statement
	closed   bool            // Track if the statement has been closed
	mu       *sync.Mutex     // Statement-specific mutex for thread safety
	useCount int64           // Number of times this statement has been used
	lastUsed time.Time       // Last time this statement was used
}

// Close closes the statement
func (s *Stmt) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters
func (s *Stmt) NumInput() int {
	// Count the number of question mark placeholders in the query
	return strings.Count(s.query, "?")
}

// Exec executes a query that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), convertValues(args))
}

// ExecContext executes a query that doesn't return rows, with optimized batch performance
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Streamlined fast path for transaction batches (most common in benchmarks)
	// In benchmarks, we've found that the simpler execution path is much faster
	// when executing multiple statements in a transaction
	if s.conn.tx != nil {
		// Skip lock acquisition if we're in a transaction
		if s.closed || s.conn.closed {
			return nil, driver.ErrBadConn
		}

		// Get transaction for fast path
		var tx storage.Transaction
		if customTx, ok := s.conn.tx.(*Tx); ok {
			tx = customTx.GetStorageTransaction()
		}

		// Optimized direct execution path for transaction operations
		query, params, err := s.prepareQueryFast(args)
		if err != nil {
			return nil, err
		}

		// Execute without extra allocations
		var storageResult storage.Result
		if len(params) > 0 {
			storageResult, err = s.conn.executor.ExecuteWithParams(ctx, tx, query, params)
		} else {
			storageResult, err = s.conn.executor.Execute(ctx, tx, query)
		}

		if err != nil {
			return nil, err
		}

		return &driverResult{
			rowsAffected: storageResult.RowsAffected(),
			lastInsertID: storageResult.LastInsertID(),
		}, nil
	}

	// For non-transaction statements, use the original thread-safe approach
	// Only lock if we have a mutex (older statements might not have one)
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if s.closed {
		return nil, driver.ErrBadConn
	}

	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	// Quick context check
	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Prepare query and params
	query, params, err := s.prepareQueryFast(args)
	if err != nil {
		return nil, err
	}

	// Get the executor and transaction
	executor := s.conn.executor
	var tx storage.Transaction

	s.conn.mu.Lock()
	if s.conn.tx != nil {
		if customTx, ok := s.conn.tx.(*Tx); ok {
			tx = customTx.GetStorageTransaction()
		}
	}
	s.conn.mu.Unlock()

	// Execute query
	var storageResult storage.Result
	if len(params) > 0 {
		storageResult, err = executor.ExecuteWithParams(ctx, tx, query, params)
	} else {
		storageResult, err = executor.Execute(ctx, tx, query)
	}

	if err != nil {
		return nil, err
	}

	return &driverResult{
		rowsAffected: storageResult.RowsAffected(),
		lastInsertID: storageResult.LastInsertID(),
	}, nil
}

// Query executes a query that returns rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), convertValues(args))
}

// QueryContext executes a query that returns rows, with enhanced concurrency safety
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Use statement mutex for thread safety
	if s.mu != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if s.closed {
		return nil, driver.ErrBadConn
	}

	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Update usage statistics
	s.useCount++
	s.lastUsed = time.Now()

	// Use optimized query preparation
	query, params, err := s.prepareQueryFast(args)
	if err != nil {
		return nil, err
	}

	// Use the connection's cached executor
	executor := s.conn.executor

	// Transaction handling with proper connection locking
	var tx storage.Transaction
	s.conn.mu.Lock()
	if s.conn.tx != nil {
		if customTx, ok := s.conn.tx.(*Tx); ok {
			tx = customTx.GetStorageTransaction()
		}
	}
	s.conn.mu.Unlock()

	// Execute the query - the executor is thread-safe
	var storageResult storage.Result

	// Wrap execution in recovery to prevent panics
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in query execution: %v", r)
		}
	}()

	if len(params) > 0 {
		storageResult, err = executor.ExecuteWithParams(ctx, tx, query, params)
	} else {
		storageResult, err = executor.Execute(ctx, tx, query)
	}

	if err != nil {
		return nil, err
	}

	// Quick validation of result set
	columns := storageResult.Columns()
	if len(columns) == 0 {
		return nil, errors.New("query executed, but no columns returned")
	}

	// Preallocate buffers for rows handling
	// This optimizes the rows scanning process
	return &Rows{
		storageResult: storageResult,
		ctx:           ctx,
		columnNames:   columns,
		scanBuffer:    make([]interface{}, len(columns), len(columns)+5),
		scanPtrBuffer: make([]interface{}, len(columns), len(columns)+5),
		valueBuffer:   make([]driver.Value, len(columns)),
	}, nil
}

// prepareQueryFast is an optimized version of prepareQuery with parameter reuse
func (s *Stmt) prepareQueryFast(args []driver.NamedValue) (string, []driver.NamedValue, error) {
	// Quick path: If no parameters, return the query as is
	if len(args) == 0 {
		return s.query, nil, nil
	}

	// Fast path: Direct parameter binding for DML statements (most common)
	// Simplified check to avoid string operations which are expensive
	queryType := queryTypeFromQuery(s.query)
	if queryType == queryTypeDML {
		// For hot path, just pass args directly without copying
		// This avoids unnecessary allocation and copy overhead
		return s.query, args, nil
	}

	// Slow path: For DDL and other commands that don't support parameters, use string interpolation
	interpolatedQuery, err := interpolateParams(s.query, args)
	if err != nil {
		return "", nil, err
	}
	return interpolatedQuery, nil, nil
}

// queryType identifies the type of SQL statement
type queryType int

const (
	queryTypeUnknown queryType = iota
	queryTypeDML               // Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)
	queryTypeDDL               // Data Definition Language (CREATE, DROP, ALTER)
)

// queryTypeFromQuery detects the type of SQL query from its first few bytes
// This is a highly optimized function that avoids string operations
func queryTypeFromQuery(q string) queryType {
	if len(q) < 6 {
		return queryTypeUnknown
	}

	// Fast check for common DML prefixes using byte comparisons
	c1, c2 := q[0], q[1]

	//FIXME : This is a very naive check, we should use a more robust parser
	// Check for 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
	if (c1 == 'S' || c1 == 's') || // SELECT
		(c1 == 'I' || c1 == 'i') || // INSERT
		(c1 == 'U' || c1 == 'u') || // UPDATE
		(c1 == 'D' || c1 == 'd' && c2 == 'E' || c2 == 'e') { // DELETE
		return queryTypeDML
	}

	//FIXME : This is a very naive check, we should use a more robust parser
	// Check for 'CREATE', 'DROP', 'ALTER'
	if (c1 == 'C' || c1 == 'c') || // CREATE
		(c1 == 'D' || c1 == 'd' && c2 == 'R' || c2 == 'r') || // DROP
		(c1 == 'A' || c1 == 'a') { // ALTER
		return queryTypeDDL
	}

	return queryTypeUnknown
}

// interpolateParams replaces placeholders with values for queries that don't support direct binding
func interpolateParams(query string, args []driver.NamedValue) (string, error) {
	// For now, we only support ? placeholders
	for _, arg := range args {
		// Find the first ? in the query
		pos := strings.Index(query, "?")
		if pos == -1 {
			return "", errors.New("placeholder count doesn't match argument count")
		}

		// Convert the value to a string
		value := arg.Value
		var valueStr string
		switch v := value.(type) {
		case nil:
			valueStr = "NULL"
		case bool:
			if v {
				valueStr = "TRUE"
			} else {
				valueStr = "FALSE"
			}
		case int64:
			valueStr = strconv.FormatInt(v, 10)
		case float64:
			valueStr = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			// Escape single quotes in strings
			valueStr = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case []byte:
			// Escape single quotes in strings
			valueStr = "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
		default:
			// Try to get a string representation
			valueStr = fmt.Sprintf("%v", v)
			// If it doesn't look like a number, quote it
			if _, err := strconv.ParseFloat(valueStr, 64); err != nil {
				valueStr = "'" + strings.ReplaceAll(valueStr, "'", "''") + "'"
			}
		}

		// Replace the placeholder with the value
		query = query[:pos] + valueStr + query[pos+1:]
	}

	// Make sure there are no more placeholders
	if strings.Contains(query, "?") {
		return "", errors.New("placeholder count doesn't match argument count")
	}

	return query, nil
}

// CheckNamedValue implements the NamedValueChecker interface
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	// Check if the named parameter is empty
	if nv.Name != "" {
		// We don't currently support named parameters with names
		return fmt.Errorf("named parameters not supported, only positional parameters")
	}

	// Validate the value based on its type
	switch v := nv.Value.(type) {
	case nil, int64, float64, bool, []byte, string, time.Time:
		// These are the standard driver.Value types, no conversion needed
		return nil
	case int, int8, int16, int32:
		// Convert integer types to int64
		nv.Value = reflect.ValueOf(v).Int()
		return nil
	case uint, uint8, uint16, uint32, uint64:
		// Convert unsigned integer types to int64
		nv.Value = int64(reflect.ValueOf(v).Uint())
		return nil
	case float32:
		// Convert float32 to float64
		nv.Value = float64(v)
		return nil
	default:
		// For complex types, try to convert to JSON (as string)
		var valStr string
		if data, err := json.Marshal(v); err == nil {
			valStr = string(data)
			nv.Value = valStr
			return nil
		}

		// If all else fails, use string representation
		nv.Value = fmt.Sprintf("%v", v)
		return nil
	}
}

// driverResult implements driver.Result
type driverResult struct {
	rowsAffected int64
	lastInsertID int64
}

// LastInsertId returns the database's auto-generated ID
func (r *driverResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected
func (r *driverResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
