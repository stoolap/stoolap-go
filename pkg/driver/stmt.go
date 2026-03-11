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

	"github.com/stoolap/stoolap-go/internal/cs"
)

// Stmt implements driver.Stmt with NamedValue support.
type Stmt struct {
	stmt *cs.Stmt
	conn *Conn
}

// Compile-time interface checks
var (
	_ driver.Stmt             = (*Stmt)(nil)
	_ driver.StmtExecContext  = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
)

// Close implements driver.Stmt.
func (s *Stmt) Close() error {
	if s.stmt != nil {
		s.stmt.Finalize()
		s.stmt = nil
	}
	return nil
}

// NumInput implements driver.Stmt.
// Returns -1 to indicate the driver doesn't know the number of placeholders.
func (s *Stmt) NumInput() int {
	return -1
}

// Exec implements driver.Stmt (deprecated, use ExecContext).
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	named := valuesToNamed(args)
	return s.ExecContext(context.Background(), named)
}

// Query implements driver.Stmt (deprecated, use QueryContext).
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	named := valuesToNamed(args)
	return s.QueryContext(context.Background(), named)
}

// ExecContext implements driver.StmtExecContext.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params []cs.Value
	var vb *cs.ValueBuf
	if len(args) > 0 {
		var verr error
		vb, verr = acquireValues(args)
		if verr != nil {
			return nil, verr
		}
		params = vb.Values
	}
	var affected int64
	var err error
	if s.conn.tx != nil {
		affected, err = s.conn.tx.StmtExec(s.stmt, params)
	} else {
		affected, err = s.stmt.Exec(params)
	}
	if vb != nil {
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

// QueryContext implements driver.StmtQueryContext.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params []cs.Value
	var vb *cs.ValueBuf
	if len(args) > 0 {
		var verr error
		vb, verr = acquireValues(args)
		if verr != nil {
			return nil, verr
		}
		params = vb.Values
	}
	var r *cs.Rows
	var err error
	if s.conn.tx != nil {
		r, err = s.conn.tx.StmtQuery(s.stmt, params)
	} else {
		r, err = s.stmt.Query(params)
	}
	if vb != nil {
		vb.Release()
	}
	if err != nil {
		return nil, err
	}
	return newDriverRows(r), nil
}
