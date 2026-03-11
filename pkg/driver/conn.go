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
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/stoolap/stoolap-go/internal/cs"
)

// Conn implements driver.Conn and optional interfaces for performance.
//
// Context cancellation is checked before each operation is dispatched to the
// engine. However, once an FFI call is in progress it runs to completion;
// mid-call cancellation is not supported because the underlying C API is
// synchronous.
type Conn struct {
	db     *cs.DB
	tx     *cs.Tx // active transaction, nil when not in a transaction
	closed bool
}

// Compile-time interface checks
var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.Validator          = (*Conn)(nil)
)

// Prepare implements driver.Conn.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := c.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt: s, conn: c}, nil
}

// Close implements driver.Conn.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.db.Close()
}

// Begin implements driver.Conn (deprecated, use BeginTx).
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.ReadOnly {
		return nil, errors.New("stoolap: read-only transactions are not supported")
	}

	var tx *cs.Tx
	var err error

	switch isolation := sql.IsolationLevel(opts.Isolation); isolation {
	case sql.LevelDefault, sql.LevelReadCommitted:
		tx, err = c.db.Begin()
	case sql.LevelSnapshot, sql.LevelRepeatableRead:
		tx, err = c.db.BeginWithIsolation(cs.IsolationSnapshot)
	default:
		return nil, errors.New("stoolap: unsupported isolation level")
	}
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &Transaction{tx: tx, conn: c}, nil
}

// ExecContext implements driver.ExecerContext.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var affected int64
	var err error

	if c.tx != nil {
		if len(args) == 0 {
			affected, err = c.tx.Exec(query)
		} else {
			vb, verr := acquireValues(args)
			if verr != nil {
				return nil, verr
			}
			affected, err = c.tx.ExecParams(query, vb.Values)
			vb.Release()
		}
	} else {
		if len(args) == 0 {
			affected, err = c.db.Exec(query)
		} else {
			vb, verr := acquireValues(args)
			if verr != nil {
				return nil, verr
			}
			affected, err = c.db.ExecParams(query, vb.Values)
			vb.Release()
		}
	}
	if err != nil {
		return nil, err
	}
	return execResult(affected), nil
}

// QueryContext implements driver.QueryerContext.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var r *cs.Rows
	var err error

	if c.tx != nil {
		if len(args) == 0 {
			r, err = c.tx.Query(query)
		} else {
			vb, verr := acquireValues(args)
			if verr != nil {
				return nil, verr
			}
			r, err = c.tx.QueryParams(query, vb.Values)
			vb.Release()
		}
	} else {
		if len(args) == 0 {
			r, err = c.db.Query(query)
		} else {
			vb, verr := acquireValues(args)
			if verr != nil {
				return nil, verr
			}
			r, err = c.db.QueryParams(query, vb.Values)
			vb.Release()
		}
	}
	if err != nil {
		return nil, err
	}
	return newDriverRows(r), nil
}

// Ping implements driver.Pinger.
func (c *Conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	r, err := c.db.Query("SELECT 1")
	if err != nil {
		return err
	}
	return r.Close()
}

// ResetSession implements driver.SessionResetter.
func (c *Conn) ResetSession(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	c.tx = nil
	return nil
}

// IsValid implements driver.Validator.
func (c *Conn) IsValid() bool {
	return !c.closed
}
