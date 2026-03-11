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
	"database/sql/driver"
	"io"

	"github.com/stoolap/stoolap-go/internal/cs"
)

// DriverRows implements driver.Rows.
type DriverRows struct {
	cRows    *cs.Rows
	cols     []string
	colTypes []int32 // cached column types from first row
	colsOK   bool
	colCount int
	closed   bool
	firstRow bool // true after first Next() populates colTypes
}

func newDriverRows(r *cs.Rows) *DriverRows {
	n := r.ColumnCount()
	return &DriverRows{
		cRows:    r,
		colCount: n,
		colTypes: make([]int32, n),
	}
}

// Columns implements driver.Rows.
func (r *DriverRows) Columns() []string {
	if !r.colsOK {
		r.cols = make([]string, r.colCount)
		for i := range r.cols {
			r.cols[i] = r.cRows.ColumnName(i)
		}
		r.colsOK = true
	}
	return r.cols
}

// Close implements driver.Rows.
func (r *DriverRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.cRows.Close()
}

// Next implements driver.Rows.
// dest is pre-allocated by database/sql with length == len(Columns()).
func (r *DriverRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	hasRow, err := r.cRows.Next()
	if err != nil {
		return err
	}
	if !hasRow {
		return io.EOF
	}

	if !r.firstRow {
		// First row: fetch and cache column types (one CGO call per column).
		// Column types are fixed per result set, so subsequent rows skip these calls.
		r.firstRow = true
		for i := 0; i < r.colCount; i++ {
			r.colTypes[i] = r.cRows.ColumnType(i)
			if r.colTypes[i] == cs.TypeNull {
				dest[i] = nil
				continue
			}
			dest[i] = columnValueTyped(r.cRows, i, r.colTypes[i])
		}
		return nil
	}

	for i := 0; i < r.colCount; i++ {
		if r.cRows.ColumnIsNull(i) {
			dest[i] = nil
			continue
		}
		dest[i] = columnValueTyped(r.cRows, i, r.colTypes[i])
	}
	return nil
}

// columnValueTyped reads a column value from the FFI rows using a pre-fetched type code.
// driver.Value is: nil, int64, float64, bool, []byte, string, time.Time
func columnValueTyped(r *cs.Rows, index int, colType int32) driver.Value {
	switch colType {
	case cs.TypeInteger:
		return r.ColumnInt64(index)
	case cs.TypeFloat:
		return r.ColumnDouble(index)
	case cs.TypeText:
		return r.ColumnText(index)
	case cs.TypeBoolean:
		return r.ColumnBool(index)
	case cs.TypeTimestamp:
		return r.ColumnTimestamp(index)
	case cs.TypeJSON:
		return r.ColumnText(index)
	case cs.TypeBlob:
		return r.ColumnBlob(index)
	default:
		return nil
	}
}
