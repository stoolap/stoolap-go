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

package wasm

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var (
	errRowsClosed      = errors.New("stoolap-wasm: rows are closed")
	errNoRow           = errors.New("stoolap-wasm: Scan called without a current row")
	errTruncated       = errors.New("stoolap-wasm: truncated packed result")
	errNotTimestamp    = errors.New("stoolap-wasm: value is not a timestamp")
	errColumnCount     = errors.New("stoolap-wasm: Scan destination count does not match column count")
	errUnsupportedDest = errors.New("stoolap-wasm: unsupported Scan destination type")
)

// Rows asked for per fetch. The first chunk is small so a point query costs a
// single crossing, then the size doubles up to maxChunkRows for scans.
const (
	firstChunkRows = 32
	maxChunkRows   = 256
	inlineColCap   = 8
)

// Rows is an iterator over query results. Rows come from the engine in packed
// chunks through stoolap_rows_fetch_chunk, so a scan crosses the WASM boundary
// once per chunk instead of once per row and column. Each chunk is copied out
// of WASM memory into a Go buffer the Rows reuses, and values are decoded from
// it on access.
type Rows struct {
	engine       *Engine
	ptr          uint32
	ctx          context.Context
	chunk        []byte // Go copy of the current packed chunk, reused across fetches
	rowOff       int    // offset of the next unread row in chunk
	rowIdx       int    // rows of the chunk consumed by Next
	chunkRows    int    // rows in the chunk
	chunkMax     int32  // rows to ask for on the next fetch
	colCount     int
	cols         []string
	colOff       []int32 // offset of each column's type tag in the current row
	colOffInline [inlineColCap]int32
	textBuf      []byte // reusable buffer for batching text copies in Scan
	textInline   [256]byte
	colNameBuf   []byte // reusable scratch for gathering column names in Columns
	err          error
	closed       bool
	done         bool // the engine cursor is exhausted
	hasRow       bool // Next returned true for the current row
}

func newRows(e *Engine, ctx context.Context, ptr uint32) *Rows {
	r := &Rows{engine: e, ctx: ctx, ptr: ptr, chunkMax: firstChunkRows}
	r.textBuf = r.textInline[:0]
	return r
}

func (r *Rows) fail(err error) {
	r.err = err
	r.done = true
	r.chunk = r.chunk[:0]
	r.rowIdx, r.chunkRows = 0, 0
	r.hasRow = false
}

// fetchChunk pulls the next chunk from the engine and positions the reader at
// its first row. It returns false when the chunk holds no rows.
func (r *Rows) fetchChunk() bool {
	e := r.engine
	e.mu.Lock()
	rc := e.call5(r.ctx, e.fnRowsFetchChunk, uint64(r.ptr), uint64(uint32(r.chunkMax)),
		uint64(e.outSlotA), uint64(e.outSlotB), uint64(e.outSlotC))
	if int32(rc) != stoolapOK {
		err := e.errFrom(r.ctx, e.fnRowsErrmsg, r.ptr)
		e.mu.Unlock()
		r.fail(err)
		return false
	}
	bufPtr := e.readU32(e.outSlotA)
	bufLen := e.readI64(e.outSlotB)
	n := int32(e.readU32(e.outSlotC))
	view, ok := e.mem.Read(bufPtr, uint32(bufLen))
	if ok && bufPtr != 0 {
		r.chunk = append(r.chunk[:0], view...)
	}
	e.mu.Unlock()
	if !ok || bufPtr == 0 || bufLen == 0 {
		r.fail(errTruncated)
		return false
	}
	if n < r.chunkMax {
		r.done = true
	} else if r.chunkMax < maxChunkRows {
		r.chunkMax *= 2
	}
	colCount, _, off, ok := packedHeader(r.chunk)
	if !ok {
		r.fail(errTruncated)
		return false
	}
	r.colCount = colCount
	if colCount <= inlineColCap {
		r.colOff = r.colOffInline[:colCount]
	} else if cap(r.colOff) >= colCount {
		r.colOff = r.colOff[:colCount]
	} else {
		r.colOff = make([]int32, colCount)
	}
	r.rowOff = off
	r.rowIdx = 0
	r.chunkRows = int(n)
	return n > 0
}

// Next advances to the next row.
func (r *Rows) Next() bool {
	if r.closed {
		return false
	}
	if r.rowIdx >= r.chunkRows {
		if r.done || !r.fetchChunk() {
			r.hasRow = false
			return false
		}
	}
	buf := r.chunk
	off := r.rowOff
	for i := range r.colOff {
		r.colOff[i] = int32(off)
		n := packedValueSize(buf, off)
		if n < 0 {
			r.fail(errTruncated)
			return false
		}
		off += n
	}
	r.rowOff = off
	r.rowIdx++
	r.hasRow = true
	return true
}

// Err returns the error that ended iteration, if any.
func (r *Rows) Err() error {
	return r.err
}

// Columns returns the column names as a Go-owned slice, safe to hold after Close().
// First call per query: 2 allocations (one string for combined name data, one []string).
// Subsequent calls on the same Rows return the cached slice with no allocation.
func (r *Rows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}
	if r.closed {
		return nil
	}
	if len(r.chunk) == 0 {
		// Every chunk carries the column header, including an empty one.
		r.fetchChunk()
		if len(r.chunk) == 0 {
			return nil
		}
	}

	// Inline scratch for per-column end offsets; fits the vast majority of tables.
	var inlineEnds [32]int
	var ends []int
	if r.colCount <= len(inlineEnds) {
		ends = inlineEnds[:r.colCount]
	} else {
		ends = make([]int, r.colCount)
	}

	// Pass 1: gather all column-name bytes into a reusable scratch buffer,
	// recording where each name ends.
	buf := r.chunk
	off := 4
	names := r.colNameBuf[:0]
	for i := range r.colCount {
		n := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		names = append(names, buf[off:off+n]...)
		off += n
		ends[i] = len(names)
	}
	r.colNameBuf = names

	// One Go-owned allocation holding all the bytes.
	all := string(names)
	cols := make([]string, r.colCount)
	prev := 0
	for i := range r.colCount {
		cols[i] = all[prev:ends[i]]
		prev = ends[i]
	}
	r.cols = cols
	return cols
}

// value returns the payload offset and type tag of column i in the current
// row. Without a current row, or out of range, it reports NULL.
func (r *Rows) value(i int) (int, byte) {
	if !r.hasRow || uint(i) >= uint(r.colCount) {
		return 0, typeNull
	}
	off := int(r.colOff[i])
	return off + 1, r.chunk[off]
}

// packedText returns a view of the text payload at off. The view aliases the
// chunk and must be copied before the next fetch.
func packedText(buf []byte, off int) string {
	n := int(binary.LittleEndian.Uint32(buf[off:]))
	if n == 0 {
		return ""
	}
	return unsafe.String(&buf[off+4], n)
}

// IsNull reports whether the current row has a NULL value at column i.
func (r *Rows) IsNull(i int) bool {
	_, tag := r.value(i)
	return tag == typeNull
}

// Int64 returns the current row's value at column i as an integer, converting
// FLOAT, BOOLEAN, TIMESTAMP (nanoseconds) and numeric TEXT the way the engine does.
func (r *Rows) Int64(i int) int64 {
	off, tag := r.value(i)
	switch tag {
	case typeInteger, typeTimestamp:
		return int64(binary.LittleEndian.Uint64(r.chunk[off:]))
	case typeFloat:
		return int64(math.Float64frombits(binary.LittleEndian.Uint64(r.chunk[off:])))
	case typeBoolean:
		if r.chunk[off] != 0 {
			return 1
		}
	case typeText:
		s := packedText(r.chunk, off)
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f)
		}
	}
	return 0
}

// Float64 returns the current row's value at column i as a float, converting
// INTEGER, BOOLEAN and numeric TEXT the way the engine does.
func (r *Rows) Float64(i int) float64 {
	off, tag := r.value(i)
	switch tag {
	case typeFloat:
		return math.Float64frombits(binary.LittleEndian.Uint64(r.chunk[off:]))
	case typeInteger:
		return float64(int64(binary.LittleEndian.Uint64(r.chunk[off:])))
	case typeBoolean:
		if r.chunk[off] != 0 {
			return 1
		}
	case typeText:
		if f, err := strconv.ParseFloat(packedText(r.chunk, off), 64); err == nil {
			return f
		}
	}
	return 0
}

// Bool returns the current row's value at column i as a boolean, converting
// INTEGER, FLOAT and TEXT ("true", "t", "yes", "y", "1" and their opposites)
// the way the engine does.
func (r *Rows) Bool(i int) bool {
	off, tag := r.value(i)
	switch tag {
	case typeBoolean:
		return r.chunk[off] != 0
	case typeInteger:
		return binary.LittleEndian.Uint64(r.chunk[off:]) != 0
	case typeFloat:
		return math.Float64frombits(binary.LittleEndian.Uint64(r.chunk[off:])) != 0
	case typeText:
		s := packedText(r.chunk, off)
		switch strings.ToLower(s) {
		case "true", "t", "yes", "y", "1":
			return true
		case "false", "f", "no", "n", "0", "":
			return false
		}
		f, err := strconv.ParseFloat(s, 64)
		return err == nil && f != 0
	}
	return false
}

// Timestamp returns the current row's TIMESTAMP value at column i. An INTEGER
// is read as nanoseconds since the Unix epoch and TEXT is parsed in the
// common forms the engine accepts (see parseTimestamp). Anything else, and
// text in no known form, gives the zero time; Scan reports it as an error.
func (r *Rows) Timestamp(i int) time.Time {
	t, _ := r.timestamp(i)
	return t
}

func (r *Rows) timestamp(i int) (time.Time, bool) {
	off, tag := r.value(i)
	switch tag {
	case typeTimestamp, typeInteger:
		nanos := int64(binary.LittleEndian.Uint64(r.chunk[off:]))
		return time.Unix(nanos/1e9, nanos%1e9).UTC(), true
	case typeText:
		return parseTimestamp(packedText(r.chunk, off))
	}
	return time.Time{}, false
}

// timestampLayouts covers the forms the engine's parse_timestamp accepts:
// RFC 3339 with Z or an offset in any spelling, ISO and SQL style with an
// optional fraction, the slash form with a time, and a bare date with
// dashes or slashes, year first or last; a slash date both the US and the
// European form can read is the US form, month first, as in the engine.
// Layouts without a zone are read as UTC, as the engine does. The engine
// also takes single digit fields and second 60; this parser does not.
var timestampLayouts = [...]string{
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.999999999-0700",
	"2006-01-02T15:04:05.999999999 Z07:00",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"01/02/2006",
	"02/01/2006",
}

// parseTimestamp parses s with timestampLayouts. A time without a date is
// that time on 1970-01-01, as in the engine.
func parseTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	for _, layout := range [...]string{"15:04:05.999999999", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return time.Date(1970, 1, 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), true
		}
	}
	return time.Time{}, false
}

// formatText renders a non-text column of the current row as the engine's
// text form, so Scan into a string matches CAST(x AS TEXT).
func (r *Rows) formatText(i int) string {
	off, tag := r.value(i)
	switch tag {
	case typeText, typeJSON:
		return strings.Clone(packedText(r.chunk, off))
	case typeInteger:
		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(r.chunk[off:])), 10)
	case typeFloat:
		return formatFloat(math.Float64frombits(binary.LittleEndian.Uint64(r.chunk[off:])))
	case typeBoolean:
		return strconv.FormatBool(r.chunk[off] != 0)
	case typeTimestamp:
		return formatTimestamp(r.Timestamp(i))
	case typeBlob:
		return formatVector(packedText(r.chunk, off))
	}
	return ""
}

// formatFloat mirrors the engine's format_float: scientific notation outside
// [1e-4, 1e15) with a bare exponent, no fraction for whole numbers, shortest
// round trip digits otherwise.
func formatFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	}
	abs := math.Abs(v)
	if abs != 0 && (abs < 1e-4 || abs >= 1e15) {
		s := strconv.FormatFloat(v, 'e', -1, 64)
		e := strings.IndexByte(s, 'e')
		mant, exp := s[:e], s[e+1:]
		neg := exp[0] == '-'
		exp = strings.TrimLeft(exp[1:], "0")
		if neg {
			return mant + "e-" + exp
		}
		return mant + "e" + exp
	}
	if v == math.Trunc(v) {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// formatTimestamp mirrors chrono's to_rfc3339 for a UTC time: the fraction is
// printed with 3, 6 or 9 digits as needed and the zone as +00:00.
func formatTimestamp(t time.Time) string {
	layout := "2006-01-02T15:04:05"
	switch ns := t.Nanosecond(); {
	case ns == 0:
	case ns%1e6 == 0:
		layout += ".000"
	case ns%1e3 == 0:
		layout += ".000000"
	default:
		layout += ".000000000"
	}
	return t.UTC().Format(layout) + "+00:00"
}

// formatVector mirrors the engine's format_vector_bytes for packed f32 data:
// "[1.0, 2.5, -3.0]", whole numbers with one decimal, others in shortest form.
func formatVector(data string) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := 0; i+4 <= len(data); i += 4 {
		if i > 0 {
			sb.WriteString(", ")
		}
		f := math.Float32frombits(uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24)
		switch f64 := float64(f); {
		case math.IsInf(f64, 1):
			sb.WriteString("inf")
		case math.IsInf(f64, -1):
			sb.WriteString("-inf")
		case f64 == math.Trunc(f64) && !math.IsNaN(f64):
			sb.WriteString(strconv.FormatFloat(f64, 'f', 1, 32))
		default:
			sb.WriteString(strconv.FormatFloat(f64, 'f', -1, 32))
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

// Scan reads the current row into dest.
// Text columns are batched into a single string allocation per row; textBuf
// is reused across rows.
func (r *Rows) Scan(dest ...any) error {
	if r.closed {
		return errRowsClosed
	}
	if !r.hasRow {
		return errNoRow
	}
	if len(dest) != r.colCount {
		return errColumnCount
	}
	buf := r.chunk
	r.textBuf = r.textBuf[:0]

	const maxTextRefs = 16
	type tref struct {
		destIdx int
		start   int
		len     int
	}
	var refs [maxTextRefs]tref
	nRefs := 0

	// appendText queues the text payload at off for the batched copy. It
	// returns false when the batch is full and the caller must copy itself.
	appendText := func(off, destIdx int) bool {
		if nRefs >= maxTextRefs {
			return false
		}
		n := int(binary.LittleEndian.Uint32(buf[off:]))
		start := len(r.textBuf)
		r.textBuf = append(r.textBuf, buf[off+4:off+4+n]...)
		refs[nRefs] = tref{destIdx: destIdx, start: start, len: n}
		nRefs++
		return true
	}

	for i, d := range dest {
		off := int(r.colOff[i]) + 1
		tag := buf[off-1]
		isNull := tag == typeNull

		switch p := d.(type) {
		case *int64:
			if isNull {
				*p = 0
			} else {
				*p = r.Int64(i)
			}
		case *float64:
			if isNull {
				*p = 0
			} else {
				*p = r.Float64(i)
			}
		case *string:
			if isNull {
				*p = ""
			} else if tag != typeText && tag != typeJSON {
				*p = r.formatText(i)
			} else if !appendText(off, i) {
				*p = strings.Clone(packedText(buf, off))
			}
		case *bool:
			if isNull {
				*p = false
			} else {
				*p = r.Bool(i)
			}
		case *time.Time:
			if isNull {
				*p = time.Time{}
			} else if t, ok := r.timestamp(i); ok {
				*p = t
			} else {
				return errNotTimestamp
			}
		case *[]byte:
			if isNull {
				*p = nil
			} else {
				*p = packedBlob(buf, off)
			}
		case *sql.NullString:
			if isNull {
				p.String = ""
				p.Valid = false
			} else if tag != typeText && tag != typeJSON {
				p.String = r.formatText(i)
				p.Valid = true
			} else if !appendText(off, i) {
				p.String = strings.Clone(packedText(buf, off))
				p.Valid = true
			}
		case *sql.NullInt64:
			if isNull {
				p.Int64 = 0
				p.Valid = false
			} else {
				p.Int64 = r.Int64(i)
				p.Valid = true
			}
		case *sql.NullFloat64:
			if isNull {
				p.Float64 = 0
				p.Valid = false
			} else {
				p.Float64 = r.Float64(i)
				p.Valid = true
			}
		case *sql.NullBool:
			if isNull {
				p.Bool = false
				p.Valid = false
			} else {
				p.Bool = r.Bool(i)
				p.Valid = true
			}
		case *sql.NullTime:
			if isNull {
				p.Time = time.Time{}
				p.Valid = false
			} else if t, ok := r.timestamp(i); ok {
				p.Time = t
				p.Valid = true
			} else {
				return errNotTimestamp
			}
		case *any:
			switch tag {
			case typeInteger:
				*p = int64(binary.LittleEndian.Uint64(buf[off:]))
			case typeFloat:
				*p = math.Float64frombits(binary.LittleEndian.Uint64(buf[off:]))
			case typeText, typeJSON:
				if !appendText(off, i) {
					*p = strings.Clone(packedText(buf, off))
				}
			case typeBoolean:
				*p = buf[off] != 0
			case typeTimestamp:
				*p = r.Timestamp(i)
			case typeBlob:
				*p = packedBlob(buf, off)
			default:
				*p = nil
			}
		default:
			return errUnsupportedDest
		}
	}

	// Batch-create all text strings from one allocation.
	// string(r.textBuf) copies once; substrings share the backing array.
	if nRefs > 0 {
		all := string(r.textBuf)
		for j := range nRefs {
			ref := refs[j]
			s := all[ref.start : ref.start+ref.len]
			switch p := dest[ref.destIdx].(type) {
			case *string:
				*p = s
			case *sql.NullString:
				p.String = s
				p.Valid = true
			case *any:
				*p = s
			}
		}
	}

	return nil
}

// FetchAll fetches all remaining rows in a single WASM call and returns them
// as a slice of []any rows. Dramatically faster for large result sets.
// After calling FetchAll, the Rows handle is consumed; call Close() afterward.
func (r *Rows) FetchAll() ([][]any, error) {
	if r.closed {
		return nil, errRowsClosed
	}
	if r.err != nil {
		return nil, r.err
	}

	// Rows already pulled into the current chunk but not consumed by Next
	// come first. The chunk buffer is reused by later fetches, so copy before
	// decoding.
	var pending [][]any
	if n := r.chunkRows - r.rowIdx; n > 0 {
		rest := make([]byte, len(r.chunk)-r.rowOff)
		copy(rest, r.chunk[r.rowOff:])
		var err error
		pending, err = parsePackedRows(rest, 0, r.colCount, n)
		if err != nil {
			return nil, err
		}
		r.rowIdx = r.chunkRows
	}
	r.hasRow = false
	if r.done {
		return pending, nil
	}
	r.done = true

	e := r.engine
	e.mu.Lock()
	rc := e.call3(r.ctx, e.fnRowsFetchAll, uint64(r.ptr), uint64(e.outSlotA), uint64(e.outSlotB))
	if int32(rc) != stoolapOK {
		err := e.errFrom(r.ctx, e.fnRowsErrmsg, r.ptr)
		e.mu.Unlock()
		r.fail(err)
		return nil, err
	}
	bufPtr := e.readU32(e.outSlotA)
	bufLen := e.readI64(e.outSlotB)
	if bufPtr == 0 || bufLen == 0 {
		e.mu.Unlock()
		return pending, nil
	}
	buf, ok := e.copyOut(bufPtr, bufLen)
	e.call2(r.ctx, e.fnBufferFree, uint64(bufPtr), uint64(bufLen))
	e.mu.Unlock()
	if !ok {
		return nil, errTruncated
	}

	colCount, rowCount, off, ok := packedHeader(buf)
	if !ok {
		return nil, errTruncated
	}
	rows, err := parsePackedRows(buf, off, colCount, rowCount)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		rows = append(pending, rows...)
	}
	return rows, nil
}

// Close closes the result set.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	e := r.engine
	e.mu.Lock()
	e.callVoid1(r.ctx, e.fnRowsClose, uint64(r.ptr))
	e.mu.Unlock()
	r.ptr = 0
	r.chunk = nil
	r.hasRow = false
	return nil
}

// ─── Packed results ─────────────────────────────────────────────────────────

// ResultSet is a complete result decoded from one WASM call: the column
// names and every row as Go values.
type ResultSet struct {
	Columns []string
	Rows    [][]any
}

// decodePacked copies a packed result out of WASM memory and decodes it. The
// engine reuses its buffer on the next call of the owning handle, so the
// caller holds e.mu until this returns.
func (e *Engine) decodePacked(bufPtr uint32, bufLen int64) ([]string, [][]any, error) {
	if bufPtr == 0 || bufLen == 0 {
		return nil, nil, nil
	}
	buf, ok := e.copyOut(bufPtr, bufLen)
	if !ok {
		return nil, nil, errTruncated
	}
	colCount, rowCount, off, ok := packedHeader(buf)
	if !ok {
		return nil, nil, errTruncated
	}
	cols := make([]string, colCount)
	nameOff := 4
	for i := range colCount {
		n := int(binary.LittleEndian.Uint16(buf[nameOff:]))
		nameOff += 2
		if n > 0 {
			cols[i] = unsafe.String(&buf[nameOff], n)
		}
		nameOff += n
	}
	rows, err := parsePackedRows(buf, off, colCount, rowCount)
	return cols, rows, err
}

// packedHeader reads the column names header of the packed result format and
// returns the column count, the row count and the offset of the first row.
func packedHeader(buf []byte) (colCount, rowCount, off int, ok bool) {
	if len(buf) < 4 {
		return 0, 0, 0, false
	}
	colCount = int(binary.LittleEndian.Uint32(buf))
	off = 4
	for range colCount {
		if off+2 > len(buf) {
			return 0, 0, 0, false
		}
		off += 2 + int(binary.LittleEndian.Uint16(buf[off:]))
	}
	if off+4 > len(buf) {
		return 0, 0, 0, false
	}
	rowCount = int(binary.LittleEndian.Uint32(buf[off:]))
	return colCount, rowCount, off + 4, true
}

// packedValueSize returns the encoded size of the value at off, including its
// type tag, or -1 when the buffer ends early or the tag is unknown.
func packedValueSize(buf []byte, off int) int {
	if off >= len(buf) {
		return -1
	}
	var n int
	switch buf[off] {
	case typeNull:
		n = 1
	case typeInteger, typeFloat, typeTimestamp:
		n = 9
	case typeBoolean:
		n = 2
	case typeText, typeJSON, typeBlob:
		if off+5 > len(buf) {
			return -1
		}
		n = 5 + int(binary.LittleEndian.Uint32(buf[off+1:]))
	default:
		return -1
	}
	if off+n > len(buf) {
		return -1
	}
	return n
}

// packedBlob copies the blob payload at off.
func packedBlob(buf []byte, off int) []byte {
	n := int(binary.LittleEndian.Uint32(buf[off:]))
	blob := make([]byte, n)
	copy(blob, buf[off+4:off+4+n])
	return blob
}

// parsePackedRows decodes rowCount rows starting at off. Strings alias buf, so
// buf must be Go-owned and is retained by the result.
func parsePackedRows(buf []byte, off, colCount, rowCount int) ([][]any, error) {
	flat := make([]any, rowCount*colCount)
	rows := make([][]any, rowCount)
	for ri := range rowCount {
		row := flat[ri*colCount : (ri+1)*colCount : (ri+1)*colCount]
		rows[ri] = row
		for c := range colCount {
			n := packedValueSize(buf, off)
			if n < 0 {
				return nil, errTruncated
			}
			switch buf[off] {
			case typeNull:
				row[c] = nil
			case typeInteger:
				row[c] = int64(binary.LittleEndian.Uint64(buf[off+1:]))
			case typeFloat:
				row[c] = math.Float64frombits(binary.LittleEndian.Uint64(buf[off+1:]))
			case typeText, typeJSON:
				row[c] = packedText(buf, off+1)
			case typeBoolean:
				row[c] = buf[off+1] != 0
			case typeTimestamp:
				nanos := int64(binary.LittleEndian.Uint64(buf[off+1:]))
				row[c] = time.Unix(nanos/1e9, nanos%1e9).UTC()
			case typeBlob:
				row[c] = packedBlob(buf, off+1)
			}
			off += n
		}
	}
	return rows, nil
}
