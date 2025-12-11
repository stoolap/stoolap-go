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
package binser

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// EngineMetadata represents serializable metadata for the database engine
type EngineMetadata struct {
	Tables    []string  // Names of tables in the database
	UpdatedAt time.Time // Last update time
}

// MarshalBinary marshals engine metadata to binary
func (e *EngineMetadata) MarshalBinary(w *Writer) {
	// Define new type for engine metadata (use 19 since 18 is the next available after TypeStruct 17)
	const TypeEngineMeta byte = 19

	// Write the type marker directly to the buffer
	w.grow(1)
	w.buf = append(w.buf, TypeEngineMeta)

	// Write table list
	w.WriteArrayHeader(len(e.Tables))
	for _, table := range e.Tables {
		w.WriteString(table)
	}

	// Write timestamp
	w.WriteTime(e.UpdatedAt)
}

// UnmarshalBinary unmarshals engine metadata from binary
func (e *EngineMetadata) UnmarshalBinary(r *Reader) error {
	// Define type constant
	const TypeEngineMeta byte = 19

	// Read type marker
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeEngineMeta {
		return fmt.Errorf("expected TypeEngineMeta (19), got %d", tp)
	}

	// Read table array
	length, err := r.ReadArrayHeader()
	if err != nil {
		return fmt.Errorf("error reading table array: %w", err)
	}

	e.Tables = make([]string, length)
	for i := 0; i < length; i++ {
		e.Tables[i], err = r.ReadString()
		if err != nil {
			return fmt.Errorf("error reading table name: %w", err)
		}
	}

	// Read timestamp
	e.UpdatedAt, err = r.ReadTime()
	if err != nil {
		return fmt.Errorf("error reading updated timestamp: %w", err)
	}

	return nil
}

// TableMetadata represents serializable metadata for a table
type TableMetadata struct {
	Name         string
	Columns      []ColumnMetadata
	RowCount     int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeletedRows  []int64 // List of logically deleted row IDs
	NeedsCompact bool    // Flag indicating the table needs compaction
}

// ColumnMetadata represents serializable metadata for a column
type ColumnMetadata struct {
	Name       string
	DataType   storage.DataType
	PrimaryKey bool
	NotNull    bool
}

// IndexMetadata represents serializable metadata for an index
type IndexMetadata struct {
	Name     string
	Columns  []string
	IsUnique bool
	Type     string // "bitmap" or "btree"
}

// MarshalBinary marshals table metadata to binary
func (t *TableMetadata) MarshalBinary(w *Writer) {
	// Write the type marker directly to the buffer
	w.grow(1)
	w.buf = append(w.buf, TypeTableMeta)

	// Write table name
	w.WriteString(t.Name)

	// Write column metadata
	// First write the array header
	w.grow(5)
	w.buf = append(w.buf, TypeArray)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(t.Columns)))

	// Then write each column inline (without their type headers since they're part of an array)
	for _, col := range t.Columns {
		// Instead of using MarshalBinary which adds a type marker, directly write column fields
		w.WriteString(col.Name)
		w.WriteInt(int(col.DataType))

		// Write flags
		var flags byte
		if col.PrimaryKey {
			flags |= 1
		}
		if col.NotNull {
			flags |= 2
		}
		w.WriteUint8(flags)
	}

	// Write row count
	w.WriteInt64(t.RowCount)

	// Write timestamps
	w.WriteTime(t.CreatedAt)
	w.WriteTime(t.UpdatedAt)

	// Write deleted rows array
	w.WriteArrayHeader(len(t.DeletedRows))
	for _, rowID := range t.DeletedRows {
		w.WriteInt64(rowID)
	}

	// Write compaction flag
	w.WriteBool(t.NeedsCompact)
}

// UnmarshalBinary unmarshals table metadata from binary
func (t *TableMetadata) UnmarshalBinary(r *Reader) error {
	// Just read the byte directly since we know the first byte should be the type
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeTableMeta {
		return ErrInvalidType
	}

	var err error

	// Read table name
	t.Name, err = r.ReadString()
	if err != nil {
		return err
	}

	// Read column metadata - use ReadType directly to see what type we encounter
	// A proper array should have the TypeArray type mark first
	tp, err = r.ReadType()
	if err != nil {
		return err
	}
	if tp != TypeArray {
		return fmt.Errorf("expected TypeArray (%d) for columns header, got %d", TypeArray, tp)
	}

	// Now read the array length
	if r.pos+3 >= len(r.buf) {
		return ErrBufferTooSmall
	}
	colCount := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4

	// Create column array
	t.Columns = make([]ColumnMetadata, colCount)

	// Read each column inline (without type markers)
	for i := 0; i < colCount; i++ {
		// Read column name
		name, err := r.ReadString()
		if err != nil {
			return fmt.Errorf("error reading column %d name: %w", i, err)
		}

		// Read data type directly as a 32-bit int since that's what storage.DataType is
		dataTypeValue := int32(0)

		// Read the type byte
		if r.pos >= len(r.buf) {
			return fmt.Errorf("error reading column %d data type - buffer too small at pos %d", i, r.pos)
		}
		tp := r.buf[r.pos]
		r.pos++

		switch tp {
		case TypeInt8:
			if r.pos >= len(r.buf) {
				return ErrBufferTooSmall
			}
			dataTypeValue = int32(int8(r.buf[r.pos]))
			r.pos++
		case TypeInt16:
			if r.pos+1 >= len(r.buf) {
				return ErrBufferTooSmall
			}
			dataTypeValue = int32(int16(r.buf[r.pos]) | int16(r.buf[r.pos+1])<<8)
			r.pos += 2
		case TypeInt32:
			if r.pos+3 >= len(r.buf) {
				return ErrBufferTooSmall
			}
			dataTypeValue = int32(binary.LittleEndian.Uint32(r.buf[r.pos:]))
			r.pos += 4
		case TypeInt64:
			if r.pos+7 >= len(r.buf) {
				return ErrBufferTooSmall
			}
			dataTypeValue = int32(binary.LittleEndian.Uint64(r.buf[r.pos:]))
			r.pos += 8
		default:
			return fmt.Errorf("error reading column %d data type: unexpected type marker %d", i, tp)
		}

		// Read flags
		flags, err := r.ReadUint8()
		if err != nil {
			return fmt.Errorf("error reading column %d flags: %w", i, err)
		}

		// Populate the column metadata
		t.Columns[i] = ColumnMetadata{
			Name:       name,
			DataType:   storage.DataType(dataTypeValue),
			PrimaryKey: (flags & 1) != 0,
			NotNull:    (flags & 2) != 0,
		}
	}

	// Read row count using the same direct approach
	rowCount := 0

	// Read the type byte
	if r.pos >= len(r.buf) {
		return fmt.Errorf("error reading row count - buffer too small at pos %d", r.pos)
	}
	tp = r.buf[r.pos]
	r.pos++

	switch tp {
	case TypeInt8:
		if r.pos >= len(r.buf) {
			return ErrBufferTooSmall
		}
		rowCount = int(int8(r.buf[r.pos]))
		r.pos++
	case TypeInt16:
		if r.pos+1 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		rowCount = int(int16(r.buf[r.pos]) | int16(r.buf[r.pos+1])<<8)
		r.pos += 2
	case TypeInt32:
		if r.pos+3 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		rowCount = int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
		r.pos += 4
	case TypeInt64:
		if r.pos+7 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		rowCount = int(binary.LittleEndian.Uint64(r.buf[r.pos:]))
		r.pos += 8
	default:
		return fmt.Errorf("error reading row count: unexpected type marker %d", tp)
	}

	t.RowCount = int64(rowCount)

	// Read created timestamp
	if r.pos >= len(r.buf) {
		return fmt.Errorf("error reading created timestamp - buffer too small at pos %d", r.pos)
	}

	// Check for the time type marker
	tp = r.buf[r.pos]
	r.pos++

	if tp != TypeTime {
		return fmt.Errorf("error reading created timestamp: expected TypeTime (%d), got %d", TypeTime, tp)
	}

	// Read the timestamp
	if r.pos+7 >= len(r.buf) {
		return ErrBufferTooSmall
	}

	nanos := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	t.CreatedAt = time.Unix(0, int64(nanos))

	// Read updated timestamp using the same approach
	if r.pos >= len(r.buf) {
		return fmt.Errorf("error reading updated timestamp - buffer too small at pos %d", r.pos)
	}

	// Check for the time type marker
	tp = r.buf[r.pos]
	r.pos++

	if tp != TypeTime {
		return fmt.Errorf("error reading updated timestamp: expected TypeTime (%d), got %d", TypeTime, tp)
	}

	// Read the timestamp
	if r.pos+7 >= len(r.buf) {
		return ErrBufferTooSmall
	}

	nanos = binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	t.UpdatedAt = time.Unix(0, int64(nanos))

	// Check if we have more data for backwards compatibility
	if r.pos >= len(r.buf) {
		// End of buffer, older format without deleted rows or compact flag
		t.DeletedRows = []int64{} // Initialize as empty array
		t.NeedsCompact = false
		return nil
	}

	// Try to read the deleted rows array
	tp, err = r.ReadType()
	if err != nil {
		// Treat as end of data (backward compatibility with old format)
		t.DeletedRows = []int64{} // Initialize as empty array
		t.NeedsCompact = false
		return nil
	}

	if tp != TypeArray {
		// Unknown type, might be from a newer format - try to handle as best we can
		// Skip this field and continue
		t.DeletedRows = []int64{} // Initialize as empty array
	} else {
		// Read array length
		if r.pos+3 >= len(r.buf) {
			return ErrBufferTooSmall
		}

		deletedCount := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
		r.pos += 4

		// Read each deleted row ID
		t.DeletedRows = make([]int64, deletedCount)
		for i := 0; i < deletedCount; i++ {
			rowID, err := r.ReadInt64()
			if err != nil {
				return fmt.Errorf("error reading deleted row ID %d: %w", i, err)
			}
			t.DeletedRows[i] = rowID
		}
	}

	// Try to read the compaction flag
	if r.pos >= len(r.buf) {
		// End of buffer, older format without compact flag
		t.NeedsCompact = false
		return nil
	}

	// Read the boolean type marker
	tp = r.buf[r.pos]
	r.pos++

	if tp != TypeBool {
		// Unknown type, might be from a newer format - skip
		t.NeedsCompact = false
		return nil
	}

	// Read the boolean value
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	t.NeedsCompact = r.buf[r.pos] != 0
	r.pos++

	return nil
}

// MarshalBinary marshals column metadata to binary
func (c *ColumnMetadata) MarshalBinary(w *Writer) {
	// Write the type marker directly to the buffer
	w.grow(1)
	w.buf = append(w.buf, TypeColMeta)

	// Write column name
	w.WriteString(c.Name)

	// Write data type
	w.WriteInt(int(c.DataType))

	// Write flags
	var flags byte
	if c.PrimaryKey {
		flags |= 1
	}
	if c.NotNull {
		flags |= 2
	}

	w.WriteUint8(flags)
}

// UnmarshalBinary unmarshals column metadata from binary
func (c *ColumnMetadata) UnmarshalBinary(r *Reader) error {
	// Just read the byte directly since we know the first byte should be the type
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeColMeta {
		return ErrInvalidType
	}

	// Read column name directly
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	// Check for string type marker
	strType := r.buf[r.pos]
	r.pos++

	if strType != TypeString {
		return fmt.Errorf("expected string type marker for column name, got %d", strType)
	}

	// Read string length
	if r.pos+3 >= len(r.buf) {
		return ErrBufferTooSmall
	}
	nameLen := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4

	// Read string data
	if r.pos+nameLen-1 >= len(r.buf) {
		return ErrBufferTooSmall
	}

	c.Name = string(r.buf[r.pos : r.pos+nameLen])
	r.pos += nameLen

	// Read data type
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	// Get the type marker for data type
	dtType := r.buf[r.pos]
	r.pos++

	var dataType int

	switch dtType {
	case TypeInt8:
		if r.pos >= len(r.buf) {
			return ErrBufferTooSmall
		}
		dataType = int(int8(r.buf[r.pos]))
		r.pos++
	case TypeInt16:
		if r.pos+1 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		dataType = int(int16(r.buf[r.pos]) | int16(r.buf[r.pos+1])<<8)
		r.pos += 2
	case TypeInt32:
		if r.pos+3 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		dataType = int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
		r.pos += 4
	case TypeInt64:
		if r.pos+7 >= len(r.buf) {
			return ErrBufferTooSmall
		}
		dataType = int(binary.LittleEndian.Uint64(r.buf[r.pos:]))
		r.pos += 8
	default:
		return fmt.Errorf("unexpected data type marker: %d", dtType)
	}

	c.DataType = storage.DataType(dataType)

	// Read flags
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	// Get the type marker for flags
	flagType := r.buf[r.pos]
	r.pos++

	if flagType != TypeUint8 {
		return fmt.Errorf("expected uint8 type marker for flags, got %d", flagType)
	}

	// Read the flags byte
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	flags := r.buf[r.pos]
	r.pos++

	c.PrimaryKey = (flags & 1) != 0
	c.NotNull = (flags & 2) != 0

	return nil
}

// MarshalBinary marshals index metadata to binary
func (i *IndexMetadata) MarshalBinary(w *Writer) {
	// Write the type marker directly to the buffer
	w.grow(1)
	w.buf = append(w.buf, TypeIdxMeta)

	// Write index name
	w.WriteString(i.Name)

	// Write columns
	w.WriteArrayHeader(len(i.Columns))
	for _, col := range i.Columns {
		w.WriteString(col)
	}

	// Write flags
	var flags byte
	if i.IsUnique {
		flags |= 1
	}

	w.WriteUint8(flags)

	// Write index type
	w.WriteString(i.Type)
}

// UnmarshalBinary unmarshals index metadata from binary
func (i *IndexMetadata) UnmarshalBinary(r *Reader) error {
	// Just read the byte directly since we know the first byte should be the type
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeIdxMeta {
		return ErrInvalidType
	}

	// Read index name using ReadString (reusing the string reader code)
	var err error
	i.Name, err = r.ReadString()
	if err != nil {
		return err
	}

	// Read array header directly
	if r.pos >= len(r.buf) {
		return ErrBufferTooSmall
	}

	// Check array marker
	arrayType := r.buf[r.pos]
	r.pos++

	if arrayType != TypeArray {
		return fmt.Errorf("expected array type marker for columns, got %d", arrayType)
	}

	// Read column count
	if r.pos+3 >= len(r.buf) {
		return ErrBufferTooSmall
	}

	colCount := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4

	// Read each column name
	i.Columns = make([]string, colCount)
	for j := 0; j < colCount; j++ {
		i.Columns[j], err = r.ReadString()
		if err != nil {
			return err
		}
	}

	// Read flags using ReadUint8
	flags, err := r.ReadUint8()
	if err != nil {
		return err
	}

	i.IsUnique = (flags & 1) != 0

	// Read index type
	i.Type, err = r.ReadString()
	if err != nil {
		return err
	}

	return nil
}
