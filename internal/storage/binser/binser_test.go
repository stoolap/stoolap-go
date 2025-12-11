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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

func TestBasicSerialization(t *testing.T) {
	// Create test data
	w := NewWriter()

	w.WriteBool(true)
	w.WriteInt8(42)
	w.WriteInt16(1000)
	w.WriteInt32(100000)
	w.WriteInt64(10000000000)
	w.WriteUint8(255)
	w.WriteUint16(65000)
	w.WriteUint32(4000000000)
	w.WriteUint64(10000000000000000000)
	w.WriteFloat32(3.14159)
	w.WriteFloat64(2.718281828459045)
	w.WriteString("Hello, binary serialization!")
	w.WriteBytes([]byte{1, 2, 3, 4, 5})
	w.WriteTime(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))

	// Get serialized data
	data := w.Bytes()

	// Read back
	r := NewReader(data)

	// Check each value
	if b, err := r.ReadBool(); err != nil || b != true {
		t.Errorf("Bool read failed: got %v, err %v", b, err)
	}

	if i, err := r.ReadInt8(); err != nil || i != 42 {
		t.Errorf("Int8 read failed: got %v, err %v", i, err)
	}

	if i, err := r.ReadInt16(); err != nil || i != 1000 {
		t.Errorf("Int16 read failed: got %v, err %v", i, err)
	}

	if i, err := r.ReadInt32(); err != nil || i != 100000 {
		t.Errorf("Int32 read failed: got %v, err %v", i, err)
	}

	if i, err := r.ReadInt64(); err != nil || i != 10000000000 {
		t.Errorf("Int64 read failed: got %v, err %v", i, err)
	}

	if u, err := r.ReadUint8(); err != nil || u != 255 {
		t.Errorf("Uint8 read failed: got %v, err %v", u, err)
	}

	if u, err := r.ReadUint16(); err != nil || u != 65000 {
		t.Errorf("Uint16 read failed: got %v, err %v", u, err)
	}

	if u, err := r.ReadUint32(); err != nil || u != 4000000000 {
		t.Errorf("Uint32 read failed: got %v, err %v", u, err)
	}

	if u, err := r.ReadUint64(); err != nil || u != 10000000000000000000 {
		t.Errorf("Uint64 read failed: got %v, err %v", u, err)
	}

	if f, err := r.ReadFloat32(); err != nil || f != 3.14159 {
		t.Errorf("Float32 read failed: got %v, err %v", f, err)
	}

	if f, err := r.ReadFloat64(); err != nil || f != 2.718281828459045 {
		t.Errorf("Float64 read failed: got %v, err %v", f, err)
	}

	if s, err := r.ReadString(); err != nil || s != "Hello, binary serialization!" {
		t.Errorf("String read failed: got %v, err %v", s, err)
	}

	if b, err := r.ReadBytes(); err != nil || len(b) != 5 || b[0] != 1 || b[4] != 5 {
		t.Errorf("Bytes read failed: got %v, err %v", b, err)
	}

	expectedTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	if tm, err := r.ReadTime(); err != nil || !tm.Equal(expectedTime) {
		t.Errorf("Time read failed: got %v, expected %v, err %v", tm, expectedTime, err)
	}
}

func TestTableMetadataSerialization(t *testing.T) {
	// Create a test table metadata structure
	tableMeta := &TableMetadata{
		Name: "TestTable",
		Columns: []ColumnMetadata{
			{
				Name:       "ID",
				DataType:   storage.INTEGER,
				PrimaryKey: true,
				NotNull:    true,
			},
			{
				Name:     "Name",
				DataType: storage.TEXT,
				NotNull:  true,
			},
			{
				Name:     "Value",
				DataType: storage.FLOAT,
			},
		},
		RowCount:  1000,
		CreatedAt: time.Now().Add(-24 * time.Hour),
		UpdatedAt: time.Now(),
	}

	// Let's trace the binary format by marshalling each column separately first
	for i, col := range tableMeta.Columns {
		colWriter := NewWriter()
		col.MarshalBinary(colWriter)
		colData := colWriter.Bytes()
		t.Logf("Column %d binary data: %v", i, colData)
	}

	// Serialize using our binary format
	writer := NewWriter()
	tableMeta.MarshalBinary(writer)
	binData := writer.Bytes()

	// Debug: print the binary data
	t.Logf("Binary data (first 20 bytes): %v", binData[:20])

	// Deserialize and check
	reader := NewReader(binData)

	// Debug trace through the binary data
	if len(binData) >= 3 {
		t.Logf("Type byte: %d, Next type (for string): %d", binData[0], binData[1])
		t.Logf("Position 10-15: %v", binData[10:15])
	}

	deserializedMeta := &TableMetadata{}
	if err := deserializedMeta.UnmarshalBinary(reader); err != nil {
		// Debug: print the first byte we're trying to read
		t.Logf("First byte: %d (expected TypeTableMeta: %d)", binData[0], TypeTableMeta)
		// Print the current position and the next few bytes
		pos := reader.pos
		if pos < len(binData) {
			endPos := pos + 10
			if endPos > len(binData) {
				endPos = len(binData)
			}
			t.Logf("Current position: %d, Next bytes: %v", pos, binData[pos:endPos])
		}
		t.Fatalf("Failed to unmarshal table metadata: %v", err)
	}

	// Check if fields match
	if deserializedMeta.Name != tableMeta.Name {
		t.Errorf("Name mismatch: got %s, expected %s", deserializedMeta.Name, tableMeta.Name)
	}

	if len(deserializedMeta.Columns) != len(tableMeta.Columns) {
		t.Errorf("Column count mismatch: got %d, expected %d", len(deserializedMeta.Columns), len(tableMeta.Columns))
	}

	if deserializedMeta.RowCount != tableMeta.RowCount {
		t.Errorf("Row count mismatch: got %d, expected %d", deserializedMeta.RowCount, tableMeta.RowCount)
	}

	// Check the first column
	if len(deserializedMeta.Columns) > 0 {
		col := deserializedMeta.Columns[0]
		expectedCol := tableMeta.Columns[0]

		if col.Name != expectedCol.Name {
			t.Errorf("Column name mismatch: got %s, expected %s", col.Name, expectedCol.Name)
		}

		if col.DataType != expectedCol.DataType {
			t.Errorf("Column type mismatch: got %d, expected %d", col.DataType, expectedCol.DataType)
		}

		if col.PrimaryKey != expectedCol.PrimaryKey {
			t.Errorf("Column PrimaryKey mismatch: got %v, expected %v", col.PrimaryKey, expectedCol.PrimaryKey)
		}

		if col.NotNull != expectedCol.NotNull {
			t.Errorf("Column NotNull mismatch: got %v, expected %v", col.NotNull, expectedCol.NotNull)
		}
	}
}

// TestColumnMetadataSerialization tests binary serialization for column metadata
func TestColumnMetadataSerialization(t *testing.T) {
	// Create a test column metadata structure
	colMeta := ColumnMetadata{
		Name:       "ID",
		DataType:   storage.INTEGER,
		PrimaryKey: true,
		NotNull:    true,
	}

	// Serialize using our binary format
	writer := NewWriter()
	colMeta.MarshalBinary(writer)
	binData := writer.Bytes()

	// Debug: print the binary data
	t.Logf("Column binary data: %v", binData)

	// Deserialize and check
	reader := NewReader(binData)
	deserializedMeta := &ColumnMetadata{}
	if err := deserializedMeta.UnmarshalBinary(reader); err != nil {
		t.Fatalf("Failed to unmarshal column metadata: %v", err)
	}

	// Check if fields match
	if deserializedMeta.Name != colMeta.Name {
		t.Errorf("Name mismatch: got %s, expected %s", deserializedMeta.Name, colMeta.Name)
	}

	if deserializedMeta.DataType != colMeta.DataType {
		t.Errorf("DataType mismatch: got %d, expected %d", deserializedMeta.DataType, colMeta.DataType)
	}

	if deserializedMeta.PrimaryKey != colMeta.PrimaryKey {
		t.Errorf("PrimaryKey mismatch: got %v, expected %v", deserializedMeta.PrimaryKey, colMeta.PrimaryKey)
	}

	if deserializedMeta.NotNull != colMeta.NotNull {
		t.Errorf("NotNull mismatch: got %v, expected %v", deserializedMeta.NotNull, colMeta.NotNull)
	}
}

func BenchmarkBinaryVsJSON(b *testing.B) {
	// Create a complex table metadata structure
	tableMeta := &TableMetadata{
		Name:      "BenchmarkTable",
		Columns:   make([]ColumnMetadata, 50), // A table with 50 columns
		RowCount:  1000000,
		CreatedAt: time.Now().Add(-24 * time.Hour),
		UpdatedAt: time.Now(),
	}

	// Fill columns
	for i := 0; i < 50; i++ {
		tableMeta.Columns[i] = ColumnMetadata{
			Name:       fmt.Sprintf("Column%d", i),
			DataType:   storage.DataType(i % 7), // Cycle through data types
			PrimaryKey: i == 0,
			NotNull:    i < 10,
		}
	}

	b.Run("BinarySerialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			writer := NewWriter()
			tableMeta.MarshalBinary(writer)
			_ = writer.Bytes()
			writer.Release()
		}
	})

	b.Run("JSONSerialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(tableMeta)
		}
	})

	// Create binary and JSON data for deserialization benchmark
	writer := NewWriter()
	tableMeta.MarshalBinary(writer)
	binData := writer.Bytes()

	jsonData, _ := json.Marshal(tableMeta)

	b.Run("BinaryDeserialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reader := NewReader(binData)
			deserializedMeta := &TableMetadata{}
			_ = deserializedMeta.UnmarshalBinary(reader)
		}
	})

	b.Run("JSONDeserialization", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			deserializedMeta := &TableMetadata{}
			_ = json.Unmarshal(jsonData, deserializedMeta)
		}
	})
}
