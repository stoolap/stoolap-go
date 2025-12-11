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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/common"
	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/binser"
)

// Duplicate the binser type constants needed for testing
const (
	TypeNull   byte = 0
	TypeUint32 byte = 8
	TypeBytes  byte = 13
)

// TestMultipleIndexesSerialization tests the serialization and deserialization of multiple indexes
func TestMultipleIndexesSerialization(t *testing.T) {
	// Create temporary directory for the test
	tmpDir := common.TempDir(t)

	// Create a simple schema for testing
	schema := &storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString},
			{ID: 2, Name: "timestamp", Type: storage.TypeTimestamp},
			{ID: 3, Name: "price", Type: storage.TypeFloat},
			{ID: 4, Name: "is_active", Type: storage.TypeBoolean},
		},
	}

	config := &storage.Config{Path: tmpDir, Persistence: storage.PersistenceConfig{Enabled: true}}

	// Create a mock engine with schemas
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	_, err = engine.CreateTable(*schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	vs, err := engine.GetVersionStore("test_table")
	if err != nil {
		t.Fatalf("Failed to get version store: %v", err)
	}

	// Create 3 different indexes to test serialization - make sure to use the same names as the keys
	idxTimestamp := NewColumnarIndex(
		"timestamp", // Key name matches the column name, not "idx_timestamp"
		"test_table",
		"timestamp",
		2,
		storage.TypeTimestamp,
		vs,
		false,
	)

	vs.AddIndex(idxTimestamp)

	idxPrice := NewColumnarIndex(
		"price", // Key name matches the column name, not "idx_price"
		"test_table",
		"price",
		3,
		storage.TypeFloat,
		vs,
		false,
	)
	vs.AddIndex(idxPrice)

	idxActive := NewColumnarIndex(
		"is_active", // Key name matches the column name, not "idx_active"
		"test_table",
		"is_active",
		4,
		storage.TypeBoolean,
		vs,
		true, // Unique index
	)
	vs.AddIndex(idxActive)

	// Create a disk version store
	dvs := engine.persistence.diskStores["test_table"]
	if dvs == nil {
		t.Fatalf("Failed to get disk version store")
	}

	// Create a metadata file path
	metaPath := filepath.Join(tmpDir, "test.meta")

	// Write the metadata with our indexes
	err = dvs.writeMetadata(metaPath, schema)
	if err != nil {
		t.Fatalf("Failed to write index metadata: %v", err)
	}

	// Print debug info about the metadata file
	metaInfo, err := os.Stat(metaPath)
	if err != nil {
		t.Logf("Failed to stat metadata file: %v", err)
	} else {
		t.Logf("Metadata file size: %d bytes", metaInfo.Size())

	}

	engine.Close()

	// Now create a new version store and disk version store to test loading
	newEngine := NewMVCCEngine(config)
	err = newEngine.Open()
	if err != nil {
		t.Fatalf("Failed to open new engine: %v", err)
	}

	newVS, err := newEngine.GetVersionStore("test_table")
	if err != nil {
		t.Fatalf("Failed to get new version store: %v", err)
	}

	newDVS := newEngine.persistence.diskStores["test_table"]
	if newDVS == nil {
		t.Fatalf("Failed to get new disk version store")
	}

	// Load the metadata
	err = newDVS.loadMetadataFile(metaPath)
	if err != nil {
		t.Errorf("Failed to load metadata: %v", err)
	}

	// Verify that we have the right number of indexes
	newVS.indexMutex.RLock()
	indexCount := len(newVS.indexes)
	newVS.indexMutex.RUnlock()

	if indexCount != 3 {
		t.Errorf("Expected 3 indexes, got %d", indexCount)
	}

	// Verify each specific index
	newVS.indexMutex.RLock()
	defer newVS.indexMutex.RUnlock()

	// Check timestamp index
	if tsIdx, ok := newVS.indexes["timestamp"].(*ColumnarIndex); ok {
		if tsIdx.dataType != storage.TypeTimestamp {
			t.Errorf("Expected timestamp index data type to be TypeTimestamp, got %v", tsIdx.dataType)
		}
	} else {
		t.Errorf("Timestamp index not found or not a ColumnarIndex")
	}

	// Check price index
	if priceIdx, ok := newVS.indexes["price"].(*ColumnarIndex); ok {
		if priceIdx.dataType != storage.TypeFloat {
			t.Errorf("Expected price index data type to be TypeFloat, got %v", priceIdx.dataType)
		}
		if priceIdx.isUnique {
			t.Errorf("Expected price index to not be unique")
		}
	} else {
		t.Errorf("Price index not found or not a ColumnarIndex")
	}

	// Check active index
	if activeIdx, ok := newVS.indexes["is_active"].(*ColumnarIndex); ok {
		if activeIdx.dataType != storage.TypeBoolean {
			t.Errorf("Expected active index data type to be TypeBoolean, got %v", activeIdx.dataType)
		}
		if !activeIdx.isUnique {
			t.Errorf("Expected active index to be unique")
		}
	} else {
		t.Errorf("Active index not found or not a ColumnarIndex")
	}
}

// TestSeparatorMarker ensures our binary separator for index metadata works correctly
func TestSeparatorMarker(t *testing.T) {
	// Create a writer with our separator markers
	writer := binser.NewWriter()

	// Write a header similar to our metadata format
	writer.WriteUint32(1) // Version
	writer.WriteInt64(time.Now().UnixNano())
	writer.WriteUint64(12345) // Schema hash
	writer.WriteBytes([]byte("mock schema"))
	writer.WriteUint16(3) // 3 indexes

	// Write three indexes with separators
	for i := 0; i < 3; i++ {
		// Write separator - use WriteByte to avoid type marker
		writer.WriteByte(0xFF)

		// Prepare mock index data
		mockData := []byte(fmt.Sprintf("index data %d", i))

		// Write length and data - bypass types for test simplicity
		writer.WriteByte(TypeUint32) // Use TypeUint32 constant from binser
		binLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(binLenBytes, uint32(len(mockData)))
		writer.WriteByte(binLenBytes[0])
		writer.WriteByte(binLenBytes[1])
		writer.WriteByte(binLenBytes[2])
		writer.WriteByte(binLenBytes[3])

		writer.WriteByte(TypeBytes) // Use TypeBytes constant from binser
		writer.WriteByte(binLenBytes[0])
		writer.WriteByte(binLenBytes[1])
		writer.WriteByte(binLenBytes[2])
		writer.WriteByte(binLenBytes[3])
		for _, b := range mockData {
			writer.WriteByte(b)
		}
	}

	// Create a reader to test reading with separators
	data := writer.Bytes()
	reader := binser.NewReader(data)

	// Skip to the indexes section
	_, _ = reader.ReadUint32()      // Skip version
	_, _ = reader.ReadInt64()       // Skip timestamp
	_, _ = reader.ReadUint64()      // Skip schema hash
	_, _ = reader.ReadBytes()       // Skip schema bytes
	count, _ := reader.ReadUint16() // Read index count

	if count != 3 {
		t.Errorf("Expected 3 indexes, got %d", count)
	}

	// Read each index with separator
	for i := 0; i < int(count); i++ {
		// Check for separator
		marker, err := reader.PeekByte()
		if err != nil {
			t.Errorf("Failed to peek separator %d: %v", i, err)
			continue
		}

		if marker != 0xFF {
			t.Errorf("Expected separator 0xFF for index %d, got %d", i, marker)
			continue
		}

		// Consume separator
		_, _ = reader.ReadByte()

		// Read type marker and skip
		typeMarker, _ := reader.ReadByte()
		if typeMarker != TypeUint32 {
			t.Errorf("Expected type marker %d for index %d length, got %d", TypeUint32, i, typeMarker)
			continue
		}

		// Read length bytes manually
		lenBytes := make([]byte, 4)
		for j := range lenBytes {
			lenBytes[j], _ = reader.ReadByte()
		}
		length := binary.LittleEndian.Uint32(lenBytes)

		// Read type marker for data
		dataTypeMarker, _ := reader.ReadByte()
		if dataTypeMarker != TypeBytes {
			t.Errorf("Expected type marker %d for index %d data, got %d", TypeBytes, i, dataTypeMarker)
			continue
		}

		// Skip length bytes for data (should be same as before)
		for range lenBytes {
			_, _ = reader.ReadByte()
		}

		// Read actual data bytes
		dataBytes := make([]byte, length)
		for j := range dataBytes {
			dataBytes[j], _ = reader.ReadByte()
		}

		expected := fmt.Sprintf("index data %d", i)
		if string(dataBytes) != expected {
			t.Errorf("Index %d: expected data %q, got %q", i, expected, string(dataBytes))
		}
	}
}
