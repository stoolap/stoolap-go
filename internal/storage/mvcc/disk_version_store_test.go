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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

func TestDiskVersionStore(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "disk_version_store_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create an engine and schema for testing
	config := &storage.Config{Path: tempDir, Persistence: storage.DefaultPersistenceConfig()}
	engine := NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()
	tableName := "test_table"

	schema := storage.Schema{
		TableName: tableName,
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString},
			{ID: 2, Name: "age", Type: storage.TypeInteger},
		},
	}

	// Create table in engine
	_, err = engine.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get version store for the table
	vs, err := engine.GetVersionStore(tableName)
	if err != nil {
		t.Fatalf("Failed to get version store: %v", err)
	}

	// Add some test data
	rows := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
		{4, "David", 40},
		{5, "Eve", 28},
	}

	for _, row := range rows {
		// Create a new row
		data := make(storage.Row, 3)
		data[0] = storage.NewIntegerValue(row.id)
		data[1] = storage.NewStringValue(row.name)
		data[2] = storage.NewIntegerValue(row.age)

		// Add to version store
		vs.AddVersion(row.id, RowVersion{
			TxnID:          4, // Use positive transaction ID
			DeletedAtTxnID: 0,
			Data:           data,
			RowID:          row.id,
			CreateTime:     GetFastTimestamp(),
		})
	}

	// Create disk version store
	dvs, err := NewDiskVersionStore(tempDir, tableName, vs)
	if err != nil {
		t.Fatalf("Failed to create disk version store: %v", err)
	}

	// Test CreateSnapshot
	t.Run("CreateSnapshot", func(t *testing.T) {
		err := dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}

		// Check if snapshot files exist
		entries, err := os.ReadDir(filepath.Join(tempDir, tableName))
		if err != nil {
			t.Fatalf("Failed to read directory: %v", err)
		}

		var foundBin, foundMeta bool
		var binPath string
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".bin" {
				foundBin = true
				binPath = filepath.Join(tempDir, tableName, entry.Name())
			} else if filepath.Ext(entry.Name()) == ".meta" {
				foundMeta = true
			}
		}

		if !foundBin || !foundMeta {
			t.Errorf("Snapshot files not created: bin=%v, meta=%v", foundBin, foundMeta)
		}

		// Debug: Verify the snapshot content
		if binPath != "" {
			reader, err := NewDiskReader(binPath)
			if err != nil {
				t.Fatalf("Failed to create disk reader: %v", err)
			}
			defer reader.Close()

			// Check the content
			t.Logf("Snapshot file: %s", binPath)
			t.Logf("Footer RowCount: %d", reader.footer.RowCount)
			t.Logf("Footer IndexSize: %d", reader.footer.IndexSize)
			t.Logf("Footer IndexOffset: %d", reader.footer.IndexOffset)
		}
	})

	// Test LoadSnapshots
	t.Run("LoadSnapshots", func(t *testing.T) {
		// Create a new disk version store to load the snapshot
		dvs2, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create second disk version store: %v", err)
		}

		err = dvs2.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots: %v", err)
		}

		// Verify readers were loaded
		if len(dvs2.readers) == 0 {
			t.Errorf("No readers loaded from snapshots")
		}
	})

	// Test GetVersionFromDisk
	t.Run("GetVersionFromDisk", func(t *testing.T) {
		// Create a new disk version store to load the snapshot
		dvs2, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store for retrieval: %v", err)
		}

		t.Logf("Number of readers before loading snapshots: %d", len(dvs2.readers))

		err = dvs2.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots: %v", err)
		}

		t.Logf("Number of readers after loading snapshots: %d", len(dvs2.readers))

		// Check if we actually have readers after loading snapshots
		if len(dvs2.readers) == 0 {
			// Debug: Check what's in the directory
			entries, _ := os.ReadDir(filepath.Join(tempDir, tableName))
			t.Logf("Directory contents of %s:", filepath.Join(tempDir, tableName))
			for _, entry := range entries {
				t.Logf("  - %s (isDir: %v)", entry.Name(), entry.IsDir())
			}
			t.Fatalf("No readers loaded from snapshots!")
		}

		// Try to retrieve each row from disk
		for _, row := range rows {
			t.Logf("Testing retrieval of row with ID %d", row.id)
			version, found := dvs2.GetVersionFromDisk(row.id)
			if !found {
				t.Errorf("Row with ID %d not found on disk", row.id)
				continue
			}

			// Verify the data
			if version.RowID != row.id {
				t.Errorf("Expected RowID %d, got %d", row.id, version.RowID)
			}

			if version.IsDeleted() {
				t.Errorf("Expected row %d to not be deleted", row.id)
			}

			if version.Data == nil {
				t.Errorf("Expected data for row %d, got nil", row.id)
				continue
			}

			// Check data values
			idVal, _ := version.Data[0].AsInt64()
			if idVal != row.id {
				t.Errorf("Expected ID %d, got %d", row.id, idVal)
			}

			nameVal, _ := version.Data[1].AsString()
			if nameVal != row.name {
				t.Errorf("Expected name %s, got %s", row.name, nameVal)
			}

			ageVal, _ := version.Data[2].AsInt64()
			if ageVal != row.age {
				t.Errorf("Expected age %d, got %d", row.age, ageVal)
			}
		}

		// Test retrieving a non-existent row
		_, found := dvs2.GetVersionFromDisk(999)
		if found {
			t.Errorf("Found a row with non-existent ID 999")
		}
	})

	// Test DiskReader schema access
	t.Run("ReaderGetSchema", func(t *testing.T) {
		// Create a new disk version store to load the snapshot
		dvs2, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store for schema test: %v", err)
		}

		err = dvs2.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots: %v", err)
		}

		if len(dvs2.readers) == 0 {
			t.Fatalf("No readers available")
		}

		// Get schema from reader
		readerSchema := dvs2.readers[0].GetSchema()
		if readerSchema == nil {
			t.Fatalf("Reader returned nil schema")
		}

		// Verify schema matches
		if readerSchema.TableName != tableName {
			t.Errorf("Expected table name %s, got %s", tableName, readerSchema.TableName)
		}

		if len(readerSchema.Columns) != len(schema.Columns) {
			t.Errorf("Expected %d columns, got %d", len(schema.Columns), len(readerSchema.Columns))
		}

		// Check column names and types
		for i, col := range schema.Columns {
			if i >= len(readerSchema.Columns) {
				break
			}
			if readerSchema.Columns[i].Name != col.Name {
				t.Errorf("Column %d: expected name %s, got %s", i, col.Name, readerSchema.Columns[i].Name)
			}
			if readerSchema.Columns[i].Type != col.Type {
				t.Errorf("Column %d: expected type %v, got %v", i, col.Type, readerSchema.Columns[i].Type)
			}
		}
	})

	// Test binary search in index
	t.Run("BinarySearchIndex", func(t *testing.T) {
		// Create a new disk version store to load the snapshot
		dvs2, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store for binary search test: %v", err)
		}

		err = dvs2.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots: %v", err)
		}

		if len(dvs2.readers) == 0 {
			t.Fatalf("No readers available")
		}

		// Test retrieving rows in non-sequential order to test binary search
		testIDs := []int64{3, 1, 5, 2, 4}
		for _, id := range testIDs {
			version, found := dvs2.GetVersionFromDisk(id)
			if !found {
				t.Errorf("Row with ID %d not found during binary search test", id)
				continue
			}

			if version.RowID != id {
				t.Errorf("Expected RowID %d, got %d", id, version.RowID)
			}
		}
	})

	// Test with deleted rows
	t.Run("DeletedRows", func(t *testing.T) {
		// Delete a row
		vs.AddVersion(3, RowVersion{
			TxnID:          5, // Different system transaction
			DeletedAtTxnID: 5,
			Data:           nil,
			RowID:          3,
			CreateTime:     GetFastTimestamp(),
		})

		// Create a new snapshot
		err := dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot with deleted row: %v", err)
		}

		// Load snapshots in a new disk version store
		dvs3, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store for deleted row test: %v", err)
		}

		err = dvs3.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots for deleted row test: %v", err)
		}

		// The deleted row should still be retrievable, but might be marked as deleted
		// Note: Some implementations may not store deleted rows at all, which is also valid
		version, found := dvs3.GetVersionFromDisk(3)
		if found && !version.IsDeleted() {
			t.Errorf("Row with ID 3 was found but not marked as deleted")
		}
	})
}

func TestDiskVersionStoreEdgeCases(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "disk_version_store_edge_cases")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create an engine and schema for testing
	config := &storage.Config{Path: tempDir, Persistence: storage.DefaultPersistenceConfig()}
	engine := NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()
	tableName := "edge_case_table"

	schema := storage.Schema{
		TableName: tableName,
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "json_data", Type: storage.TypeJSON},
			{ID: 2, Name: "nullable", Type: storage.TypeString, Nullable: true},
			{ID: 3, Name: "timestamp", Type: storage.TypeTimestamp},
			{ID: 4, Name: "boolean", Type: storage.TypeBoolean},
			{ID: 5, Name: "float", Type: storage.TypeFloat},
		},
	}

	// Create table in engine
	_, err = engine.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get version store for the table
	vs, err := engine.GetVersionStore(tableName)
	if err != nil {
		t.Fatalf("Failed to get version store: %v", err)
	}

	// Create disk version store
	dvs, err := NewDiskVersionStore(tempDir, tableName, vs)
	if err != nil {
		t.Fatalf("Failed to create disk version store: %v", err)
	}

	// Test with empty version store
	t.Run("EmptyVersionStore", func(t *testing.T) {
		err := dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot with empty version store: %v", err)
		}

		// Load snapshots
		err = dvs.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots for empty version store: %v", err)
		}

		// Try to get a version
		_, found := dvs.GetVersionFromDisk(1)
		if found {
			t.Errorf("Found a row in an empty version store")
		}
	})

	// Test with all data types
	t.Run("AllDataTypes", func(t *testing.T) {
		// Create a row with all data types
		row := make(storage.Row, 6)
		row[0] = storage.NewIntegerValue(1)
		row[1] = storage.NewJSONValue(`{"key": "value"}`)
		row[2] = storage.NewNullValue(storage.TypeString) // NULL value
		row[3] = storage.NewTimestampValue(time.Now())    // Timestamp
		row[4] = storage.NewBooleanValue(true)
		row[5] = storage.NewFloatValue(123.456)

		// Add to version store
		vs.AddVersion(1, RowVersion{
			TxnID:          1,
			DeletedAtTxnID: 0,
			Data:           row,
			RowID:          1,
			CreateTime:     GetFastTimestamp(),
		})

		// Create snapshot
		err := dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot with all data types: %v", err)
		}

		// Load snapshots
		err = dvs.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load snapshots for all data types: %v", err)
		}

		// Get the version
		version, found := dvs.GetVersionFromDisk(1)
		if !found {
			t.Errorf("Row with all data types not found")
			return
		}

		// Verify data types
		if version.Data == nil || len(version.Data) != 6 {
			t.Errorf("Expected 6 columns, got %d", len(version.Data))
			return
		}

		// Check integer
		idVal, ok := version.Data[0].AsInt64()
		if !ok || idVal != 1 {
			t.Errorf("Failed to get integer value: %v", err)
		}

		// Check JSON
		jsonVal, ok := version.Data[1].AsJSON()
		if !ok || jsonVal != `{"key": "value"}` {
			t.Errorf("Failed to get JSON value: %v, got %s", err, jsonVal)
		}

		// Check NULL
		if !version.Data[2].IsNull() {
			t.Errorf("Expected NULL value, got non-NULL")
		}

		// Check timestamp
		_, ok = version.Data[3].AsTimestamp()
		if !ok {
			t.Errorf("Failed to get timestamp value: %v", err)
		}

		// Check boolean
		boolVal, ok := version.Data[4].AsBoolean()
		if !ok || !boolVal {
			t.Errorf("Failed to get boolean value: %v", err)
		}

		// Check float
		floatVal, ok := version.Data[5].AsFloat64()
		if !ok || floatVal != 123.456 {
			t.Errorf("Failed to get float value: %v, got %f", err, floatVal)
		}
	})

	// Test creating a snapshot when directory doesn't exist
	t.Run("MissingDirectory", func(t *testing.T) {
		nonExistentDir := filepath.Join(tempDir, "non_existent")
		dvs2, err := NewDiskVersionStore(nonExistentDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store with non-existent directory: %v", err)
		}

		// This should create the directory
		err = dvs2.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot with non-existent directory: %v", err)
		}

		// Directory should now exist
		_, err = os.Stat(filepath.Join(nonExistentDir, tableName))
		if os.IsNotExist(err) {
			t.Errorf("Directory was not created")
		}
	})

	// Test with multiple snapshots
	t.Run("MultipleSnapshots", func(t *testing.T) {
		// Create first snapshot
		err := dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create first snapshot: %v", err)
		}

		// Add more data
		row := make(storage.Row, 6)
		row[0] = storage.NewIntegerValue(2)
		row[1] = storage.NewJSONValue(`{"key2": "value2"}`)
		row[2] = storage.NewStringValue("not null")
		row[3] = storage.NewTimestampValue(time.Now())
		row[4] = storage.NewBooleanValue(false)
		row[5] = storage.NewFloatValue(789.012)

		vs.AddVersion(2, RowVersion{
			TxnID:          2,
			DeletedAtTxnID: 0,
			Data:           row,
			RowID:          2,
			CreateTime:     GetFastTimestamp(),
		})

		// Create second snapshot
		err = dvs.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create second snapshot: %v", err)
		}

		// Load snapshots
		dvs2, err := NewDiskVersionStore(tempDir, tableName, vs)
		if err != nil {
			t.Fatalf("Failed to create disk version store for multiple snapshots: %v", err)
		}

		err = dvs2.LoadSnapshots()
		if err != nil {
			t.Fatalf("Failed to load multiple snapshots: %v", err)
		}

		// Should have loaded newest reader
		if len(dvs2.readers) < 1 {
			t.Errorf("Expected at least 1 reader, got %d", len(dvs2.readers))
		}

		// Both rows should be accessible
		for id := int64(1); id <= 2; id++ {
			version, found := dvs2.GetVersionFromDisk(id)
			if !found {
				t.Errorf("Row with ID %d not found", id)
				continue
			}

			if version.RowID != id {
				t.Errorf("Expected RowID %d, got %d", id, version.RowID)
			}
		}
	})
}

func TestDiskAppenderFinalize(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "disk_appender_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create appender
	filePath := filepath.Join(tempDir, "test-appender.bin")
	appender, err := NewDiskAppender(filePath)
	if err != nil {
		t.Fatalf("Failed to create disk appender: %v", err)
	}
	defer appender.Close()

	// Create a schema for testing
	schema := &storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger},
		},
	}

	// Write schema
	err = appender.WriteSchema(schema)
	if err != nil {
		t.Fatalf("Failed to write schema: %v", err)
	}

	// Create row versions
	for i := int64(1); i <= 10; i++ {
		row := make(storage.Row, 1)
		row[0] = storage.NewIntegerValue(i)

		version := RowVersion{
			TxnID:          3,
			DeletedAtTxnID: 0,
			Data:           row,
			RowID:          i,
			CreateTime:     GetFastTimestamp(),
		}

		err = appender.AppendRow(version)
		if err != nil {
			t.Fatalf("Failed to append row %d: %v", i, err)
		}
	}

	// Finalize
	err = appender.Finalize()
	if err != nil {
		t.Fatalf("Failed to finalize appender: %v", err)
	}

	// Verify the file exists and has the expected size
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Errorf("Expected non-empty file, got empty file")
	}

	// Try to read the file with a DiskReader
	reader, err := NewDiskReader(filePath)
	if err != nil {
		t.Fatalf("Failed to create disk reader: %v", err)
	}
	defer reader.Close()

	// Verify the file has the expected content
	for i := int64(1); i <= 10; i++ {
		version, found := reader.GetRow(i)
		if !found {
			t.Errorf("Row with ID %d not found", i)
			continue
		}

		if version.RowID != i {
			t.Errorf("Expected RowID %d, got %d", i, version.RowID)
		}

		if version.IsDeleted() {
			t.Errorf("Expected row %d to not be deleted", i)
		}

		if version.Data == nil || len(version.Data) != 1 {
			t.Errorf("Expected 1 column for row %d, got %d", i, len(version.Data))
			continue
		}

		val, ok := version.Data[0].AsInt64()
		if !ok || val != i {
			t.Errorf("Expected value %d for row %d, got %d", i, i, val)
		}
	}
}
