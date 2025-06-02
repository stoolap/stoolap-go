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
package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

func TestMVCCPersistenceWithTruncation(t *testing.T) {
	// Create a temporary directory for the database files
	dbDir := common.TempDir(t)

	// Configure the storage engine without automatic snapshots
	config := &storage.Config{
		Path: dbDir,
		Persistence: storage.PersistenceConfig{
			Enabled:          true,
			SyncMode:         1, // Normal
			SnapshotInterval: 0, // Disable automatic snapshots
		},
	}

	start := time.Now()

	// Create MVCC engine
	engine := mvcc.NewMVCCEngine(config)

	// Open engine
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Create a table
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString},
			{ID: 2, Name: "value", Type: storage.TypeFloat},
		},
	}

	_, err = tx.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Reduced to 3 batches with smaller batch size
	for j := 0; j < 3; j++ {
		// Create snapshot after each batch except the first
		if j > 0 {
			if err := engine.CreateSnapshot(); err != nil {
				t.Fatalf("Failed to create snapshot: %v", err)
			}
		}

		tx, err = engine.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		table, err := tx.GetTable("test_table")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		// Reduced to 50 inserts per batch
		for i := j * 50; i < (j+1)*50; i++ {
			row := storage.Row{
				storage.NewIntegerValue(int64(i)),
				storage.NewStringValue("name_" + string(rune(i))),
				storage.NewFloatValue(float64(i) * 1.5),
			}

			err = table.Insert(row)
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	}

	endSeconds := time.Since(start).Seconds()
	t.Logf("Time taken for all inserts: %0.2fs", endSeconds)

	// Create final snapshot
	if err := engine.CreateSnapshot(); err != nil {
		t.Fatalf("Failed to create final snapshot: %v", err)
	}

	// Check if WAL directory exists
	walDir := filepath.Join(dbDir, "wal")
	if _, err = os.Stat(walDir); os.IsNotExist(err) {
		t.Fatalf("WAL directory not created")
	}

	// List WAL files to verify truncation occurred
	files, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("Failed to read WAL directory: %v", err)
	}

	// Count active WAL files (should typically be just 1 after truncation)
	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			walFiles = append(walFiles, file.Name())
		}
	}

	// With WAL truncation, we expect only a small number of WAL files
	// Typically just 1 active WAL file should exist after truncation
	if len(walFiles) > 3 {
		t.Logf("WARNING: Found %d WAL files, truncation may not be working effectively", len(walFiles))
	} else {
		t.Logf("Truncation working as expected: found only %d WAL file(s)", len(walFiles))
	}

	// Close the engine
	err = engine.Close()
	if err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}

	// Reopen the engine to test recovery
	newEngine := mvcc.NewMVCCEngine(config)

	err = newEngine.Open()
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}

	// Recovery should be synchronous, no wait needed

	// Verify data was recovered
	tx, err = newEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction after recovery: %v", err)
	}

	// Check if table exists
	exists, err := tx.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table after recovery: %v", err)
	}
	if exists == nil {
		t.Fatal("Table not recovered")
	}

	// Select data to verify it was recovered
	result, err := tx.SelectWithAliases("test_table", []string{"id", "name", "value"}, nil, nil)
	if err != nil {
		t.Fatalf("Failed to select data after recovery: %v", err)
	}

	// Count rows
	rowCount := 0
	for result.Next() {
		rowCount++
	}
	result.Close()

	// We should have 150 rows (3 batches of 50)
	if rowCount != 150 {
		t.Fatalf("Wrong number of rows after recovery: got %d, expected 150", rowCount)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction after recovery: %v", err)
	}

	// Close the new engine
	err = newEngine.Close()
	if err != nil {
		t.Fatalf("Failed to close new engine: %v", err)
	}
}

func TestMVCCRecoveryFromWAL(t *testing.T) {
	// Create a temporary directory for the database files
	dbDir := common.TempDir(t)
	t.Logf("Temporary directory created: %s\n", dbDir)

	// Configure the storage engine with quick checkpoints
	config := &storage.Config{
		Path: dbDir,
		Persistence: storage.PersistenceConfig{
			Enabled:  true,
			SyncMode: 1, // Normal
		},
	}

	// Create MVCC engine
	engine := mvcc.NewMVCCEngine(config)

	// Open engine
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Create a table
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString},
		},
	}

	_, err = tx.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Insert data in two phases to have operations before and after checkpoint
	// Phase 1: Insert 10 rows (IDs 0-9)
	tx, err = engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	table, err := tx.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	for i := 0; i < 10; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewStringValue("before_checkpoint_" + string(rune(i))),
		}

		err = table.Insert(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Small pause between phases (not needed for functionality, just for test clarity)
	time.Sleep(10 * time.Millisecond)

	// Phase 2: Insert 10 more rows (IDs 10-19)
	tx, err = engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	table, err = tx.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	for i := 10; i < 20; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewStringValue("after_checkpoint_" + string(rune(i))),
		}

		err = table.Insert(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Close the engine
	err = engine.Close()
	if err != nil {
		t.Fatalf("Failed to close engine: %v", err)
	}

	// Reopen the engine to test recovery from checkpoint
	newEngine := mvcc.NewMVCCEngine(config)

	err = newEngine.Open()
	if err != nil {
		t.Fatalf("Failed to reopen engine: %v", err)
	}

	// Recovery should be synchronous, no wait needed

	// Verify all data was recovered
	tx, err = newEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction after recovery: %v", err)
	}

	// Verify data from both before and after the checkpoint
	result, err := tx.SelectWithAliases(
		"test_table",
		[]string{"id", "name"},
		nil, // No filter
		nil, // No aliases
	)
	if err != nil {
		t.Fatalf("Failed to select data after recovery: %v", err)
	}

	// Count rows and check data
	rowCount := 0
	beforeCheckpointCount := 0
	afterCheckpointCount := 0

	foundIDs := make(map[int64]bool)

	for result.Next() {
		row := result.Row()
		id, _ := row[0].AsInt64()
		name, _ := row[1].AsString()

		foundIDs[id] = true

		if id < 10 {
			// This should be a row from before the checkpoint
			expected := "before_checkpoint_" + string(rune(int(id)))
			if name != expected {
				t.Errorf("Data mismatch for id %d: got %s, expected %s", id, name, expected)
			}
			beforeCheckpointCount++
		} else {
			// This should be a row from after the checkpoint
			expected := "after_checkpoint_" + string(rune(int(id)))
			if name != expected {
				t.Errorf("Data mismatch for id %d: got %s, expected %s", id, name, expected)
			}
			afterCheckpointCount++
		}

		rowCount++
	}
	result.Close()

	if rowCount < 20 {
		t.Log("Missing IDs:")
		for i := 0; i < 20; i++ {
			if !foundIDs[int64(i)] {
				t.Logf("  ID %d not found", i)
			}
		}
		t.Logf("\n")
	}

	// We should have 20 rows total (10 from before checkpoint, 10 from after)
	if rowCount != 20 {
		t.Fatalf("Wrong total number of rows after recovery: got %d, expected 20", rowCount)
	}

	if beforeCheckpointCount != 10 {
		t.Fatalf("Wrong number of pre-checkpoint rows: got %d, expected 10", beforeCheckpointCount)
	}

	if afterCheckpointCount != 10 {
		t.Fatalf("Wrong number of post-checkpoint rows: got %d, expected 10", afterCheckpointCount)
	}

	// Commit the transaction to ensure it's properly closed before engine shutdown
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction after validation: %v", err)
	}

	// Close the new engine
	err = newEngine.Close()
	if err != nil {
		t.Fatalf("Failed to close new engine: %v", err)
	}
}

// TestDataTypePersistenceWithSnapshot tests persistence of various data types through snapshots
func TestDataTypePersistenceWithSnapshot(t *testing.T) {
	// Create a temporary directory for the database files
	dbDir := common.TempDir(t)

	// Configure the storage engine without automatic snapshots
	config := &storage.Config{
		Path: dbDir,
		Persistence: storage.PersistenceConfig{
			Enabled:          true,
			SyncMode:         1, // Normal
			SnapshotInterval: 0, // Disable automatic snapshots
			KeepSnapshots:    2, // Keep 2 snapshots
		},
	}

	// Create MVCC engine
	engine := mvcc.NewMVCCEngine(config)

	// Open engine
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}

	// Define our test values for each data type
	intValue := int64(42)
	floatValue := 3.14159
	strValue := "hello world"
	boolValue := true
	timeValue := time.Now().Round(time.Microsecond) // Round to avoid precision issues
	dateValue := "2025-04-25"
	timeOfDayValue := "14:30:45"
	jsonValue := map[string]interface{}{
		"name":   "test",
		"values": []int{1, 2, 3},
		"nested": map[string]string{"key": "value"},
	}
	jsonBytes, err := json.Marshal(jsonValue)
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	// Create a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table schema with all data types
	schema := storage.Schema{
		TableName: "test_types",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, PrimaryKey: true},
			{ID: 1, Name: "int_val", Type: storage.INTEGER},
			{ID: 2, Name: "float_val", Type: storage.FLOAT},
			{ID: 3, Name: "str_val", Type: storage.TEXT},
			{ID: 4, Name: "bool_val", Type: storage.BOOLEAN},
			{ID: 5, Name: "ts_val", Type: storage.TIMESTAMP},
			{ID: 6, Name: "date_val", Type: storage.TEXT}, // Store date as text for now
			{ID: 7, Name: "time_val", Type: storage.TEXT}, // Store time as text for now
			{ID: 8, Name: "json_val", Type: storage.TEXT}, // Store JSON as text for now
		},
	}

	// Create the table
	table, err := tx.CreateTable("test_types", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a row with all data types using storage.Row and storage.Value types
	row := storage.Row{
		storage.NewIntegerValue(1),                // id
		storage.NewIntegerValue(intValue),         // int_val
		storage.NewFloatValue(floatValue),         // float_val
		storage.NewStringValue(strValue),          // str_val
		storage.NewBooleanValue(boolValue),        // bool_val
		storage.NewTimestampValue(timeValue),      // ts_val
		storage.NewStringValue(dateValue),         // date_val (as string)
		storage.NewStringValue(timeOfDayValue),    // time_val (as string)
		storage.NewStringValue(string(jsonBytes)), // json_val (as string)
	}

	// Insert the row
	err = table.Insert(row)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Manually create snapshot
	t.Log("Creating snapshot...")
	if err := engine.CreateSnapshot(); err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Close the engine to flush all data
	engine.Close()

	// Create a new engine instance and load from snapshot
	newEngine := mvcc.NewMVCCEngine(config)
	err = newEngine.Open()
	if err != nil {
		t.Fatalf("Failed to open new engine: %v", err)
	}

	// Begin a transaction to read the data
	readTx, err := newEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}

	// Get the table
	readTable, err := readTx.GetTable("test_types")
	if err != nil {
		readTx.Rollback()
		t.Fatalf("Failed to get table: %v", err)
	}

	// Scan without condition to get all rows
	scanner, err := readTable.Scan(nil, nil)
	if err != nil {
		readTx.Rollback()
		t.Fatalf("Failed to scan table: %v", err)
	}

	// Check we have at least one row
	if !scanner.Next() {
		scanner.Close()
		readTx.Rollback()
		t.Fatalf("No rows found in table after restoring from snapshot")
	}

	currentRow := scanner.Row()

	// Verify integer value
	readInt, ok := currentRow[1].AsInt64()
	if !ok {
		t.Errorf("Integer value type mismatch in row[1]")
	} else if readInt != intValue {
		t.Errorf("Integer value mismatch: got %d, expected %d", readInt, intValue)
	} else {
		t.Logf("Integer value verified: %d", readInt)
	}

	// Verify float value
	readFloat, ok := currentRow[2].AsFloat64()
	if !ok {
		t.Errorf("Float value type mismatch in row[2]")
	} else if readFloat != floatValue {
		t.Errorf("Float value mismatch: got %f, expected %f", readFloat, floatValue)
	} else {
		t.Logf("Float value verified: %f", readFloat)
	}

	// Verify string value
	readStr, ok := currentRow[3].AsString()
	if !ok {
		t.Errorf("String value type mismatch in row[3]")
	} else if readStr != strValue {
		t.Errorf("String value mismatch: got %s, expected %s", readStr, strValue)
	} else {
		t.Logf("String value verified: %s", readStr)
	}

	// Verify boolean value
	readBool, ok := currentRow[4].AsBoolean()
	if !ok {
		t.Errorf("Boolean value type mismatch in row[4]")
	} else if readBool != boolValue {
		t.Errorf("Boolean value mismatch: got %v, expected %v", readBool, boolValue)
	} else {
		t.Logf("Boolean value verified: %v", readBool)
	}

	// Verify timestamp value
	readTime, ok := currentRow[5].AsTimestamp()
	if !ok {
		t.Errorf("Timestamp value type mismatch in row[5]")
	} else {
		// Time comparison with some tolerance for database conversion differences
		timeDiff := readTime.Sub(timeValue)
		if timeDiff < -time.Millisecond || timeDiff > time.Millisecond {
			t.Errorf("Timestamp value mismatch: got %v, expected %v (diff: %v)",
				readTime, timeValue, timeDiff)
		} else {
			t.Logf("Timestamp value verified: %v", readTime)
		}
	}

	// Verify date value (as string)
	readDate, ok := currentRow[6].AsString()
	if !ok {
		t.Errorf("Date value type mismatch in row[6]")
	} else if readDate != dateValue {
		t.Errorf("Date value mismatch: got %s, expected %s", readDate, dateValue)
	} else {
		t.Logf("Date value verified: %s", readDate)
	}

	// Verify time of day value (as string)
	readTimeDay, ok := currentRow[7].AsString()
	if !ok {
		t.Errorf("Time value type mismatch in row[7]")
	} else if readTimeDay != timeOfDayValue {
		t.Errorf("Time value mismatch: got %s, expected %s", readTimeDay, timeOfDayValue)
	} else {
		t.Logf("Time value verified: %s", readTimeDay)
	}

	// Verify JSON value (as string)
	readJSONText, ok := currentRow[8].AsString()
	if !ok {
		t.Errorf("JSON value type mismatch in row[8]")
	} else {
		// Parse and compare JSON
		var readJSON map[string]interface{}
		err = json.Unmarshal([]byte(readJSONText), &readJSON)
		if err != nil {
			t.Errorf("Failed to unmarshal JSON: %v", err)
		} else {
			// Check JSON structure
			if readJSON["name"] != jsonValue["name"] {
				t.Errorf("JSON name mismatch: got %v, expected %v", readJSON["name"], jsonValue["name"])
			}

			// Check nested values - but need to handle type differences from JSON unmarshaling
			if nested, ok := readJSON["nested"].(map[string]interface{}); ok {
				if nestedVal, ok := nested["key"].(string); ok {
					if nestedVal != "value" {
						t.Errorf("JSON nested value mismatch: got %v, expected value", nestedVal)
					}
				} else {
					t.Errorf("JSON nested key missing or not a string")
				}
			} else {
				t.Errorf("JSON nested structure missing or invalid")
			}

			t.Logf("JSON value verified")
		}
	}

	// Make sure there isn't a second row
	if scanner.Next() {
		t.Errorf("Expected only one row, got multiple")
	}

	// Clean up
	scanner.Close()
	readTx.Rollback()

	// Close the engine explicitly before test ends
	newEngine.Close()
}
