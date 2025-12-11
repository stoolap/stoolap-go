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
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestMVCCTableDataTypeValidation(t *testing.T) {
	// Create engine and transaction
	config := &storage.Config{
		Path: "memory://",
	}

	engine := mvcc.NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	txn, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn.Rollback()

	// Create a schema with all supported data types
	schema := storage.Schema{
		TableName: "all_types_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "int_col", Type: storage.TypeInteger, Nullable: true},
			{ID: 2, Name: "float_col", Type: storage.TypeFloat, Nullable: true},
			{ID: 3, Name: "string_col", Type: storage.TypeString, Nullable: true},
			{ID: 4, Name: "bool_col", Type: storage.TypeBoolean, Nullable: true},
			{ID: 5, Name: "timestamp_col", Type: storage.TypeTimestamp, Nullable: true},
			{ID: 8, Name: "json_col", Type: storage.TypeJSON, Nullable: true},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create table
	table, err := txn.CreateTable("all_types_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test 1: Insert a valid row with all data types
	now := time.Now()
	validRow := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(validRow)
	if err != nil {
		t.Errorf("Failed to insert valid row: %v", err)
	}

	// Test 2: Integer type mismatch
	invalidIntRow := storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("not an integer"), // Type mismatch
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidIntRow)
	if err == nil {
		t.Errorf("Expected error for integer type mismatch, got nil")
	}

	// Test 3: Float type mismatch
	invalidFloatRow := storage.Row{
		storage.NewIntegerValue(3),
		storage.NewIntegerValue(42),
		storage.NewBooleanValue(true), // Type mismatch
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidFloatRow)
	if err == nil {
		t.Errorf("Expected error for float type mismatch, got nil")
	}

	// Test 4: String type mismatch
	invalidStringRow := storage.Row{
		storage.NewIntegerValue(4),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewIntegerValue(123), // Type mismatch - Integer value in string column
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidStringRow)
	if err == nil {
		t.Errorf("Expected error for string type mismatch, got nil")
	}

	// Test 5: Boolean type mismatch
	invalidBoolRow := storage.Row{
		storage.NewIntegerValue(5),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewIntegerValue(1), // Type mismatch
		storage.NewTimestampValue(now),
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidBoolRow)
	if err == nil {
		t.Errorf("Expected error for boolean type mismatch, got nil")
	}

	// Test 6: Timestamp type mismatch
	invalidTimestampRow := storage.Row{
		storage.NewIntegerValue(6),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewStringValue("not a timestamp"), // Type mismatch
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidTimestampRow)
	if err == nil {
		t.Errorf("Expected error for timestamp type mismatch, got nil")
	}

	// Test 7: Date type mismatch
	invalidDateRow := storage.Row{
		storage.NewIntegerValue(7),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewIntegerValue(20230101), // Type mismatch
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidDateRow)
	if err == nil {
		t.Errorf("Expected error for date type mismatch, got nil")
	}

	// Test 8: Time type mismatch
	invalidTimeRow := storage.Row{
		storage.NewIntegerValue(8),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewIntegerValue(1200), // Type mismatch
		storage.NewJSONValue(`{"key":"value"}`),
	}

	err = table.Insert(invalidTimeRow)
	if err == nil {
		t.Errorf("Expected error for time type mismatch, got nil")
	}

	// Test 9: JSON type mismatch
	invalidJSONRow := storage.Row{
		storage.NewIntegerValue(9),
		storage.NewIntegerValue(42),
		storage.NewFloatValue(3.14),
		storage.NewStringValue("hello"),
		storage.NewBooleanValue(true),
		storage.NewTimestampValue(now),
		storage.NewBooleanValue(false), // Type mismatch
	}

	err = table.Insert(invalidJSONRow)
	if err == nil {
		t.Errorf("Expected error for JSON type mismatch, got nil")
	}

	// Test 10: All NULL values (should be valid since all columns except ID are nullable)
	nullRow := storage.Row{
		storage.NewIntegerValue(10),
		storage.NewNullIntegerValue(),
		storage.NewNullFloatValue(),
		storage.NewNullStringValue(),
		storage.NewNullBooleanValue(),
		storage.NewNullTimestampValue(),
		storage.NewNullJSONValue(),
	}

	err = table.Insert(nullRow)
	if err != nil {
		t.Errorf("Failed to insert row with NULL values: %v", err)
	}

	// Final verification - we should still have exactly 2 rows (the valid ones)
	finalScanner, err := table.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create final scanner: %v", err)
	}
	defer finalScanner.Close()

	finalRowCount := 0
	for finalScanner.Next() {
		finalRowCount++
	}

	if finalRowCount != 2 {
		t.Errorf("Expected 2 valid rows in final count, got %d", finalRowCount)
	}
}
