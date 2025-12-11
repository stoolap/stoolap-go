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
)

// This test validates the basic column value functionality
func TestColumnValues(t *testing.T) {
	// Test integer values
	intValue := storage.NewIntegerValue(42)
	if intValue.Type() != storage.INTEGER {
		t.Errorf("Expected TypeInteger, got %v", intValue.Type())
	}
	if intValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if val, ok := intValue.AsInt64(); !ok || val != 42 {
		t.Errorf("Expected int value 42, got %v (ok=%v)", val, ok)
	}

	// Test null integer
	nullInt := storage.NewNullIntegerValue()
	if nullInt.Type() != storage.INTEGER {
		t.Errorf("Expected TypeInteger for null, got %v", nullInt.Type())
	}
	if !nullInt.IsNull() {
		t.Errorf("Expected null value")
	}

	// Test string values
	strValue := storage.NewStringValue("hello")
	if strValue.Type() != storage.TEXT {
		t.Errorf("Expected TypeString, got %v", strValue.Type())
	}
	if strValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if val, ok := strValue.AsString(); !ok || val != "hello" {
		t.Errorf("Expected string value 'hello', got %v (ok=%v)", val, ok)
	}

	// Test null string
	nullStr := storage.NewNullStringValue()
	if nullStr.Type() != storage.TEXT {
		t.Errorf("Expected TypeString for null, got %v", nullStr.Type())
	}
	if !nullStr.IsNull() {
		t.Errorf("Expected null value")
	}

	// Test float values
	floatValue := storage.NewFloatValue(3.14)
	if floatValue.Type() != storage.FLOAT {
		t.Errorf("Expected TypeFloat, got %v", floatValue.Type())
	}
	if floatValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if val, ok := floatValue.AsFloat64(); !ok || val != 3.14 {
		t.Errorf("Expected float value 3.14, got %v (ok=%v)", val, ok)
	}

	// Test boolean values
	boolValue := storage.NewBooleanValue(true)
	if boolValue.Type() != storage.BOOLEAN {
		t.Errorf("Expected TypeBoolean, got %v", boolValue.Type())
	}
	if boolValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if val, ok := boolValue.AsBoolean(); !ok || !val {
		t.Errorf("Expected boolean value true, got %v (ok=%v)", val, ok)
	}

	// Test date/time values
	now := time.Now()
	timestampValue := storage.NewTimestampValue(now)
	if timestampValue.Type() != storage.TIMESTAMP {
		t.Errorf("Expected TypeTimestamp, got %v", timestampValue.Type())
	}
	if timestampValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if _, ok := timestampValue.AsTimestamp(); !ok {
		t.Errorf("Failed to get timestamp value (ok=%v)", ok)
	}

	// Test JSON values
	jsonValue := storage.NewJSONValue(`{"name":"test","value":123}`)
	if jsonValue.Type() != storage.JSON {
		t.Errorf("Expected TypeJSON, got %v", jsonValue.Type())
	}
	if jsonValue.IsNull() {
		t.Errorf("Expected non-null value")
	}
	if val, ok := jsonValue.AsJSON(); !ok || val != `{"name":"test","value":123}` {
		t.Errorf("Expected JSON string, got %v (ok=%v)", val, ok)
	}
}

// Test row operations
func TestRows(t *testing.T) {
	// Create a row with mixed value types
	row := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("test"),
		storage.NewBooleanValue(true),
		storage.NewFloatValue(3.14),
		storage.NewNullIntegerValue(),
	}

	// Verify row length
	if len(row) != 5 {
		t.Errorf("Expected row length 5, got %d", len(row))
	}

	// Verify individual values
	if id, ok := row[0].AsInt64(); !ok || id != 1 {
		t.Errorf("Row[0]: Expected id=1, got %v (ok=%v)", id, ok)
	}

	if name, ok := row[1].AsString(); !ok || name != "test" {
		t.Errorf("Row[1]: Expected name='test', got %v (ok=%v)", name, ok)
	}

	if active, ok := row[2].AsBoolean(); !ok || !active {
		t.Errorf("Row[2]: Expected active=true, got %v (ok=%v)", active, ok)
	}

	if value, ok := row[3].AsFloat64(); !ok || value != 3.14 {
		t.Errorf("Row[3]: Expected value=3.14, got %v (ok=%v)", value, ok)
	}

	if !row[4].IsNull() {
		t.Errorf("Row[4]: Expected NULL value")
	}
}
