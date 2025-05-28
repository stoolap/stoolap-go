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
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/storage"
)

// testColumnValue is a simple implementation of storage.ColumnValue for testing
type testColumnValue struct {
	dataType    storage.DataType
	intValue    int64
	floatValue  float64
	stringValue string
	boolValue   bool
	timeValue   time.Time
	isNil       bool
}

func (v *testColumnValue) Type() storage.DataType {
	return v.dataType
}

func (v *testColumnValue) IsNull() bool {
	return v.isNil
}

func (v *testColumnValue) AsInt64() (int64, bool) {
	if v.isNil || v.dataType != storage.INTEGER {
		return 0, false
	}
	return v.intValue, true
}

func (v *testColumnValue) AsFloat64() (float64, bool) {
	if v.isNil || v.dataType != storage.FLOAT {
		return 0, false
	}
	return v.floatValue, true
}

func (v *testColumnValue) AsBoolean() (bool, bool) {
	if v.isNil || v.dataType != storage.BOOLEAN {
		return false, false
	}
	return v.boolValue, true
}

func (v *testColumnValue) AsString() (string, bool) {
	if v.isNil || v.dataType != storage.TEXT {
		return "", false
	}
	return v.stringValue, true
}

func (v *testColumnValue) AsTimestamp() (time.Time, bool) {
	if v.isNil || v.dataType != storage.TIMESTAMP {
		return time.Time{}, false
	}
	return v.timeValue, true
}

func (v *testColumnValue) AsJSON() (string, bool) {
	if v.isNil || v.dataType != storage.JSON {
		return "", false
	}
	return v.stringValue, true
}

func (v *testColumnValue) AsInterface() interface{} {
	if v.isNil {
		return nil
	}

	switch v.dataType {
	case storage.INTEGER:
		return v.intValue
	case storage.FLOAT:
		return v.floatValue
	case storage.TEXT:
		return v.stringValue
	case storage.BOOLEAN:
		return v.boolValue
	case storage.TIMESTAMP:
		return v.timeValue
	case storage.JSON:
		return v.stringValue
	default:
		return nil
	}
}

func (v *testColumnValue) Equals(val storage.ColumnValue) bool {
	return false
}

func (v *testColumnValue) Compare(val storage.ColumnValue) (int, error) {
	return 0, nil
}

func TestSimpleParameter(t *testing.T) {
	// Create a memory engine for testing
	t.Log("Opening database connection...")
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	t.Log("Database connection opened successfully")
	defer db.Close()

	// Debug - verify engine is not nil
	if db == nil {
		t.Fatal("db is nil")
	}

	engine := db.Engine()
	if engine == nil {
		t.Fatal("engine is nil")
	}

	// Get the executor and verify it's not nil
	exec := db.Executor()
	if exec == nil {
		t.Fatal("executor is nil")
	}

	// Try directly with the engine
	t.Log("Trying direct transaction with the engine")
	tx, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Begin the transaction
	err = tx.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction context: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err = tx.CreateTable("test_table", schema)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Begin another transaction using the engine
	tx, err = engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Begin the transaction
	err = tx.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction context: %v", err)
	}

	// Get the table first
	table, err := tx.GetTable("test_table")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to get table: %v", err)
	}

	// Create a proper row with a ColumnValue that implements the interface
	// This is a simplified test value
	row := make(storage.Row, 1)
	// Use a test-specific implementation for the column value
	columnValue := &testColumnValue{
		dataType: storage.INTEGER,
		intValue: 42,
		isNil:    false,
	}
	row[0] = columnValue

	// Insert the row
	err = table.Insert(row)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	// Prepare parameter using the correct driver.NamedValue type
	idParam := []driver.NamedValue{
		{
			Name:    "",        // Use positional parameters
			Ordinal: 1,         // First parameter
			Value:   int64(42), // Using int64 for consistency
		},
	}

	// Try to use ExecuteWithParams to query with a parameter
	result, err := exec.ExecuteWithParams(context.Background(), nil,
		"SELECT * FROM test_table WHERE id = ?", idParam)

	if err != nil {
		t.Fatalf("Error using SQL executor with parameters: %v", err)
	} else {
		// Check if we have a result
		if result != nil {
			defer result.Close()

			// Check if we found the row
			if result.Next() {
				var id int
				err = result.Scan(&id)
				if err != nil {
					t.Fatalf("Error scanning result: %v", err)
				} else if id == 42 {
					t.Log("Successfully read row with id=42 using parameter binding")
				} else {
					t.Fatalf("Unexpected ID value: %d", id)
				}
			} else {
				t.Log("No rows found in result")
			}
		} else {
			t.Log("Result is nil")
		}
	}
}
