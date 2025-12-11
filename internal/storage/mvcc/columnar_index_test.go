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
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

func TestColumnarIndex_Drop(t *testing.T) {
	// Setup: Create engine and table with index
	config := &storage.Config{}
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create schema
	schema := storage.Schema{
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER, PrimaryKey: true},
			{Name: "value", Type: storage.INTEGER, PrimaryKey: false},
		},
		TableName: "test_drop",
	}

	// Begin transaction and create table
	txnInterface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	txn := txnInterface.(*MVCCTransaction)

	_, err = txn.CreateTable("test_drop", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit first transaction
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create index in second transaction
	txn2Interface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}
	txn2 := txn2Interface.(*MVCCTransaction)

	err = txn2.CreateTableColumnarIndex("test_drop", "value", false)
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	// Test dropping the index
	txn3Interface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin third transaction: %v", err)
	}
	txn3 := txn3Interface.(*MVCCTransaction)

	// Drop the index
	err = txn3.DropTableColumnarIndex("test_drop", "value")
	if err != nil {
		t.Fatalf("Failed to drop columnar index: %v", err)
	}

	// Commit third transaction
	err = txn3.Commit()
	if err != nil {
		t.Fatalf("Failed to commit third transaction: %v", err)
	}

	// Verify the index is gone
	txn4Interface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin fourth transaction: %v", err)
	}
	txn4 := txn4Interface.(*MVCCTransaction)

	// Get the table
	table, err := txn4.GetTable("test_drop")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Cast to MVCCTable
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		t.Fatal("Table is not an MVCCTable")
	}

	// Try to get the dropped index - should fail
	_, err = mvccTable.GetColumnarIndex("value")
	if err == nil {
		t.Fatal("Expected error getting dropped index, but got nil")
	}

	// Cleanup
	txn4.Rollback()
}

func TestColumnarIndex_Basic(t *testing.T) {
	// Create a properly initialized engine for the test
	config := &storage.Config{}
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a version store with the engine
	vs := NewVersionStore("test_table", engine)

	// Create a columnar index
	index := NewColumnarIndex(
		"test_index",
		"test_table",
		"test_column",
		0,
		storage.INTEGER,
		vs,
		false, // isUnique
	)

	// Test adding values
	for i := int64(1); i <= 10; i++ {
		value := storage.NewIntegerValue(i)
		err := index.Add([]storage.ColumnValue{value}, i, 0)
		if err != nil {
			t.Fatalf("Failed to add value %d: %v", i, err)
		}
	}

	// Test finding values
	result, err := index.Find([]storage.ColumnValue{storage.NewIntegerValue(5)})
	if err != nil {
		t.Fatalf("Error finding value: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}

	if result[0].RowID != 5 {
		t.Fatalf("Expected row ID 5, got %d", result[0].RowID)
	}

	// Test range queries
	rangeResult, err := index.FindRange(
		[]storage.ColumnValue{storage.NewIntegerValue(3)},
		[]storage.ColumnValue{storage.NewIntegerValue(7)},
		true, // includeMin
		true, // includeMax
	)

	if err != nil {
		t.Fatalf("Error finding range: %v", err)
	}

	if len(rangeResult) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(rangeResult))
	}

	// Check that rows 3, 4, 5, 6, 7 are in the results
	foundRows := make(map[int64]bool)
	for _, entry := range rangeResult {
		foundRows[entry.RowID] = true
	}

	for i := int64(3); i <= 7; i++ {
		if !foundRows[i] {
			t.Fatalf("Expected to find row ID %d in results", i)
		}
	}
}

func TestColumnarIndex_UniqueConstraint(t *testing.T) {
	// Setup: Create engine and table with unique columnar index
	config := &storage.Config{}
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create schema with different data types to test
	schema := storage.Schema{
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER, PrimaryKey: true},
			{Name: "int_val", Type: storage.INTEGER, PrimaryKey: false, Nullable: true},
			{Name: "float_val", Type: storage.FLOAT, PrimaryKey: false, Nullable: true},
			{Name: "text_val", Type: storage.TEXT, PrimaryKey: false, Nullable: true},
			{Name: "bool_val", Type: storage.BOOLEAN, PrimaryKey: false, Nullable: true},
			{Name: "ts_val", Type: storage.TIMESTAMP, PrimaryKey: false, Nullable: true},
		},
		TableName: "test_unique",
	}

	// Begin transaction and create table
	txnInterface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	txn := txnInterface.(*MVCCTransaction)

	tbl, err := txn.CreateTable("test_unique", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	table := tbl.(*MVCCTable)

	// Create unique columnar indexes on each column
	err = table.CreateColumnarIndex("int_val", true)
	if err != nil {
		t.Fatalf("Failed to create int columnar index: %v", err)
	}

	err = table.CreateColumnarIndex("float_val", true)
	if err != nil {
		t.Fatalf("Failed to create float columnar index: %v", err)
	}

	err = table.CreateColumnarIndex("text_val", true)
	if err != nil {
		t.Fatalf("Failed to create text columnar index: %v", err)
	}

	err = table.CreateColumnarIndex("bool_val", true)
	if err != nil {
		t.Fatalf("Failed to create boolean columnar index: %v", err)
	}

	err = table.CreateColumnarIndex("ts_val", true)
	if err != nil {
		t.Fatalf("Failed to create timestamp columnar index: %v", err)
	}

	// Insert test data - first row should succeed
	now := time.Now()
	row1 := []storage.ColumnValue{
		storage.NewIntegerValue(1),     // id
		storage.NewIntegerValue(100),   // int_val
		storage.NewFloatValue(3.14),    // float_val
		storage.NewStringValue("test"), // text_val
		storage.NewBooleanValue(true),  // bool_val
		storage.NewTimestampValue(now), // ts_val
	}

	err = table.Insert(row1)
	if err != nil {
		t.Fatalf("Failed to insert first row: %v", err)
	}

	// Test unique constraint for INTEGER
	row2 := []storage.ColumnValue{
		storage.NewIntegerValue(2),                    // id
		storage.NewIntegerValue(100),                  // int_val - DUPLICATE
		storage.NewFloatValue(2.71),                   // float_val
		storage.NewStringValue("unique"),              // text_val
		storage.NewBooleanValue(false),                // bool_val
		storage.NewTimestampValue(now.Add(time.Hour)), // ts_val
	}

	var uniqueErr *storage.ErrUniqueConstraint

	err = table.Insert(row2)
	if err == nil {
		t.Errorf("Expected unique constraint violation for integer column, but insertion succeeded")
	} else if !errors.As(err, &uniqueErr) {
		t.Errorf("Expected ErrUniqueConstraint, got: %v", err)
	}

	// Test unique constraint for FLOAT
	row3 := []storage.ColumnValue{
		storage.NewIntegerValue(3),                        // id
		storage.NewIntegerValue(300),                      // int_val
		storage.NewFloatValue(3.14),                       // float_val - DUPLICATE
		storage.NewStringValue("third"),                   // text_val
		storage.NewBooleanValue(true),                     // bool_val
		storage.NewTimestampValue(now.Add(2 * time.Hour)), // ts_val
	}

	err = table.Insert(row3)
	if err == nil {
		t.Errorf("Expected unique constraint violation for float column, but insertion succeeded")
	} else if !errors.As(err, &uniqueErr) {
		t.Errorf("Expected ErrUniqueConstraint, got: %v", err)
	}

	// Test unique constraint for TEXT
	row4 := []storage.ColumnValue{
		storage.NewIntegerValue(4),                        // id
		storage.NewIntegerValue(400),                      // int_val
		storage.NewFloatValue(4.44),                       // float_val
		storage.NewStringValue("test"),                    // text_val - DUPLICATE
		storage.NewBooleanValue(false),                    // bool_val
		storage.NewTimestampValue(now.Add(3 * time.Hour)), // ts_val
	}

	err = table.Insert(row4)
	if err == nil {
		t.Errorf("Expected unique constraint violation for text column, but insertion succeeded")
	} else if !errors.As(err, &uniqueErr) {
		t.Errorf("Expected ErrUniqueConstraint, got: %v", err)
	}

	// Test unique constraint for TIMESTAMP
	row5 := []storage.ColumnValue{
		storage.NewIntegerValue(5),      // id
		storage.NewIntegerValue(500),    // int_val
		storage.NewFloatValue(5.55),     // float_val
		storage.NewStringValue("fifth"), // text_val
		storage.NewBooleanValue(true),   // bool_val
		storage.NewTimestampValue(now),  // ts_val - DUPLICATE
	}

	err = table.Insert(row5)
	if err == nil {
		t.Errorf("Expected unique constraint violation for timestamp column, but insertion succeeded")
	} else if !errors.As(err, &uniqueErr) {
		t.Errorf("Expected ErrUniqueConstraint, got: %v", err)
	}

	// Test that NULL values are allowed in unique columns (multiple NULLs should be allowed)
	row6 := []storage.ColumnValue{
		storage.NewIntegerValue(6),              // id
		storage.NewNullValue(storage.INTEGER),   // int_val - NULL
		storage.NewNullValue(storage.FLOAT),     // float_val - NULL
		storage.NewNullValue(storage.TEXT),      // text_val - NULL
		storage.NewNullValue(storage.BOOLEAN),   // bool_val - NULL
		storage.NewNullValue(storage.TIMESTAMP), // ts_val - NULL
	}

	err = table.Insert(row6)
	if err != nil {
		t.Fatalf("Failed to insert NULL row: %v", err)
	}

	// Insert another row with NULLs to verify multiple NULLs are allowed in unique columns
	row7 := []storage.ColumnValue{
		storage.NewIntegerValue(7),              // id
		storage.NewNullValue(storage.INTEGER),   // int_val - NULL
		storage.NewNullValue(storage.FLOAT),     // float_val - NULL
		storage.NewNullValue(storage.TEXT),      // text_val - NULL
		storage.NewNullValue(storage.BOOLEAN),   // bool_val - NULL
		storage.NewNullValue(storage.TIMESTAMP), // ts_val - NULL
	}

	err = table.Insert(row7)
	if err != nil {
		t.Fatalf("Failed to insert second NULL row: %v", err)
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestColumnarIndex_Integration(t *testing.T) {
	// Create a schema with two INTEGER columns (value is not a primary key)
	schema := storage.Schema{
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER, PrimaryKey: true},
			{Name: "value", Type: storage.INTEGER, PrimaryKey: false},
		},
		TableName: "test_integration",
	}

	// Create engine and transaction
	config := &storage.Config{}
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	txnInterface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	txn := txnInterface.(*MVCCTransaction)

	// Create table through transaction
	table, err := txn.CreateTable("test_integration", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Cast to MVCCTable for direct access
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		t.Fatal("Table is not an MVCCTable")
	}

	// Insert some data through the transaction table
	for i := 1; i <= 10; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),      // id
			storage.NewIntegerValue(int64(i * 10)), // value
		}
		err := mvccTable.Insert(row)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Start a new transaction for creating the index
	txn2Interface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}
	txn2 := txn2Interface.(*MVCCTransaction)

	// Get the table in the new transaction
	_, err = txn2.GetTable("test_integration")
	if err != nil {
		t.Fatalf("Failed to get table in second transaction: %v", err)
	}

	// Create columnar index using the transaction method
	err = txn2.CreateTableColumnarIndex("test_integration", "value", false)
	if err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Commit the second transaction
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	// Create a new transaction to access the table with index
	txn3Interface, err := engine.BeginTx(context.Background(), sql.LevelReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin third transaction: %v", err)
	}
	txn3 := txn3Interface.(*MVCCTransaction)

	// Get the table
	table3, err := txn3.GetTable("test_integration")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Cast to MVCCTable for direct access
	mvccTable3, ok := table3.(*MVCCTable)
	if !ok {
		t.Fatal("Table is not an MVCCTable")
	}

	// Get columnar index
	index, err := mvccTable3.GetColumnarIndex("value")
	if err != nil {
		t.Fatalf("Failed to get columnar index: %v", err)
	}

	// Verify index existence
	if index == nil {
		t.Fatal("Expected index to be created, but got nil")
	}

	// Test using both serializeValue and direct match
	searchValue := storage.NewIntegerValue(50) // value for id=5

	// Test the index by finding a specific value
	result, err := index.Find([]storage.ColumnValue{searchValue})
	if err != nil {
		t.Fatalf("Error finding value: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("Expected at least one result, got none")
	}

	// Rollback the transaction (cleanup)
	txn3.Rollback()
}
