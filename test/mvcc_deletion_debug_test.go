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

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/expression"
	"github.com/stoolap/stoolap/internal/storage/mvcc"
)

// TestMVCCDeletionDebug tests deletion visibility with detailed logging
func TestMVCCDeletionDebug(t *testing.T) {
	// Create engine
	engine := mvcc.NewMVCCEngine(&storage.Config{
		Path: "memory://",
	})
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Set SNAPSHOT isolation
	if err := engine.SetIsolationLevel(storage.SnapshotIsolation); err != nil {
		t.Fatalf("Failed to set isolation level: %v", err)
	}

	// Create table in a transaction
	txCreate, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin create transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TEXT, Nullable: false},
		},
	}

	_, err = txCreate.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := txCreate.Commit(); err != nil {
		t.Fatalf("Failed to commit table creation: %v", err)
	}

	// Insert initial data
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin TX1: %v", err)
	}
	t.Logf("TX1 (inserter) ID: %d", tx1.ID())

	table1, err := tx1.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Insert two rows
	err = table1.Insert(storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("one"),
	})
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}

	err = table1.Insert(storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("two"),
	})
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit TX1: %v", err)
	}
	t.Log("TX1 committed")

	// Start reader transaction BEFORE deletion
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin TX2: %v", err)
	}
	t.Logf("TX2 (reader) ID: %d", tx2.ID())

	table2, err := tx2.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Count rows before deletion
	rows2, err := table2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	count := 0
	for rows2.Next() {
		count++
		row := rows2.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()
		t.Logf("TX2 sees row: id=%d, value=%s", id, value)
	}
	rows2.Close()
	t.Logf("TX2 initial count: %d rows", count)

	// Start deleter transaction
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin TX3: %v", err)
	}
	t.Logf("TX3 (deleter) ID: %d", tx3.ID())

	table3, err := tx3.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Delete row 2
	deleteExpr := expression.NewSimpleExpression(
		"id",
		storage.EQ,
		storage.NewIntegerValue(2),
	)
	// Get schema and prepare expression
	tableSchema := table3.Schema()
	preparedDeleteExpr := deleteExpr.PrepareForSchema(tableSchema)

	deleted, err := table3.Delete(preparedDeleteExpr)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	t.Logf("TX3 deleted %d rows", deleted)

	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit TX3: %v", err)
	}
	t.Log("TX3 committed deletion")

	// TX2 should still see both rows in SNAPSHOT isolation
	rows2Again, err := table2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan again: %v", err)
	}
	count2 := 0
	for rows2Again.Next() {
		count2++
		row := rows2Again.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()
		t.Logf("TX2 still sees row: id=%d, value=%s", id, value)
	}
	rows2Again.Close()
	t.Logf("TX2 count after deletion: %d rows", count2)

	if count2 != 2 {
		t.Errorf("SNAPSHOT: TX2 should still see 2 rows, got %d", count2)
	}

	// Specifically check if TX2 can see the deleted row
	scanExpr := expression.NewSimpleExpression(
		"id",
		storage.EQ,
		storage.NewIntegerValue(2),
	)
	tableSchema2 := table2.Schema()
	preparedScanExpr := scanExpr.PrepareForSchema(tableSchema2)

	rows2Specific, err := table2.Scan(nil, preparedScanExpr)
	if err != nil {
		t.Fatalf("Failed to scan for specific row: %v", err)
	}
	foundDeleted := false
	for rows2Specific.Next() {
		foundDeleted = true
		row := rows2Specific.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()
		t.Logf("TX2 can see deleted row: id=%d, value=%s", id, value)
	}
	rows2Specific.Close()

	if !foundDeleted {
		t.Error("SNAPSHOT: TX2 should still see the deleted row with id=2")
	}

	tx2.Commit()

	// New transaction should NOT see the deleted row
	tx4, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin TX4: %v", err)
	}
	t.Logf("TX4 (new reader) ID: %d", tx4.ID())

	table4, err := tx4.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	rows4, err := table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	count4 := 0
	for rows4.Next() {
		count4++
		row := rows4.Row()
		id, _ := row[0].AsInt64()
		value, _ := row[1].AsString()
		t.Logf("TX4 sees row: id=%d, value=%s", id, value)
	}
	rows4.Close()
	t.Logf("TX4 count: %d rows", count4)

	if count4 != 1 {
		t.Errorf("New transaction should see 1 row after deletion, got %d", count4)
	}

	tx4.Commit()
}
