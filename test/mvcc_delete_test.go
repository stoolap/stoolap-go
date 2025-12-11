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
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestMVCCDeleteWithComplexWhere tests the DELETE operation with complex WHERE clauses
func TestMVCCDeleteWithComplexWhere(t *testing.T) {
	// Create the engine
	engine := mvcc.NewMVCCEngine(&storage.Config{
		Path: "memory://",
	})

	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Initialize a schema for our test table
	schema := storage.Schema{
		TableName: "delete_test",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "category", Type: storage.TypeString, Nullable: false},
			{ID: 2, Name: "value", Type: storage.TypeInteger, Nullable: false},
			{ID: 3, Name: "active", Type: storage.TypeBoolean, Nullable: false},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Transaction 1: Create the table
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Create the test table
	table1, err := tx1.CreateTable("delete_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows with different characteristics
	rows := []storage.Row{
		{storage.NewIntegerValue(1), storage.NewStringValue("A"), storage.NewIntegerValue(10), storage.NewBooleanValue(true)},
		{storage.NewIntegerValue(2), storage.NewStringValue("A"), storage.NewIntegerValue(20), storage.NewBooleanValue(true)},
		{storage.NewIntegerValue(3), storage.NewStringValue("B"), storage.NewIntegerValue(30), storage.NewBooleanValue(true)},
		{storage.NewIntegerValue(4), storage.NewStringValue("B"), storage.NewIntegerValue(40), storage.NewBooleanValue(false)},
		{storage.NewIntegerValue(5), storage.NewStringValue("C"), storage.NewIntegerValue(50), storage.NewBooleanValue(true)},
		{storage.NewIntegerValue(6), storage.NewStringValue("C"), storage.NewIntegerValue(60), storage.NewBooleanValue(false)},
	}

	for _, row := range rows {
		if err := table1.Insert(row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Commit transaction 1
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Test Case 1: Delete with simple equality condition
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	table2, err := tx2.GetTable("delete_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 2: %v", err)
	}

	// Delete where category = 'A'
	expr1 := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		category, _ := row[1].AsString()
		return category == "A", nil
	})

	deleteCount1, err := table2.Delete(expr1)
	if err != nil {
		t.Fatalf("Failed to delete rows in transaction 2: %v", err)
	}

	if deleteCount1 != 2 {
		t.Errorf("Expected 2 rows deleted with category = 'A', got %d", deleteCount1)
	}

	// Verify the deleted rows
	scanner2, err := table2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 2: %v", err)
	}

	rowCount2 := 0
	for scanner2.Next() {
		row := scanner2.Row()
		category, _ := row[1].AsString()
		if category == "A" {
			t.Errorf("Found row with category 'A' that should have been deleted")
		}
		rowCount2++
	}
	scanner2.Close()

	if rowCount2 != 4 {
		t.Errorf("Expected 4 rows remaining after delete (6 - 2), got %d", rowCount2)
	}

	// Commit transaction 2
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Test Case 2: Delete with complex conditions (AND, comparison operators)
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	table3, err := tx3.GetTable("delete_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 3: %v", err)
	}

	// Delete where category = 'B' AND value > 30
	expr2 := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		category, _ := row[1].AsString()
		value, _ := row[2].AsInt64()
		return category == "B" && value > 30, nil
	})

	deleteCount2, err := table3.Delete(expr2)
	if err != nil {
		t.Fatalf("Failed to delete rows in transaction 3: %v", err)
	}

	if deleteCount2 != 1 {
		t.Errorf("Expected 1 row deleted with category = 'B' AND value > 30, got %d", deleteCount2)
	}

	// Verify the deleted rows
	scanner3, err := table3.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 3: %v", err)
	}

	rowCount3 := 0
	categoryBWithHighValue := false
	for scanner3.Next() {
		row := scanner3.Row()
		category, _ := row[1].AsString()
		value, _ := row[2].AsInt64()
		if category == "B" && value > 30 {
			categoryBWithHighValue = true
		}
		rowCount3++
	}
	scanner3.Close()

	if categoryBWithHighValue {
		t.Errorf("Found row with category 'B' and value > 30 that should have been deleted")
	}

	if rowCount3 != 3 {
		t.Errorf("Expected 3 rows remaining after delete (4 - 1), got %d", rowCount3)
	}

	// Commit transaction 3
	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 3: %v", err)
	}

	// Test Case 3: Delete with complex conditions (OR, NOT)
	tx4, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	table4, err := tx4.GetTable("delete_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 4: %v", err)
	}

	// Count how many rows we start with
	startScanner, err := table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table before deletion: %v", err)
	}

	startRowCount := 0
	t.Logf("Rows before category = 'C' OR active = false deletion:")
	for startScanner.Next() {
		row := startScanner.Row()
		id, _ := row[0].AsInt64()
		category, _ := row[1].AsString()
		value, _ := row[2].AsInt64()
		active, _ := row[3].AsBoolean()
		t.Logf("  Row: ID=%d, category=%s, value=%d, active=%v", id, category, value, active)
		startRowCount++
	}
	startScanner.Close()
	t.Logf("Starting with %d rows before last delete", startRowCount)

	// Delete where (category = 'C' OR active = false)
	expr3 := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		category, _ := row[1].AsString()
		active, _ := row[3].AsBoolean()
		shouldDelete := category == "C" || !active
		t.Logf("Delete evaluation: ID=%v, category=%s, active=%v, shouldDelete=%v",
			row[0], category, active, shouldDelete)
		return shouldDelete, nil
	})

	deleteCount3, err := table4.Delete(expr3)
	if err != nil {
		t.Fatalf("Failed to delete rows in transaction 4: %v", err)
	}

	// We expect to delete rows with category C (which should be 2) OR active=false (there should be 0 left)
	expectedDelete := 2
	if deleteCount3 != expectedDelete {
		t.Errorf("Expected %d rows deleted with category = 'C' OR active = false, got %d",
			expectedDelete, deleteCount3)
	}

	// Verify the deleted rows
	scanner4, err := table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 4: %v", err)
	}

	rowCount4 := 0
	t.Logf("Remaining rows after category = 'C' OR active = false deletion:")
	for scanner4.Next() {
		row := scanner4.Row()
		id, _ := row[0].AsInt64()
		category, _ := row[1].AsString()
		value, _ := row[2].AsInt64()
		active, _ := row[3].AsBoolean()
		t.Logf("  Row: ID=%d, category=%s, value=%d, active=%v", id, category, value, active)

		if category == "C" || !active {
			t.Errorf("Found row that should have been deleted: ID=%d, category=%s, value=%d, active=%v",
				id, category, value, active)
		}
		rowCount4++
	}
	scanner4.Close()

	// We expect 1 row to remain (ID=3, category=B, active=true)
	expectedRemaining := 1
	if rowCount4 != expectedRemaining {
		t.Errorf("Expected %d row remaining after delete, got %d", expectedRemaining, rowCount4)
	}

	// Commit transaction 4
	if err := tx4.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 4: %v", err)
	}

	// Final verification with a new transaction
	tx5, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 5: %v", err)
	}

	table5, err := tx5.GetTable("delete_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 5: %v", err)
	}

	scanner5, err := table5.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 5: %v", err)
	}

	rowCount5 := 0
	for scanner5.Next() {
		rowCount5++
	}
	scanner5.Close()

	if rowCount5 != 1 {
		t.Errorf("Expected 1 row in final state after all deletes, got %d", rowCount5)
	}

	// Clean up
	tx5.Rollback()
}
