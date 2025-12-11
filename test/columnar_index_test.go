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
	"strconv"
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

func TestColumnarIndex(t *testing.T) {
	// Create a test config
	config := &storage.Config{
		Path: "memory://",
	}

	// Create a test engine
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test table
	schema := storage.Schema{
		TableName: "test_columnar_index",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.TypeInteger, PrimaryKey: true, Nullable: false},
			{Name: "name", Type: storage.TypeString, Nullable: false},
			{Name: "age", Type: storage.TypeInteger, Nullable: false},
			{Name: "enabled", Type: storage.TypeBoolean, Nullable: false},
		},
	}

	table, err := tx.CreateTable("test_columnar_index", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),                 // id
			storage.NewStringValue("user_" + strconv.Itoa(i)), // name
			storage.NewIntegerValue(int64(20 + i%30)),         // age
			storage.NewBooleanValue(i%2 == 0),                 // enabled
		}
		if err := table.Insert(row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create columnar indexes
	mvccTable, ok := table.(*mvcc.MVCCTable)
	if !ok {
		t.Fatalf("Expected MVCCTableWrapper, got %T", table)
	}

	if err := mvccTable.CreateColumnarIndex("age", false); err != nil {
		t.Fatalf("Failed to create columnar index on age: %v", err)
	}

	if err := mvccTable.CreateColumnarIndex("enabled", false); err != nil {
		t.Fatalf("Failed to create columnar index on enabled: %v", err)
	}

	// Create a filter for age = 25
	ageExpr := expression.NewSimpleExpression("age", storage.EQ, 25)
	ageExpr.PrepareForSchema(schema)

	// Test querying with columnar index
	scanner, err := mvccTable.Scan(nil, ageExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count rows that match age = 25
	ageMatches := 0
	for scanner.Next() {
		row := scanner.Row()
		val, ok := row[2].AsInt64()
		if ok && val == 25 {
			ageMatches++
		}
	}

	// Calculate the expected number of rows with age = 25
	expectedAgeMatches := 0
	for i := 1; i <= 100; i++ {
		if (20 + i%30) == 25 {
			expectedAgeMatches++
		}
	}

	if ageMatches != expectedAgeMatches {
		t.Errorf("Expected %d rows with age=25, got %d", expectedAgeMatches, ageMatches)
	}

	// Test with enabled = true
	enabledExpr := expression.NewSimpleExpression("enabled", storage.EQ, true)
	enabledExpr.PrepareForSchema(schema)

	scanner, err = mvccTable.Scan(nil, enabledExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count rows that match enabled = true
	enabledTrueCount := 0
	for scanner.Next() {
		row := scanner.Row()
		boolVal, ok := row[3].AsBoolean()
		if ok && boolVal {
			enabledTrueCount++
		}
	}

	if enabledTrueCount != 50 {
		t.Errorf("Expected 50 rows with enabled=true, got %d", enabledTrueCount)
	}

	// Test AND condition (age > 40 AND enabled = true)
	ageGtExpr := expression.NewSimpleExpression("age", storage.GT, 40)
	ageGtExpr.PrepareForSchema(schema)

	andExpr := &expression.AndExpression{
		Expressions: []storage.Expression{ageGtExpr, enabledExpr},
	}
	andExpr.PrepareForSchema(schema)

	scanner, err = mvccTable.Scan(nil, andExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count rows that match age > 40 AND enabled = true
	andMatches := 0
	for scanner.Next() {
		row := scanner.Row()
		age, ok1 := row[2].AsInt64()
		enabled, ok2 := row[3].AsBoolean()
		if ok1 && ok2 && age > 40 && enabled {
			andMatches++
		}
	}

	// Calculate expected matches (approximately 10 rows with age > 40 and enabled = true)
	expectedAndMatches := 0
	for i := 1; i <= 100; i++ {
		age := 20 + i%30
		enabled := i%2 == 0
		if age > 40 && enabled {
			expectedAndMatches++
		}
	}

	if andMatches != expectedAndMatches {
		t.Errorf("Expected %d rows with age>40 AND enabled=true, got %d", expectedAndMatches, andMatches)
	}

	// Test dropping an index
	if err := mvccTable.DropColumnarIndex("age"); err != nil {
		t.Fatalf("Failed to drop columnar index: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestColumnarIndexWithTransaction(t *testing.T) {
	// Create a test config
	config := &storage.Config{
		Path: "memory://",
	}

	// Create a test engine
	engine := mvcc.NewMVCCEngine(config)
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Create schema and table in the first transaction
	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "test_transaction",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.TypeInteger, PrimaryKey: true, Nullable: false},
			{Name: "category", Type: storage.TypeString, Nullable: false},
			{Name: "value", Type: storage.TypeInteger, Nullable: false},
		},
	}

	table, err := tx1.CreateTable("test_transaction", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	for i := 1; i <= 10; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),                  // id
			storage.NewStringValue("cat_" + strconv.Itoa(i%3)), // category
			storage.NewIntegerValue(int64(i * 10)),             // value
		}
		if err := table.Insert(row); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Create columnar index
	mvccTable, ok := table.(*mvcc.MVCCTable)
	if !ok {
		t.Fatalf("Expected MVCCTableWrapper, got %T", table)
	}

	if err := mvccTable.CreateColumnarIndex("category", false); err != nil {
		t.Fatalf("Failed to create columnar index: %v", err)
	}

	// Commit first transaction
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit first transaction: %v", err)
	}

	// Start second transaction
	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Get the table in the second transaction
	table2, err := tx2.GetTable("test_transaction")
	if err != nil {
		t.Fatalf("Failed to get table in second transaction: %v", err)
	}

	// Create a filter for category = "cat_1"
	categoryExpr := expression.NewSimpleExpression("category", storage.EQ, "cat_1")
	categoryExpr.PrepareForSchema(schema)

	// Query using the columnar index
	scanner, err := table2.Scan(nil, categoryExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count rows with category = "cat_1"
	matches := 0
	for scanner.Next() {
		row := scanner.Row()
		cat, ok := row[1].AsString()
		if ok && cat == "cat_1" {
			matches++
		}
	}

	// Should have about 3-4 rows with category = "cat_1"
	expectedMatches := 0
	for i := 1; i <= 10; i++ {
		if i%3 == 1 {
			expectedMatches++
		}
	}

	if matches != expectedMatches {
		t.Errorf("Expected %d rows with category='cat_1', got %d", expectedMatches, matches)
	}

	// Update some rows in the second transaction
	updateExpr := expression.NewSimpleExpression("category", storage.EQ, "cat_1")
	updateExpr.PrepareForSchema(schema)

	updatedCount, err := table2.Update(updateExpr, func(row storage.Row) (storage.Row, bool) {
		// Change category from "cat_1" to "cat_updated"
		updatedRow := make(storage.Row, len(row))
		copy(updatedRow, row)
		updatedRow[1] = storage.NewStringValue("cat_updated")
		return updatedRow, true
	})

	if err != nil {
		t.Fatalf("Failed to update rows: %v", err)
	}

	if updatedCount != expectedMatches {
		t.Errorf("Expected to update %d rows, updated %d", expectedMatches, updatedCount)
	}

	// Query again for now-updated rows
	updatedExpr := expression.NewSimpleExpression("category", storage.EQ, "cat_updated")
	updatedExpr.PrepareForSchema(schema)

	scanner, err = table2.Scan(nil, updatedExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count updated rows
	updatedMatches := 0
	for scanner.Next() {
		row := scanner.Row()
		cat, ok := row[1].AsString()
		if ok && cat == "cat_updated" {
			updatedMatches++
		}
	}

	if updatedMatches != expectedMatches {
		t.Errorf("Expected %d rows with category='cat_updated', got %d", expectedMatches, updatedMatches)
	}

	// Commit the second transaction
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	// Start a third transaction to verify changes were persisted
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin third transaction: %v", err)
	}

	// Get the table in the third transaction
	table3, err := tx3.GetTable("test_transaction")
	if err != nil {
		t.Fatalf("Failed to get table in third transaction: %v", err)
	}

	// Query for updated rows
	scanner, err = table3.Scan(nil, updatedExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Count updated rows in the third transaction
	finalMatches := 0
	for scanner.Next() {
		row := scanner.Row()
		cat, ok := row[1].AsString()
		if ok && cat == "cat_updated" {
			finalMatches++
		}
	}

	if finalMatches != expectedMatches {
		t.Errorf("Expected %d rows with category='cat_updated' after commit, got %d", expectedMatches, finalMatches)
	}

	// Check that the original category no longer exists
	scanner, err = table3.Scan(nil, categoryExpr)
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}
	defer scanner.Close()

	// Should have no matches for the original category
	noMatches := 0
	for scanner.Next() {
		row := scanner.Row()
		cat, ok := row[1].AsString()
		if ok && cat == "cat_1" {
			noMatches++
		}
	}

	if noMatches != 0 {
		t.Errorf("Expected 0 rows with category='cat_1' after updates, got %d", noMatches)
	}

	// Commit the third transaction
	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit third transaction: %v", err)
	}
}
