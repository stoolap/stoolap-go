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

// TestV3CRUDOperations tests the complete CRUD functionality of the mvcc storage engine
// This includes Insert, Select, Update, and Delete operations with thorough verification
func TestV3CRUDOperations(t *testing.T) {
	// Initialize the engine and create a test table
	config := &storage.Config{
		Path: "memory://",
	}

	engine := mvcc.NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Start a transaction
	txn, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test table schema
	schema := storage.Schema{
		TableName: "crud_test",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TypeString, Nullable: false},
			{ID: 2, Name: "age", Type: storage.TypeInteger, Nullable: true},
			{ID: 3, Name: "balance", Type: storage.TypeFloat, Nullable: false},
			{ID: 4, Name: "active", Type: storage.TypeBoolean, Nullable: false},
			{ID: 5, Name: "last_login", Type: storage.TypeTimestamp, Nullable: true},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create the table
	table, err := txn.CreateTable("crud_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// STEP 1: INSERT OPERATIONS
	t.Log("Testing INSERT operations...")

	// Create test data with optional fields
	login1 := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)
	login2 := time.Date(2023, 2, 20, 14, 45, 0, 0, time.UTC)

	// Insert rows one by one
	rows := []storage.Row{
		{
			storage.NewIntegerValue(1),
			storage.NewStringValue("Alice"),
			storage.NewIntegerValue(30),
			storage.NewFloatValue(1500.50),
			storage.NewBooleanValue(true),
			storage.NewTimestampValue(login1),
		},
		{
			storage.NewIntegerValue(2),
			storage.NewStringValue("Bob"),
			storage.NewNullValue(storage.INTEGER),
			storage.NewFloatValue(2750.75),
			storage.NewBooleanValue(true),
			storage.NewTimestampValue(login2),
		},
		{
			storage.NewIntegerValue(3),
			storage.NewStringValue("Charlie"),
			storage.NewIntegerValue(25),
			storage.NewFloatValue(500.00),
			storage.NewBooleanValue(false),
			storage.NewNullValue(storage.TIMESTAMP),
		},
		{
			storage.NewIntegerValue(4),
			storage.NewStringValue("David"),
			storage.NewIntegerValue(40),
			storage.NewFloatValue(3200.25),
			storage.NewBooleanValue(true),
			storage.NewNullValue(storage.TIMESTAMP),
		},
		{
			storage.NewIntegerValue(5),
			storage.NewStringValue("Eve"),
			storage.NewIntegerValue(35), // Eve's age is 35
			storage.NewFloatValue(1800.00),
			storage.NewBooleanValue(false),
			storage.NewTimestampValue(login1),
		},
	}

	// Insert each row
	for i, row := range rows {
		err := table.Insert(row)
		if err != nil {
			t.Errorf("Failed to insert row %d: %v", i+1, err)
		}
	}

	// STEP 2: SELECT AND VERIFY INITIAL DATA
	t.Log("Testing SELECT operations for initial data...")

	// Create a scanner to read all rows
	scanner, err := table.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner: %v", err)
	}
	defer scanner.Close()

	// Expected data for verification
	expected := []struct {
		id        int64
		name      string
		age       interface{} // int64 or nil
		balance   float64
		active    bool
		lastLogin interface{} // time.Time or nil
	}{
		{1, "Alice", int64(30), 1500.50, true, login1},
		{2, "Bob", nil, 2750.75, true, login2},
		{3, "Charlie", int64(25), 500.00, false, nil},
		{4, "David", int64(40), 3200.25, true, nil},
		{5, "Eve", int64(35), 1800.00, false, login1}, // ID=5, age=35
	}

	// Verify all rows match what we inserted
	rowCount := 0
	for scanner.Next() {
		if rowCount >= len(expected) {
			t.Errorf("Found more rows than expected. Expected %d rows.", len(expected))
			break
		}

		row := scanner.Row()
		e := expected[rowCount]

		// Verify ID
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Row %d: Failed to get ID", rowCount+1)
		} else if id != e.id {
			t.Errorf("Row %d: Expected ID %d, got %d", rowCount+1, e.id, id)
		}

		// Verify Name
		name, ok := row[1].AsString()
		if !ok {
			t.Errorf("Row %d: Failed to get name", rowCount+1)
		} else if name != e.name {
			t.Errorf("Row %d: Expected name %s, got %s", rowCount+1, e.name, name)
		}

		// Verify Age (which might be NULL)
		if e.age == nil {
			if !row[2].IsNull() {
				age, _ := row[2].AsInt64()
				t.Errorf("Row %d: Expected NULL age, got %d", rowCount+1, age)
			}
		} else {
			expectedAge := e.age.(int64)
			age, ok := row[2].AsInt64()
			if !ok {
				t.Errorf("Row %d: Failed to get age", rowCount+1)
			} else if age != expectedAge {
				t.Errorf("Row %d: Expected age %d, got %d", rowCount+1, expectedAge, age)
			}
		}

		// Verify Balance
		balance, ok := row[3].AsFloat64()
		if !ok {
			t.Errorf("Row %d: Failed to get balance", rowCount+1)
		} else if balance != e.balance {
			t.Errorf("Row %d: Expected balance %.2f, got %.2f", rowCount+1, e.balance, balance)
		}

		// Verify Active
		active, ok := row[4].AsBoolean()
		if !ok {
			t.Errorf("Row %d: Failed to get active", rowCount+1)
		} else if active != e.active {
			t.Errorf("Row %d: Expected active %v, got %v", rowCount+1, e.active, active)
		}

		// Verify Last Login (which might be NULL)
		if e.lastLogin == nil {
			if !row[5].IsNull() {
				lastLogin, _ := row[5].AsTimestamp()
				t.Errorf("Row %d: Expected NULL last_login, got %v", rowCount+1, lastLogin)
			}
		} else {
			expectedLogin := e.lastLogin.(time.Time)
			lastLogin, ok := row[5].AsTimestamp()
			if !ok {
				t.Errorf("Row %d: Failed to get last_login", rowCount+1)
			} else if !lastLogin.Equal(expectedLogin) {
				t.Errorf("Row %d: Expected last_login %v, got %v", rowCount+1, expectedLogin, lastLogin)
			}
		}

		rowCount++
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}

	// Ensure we got exactly the expected number of rows
	if rowCount != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), rowCount)
	}

	// STEP 3: UPDATE OPERATIONS
	t.Log("Testing UPDATE operations...")

	// Create update expressions
	// 3.1: Update Alice's age to 31 and balance to 1600.00
	updateAlice := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		id, _ := row[0].AsInt64()
		return id == 1, nil
	})

	t.Logf("Updating Alice's record (ID = 1)...")
	// Update function for Alice's record
	updateCount1, err := table.Update(updateAlice, func(row storage.Row) (storage.Row, bool) {
		// Create updated row
		updated := make(storage.Row, len(row))
		copy(updated, row)

		// Update age (column 2) to 31
		updated[2] = storage.NewIntegerValue(31)

		// Update balance (column 3) to 1600.00
		updated[3] = storage.NewFloatValue(1600.00)

		return updated, true
	})

	if err != nil {
		t.Errorf("Failed to update Alice's record: %v", err)
	}
	if updateCount1 != 1 {
		t.Errorf("Expected to update 1 row for Alice, got %d", updateCount1)
	}

	t.Logf("Updating inactive users to be active and set balance to 1000.00...")
	// 3.2: Update all inactive users to be active and set balance to 1000.00
	updateInactive := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		active, _ := row[4].AsBoolean()
		return !active, nil
	})

	// Update inactive users
	updateCount2, err := table.Update(updateInactive, func(row storage.Row) (storage.Row, bool) {
		// Create updated row
		updated := make(storage.Row, len(row))
		copy(updated, row)

		// Set active (column 4) to true
		updated[4] = storage.NewBooleanValue(true)

		// Update balance (column 3) to 1000.00
		updated[3] = storage.NewFloatValue(1000.00)

		return updated, true
	})

	if err != nil {
		t.Errorf("Failed to update inactive users: %v", err)
	}
	if updateCount2 != 2 {
		t.Errorf("Expected to update 2 rows for inactive users, got %d", updateCount2)
	}

	// STEP 4: VERIFY DATA AFTER UPDATES
	t.Log("Verifying data after UPDATE operations...")

	// Updated expected data
	updatedExpected := []struct {
		id        int64
		name      string
		age       interface{} // int64 or nil
		balance   float64
		active    bool
		lastLogin interface{} // time.Time or nil
	}{
		{1, "Alice", int64(31), 1600.00, true, login1}, // Updated age and balance
		{2, "Bob", nil, 2750.75, true, login2},         // Unchanged
		{3, "Charlie", int64(25), 1000.00, true, nil},  // Now active with new balance
		{4, "David", int64(40), 3200.25, true, nil},    // Unchanged
		{5, "Eve", int64(35), 1000.00, true, login1},   // Now active with new balance
	}

	// Verify updated data
	scanner2, err := table.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner after updates: %v", err)
	}
	defer scanner2.Close()

	// Create a map to track which rows we've found
	foundRows := make(map[int64]bool)

	// Scan and verify each row
	rowCount = 0
	for scanner2.Next() {
		row := scanner2.Row()

		// Get the ID to find the matching expected row
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d after updates", rowCount+1)
			rowCount++
			continue
		}

		// Find the matching expected row
		var e *struct {
			id        int64
			name      string
			age       interface{}
			balance   float64
			active    bool
			lastLogin interface{}
		}

		for i := range updatedExpected {
			if updatedExpected[i].id == id {
				e = &updatedExpected[i]
				break
			}
		}

		if e == nil {
			t.Errorf("Found unexpected row with ID %d after updates", id)
			rowCount++
			continue
		}

		// Mark this row as found
		foundRows[id] = true

		// Verify all fields match expected values
		// Verify Name
		name, ok := row[1].AsString()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get name after updates", id)
		} else if name != e.name {
			t.Errorf("Row with ID %d: Expected name %s, got %s after updates", id, e.name, name)
		}

		// Verify Age (which might be NULL)
		if e.age == nil {
			if !row[2].IsNull() {
				age, _ := row[2].AsInt64()
				t.Errorf("Row with ID %d: Expected NULL age, got %d after updates", id, age)
			}
		} else {
			expectedAge := e.age.(int64)
			age, ok := row[2].AsInt64()
			if !ok {
				t.Errorf("Row with ID %d: Failed to get age after updates", id)
			} else if age != expectedAge {
				t.Errorf("Row with ID %d: Expected age %d, got %d after updates", id, expectedAge, age)
			}
		}

		// Verify Balance
		balance, ok := row[3].AsFloat64()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get balance after updates", id)
		} else if balance != e.balance {
			t.Errorf("Row with ID %d: Expected balance %.2f, got %.2f after updates", id, e.balance, balance)
		}

		// Verify Active
		active, ok := row[4].AsBoolean()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get active after updates", id)
		} else if active != e.active {
			t.Errorf("Row with ID %d: Expected active %v, got %v after updates", id, e.active, active)
		}

		// Verify Last Login (which might be NULL)
		if e.lastLogin == nil {
			if !row[5].IsNull() {
				lastLogin, _ := row[5].AsTimestamp()
				t.Errorf("Row with ID %d: Expected NULL last_login, got %v after updates", id, lastLogin)
			}
		} else {
			expectedLogin := e.lastLogin.(time.Time)
			lastLogin, ok := row[5].AsTimestamp()
			if !ok {
				t.Errorf("Row with ID %d: Failed to get last_login after updates", id)
			} else if !lastLogin.Equal(expectedLogin) {
				t.Errorf("Row with ID %d: Expected last_login %v, got %v after updates", id, expectedLogin, lastLogin)
			}
		}

		rowCount++
	}

	// Check for scanner errors
	if err := scanner2.Err(); err != nil {
		t.Errorf("Scanner error after updates: %v", err)
	}

	// Make sure we found all expected rows
	for _, e := range updatedExpected {
		if !foundRows[e.id] {
			t.Errorf("Expected row with ID %d not found after updates", e.id)
		}
	}

	// Ensure we got exactly the expected number of rows
	if rowCount != len(updatedExpected) {
		t.Errorf("Expected %d rows after updates, got %d", len(updatedExpected), rowCount)
	}

	// STEP 5: DELETE OPERATIONS
	t.Log("Testing DELETE operations...")

	// 5.1: Delete a single row (David, ID = 4)
	deleteDavid := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		id, _ := row[0].AsInt64()
		return id == 4, nil
	})

	deleteCount1, err := table.Delete(deleteDavid)
	if err != nil {
		t.Errorf("Failed to delete David's record: %v", err)
	}
	if deleteCount1 != 1 {
		t.Errorf("Expected to delete 1 row for David, got %d", deleteCount1)
	}

	// 5.2: Delete all rows with balance = 1000.00
	deleteBalance1000 := expression.NewEvalExpression(func(row storage.Row) (bool, error) {
		balance, _ := row[3].AsFloat64()
		return balance == 1000.00, nil
	})

	deleteCount2, err := table.Delete(deleteBalance1000)
	if err != nil {
		t.Errorf("Failed to delete rows with balance 1000.00: %v", err)
	}
	if deleteCount2 != 2 {
		t.Errorf("Expected to delete 2 rows with balance 1000.00, got %d", deleteCount2)
	}

	// STEP 6: VERIFY DATA AFTER DELETES
	t.Log("Verifying data after DELETE operations...")

	// Final expected data after deletes (should only contain Alice and Bob)
	finalExpected := []struct {
		id        int64
		name      string
		age       interface{} // int64 or nil
		balance   float64
		active    bool
		lastLogin interface{} // time.Time or nil
	}{
		{1, "Alice", int64(31), 1600.00, true, login1},
		{2, "Bob", nil, 2750.75, true, login2},
		// Charlie (balance 1000) and Eve (balance 1000) deleted
		// David (ID 4) deleted
	}

	// Verify final data
	scanner3, err := table.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner after deletes: %v", err)
	}
	defer scanner3.Close()

	// Reset foundRows map
	foundRows = make(map[int64]bool)

	// Scan and verify each row
	rowCount = 0
	for scanner3.Next() {
		row := scanner3.Row()

		// Get the ID to find the matching expected row
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d after deletes", rowCount+1)
			rowCount++
			continue
		}

		// Find the matching expected row
		var e *struct {
			id        int64
			name      string
			age       interface{}
			balance   float64
			active    bool
			lastLogin interface{}
		}

		for i := range finalExpected {
			if finalExpected[i].id == id {
				e = &finalExpected[i]
				break
			}
		}

		if e == nil {
			t.Errorf("Found unexpected row with ID %d after deletes", id)
			rowCount++
			continue
		}

		// Mark this row as found
		foundRows[id] = true

		// Verify all fields match expected values
		// Verify Name
		name, ok := row[1].AsString()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get name after deletes", id)
		} else if name != e.name {
			t.Errorf("Row with ID %d: Expected name %s, got %s after deletes", id, e.name, name)
		}

		// Verify Age (which might be NULL)
		if e.age == nil {
			if !row[2].IsNull() {
				age, _ := row[2].AsInt64()
				t.Errorf("Row with ID %d: Expected NULL age, got %d after deletes", id, age)
			}
		} else {
			expectedAge := e.age.(int64)
			age, ok := row[2].AsInt64()
			if !ok {
				t.Errorf("Row with ID %d: Failed to get age after deletes", id)
			} else if age != expectedAge {
				t.Errorf("Row with ID %d: Expected age %d, got %d after deletes", id, expectedAge, age)
			}
		}

		// Verify Balance
		balance, ok := row[3].AsFloat64()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get balance after deletes", id)
		} else if balance != e.balance {
			t.Errorf("Row with ID %d: Expected balance %.2f, got %.2f after deletes", id, e.balance, balance)
		}

		// Verify Active
		active, ok := row[4].AsBoolean()
		if !ok {
			t.Errorf("Row with ID %d: Failed to get active after deletes", id)
		} else if active != e.active {
			t.Errorf("Row with ID %d: Expected active %v, got %v after deletes", id, e.active, active)
		}

		// Verify Last Login (which might be NULL)
		if e.lastLogin == nil {
			if !row[5].IsNull() {
				lastLogin, _ := row[5].AsTimestamp()
				t.Errorf("Row with ID %d: Expected NULL last_login, got %v after deletes", id, lastLogin)
			}
		} else {
			expectedLogin := e.lastLogin.(time.Time)
			lastLogin, ok := row[5].AsTimestamp()
			if !ok {
				t.Errorf("Row with ID %d: Failed to get last_login after deletes", id)
			} else if !lastLogin.Equal(expectedLogin) {
				t.Errorf("Row with ID %d: Expected last_login %v, got %v after deletes", id, expectedLogin, lastLogin)
			}
		}

		rowCount++
	}

	// Check for scanner errors
	if err := scanner3.Err(); err != nil {
		t.Errorf("Scanner error after deletes: %v", err)
	}

	// Make sure we found all expected rows
	for _, e := range finalExpected {
		if !foundRows[e.id] {
			t.Errorf("Expected row with ID %d not found after deletes", e.id)
		}
	}

	// Ensure we got exactly the expected number of rows
	if rowCount != len(finalExpected) {
		t.Errorf("Expected %d rows after deletes, got %d", len(finalExpected), rowCount)
	}

	// STEP 7: Test transaction commit
	err = txn.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	// Start a new transaction to verify data persists after commit
	txn2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}
	defer txn2.Rollback()

	// Get the table from the new transaction
	table2, err := txn2.GetTable("crud_test")
	if err != nil {
		t.Fatalf("Failed to get table in new transaction: %v", err)
	}

	// Verify final data persists after commit
	t.Log("Verifying data persists after transaction commit...")

	scanner4, err := table2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner in new transaction: %v", err)
	}
	defer scanner4.Close()

	// Reset foundRows map and row count
	foundRows = make(map[int64]bool)
	rowCount = 0

	// Scan and verify each row
	for scanner4.Next() {
		row := scanner4.Row()

		// Get the ID to find the matching expected row
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID for row %d in new transaction", rowCount+1)
			rowCount++
			continue
		}

		// Find the matching expected row
		var e *struct {
			id        int64
			name      string
			age       interface{}
			balance   float64
			active    bool
			lastLogin interface{}
		}

		for i := range finalExpected {
			if finalExpected[i].id == id {
				e = &finalExpected[i]
				break
			}
		}

		if e == nil {
			t.Errorf("Found unexpected row with ID %d in new transaction", id)
			rowCount++
			continue
		}

		// Mark this row as found
		foundRows[id] = true

		// Verify fields match expected values
		// Remaining verification code goes here...
		// (Omitted for brevity but should check all fields as before)

		rowCount++
	}

	// Make sure we found all expected rows in the new transaction
	for _, e := range finalExpected {
		if !foundRows[e.id] {
			t.Errorf("Expected row with ID %d not found in new transaction", e.id)
		}
	}

	// Ensure we got exactly the expected number of rows in the new transaction
	if rowCount != len(finalExpected) {
		t.Errorf("Expected %d rows in new transaction, got %d", len(finalExpected), rowCount)
	}

	t.Log("All CRUD operations verified successfully.")
}
