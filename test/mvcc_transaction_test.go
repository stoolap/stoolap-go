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

func TestBasicTransactionOperations(t *testing.T) {
	config := &storage.Config{
		Path: "memory://",
	}

	testEngine := mvcc.NewMVCCEngine(config)
	err := testEngine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer testEngine.Close()

	// Begin a transaction
	txn, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Test transaction ID
	if txn.ID() <= 0 {
		t.Errorf("Expected positive transaction ID, got %d", txn.ID())
	}

	// Create a table
	schema := storage.Schema{
		TableName: "txn_test_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TypeString, Nullable: false},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	table, err := txn.CreateTable("txn_test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	row := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("test value"),
	}
	err = table.Insert(row)
	if err != nil {
		t.Errorf("Failed to insert row: %v", err)
	}

	// Check table exists in transaction
	tableList, err := txn.ListTables()
	if err != nil {
		t.Errorf("Failed to list tables: %v", err)
	}
	if len(tableList) != 1 || tableList[0] != "txn_test_table" {
		t.Errorf("Expected ['txn_test_table'], got %v", tableList)
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	// Check the transaction is closed
	_, err = txn.ListTables()
	if err == nil {
		t.Errorf("Expected error when listing tables after commit, got nil")
	}

	// Begin a new transaction
	txn2, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Verify the table still exists
	tableList, err = txn2.ListTables()
	if err != nil {
		t.Errorf("Failed to list tables in second transaction: %v", err)
	}
	if len(tableList) != 1 || tableList[0] != "txn_test_table" {
		t.Errorf("Expected ['txn_test_table'], got %v", tableList)
	}

	// Get the table
	table2, err := txn2.GetTable("txn_test_table")
	if err != nil {
		t.Errorf("Failed to get table in second transaction: %v", err)
	}
	if table2 != nil && table2.Name() != "txn_test_table" {
		t.Errorf("Expected table name 'txn_test_table', got '%s'", table2.Name())
	}

	// Rollback the transaction
	err = txn2.Rollback()
	if err != nil {
		t.Errorf("Failed to rollback transaction: %v", err)
	}
}

func TestTransactionIsolation(t *testing.T) {
	// t.Skip("The current MVCC design has several fundamental issues that prevent proper isolation between transactions")
	config := &storage.Config{
		Path: "memory://",
	}

	testEngine := mvcc.NewMVCCEngine(config)
	err := testEngine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer testEngine.Close()

	// Start with a clean database
	txnSetup, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin setup transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "isolation_test",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "value", Type: storage.TypeString, Nullable: false},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = txnSetup.CreateTable("isolation_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = txnSetup.Commit()
	if err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	// Begin transaction 1
	txn1, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Get the table
	table1, err := txn1.GetTable("isolation_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 1: %v", err)
	}

	// Insert a row in transaction 1
	row1 := storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("value from txn1"),
	}
	err = table1.Insert(row1)
	if err != nil {
		t.Errorf("Failed to insert row in transaction 1: %v", err)
	}

	// Begin transaction 2
	txn2, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Get the table
	table2, err := txn2.GetTable("isolation_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 2: %v", err)
	}

	// Insert a row in transaction 2
	row2 := storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("value from txn2"),
	}
	err = table2.Insert(row2)
	if err != nil {
		t.Errorf("Failed to insert row in transaction 2: %v", err)
	}

	// Scan table in transaction 1
	scanner1, err := table1.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 1: %v", err)
	}
	defer scanner1.Close()

	// Transaction 1 should only see its own row
	rowCount := 0
	for scanner1.Next() {
		row := scanner1.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID in transaction 1")
		}
		if id != 1 {
			t.Errorf("Expected ID 1 in transaction 1, got %d", id)
		}
		value, ok := row[1].AsString()
		if !ok {
			t.Errorf("Failed to get value in transaction 1")
		}
		if value != "value from txn1" {
			t.Errorf("Expected value 'value from txn1' in transaction 1, got '%s'", value)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Errorf("Expected 1 row in transaction 1, got %d", rowCount)
	}

	// Scan table in transaction 2
	scanner2, err := table2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 2: %v", err)
	}
	defer scanner2.Close()

	// Transaction 2 should only see its own row
	rowCount = 0
	for scanner2.Next() {
		row := scanner2.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID in transaction 2")
		}
		if id != 2 {
			t.Errorf("Expected ID 2 in transaction 2, got %d", id)
		}
		value, ok := row[1].AsString()
		if !ok {
			t.Errorf("Failed to get value in transaction 2")
		}
		if value != "value from txn2" {
			t.Errorf("Expected value 'value from txn2' in transaction 2, got '%s'", value)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Errorf("Expected 1 row in transaction 2, got %d", rowCount)
	}

	// Commit transaction 1
	err = txn1.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction 1: %v", err)
	}

	// Begin transaction 3
	txn3, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Get the table
	table3, err := txn3.GetTable("isolation_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 3: %v", err)
	}

	// Scan table in transaction 3
	scanner3, err := table3.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 3: %v", err)
	}
	defer scanner3.Close()

	// Transaction 3 should see transaction 1's committed row
	rowCount = 0
	for scanner3.Next() {
		row := scanner3.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID in transaction 3")
		}
		if id != 1 {
			t.Errorf("Expected ID 1 in transaction 3, got %d", id)
		}
		value, ok := row[1].AsString()
		if !ok {
			t.Errorf("Failed to get value in transaction 3")
		}
		if value != "value from txn1" {
			t.Errorf("Expected value 'value from txn1' in transaction 3, got '%s'", value)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Errorf("Expected 1 row in transaction 3, got %d", rowCount)
	}

	// Rollback transaction 2
	err = txn2.Rollback()
	if err != nil {
		t.Errorf("Failed to rollback transaction 2: %v", err)
	}

	// Begin transaction 4
	txn4, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	// Get the table
	table4, err := txn4.GetTable("isolation_test")
	if err != nil {
		t.Fatalf("Failed to get table in transaction 4: %v", err)
	}

	// Scan table in transaction 4
	scanner4, err := table4.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan table in transaction 4: %v", err)
	}
	defer scanner4.Close()

	// Transaction 4 should only see transaction 1's committed row, not transaction 2's rolled back row
	rowCount = 0
	for scanner4.Next() {
		row := scanner4.Row()
		id, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get ID in transaction 4")
		}
		if id != 1 {
			t.Errorf("Expected ID 1 in transaction 4, got %d", id)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Errorf("Expected 1 row in transaction 4, got %d", rowCount)
	}

	// Commit transaction 3 and transaction 4
	err = txn3.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction 3: %v", err)
	}

	err = txn4.Commit()
	if err != nil {
		t.Errorf("Failed to commit transaction 4: %v", err)
	}
}

func TestTransactionConcurrency(t *testing.T) {
	config := &storage.Config{
		Path: "memory://",
	}

	testEngine := mvcc.NewMVCCEngine(config)
	err := testEngine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer testEngine.Close()

	// Start with a clean database
	txnSetup, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin setup transaction: %v", err)
	}

	schema := storage.Schema{
		TableName: "concurrency_test",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.TypeInteger, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "thread", Type: storage.TypeInteger, Nullable: false},
			{ID: 2, Name: "value", Type: storage.TypeInteger, Nullable: false},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = txnSetup.CreateTable("concurrency_test", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = txnSetup.Commit()
	if err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	// Create multiple goroutines to simulate concurrent transactions
	const numThreads = 10
	const rowsPerThread = 100
	done := make(chan bool, numThreads)

	for thread := 0; thread < numThreads; thread++ {
		go func(threadID int) {
			// Begin a transaction
			txn, err := testEngine.BeginTransaction()
			if err != nil {
				t.Errorf("Thread %d: Failed to begin transaction: %v", threadID, err)
				done <- false
				return
			}

			// Get the table
			table, err := txn.GetTable("concurrency_test")
			if err != nil {
				t.Errorf("Thread %d: Failed to get table: %v", threadID, err)
				txn.Rollback()
				done <- false
				return
			}

			// Insert rows
			for i := 0; i < rowsPerThread; i++ {
				row := storage.Row{
					storage.NewIntegerValue(int64(threadID*rowsPerThread + i)),
					storage.NewIntegerValue(int64(threadID)),
					storage.NewIntegerValue(int64(i)),
				}
				err = table.Insert(row)
				if err != nil {
					t.Errorf("Thread %d: Failed to insert row %d: %v", threadID, i, err)
					txn.Rollback()
					done <- false
					return
				}
			}

			// Commit the transaction
			err = txn.Commit()
			if err != nil {
				t.Errorf("Thread %d: Failed to commit transaction: %v", threadID, err)
				done <- false
				return
			}

			done <- true
		}(thread)
	}

	// Wait for all goroutines to complete
	successCount := 0
	for i := 0; i < numThreads; i++ {
		if <-done {
			successCount++
		}
	}

	if successCount != numThreads {
		t.Errorf("Expected %d successful transactions, got %d", numThreads, successCount)
	}

	// Verify the data
	verifyTxn, err := testEngine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin verification transaction: %v", err)
	}

	verifyTable, err := verifyTxn.GetTable("concurrency_test")
	if err != nil {
		t.Fatalf("Failed to get table for verification: %v", err)
	}

	scanner, err := verifyTable.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to create scanner for verification: %v", err)
	}
	defer scanner.Close()

	rowCount := 0
	threadCounts := make([]int, numThreads)
	rowsFound := make(map[int64]bool)

	for scanner.Next() {
		row := scanner.Row()

		threadID, ok := row[1].AsInt64()
		if !ok {
			t.Errorf("Failed to get thread ID for row %d", rowCount)
		}

		rowID, ok := row[0].AsInt64()
		if !ok {
			t.Errorf("Failed to get row ID for row %d", rowCount)
		}

		// Mark this row ID as found
		rowsFound[rowID] = true

		if threadID < 0 || threadID >= int64(numThreads) {
			t.Errorf("Invalid thread ID %d for row %d", threadID, rowCount)
		} else {
			threadCounts[threadID]++
		}

		rowCount++
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error during verification: %v", err)
	}

	if rowCount != numThreads*rowsPerThread {
		t.Errorf("Expected %d total rows, got %d", numThreads*rowsPerThread, rowCount)

		// Find which rows are missing
		for thread := 0; thread < numThreads; thread++ {
			for i := 0; i < rowsPerThread; i++ {
				rowID := int64(thread*rowsPerThread + i)
				if !rowsFound[rowID] {
					t.Logf("Missing row ID: %d (thread %d, row %d)", rowID, thread, i)
				}
			}
		}
	}

	for thread, count := range threadCounts {
		if count != rowsPerThread {
			t.Errorf("Thread %d: Expected %d rows, got %d", thread, rowsPerThread, count)

			// Find which rows are missing for this thread
			for i := 0; i < rowsPerThread; i++ {
				rowID := int64(thread*rowsPerThread + i)
				if !rowsFound[rowID] {
					t.Logf("Thread %d: Missing row ID: %d (row %d)", thread, rowID, i)
				}
			}
		}
	}

	err = verifyTxn.Commit()
	if err != nil {
		t.Errorf("Failed to commit verification transaction: %v", err)
	}
}

// TestTransactionVersionStorePool tests the TransactionVersionStore pool functionality
func TestTransactionVersionStorePool(t *testing.T) {
	// Create an engine
	engine := mvcc.NewMVCCEngine(&storage.Config{Path: "test.db"})
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Begin a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a table with some columns
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER, PrimaryKey: true},
			{Name: "val", Type: storage.TEXT},
		},
	}
	_, err = tx.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Do a sequence of transactions to test pool reuse
	for i := 0; i < 10; i++ {
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		table, err := tx.GetTable("test_table")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		// Commit or rollback alternately
		if i%2 == 0 {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}
		if err != nil {
			t.Fatalf("Failed to end transaction: %v", err)
		}

		// Table should be valid until transaction end
		if table == nil {
			t.Fatalf("Table is nil after transaction end")
		}
	}
}
