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
	"fmt"
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// BenchmarkRangeScanner benchmarks the performance of our optimized RangeScanner implementation
func BenchmarkRangeScanner(b *testing.B) {
	// Set up test data
	engine := setupTestMVCCEngine()
	defer engine.Close()

	// Create schema with primary key
	schema := storage.Schema{
		TableName: "test_range_scan",
		Columns: []storage.SchemaColumn{
			{
				Name:       "id",
				Type:       storage.INTEGER,
				PrimaryKey: true,
			},
			{
				Name: "value",
				Type: storage.INTEGER,
			},
			{
				Name: "name",
				Type: storage.TEXT,
			},
		},
	}

	// Create table
	_, err := engine.CreateTable(schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	tx, err := engine.BeginTransaction()
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	table, err := tx.GetTable("test_range_scan")
	if err != nil {
		b.Fatalf("Failed to get table: %v", err)
	}

	// Create columnar index on the value column for optimized scans
	mvccTable := table.(*MVCCTable)
	err = mvccTable.CreateColumnarIndex("value", false)
	if err != nil {
		b.Fatalf("Failed to create columnar index: %v", err)
	}

	// Commit the transaction to persist the columnar index
	err = tx.Commit()
	if err != nil {
		b.Fatalf("Failed to commit index creation: %v", err)
	}

	// Start a new transaction for the data insertion
	tx, err = engine.BeginTransaction()
	if err != nil {
		b.Fatalf("Failed to begin transaction for data insertion: %v", err)
	}

	// Get the table reference for the new transaction
	table, err = tx.GetTable("test_range_scan")
	if err != nil {
		b.Fatalf("Failed to get table in new transaction: %v", err)
	}

	// Insert test data
	numRows := 10000
	for i := 1; i <= numRows; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewIntegerValue(int64(i * 10)),
			storage.NewStringValue("test" + string(rune(i%26+65))),
		}
		if err := table.Insert(row); err != nil {
			b.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Commit changes
	if err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	b.Run("StandardScanner", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")

			mvccTable := table.(*MVCCTable)

			// Create expression for id > 100 AND id <= 1000
			expr1 := &expression.SimpleExpression{
				Column:   "id",
				Operator: storage.GT,
				Value:    int64(100),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "id",
				Operator: storage.LTE,
				Value:    int64(1000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned
			if rowCount != 900 {
				b.Fatalf("Expected 900 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("RangeScanner", func(b *testing.B) {
		// This will use our optimized RangeScanner implementation
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Create expression for id > 100 AND id <= 1000
			expr1 := &expression.SimpleExpression{
				Column:   "id",
				Operator: storage.GT,
				Value:    int64(100),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "id",
				Operator: storage.LTE,
				Value:    int64(1000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned
			if rowCount != 900 {
				b.Fatalf("Expected 900 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("DirectRangeScanner", func(b *testing.B) {
		// Create and use RangeScanner directly for better performance measurement
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// We need the schema for the scanner
			schema := mvccTable.Schema()

			// Create a range scanner directly
			scanner := NewRangeScanner(
				mvccTable.versionStore,
				101,  // id > 100
				1000, // id <= 1000
				true, // inclusive end
				txn.ID(),
				schema,
				nil, // full projection
			)

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned
			if rowCount != 900 {
				b.Fatalf("Expected 900 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("ColumnarIndexScan", func(b *testing.B) {
		// Test scanning with a columnar index on the value column
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Create expression for value > 1000 AND value <= 10000
			// (this corresponds to rows with id 100-1000)
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(10000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}
			// Create a schema-aware expression to ensure columnar index is used properly
			schema := mvccTable.Schema()
			andExpr.PrepareForSchema(schema)

			// Get scanner with full projection, using schema-aware expression for column mapping
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned
			if rowCount != 900 {
				b.Fatalf("Expected 900 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("DirectColumnarAccess", func(b *testing.B) {
		// Test directly using columnar index filters on value column
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Get the columnar index
			colIndex, err := mvccTable.GetColumnarIndex("value")
			if err != nil {
				b.Fatalf("Failed to get columnar index: %v", err)
			}

			// Create a schema-aware expression
			schema := mvccTable.Schema()
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(10000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}
			andExpr.PrepareForSchema(schema)

			// Get the row IDs directly
			rowIDs := colIndex.GetFilteredRowIDs(andExpr)

			// Count rows (no scanning, just counting)
			rowCount := len(rowIDs)

			// Verify correct number of rows returned (should be 900)
			if rowCount != 900 {
				b.Fatalf("Expected 900 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("NameColumnScan", func(b *testing.B) {
		// Test scanning with a filter on the name column
		// This column doesn't have a columnar index, so it should be slower
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Create expression to get rows with name containing "testA" through "testZ"
			// This should return about the same number of rows as the other benchmarks
			expr1 := &expression.SimpleExpression{
				Column:   "name",
				Operator: storage.GTE,
				Value:    "testA",
			}
			expr2 := &expression.SimpleExpression{
				Column:   "name",
				Operator: storage.LTE,
				Value:    "testZ",
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// We don't verify the exact count here because we're just benchmarking performance

			txn.Rollback()
		}
	})

	// Now create a columnar index on the name column and compare performance
	createNameColumnIndex(b, engine)

	b.Run("NameColumnWithIndexScan", func(b *testing.B) {
		// Test scanning with a filter on the name column, now with a columnar index
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Create expression to get rows with name containing "testA" through "testZ"
			expr1 := &expression.SimpleExpression{
				Column:   "name",
				Operator: storage.GTE,
				Value:    "testA",
			}
			expr2 := &expression.SimpleExpression{
				Column:   "name",
				Operator: storage.LTE,
				Value:    "testZ",
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// We don't verify the exact count here because we're just benchmarking performance

			txn.Rollback()
		}
	})

	// Test value column without index to truly compare performance difference
	b.Run("ValueColumnNoIndexScan", func(b *testing.B) {
		// First, drop the value column index if it exists
		dropValueColumnIndex(b, engine)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_scan")
			mvccTable := table.(*MVCCTable)

			// Create expression for value > 1000 AND value <= 10000
			// (this corresponds to rows with id 100-1000)
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(10000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, andExpr)
			if err != nil {
				b.Fatalf("Failed to scan: %v", err)
			}

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// We don't verify the exact count here because we're just benchmarking performance

			txn.Rollback()
		}
	})
}

// Helper function to create columnar index on name column
func createNameColumnIndex(b *testing.B, engine *MVCCEngine) {
	// Create a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get the table
	table, err := tx.GetTable("test_range_scan")
	if err != nil {
		b.Fatalf("Failed to get table: %v", err)
	}

	// Cast to MVCCTable
	mvccTable := table.(*MVCCTable)

	// Create the columnar index on name column
	err = mvccTable.CreateColumnarIndex("name", false)
	if err != nil {
		b.Fatalf("Failed to create columnar index on name: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		b.Fatalf("Failed to commit index creation: %v", err)
	}
}

// Helper function to drop the value column index
func dropValueColumnIndex(b *testing.B, engine *MVCCEngine) {
	// Create a transaction
	tx, err := engine.BeginTransaction()
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get the table
	table, err := tx.GetTable("test_range_scan")
	if err != nil {
		b.Fatalf("Failed to get table: %v", err)
	}

	// Cast to MVCCTable
	mvccTable := table.(*MVCCTable)

	// Drop the columnar index on value column if it exists
	// First check if it exists to avoid errors
	_, err = mvccTable.GetColumnarIndex("value")
	if err == nil {
		// Index exists, drop it
		err = mvccTable.DropColumnarIndex("value")
		if err != nil {
			b.Fatalf("Failed to drop columnar index on value: %v", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		b.Fatalf("Failed to commit index drop: %v", err)
	}
}

// setupTestMVCCEngine creates a test engine for benchmarking
func setupTestMVCCEngine() *MVCCEngine {
	engine := NewMVCCEngine(&storage.Config{Path: ":memory:"})
	err := engine.Open()
	if err != nil {
		panic(fmt.Sprintf("Failed to open engine: %v", err))
	}
	return engine
}
