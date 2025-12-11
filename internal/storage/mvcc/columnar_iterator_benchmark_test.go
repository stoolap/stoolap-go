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
	"testing"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// BenchmarkColumnarIterator benchmarks the performance of our new ColumnarIndexIterator implementation
func BenchmarkColumnarIterator(b *testing.B) {
	// Set up test data
	engine := setupTestMVCCEngine()
	defer engine.Close()

	// Create schema with primary key
	schema := storage.Schema{
		TableName: "test_iterator",
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

	table, err := tx.GetTable("test_iterator")
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
	table, err = tx.GetTable("test_iterator")
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

	// ==================== Benchmark Tests ====================

	b.Run("StandardScan", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_iterator")
			mvccTable := table.(*MVCCTable)

			// Create expression for value > 1000 AND value <= 5000
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(5000),
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

			// Verify correct number of rows returned (should be 400)
			if rowCount != 400 {
				b.Fatalf("Expected 400 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("OptimizedIterator", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_iterator")
			mvccTable := table.(*MVCCTable)

			// Create expression for value > 1000 AND value <= 5000
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(5000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			andExpr.PrepareForSchema(mvccTable.Schema())

			// Get row IDs directly
			rowIDs := mvccTable.GetFilteredRowIDs(andExpr)
			if len(rowIDs) == 0 {
				b.Fatalf("No row IDs returned from columnar index")
			}

			// Create the iterator directly
			scanner := NewColumnarIndexIterator(
				mvccTable.versionStore,
				rowIDs,
				txn.ID(),
				mvccTable.Schema(),
				nil, // All columns
			)

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned (should be 400)
			if rowCount != 400 {
				b.Fatalf("Expected 400 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("DirectRowsToMap", func(b *testing.B) {
		// This benchmark measures the old approach of materializing a map of rows
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_iterator")
			mvccTable := table.(*MVCCTable)

			// Create expression for value > 1000 AND value <= 5000
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(5000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			andExpr.PrepareForSchema(mvccTable.Schema())

			// Get rows using the old approach
			rows := mvccTable.GetRowsWithFilter(andExpr)

			// Create scanner using the rows map
			scanner := NewMVCCScanner(rows, mvccTable.Schema(), nil, nil)

			// Count rows
			rowCount := 0
			for scanner.Next() {
				rowCount++
				_ = scanner.Row()
			}
			scanner.Close()

			// Verify correct number of rows returned (should be 400)
			if rowCount != 400 {
				b.Fatalf("Expected 400 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("FullScan_OriginalPath", func(b *testing.B) {
		// Full end-to-end benchmark showing the unmodified scan path
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_iterator")

			// Create expression for value > 1000 AND value <= 5000
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(5000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection (uses the scan method which automatically chooses best path)
			scanner, err := table.Scan(nil, andExpr)
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

			// Verify correct number of rows returned (should be 400)
			if rowCount != 400 {
				b.Fatalf("Expected 400 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})

	b.Run("FullScan_NewIteratorPath", func(b *testing.B) {
		// Full end-to-end benchmark showing the scan path with our modifications
		// Since we've already updated the scan method to use the iterator,
		// this should automatically use our optimized iterator path

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_iterator")

			// Create expression for value > 1000 AND value <= 5000
			expr1 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.GT,
				Value:    int64(1000),
			}
			expr2 := &expression.SimpleExpression{
				Column:   "value",
				Operator: storage.LTE,
				Value:    int64(5000),
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			// Get scanner with full projection (uses the scan method which now uses the new iterator)
			scanner, err := table.Scan(nil, andExpr)
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

			// Verify correct number of rows returned (should be 400)
			if rowCount != 400 {
				b.Fatalf("Expected 400 rows, got %d", rowCount)
			}

			txn.Rollback()
		}
	})
}
