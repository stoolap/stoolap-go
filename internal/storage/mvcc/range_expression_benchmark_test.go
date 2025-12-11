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
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// BenchmarkRangeExpressionInt64 benchmarks the performance of RangeExpression vs separate conditions
func BenchmarkRangeExpressionInt64(b *testing.B) {
	// Set up test data
	engine := setupTestMVCCEngine()
	defer engine.Close()

	// Create schema with primary key
	schema := storage.Schema{
		TableName: "test_range_expr",
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

	table, err := tx.GetTable("test_range_expr")
	if err != nil {
		b.Fatalf("Failed to get table: %v", err)
	}

	// Insert test data
	numRows := 10000
	for i := 1; i <= numRows; i++ {
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewIntegerValue(int64(i * 10)),
		}
		if err := table.Insert(row); err != nil {
			b.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Commit changes
	if err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	b.Run("SimpleExpressionAND", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_expr")

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

	b.Run("RangeExpression", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_expr")

			mvccTable := table.(*MVCCTable)

			// Create a RangeExpression for the same condition
			rangeExpr := expression.NewRangeExpression(
				"id",
				int64(100),  // min value
				int64(1000), // max value
				false,       // exclude min (>)
				true,        // include max (<=)
			)

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, rangeExpr)
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
}

// BenchmarkRangeExpressionTime benchmarks the performance of RangeExpression vs separate conditions for time values
func BenchmarkRangeExpressionTime(b *testing.B) {
	// Set up test data
	engine := setupTestMVCCEngine()
	defer engine.Close()

	// Create schema with primary key and timestamp
	schema := storage.Schema{
		TableName: "test_range_time",
		Columns: []storage.SchemaColumn{
			{
				Name:       "id",
				Type:       storage.INTEGER,
				PrimaryKey: true,
			},
			{
				Name: "timestamp",
				Type: storage.TIMESTAMP,
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

	table, err := tx.GetTable("test_range_time")
	if err != nil {
		b.Fatalf("Failed to get table: %v", err)
	}

	// Create columnar index on the timestamp column for optimized scans
	mvccTable := table.(*MVCCTable)
	err = mvccTable.CreateColumnarIndex("timestamp", false)
	if err != nil {
		b.Fatalf("Failed to create columnar index: %v", err)
	}

	// Base timestamp
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insert test data
	numRows := 10000
	for i := 1; i <= numRows; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		row := storage.Row{
			storage.NewIntegerValue(int64(i)),
			storage.NewTimestampValue(ts),
		}
		if err := table.Insert(row); err != nil {
			b.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Commit changes
	if err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	// Test timestamps for range: January 1, 2020 00:00 + 100 hours to January 1, 2020 00:00 + 1000 hours
	startTime := baseTime.Add(100 * time.Hour) // 100th hour
	endTime := baseTime.Add(1000 * time.Hour)  // 1000th hour

	b.Run("TimestampSimpleExpressionAND", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_time")

			mvccTable := table.(*MVCCTable)

			// Create expression for timestamp > startTime AND timestamp <= endTime
			expr1 := &expression.SimpleExpression{
				Column:   "timestamp",
				Operator: storage.GT,
				Value:    startTime,
			}
			expr2 := &expression.SimpleExpression{
				Column:   "timestamp",
				Operator: storage.LTE,
				Value:    endTime,
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

	b.Run("TimestampRangeExpression", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_time")

			mvccTable := table.(*MVCCTable)

			// Create a RangeExpression for the same condition
			rangeExpr := expression.NewRangeExpression(
				"timestamp",
				startTime, // min value
				endTime,   // max value
				false,     // exclude min (>)
				true,      // include max (<=)
			)

			// Get scanner with full projection
			scanner, err := mvccTable.Scan(nil, rangeExpr)
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

	// A benchmark specifically for columnar index access to measure improvements from our optimization
	b.Run("TimestampRangeExpressionWithColumnarIndex", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := engine.BeginTransaction()
			table, _ := txn.GetTable("test_range_time")

			mvccTable := table.(*MVCCTable)

			// Create a RangeExpression for the same condition
			rangeExpr := expression.NewRangeExpression(
				"timestamp",
				startTime, // min value
				endTime,   // max value
				false,     // exclude min (>)
				true,      // include max (<=)
			)

			// Get the columnar index directly
			colIndex, err := mvccTable.GetColumnarIndex("timestamp")
			if err != nil {
				b.Fatalf("Failed to get columnar index: %v", err)
			}

			// Get the row IDs using our optimized implementation
			rowIDs := GetRowIDsFromColumnarIndex(rangeExpr, colIndex)

			// Verify correct number of rows returned
			if len(rowIDs) != 900 {
				b.Fatalf("Expected 900 rows, got %d", len(rowIDs))
			}

			txn.Rollback()
		}
	})
}
