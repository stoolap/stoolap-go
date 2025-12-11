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

// setupBenchmarkTable creates a table with 10,000 rows for benchmarking
func setupBenchmarkTable(b *testing.B) (*mvcc.MVCCTable, storage.Schema) {
	// Create a schema with a primary key
	schema := storage.Schema{
		TableName: "bench_table",
		Columns: []storage.SchemaColumn{
			{ID: 0, Name: "id", Type: storage.INTEGER, Nullable: false, PrimaryKey: true},
			{ID: 1, Name: "name", Type: storage.TEXT, Nullable: true, PrimaryKey: false},
			{ID: 2, Name: "value", Type: storage.FLOAT, Nullable: true, PrimaryKey: false},
		},
	}

	// Create an MVCC engine and table
	config := &storage.Config{}
	engine := mvcc.NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		b.Fatalf("Failed to open engine: %v", err)
	}

	// Begin a transaction
	txn, err := engine.BeginTransaction()
	if err != nil {
		b.Fatalf("Failed to begin transaction: %v", err)
	}
	mvccTxn := txn.(*mvcc.MVCCTransaction)

	// Create a table
	table, err := mvccTxn.CreateTable("bench_table", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}
	mvccTable := table.(*mvcc.MVCCTable)

	// Insert test data
	for i := int64(1); i <= 10000; i++ {
		row := storage.Row{
			storage.NewIntegerValue(i),
			storage.NewStringValue("name" + strconv.FormatInt(i, 10)),
			storage.NewFloatValue(float64(i) * 1.5),
		}
		if err := mvccTable.Insert(row); err != nil {
			b.Fatalf("Failed to insert test row: %v", err)
		}
	}

	return mvccTable, schema
}

// BenchmarkPrimaryKeyFastPath benchmarks the optimized fast path for primary key equality
func BenchmarkPrimaryKeyFastPath(b *testing.B) {
	mvccTable, _ := setupBenchmarkTable(b)

	// Create a simple expression for id = 5000 (middle of the range)
	expr := &expression.SimpleExpression{
		Column:   "id",
		Operator: storage.EQ,
		Value:    int64(5000),
	}

	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		scanner, err := mvccTable.Scan([]int{0, 1, 2}, expr)
		if err != nil {
			b.Fatalf("Failed to scan: %v", err)
		}

		// Make sure we process the results
		for scanner.Next() {
			_ = scanner.Row()
		}
		scanner.Close()
	}
}

// BenchmarkPrimaryKeyUpdate benchmarks the optimized fast path for primary key update
func BenchmarkPrimaryKeyUpdate(b *testing.B) {
	mvccTable, _ := setupBenchmarkTable(b)

	// Create a simple expression for id = 5000 (middle of the range)
	expr := &expression.SimpleExpression{
		Column:   "id",
		Operator: storage.EQ,
		Value:    int64(5000),
	}

	// Simple updater function
	updater := func(row storage.Row) (storage.Row, bool) {
		newRow := make(storage.Row, len(row))
		copy(newRow, row)
		newRow[1] = storage.NewStringValue("updated")
		return newRow, true
	}

	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_, err := mvccTable.Update(expr, updater)
		if err != nil {
			b.Fatalf("Failed to update: %v", err)
		}
	}
}
