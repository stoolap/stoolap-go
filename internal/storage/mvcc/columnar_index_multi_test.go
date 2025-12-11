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
	"math/rand"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
)

// TestMultiColumnarIndexBasic tests the basic functionality of the multi-column index
func TestMultiColumnarIndexBasic(t *testing.T) {
	// Create a multi-column index with two string columns
	idx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"first_name", "last_name"},
		[]int{0, 1},
		[]storage.DataType{storage.TEXT, storage.TEXT},
		nil, false,
	)

	// Add some test data
	err := idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("John"),
			storage.NewStringValue("Doe"),
		},
		1, 0,
	)
	if err != nil {
		t.Errorf("Failed to add values: %v", err)
	}

	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("Jane"),
			storage.NewStringValue("Doe"),
		},
		2, 0,
	)
	if err != nil {
		t.Errorf("Failed to add values: %v", err)
	}

	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("John"),
			storage.NewStringValue("Smith"),
		},
		3, 0,
	)
	if err != nil {
		t.Errorf("Failed to add values: %v", err)
	}

	// Test exact match with two columns
	rowIDs := idx.GetRowIDsEqual([]storage.ColumnValue{
		storage.NewStringValue("John"),
		storage.NewStringValue("Doe"),
	})
	if len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Errorf("Expected row ID 1 for John Doe, got %v", rowIDs)
	}

	// Test removing values
	err = idx.Remove(
		[]storage.ColumnValue{
			storage.NewStringValue("John"),
			storage.NewStringValue("Doe"),
		},
		1, 0,
	)
	if err != nil {
		t.Errorf("Failed to remove values: %v", err)
	}

	// Verify removal
	rowIDs = idx.GetRowIDsEqual([]storage.ColumnValue{
		storage.NewStringValue("John"),
		storage.NewStringValue("Doe"),
	})
	if len(rowIDs) != 0 {
		t.Errorf("Expected no rows after removal, got %v", rowIDs)
	}
}

// TestMultiColumnarIndexUnique tests unique constraint enforcement
func TestMultiColumnarIndexUnique(t *testing.T) {
	// Test multi-column unique index
	idx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"region", "user_id"},
		[]int{0, 1},
		[]storage.DataType{storage.TEXT, storage.INTEGER},
		nil, true, // isUnique=true
	)

	// Add first value
	err := idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("USA"),
			storage.NewIntegerValue(1),
		},
		1, 0,
	)
	if err != nil {
		t.Errorf("Failed to add unique values: %v", err)
	}

	// Add different unique value
	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("USA"),
			storage.NewIntegerValue(2),
		},
		2, 0,
	)
	if err != nil {
		t.Errorf("Failed to add different unique values: %v", err)
	}

	// Try to add duplicate (should fail)
	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("USA"),
			storage.NewIntegerValue(1),
		},
		3, 0,
	)
	if err == nil {
		t.Errorf("Expected unique constraint violation for multi-column index, got nil")
	}

	// NULL values should not violate uniqueness constraint
	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("USA"),
			nil, // NULL user_id
		},
		4, 0,
	)
	if err != nil {
		t.Errorf("Failed to add NULL value with unique constraint: %v", err)
	}

	err = idx.Add(
		[]storage.ColumnValue{
			storage.NewStringValue("USA"),
			nil, // Another NULL user_id
		},
		5, 0,
	)
	if err != nil {
		t.Errorf("Failed to add second NULL value with unique constraint: %v", err)
	}
}

// TestMultiColumnarIndexRangeQueries tests range queries
func TestMultiColumnarIndexRangeQueries(t *testing.T) {
	// Create a multi-column index for timestamp and integer data
	idx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"timestamp", "value"},
		[]int{0, 1},
		[]storage.DataType{storage.TIMESTAMP, storage.INTEGER},
		nil, false,
	)

	// Sample times
	t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)

	// Add test data
	// t1 with values 10, 20
	idx.Add([]storage.ColumnValue{storage.NewTimestampValue(t1), storage.NewIntegerValue(10)}, 1, 0)
	idx.Add([]storage.ColumnValue{storage.NewTimestampValue(t1), storage.NewIntegerValue(20)}, 2, 0)

	// t2 with values 10, 30
	idx.Add([]storage.ColumnValue{storage.NewTimestampValue(t2), storage.NewIntegerValue(10)}, 3, 0)
	idx.Add([]storage.ColumnValue{storage.NewTimestampValue(t2), storage.NewIntegerValue(30)}, 4, 0)

	// t3 with value 40
	idx.Add([]storage.ColumnValue{storage.NewTimestampValue(t3), storage.NewIntegerValue(40)}, 5, 0)

	// Test range query on first column only
	minTime := storage.NewTimestampValue(t1)
	maxTime := storage.NewTimestampValue(t2)
	rowIDs := idx.GetRowIDsInRange([]storage.ColumnValue{minTime}, []storage.ColumnValue{maxTime}, true, true)
	if len(rowIDs) != 4 {
		t.Errorf("Expected 4 rows in time range t1-t2, got %d", len(rowIDs))
	}

	// Test range query with both columns
	// Range: t1-t2 for timestamp, 15-35 for value
	minValues := []storage.ColumnValue{
		storage.NewTimestampValue(t1),
		storage.NewIntegerValue(15),
	}
	maxValues := []storage.ColumnValue{
		storage.NewTimestampValue(t2),
		storage.NewIntegerValue(35),
	}
	rowIDs = idx.GetRowIDsInRange(minValues, maxValues, true, true)
	// The correct result should be rows 2 and 4
	// Row 2: timestamp t1, value 20 (in range 15-35)
	// Row 4: timestamp t2, value 30 (in range 15-35)
	// Row 3 has value 10 which is not in range 15-35
	if len(rowIDs) != 2 || !contains(rowIDs, 2) || !contains(rowIDs, 4) {
		t.Errorf("Expected rows 2 and 4 for composite range query, got %v", rowIDs)
	}
}

// TestMultiColumnarIndexFiltering tests filtering with expressions
func TestMultiColumnarIndexFiltering(t *testing.T) {
	// Test single-column index filtering
	singleIdx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"num"},
		[]int{0},
		[]storage.DataType{storage.INTEGER},
		nil, false,
	)

	// Add test data
	for i := 0; i < 100; i++ {
		err := singleIdx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
		if err != nil {
			t.Errorf("Failed to add single-column value: %v", err)
		}
	}

	// Test with simple expression (num = 42)
	expr := &expression.SimpleExpression{
		Column:   "num",
		Operator: storage.EQ,
		Value:    int64(42),
	}
	rowIDs := singleIdx.GetFilteredRowIDs(expr)
	if len(rowIDs) != 1 || rowIDs[0] != 42 {
		t.Errorf("Expected row ID 42 for num=42, got %v", rowIDs)
	}

	// Test with range expression (num > 90)
	expr = &expression.SimpleExpression{
		Column:   "num",
		Operator: storage.GT,
		Value:    int64(90),
	}
	rowIDs = singleIdx.GetFilteredRowIDs(expr)
	if len(rowIDs) != 9 { // 91-99
		t.Errorf("Expected 9 row IDs for num>90, got %d", len(rowIDs))
	}

	// Test multi-column index filtering
	multiIdx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"country", "city", "population"},
		[]int{0, 1, 2},
		[]storage.DataType{storage.TEXT, storage.TEXT, storage.INTEGER},
		nil, false,
	)

	// Add test data
	multiIdx.Add([]storage.ColumnValue{
		storage.NewStringValue("USA"),
		storage.NewStringValue("New York"),
		storage.NewIntegerValue(8000000),
	}, 1, 0)

	multiIdx.Add([]storage.ColumnValue{
		storage.NewStringValue("USA"),
		storage.NewStringValue("Los Angeles"),
		storage.NewIntegerValue(4000000),
	}, 2, 0)

	multiIdx.Add([]storage.ColumnValue{
		storage.NewStringValue("France"),
		storage.NewStringValue("Paris"),
		storage.NewIntegerValue(2000000),
	}, 3, 0)

	// Test filtering with simple expression on first column
	expr = &expression.SimpleExpression{
		Column:   "country",
		Operator: storage.EQ,
		Value:    "USA",
	}
	rowIDs = multiIdx.GetFilteredRowIDs(expr)
	if len(rowIDs) != 2 || !contains(rowIDs, 1) || !contains(rowIDs, 2) {
		t.Errorf("Expected rows 1 and 2 for country=USA, got %v", rowIDs)
	}

	// Test filtering with AND expression for multiple columns
	expr1 := &expression.SimpleExpression{
		Column:   "country",
		Operator: storage.EQ,
		Value:    "USA",
	}
	expr2 := &expression.SimpleExpression{
		Column:   "city",
		Operator: storage.EQ,
		Value:    "New York",
	}
	andExpr := &expression.AndExpression{
		Expressions: []storage.Expression{expr1, expr2},
	}
	rowIDs = multiIdx.GetFilteredRowIDs(andExpr)
	if len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Errorf("Expected row 1 for country=USA AND city=New York, got %v", rowIDs)
	}

	// Test filtering with complex range condition
	// e.g., country = 'USA' AND population > 5000000
	expr1 = &expression.SimpleExpression{
		Column:   "country",
		Operator: storage.EQ,
		Value:    "USA",
	}
	expr2 = &expression.SimpleExpression{
		Column:   "population",
		Operator: storage.GT,
		Value:    5000000,
	}
	andExpr = &expression.AndExpression{
		Expressions: []storage.Expression{expr1, expr2},
	}
	rowIDs = multiIdx.GetFilteredRowIDs(andExpr)
	if len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Errorf("Expected row 1 for USA with population > 5M, got %v", rowIDs)
	}
}

// TestMultiColumnarIndexBuild tests building the index from a version store
func TestMultiColumnarIndexBuild(t *testing.T) {
	// Create a memory-only engine
	config := &storage.Config{
		Path: "", // Memory-only
	}
	engine := NewMVCCEngine(config)
	err := engine.Open()
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// Set up the schema for the test table
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{
				ID:         0,
				Name:       "id",
				Type:       storage.INTEGER,
				PrimaryKey: true,
			},
			{
				ID:   1,
				Name: "name",
				Type: storage.TEXT,
			},
		},
	}

	// Create the table with the schema
	_, err = engine.CreateTable(schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get the version store
	vs := engine.versionStores["test_table"]

	// Start a transaction to insert data
	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get the table
	table, err := tx.GetTable("test_table")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	// Insert test data
	err = table.Insert(storage.Row{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
	})
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}

	err = table.Insert(storage.Row{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
	})
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create a multi-column index and build it from the version store
	idx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"id", "name"},
		[]int{0, 1},
		[]storage.DataType{storage.INTEGER, storage.TEXT},
		vs, false,
	)

	// Build the index
	err = idx.Build()
	if err != nil {
		t.Errorf("Failed to build index: %v", err)
	}

	// Verify that rows were indexed
	rowIDs := idx.GetRowIDsEqual([]storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
	})
	if len(rowIDs) != 1 || rowIDs[0] != 1 {
		t.Errorf("Expected row 1 for id=1, name=Alice, got %v", rowIDs)
	}

	rowIDs = idx.GetRowIDsEqual([]storage.ColumnValue{
		storage.NewIntegerValue(3),
		storage.NewStringValue("Charlie"),
	})
	if len(rowIDs) != 0 {
		t.Errorf("Expected no results for non-existent row, got %v", rowIDs)
	}
}

// BenchmarkMultiColumnarIndexSingleIntAdd benchmarks adding integers to a single-column index
func BenchmarkMultiColumnarIndexSingleIntAdd(b *testing.B) {
	idx := NewMultiColumnarIndex(
		"bench_idx",
		"bench_table",
		[]string{"num"},
		[]int{0},
		[]storage.DataType{storage.INTEGER},
		nil,
		false,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		num := int64(i % 10000)
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(num)}, int64(i), 0)
	}
}

// BenchmarkMultiColumnarIndexSingleTimestampAdd benchmarks adding timestamps
func BenchmarkMultiColumnarIndexSingleTimestampAdd(b *testing.B) {
	idx := NewMultiColumnarIndex(
		"bench_idx",
		"bench_table",
		[]string{"ts"},
		[]int{0},
		[]storage.DataType{storage.TIMESTAMP},
		nil,
		false,
	)

	// Base timestamp
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add time with 1-minute intervals
		ts := baseTime.Add(time.Duration(i%10000) * time.Minute)
		idx.Add([]storage.ColumnValue{storage.NewTimestampValue(ts)}, int64(i), 0)
	}
}

// BenchmarkMultiColumnarIndexSingleIntRange benchmarks integer range queries
func BenchmarkMultiColumnarIndexSingleIntRange(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewMultiColumnarIndex(
		"bench_idx",
		"bench_table",
		[]string{"num"},
		[]int{0},
		[]storage.DataType{storage.INTEGER},
		nil,
		false,
	)

	// Add 10000 rows with sequential integer data
	for i := 0; i < 10000; i++ {
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
	}

	// Generate random range bounds
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	minVals := make([]int64, b.N)
	maxVals := make([]int64, b.N)

	for i := 0; i < b.N; i++ {
		minVals[i] = int64(r.Intn(8000))
		maxVals[i] = minVals[i] + int64(r.Intn(2000)) + 1 // Ensure range is at least 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.GetRowIDsInRange(
			[]storage.ColumnValue{storage.NewIntegerValue(minVals[i])},
			[]storage.ColumnValue{storage.NewIntegerValue(maxVals[i])},
			true, true,
		)
	}
}

// BenchmarkMultiColumnarIndexSingleIntFilter benchmarks filtering on integer column
func BenchmarkMultiColumnarIndexSingleIntFilter(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewMultiColumnarIndex(
		"bench_idx",
		"bench_table",
		[]string{"num"},
		[]int{0},
		[]storage.DataType{storage.INTEGER},
		nil,
		false,
	)

	// Add 10000 rows with sequential integer data
	for i := 0; i < 10000; i++ {
		idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
	}

	// Generate random filter values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	filterValues := make([]int64, b.N)
	operators := []storage.Operator{storage.GT, storage.LT, storage.EQ, storage.GTE, storage.LTE}

	for i := 0; i < b.N; i++ {
		filterValues[i] = int64(r.Intn(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create expression: num <op> value
		op := operators[i%len(operators)]
		expr := &expression.SimpleExpression{
			Column:   "num",
			Operator: op,
			Value:    filterValues[i],
		}

		idx.GetFilteredRowIDs(expr)
	}
}

// BenchmarkMultiColumnarIndexMultiColumn benchmarks operations on multi-column index
func BenchmarkMultiColumnarIndexMultiColumn(b *testing.B) {
	// Setup: create and populate multi-column index with test data
	idx := NewMultiColumnarIndex(
		"bench_idx",
		"bench_table",
		[]string{"str", "num"},
		[]int{0, 1},
		[]storage.DataType{storage.TEXT, storage.INTEGER},
		nil,
		false,
	)

	// Add test data
	for i := 0; i < 10000; i++ {
		// Use string format to ensure unique-ish values
		str := fmt.Sprintf("str-%d", i%1000)
		num := int64(i % 5000)

		idx.Add([]storage.ColumnValue{
			storage.NewStringValue(str),
			storage.NewIntegerValue(num),
		}, int64(i), 0)
	}

	// Benchmark exact match lookups
	b.Run("ExactMatch", func(b *testing.B) {
		// Generate random values to look up
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		lookupStrs := make([]string, b.N)
		lookupNums := make([]int64, b.N)

		for i := 0; i < b.N; i++ {
			lookupStrs[i] = fmt.Sprintf("str-%d", r.Intn(1000))
			lookupNums[i] = int64(r.Intn(5000))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.GetRowIDsEqual([]storage.ColumnValue{
				storage.NewStringValue(lookupStrs[i]),
				storage.NewIntegerValue(lookupNums[i]),
			})
		}
	})

	// Benchmark AND filtering
	b.Run("AndFiltering", func(b *testing.B) {
		// Generate random filter values
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		strVals := make([]string, b.N)
		numOps := make([]storage.Operator, b.N)
		numVals := make([]int64, b.N)

		for i := 0; i < b.N; i++ {
			strVals[i] = fmt.Sprintf("str-%d", r.Intn(1000))
			numOps[i] = []storage.Operator{storage.GT, storage.LT, storage.GTE, storage.LTE}[r.Intn(4)]
			numVals[i] = int64(r.Intn(5000))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create expressions: str = X AND num <op> Y
			expr1 := &expression.SimpleExpression{
				Column:   "str",
				Operator: storage.EQ,
				Value:    strVals[i],
			}
			expr2 := &expression.SimpleExpression{
				Column:   "num",
				Operator: numOps[i],
				Value:    numVals[i],
			}
			andExpr := &expression.AndExpression{
				Expressions: []storage.Expression{expr1, expr2},
			}

			idx.GetFilteredRowIDs(andExpr)
		}
	})
}

// Benchmark expression filtering on the multi-column index
func BenchmarkMultiColumnarIndexFiltering(b *testing.B) {
	// Run both optimized and baseline variants
	b.Run("OptimizedImplementation", func(b *testing.B) {
		runMultiColumnarIndexFiltering(b)
	})

	// Also measure the simpler case of filtering on just first column
	b.Run("FirstColumnOnly", func(b *testing.B) {
		runMultiColumnarIndexFirstColumnFiltering(b)
	})

	// Measure performance with integer equality on first column
	b.Run("IntegerEquality", func(b *testing.B) {
		runMultiColumnarIndexIntegerFiltering(b, storage.EQ)
	})

	// Measure performance with integer range on first column
	b.Run("IntegerRange", func(b *testing.B) {
		runMultiColumnarIndexIntegerFiltering(b, storage.GT)
	})
}

// Helper function that runs the full multi-column filtering benchmark
func runMultiColumnarIndexFiltering(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewMultiColumnarIndex(
		"bench_idx", "bench_table",
		[]string{"category", "subcategory", "price"},
		[]int{0, 1, 2},
		[]storage.DataType{storage.TEXT, storage.TEXT, storage.INTEGER},
		nil, false,
	)

	// Create test data with several categories and subcategories
	categories := []string{"electronics", "clothing", "books", "furniture", "food"}
	subcategories := map[string][]string{
		"electronics": {"phones", "laptops", "tablets", "cameras"},
		"clothing":    {"shirts", "pants", "shoes", "accessories"},
		"books":       {"fiction", "non-fiction", "education", "comics"},
		"furniture":   {"chairs", "tables", "beds", "storage"},
		"food":        {"fruits", "vegetables", "meat", "dairy"},
	}

	// Add 10000 rows of test data
	rowID := int64(0)
	for i := 0; i < 10000; i++ {
		category := categories[i%len(categories)]
		subCats := subcategories[category]
		subcategory := subCats[i%len(subCats)]
		price := int64(10 + (i % 990)) // Prices between 10 and 999

		idx.Add([]storage.ColumnValue{
			storage.NewStringValue(category),
			storage.NewStringValue(subcategory),
			storage.NewIntegerValue(price),
		}, rowID, 0)
		rowID++
	}

	// Generate random filtering expressions
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	categoryIndices := make([]int, b.N)

	for i := 0; i < b.N; i++ {
		categoryIndices[i] = r.Intn(len(categories))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		category := categories[categoryIndices[i]]

		// Create expressions: category = X AND price > Y
		expr1 := &expression.SimpleExpression{
			Column:   "category",
			Operator: storage.EQ,
			Value:    category,
		}
		expr2 := &expression.SimpleExpression{
			Column:   "price",
			Operator: storage.GT,
			Value:    int64(500),
		}
		andExpr := &expression.AndExpression{
			Expressions: []storage.Expression{expr1, expr2},
		}

		idx.GetFilteredRowIDs(andExpr)
	}
}

// Helper function that runs filtering benchmark on just the first column
func runMultiColumnarIndexFirstColumnFiltering(b *testing.B) {
	// Setup: create and populate index with test data
	idx := NewMultiColumnarIndex(
		"bench_idx", "bench_table",
		[]string{"category", "subcategory", "price"},
		[]int{0, 1, 2},
		[]storage.DataType{storage.TEXT, storage.TEXT, storage.INTEGER},
		nil, false,
	)

	// Create test data with categories
	categories := []string{"electronics", "clothing", "books", "furniture", "food"}
	subcategories := map[string][]string{
		"electronics": {"phones", "laptops", "tablets", "cameras"},
		"clothing":    {"shirts", "pants", "shoes", "accessories"},
		"books":       {"fiction", "non-fiction", "education", "comics"},
		"furniture":   {"chairs", "tables", "beds", "storage"},
		"food":        {"fruits", "vegetables", "meat", "dairy"},
	}

	// Add 10000 rows of test data
	rowID := int64(0)
	for i := 0; i < 10000; i++ {
		category := categories[i%len(categories)]
		subCats := subcategories[category]
		subcategory := subCats[i%len(subCats)]
		price := int64(10 + (i % 990)) // Prices between 10 and 999

		idx.Add([]storage.ColumnValue{
			storage.NewStringValue(category),
			storage.NewStringValue(subcategory),
			storage.NewIntegerValue(price),
		}, rowID, 0)
		rowID++
	}

	// Generate random filtering expressions
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	categoryIndices := make([]int, b.N)

	for i := 0; i < b.N; i++ {
		categoryIndices[i] = r.Intn(len(categories))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		category := categories[categoryIndices[i]]

		// Create expression: category = X (only first column)
		expr := &expression.SimpleExpression{
			Column:   "category",
			Operator: storage.EQ,
			Value:    category,
		}

		idx.GetFilteredRowIDs(expr)
	}
}

// Helper function that runs filtering benchmark on integer values with specified operator
func runMultiColumnarIndexIntegerFiltering(b *testing.B, operator storage.Operator) {
	// Setup index with integer as first column for direct comparison
	idx := NewMultiColumnarIndex(
		"bench_idx", "bench_table",
		[]string{"id", "category", "price"},
		[]int{0, 1, 2},
		[]storage.DataType{storage.INTEGER, storage.TEXT, storage.INTEGER},
		nil, false,
	)

	// Add 10000 rows of test data with sequential IDs
	for i := 0; i < 10000; i++ {
		// Create some clustering to test range queries better
		id := int64(i/10*10 + (i % 10)) // Creates groups of 10
		category := "category-" + fmt.Sprintf("%d", i%5)
		price := int64(100 + (i % 900))

		idx.Add([]storage.ColumnValue{
			storage.NewIntegerValue(id),
			storage.NewStringValue(category),
			storage.NewIntegerValue(price),
		}, int64(i), 0)
	}

	// Generate random filter values appropriate for the operator
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	filterValues := make([]int64, b.N)

	// Create appropriate test values based on operator
	for i := 0; i < b.N; i++ {
		if operator == storage.EQ {
			// For equality, we need values that exist in the data
			filterValues[i] = int64(r.Intn(10000)/10*10 + (r.Intn(10)))
		} else {
			// For range queries, use a wider range
			filterValues[i] = int64(r.Intn(9000))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create expression with the specified operator
		expr := &expression.SimpleExpression{
			Column:   "id",
			Operator: operator,
			Value:    filterValues[i],
		}

		idx.GetFilteredRowIDs(expr)
	}
}

// Comparison benchmark between all three implementations for integer range queries
func BenchmarkComparison_SingleIntRange(b *testing.B) {
	// Setup data once for all sub-benchmarks
	setupData := func(b *testing.B) ([]int64, []int64) {
		b.Helper()
		// Generate random range bounds
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		minVals := make([]int64, b.N)
		maxVals := make([]int64, b.N)

		for i := 0; i < b.N; i++ {
			minVals[i] = int64(r.Intn(8000))
			maxVals[i] = minVals[i] + int64(r.Intn(2000)) + 1 // Ensure range is at least 1
		}
		return minVals, maxVals
	}

	// Benchmark ColumnarIndex
	b.Run("ColumnarIndex", func(b *testing.B) {
		minVals, maxVals := setupData(b)

		idx := NewColumnarIndex(
			"bench_idx",
			"bench_table",
			"num",
			0,
			storage.INTEGER,
			nil,
			false,
		)

		// Add 10000 rows with sequential integer data
		for i := 0; i < 10000; i++ {
			idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.GetRowIDsInRange(
				[]storage.ColumnValue{storage.NewIntegerValue(minVals[i%len(minVals)])},
				[]storage.ColumnValue{storage.NewIntegerValue(maxVals[i%len(maxVals)])},
				true, true,
			)
		}
	})

	// Benchmark MultiColumnarIndex
	b.Run("MultiColumnarIndex", func(b *testing.B) {
		minVals, maxVals := setupData(b)

		idx := NewMultiColumnarIndex(
			"bench_idx",
			"bench_table",
			[]string{"num"},
			[]int{0},
			[]storage.DataType{storage.INTEGER},
			nil,
			false,
		)

		// Add 10000 rows with sequential integer data
		for i := 0; i < 10000; i++ {
			idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.GetRowIDsInRange(
				[]storage.ColumnValue{storage.NewIntegerValue(minVals[i%len(minVals)])},
				[]storage.ColumnValue{storage.NewIntegerValue(maxVals[i%len(maxVals)])},
				true, true,
			)
		}
	})
}

// Comparison benchmark between all three implementations for filter operations
func BenchmarkComparison_SingleIntFilter(b *testing.B) {
	// Setup data once for all sub-benchmarks
	setupData := func(b *testing.B) ([]int64, []storage.Operator) {
		b.Helper()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		filterValues := make([]int64, b.N)
		operators := []storage.Operator{storage.GT, storage.LT, storage.EQ, storage.GTE, storage.LTE}

		for i := 0; i < b.N; i++ {
			filterValues[i] = int64(r.Intn(10000))
		}
		return filterValues, operators
	}

	// Benchmark ColumnarIndex
	b.Run("ColumnarIndex", func(b *testing.B) {
		filterValues, operators := setupData(b)

		idx := NewColumnarIndex(
			"bench_idx",
			"bench_table",
			"num",
			0,
			storage.INTEGER,
			nil,
			false,
		)

		// Add 10000 rows with sequential integer data
		for i := 0; i < 10000; i++ {
			idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create expression: num <op> value
			op := operators[i%len(operators)]
			expr := &expression.SimpleExpression{
				Column:   "num",
				Operator: op,
				Value:    filterValues[i%len(filterValues)],
			}

			idx.GetFilteredRowIDs(expr)
		}
	})

	// Benchmark MultiColumnarIndex
	b.Run("MultiColumnarIndex", func(b *testing.B) {
		filterValues, operators := setupData(b)

		idx := NewMultiColumnarIndex(
			"bench_idx",
			"bench_table",
			[]string{"num"},
			[]int{0},
			[]storage.DataType{storage.INTEGER},
			nil,
			false,
		)

		// Add 10000 rows with sequential integer data
		for i := 0; i < 10000; i++ {
			idx.Add([]storage.ColumnValue{storage.NewIntegerValue(int64(i))}, int64(i), 0)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Create expression: num <op> value
			op := operators[i%len(operators)]
			expr := &expression.SimpleExpression{
				Column:   "num",
				Operator: op,
				Value:    filterValues[i%len(filterValues)],
			}

			idx.GetFilteredRowIDs(expr)
		}
	})
}

// TestMultiColumnarIndexIntegerRangeQueries tests range queries with integers
func TestMultiColumnarIndexIntegerRangeQueries(t *testing.T) {
	// Create a multi-column index for integer data
	idx := NewMultiColumnarIndex(
		"test_idx", "test_table",
		[]string{"id", "value"},
		[]int{0, 1},
		[]storage.DataType{storage.INTEGER, storage.INTEGER},
		nil, false,
	)

	// Add test data
	// id=1 with values 10, 20
	idx.Add([]storage.ColumnValue{storage.NewIntegerValue(1), storage.NewIntegerValue(10)}, 1, 0)
	idx.Add([]storage.ColumnValue{storage.NewIntegerValue(1), storage.NewIntegerValue(20)}, 2, 0)

	// id=2 with values 10, 30
	idx.Add([]storage.ColumnValue{storage.NewIntegerValue(2), storage.NewIntegerValue(10)}, 3, 0)
	idx.Add([]storage.ColumnValue{storage.NewIntegerValue(2), storage.NewIntegerValue(30)}, 4, 0)

	// id=3 with value 40
	idx.Add([]storage.ColumnValue{storage.NewIntegerValue(3), storage.NewIntegerValue(40)}, 5, 0)

	// Test range query on first column only
	minID := storage.NewIntegerValue(1)
	maxID := storage.NewIntegerValue(2)
	rowIDs := idx.GetRowIDsInRange([]storage.ColumnValue{minID}, []storage.ColumnValue{maxID}, true, true)
	if len(rowIDs) != 4 {
		t.Errorf("Expected 4 rows in ID range 1-2, got %d", len(rowIDs))
	}

	// Test range query with both columns
	// Range: id 1-2, value 15-35
	minValues := []storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewIntegerValue(15),
	}
	maxValues := []storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewIntegerValue(35),
	}
	rowIDs = idx.GetRowIDsInRange(minValues, maxValues, true, true)
	// The correct result should be rows 2 and 4
	// Row 2: id 1, value 20 (in range 15-35)
	// Row 4: id 2, value 30 (in range 15-35)
	if len(rowIDs) != 2 || !contains(rowIDs, 2) || !contains(rowIDs, 4) {
		t.Errorf("Expected rows 2 and 4 for composite range query, got %v", rowIDs)
	}
}

// Helper function to check if a slice contains a specific value
func contains(slice []int64, val int64) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
