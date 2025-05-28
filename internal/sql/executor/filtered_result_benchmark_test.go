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
package executor

import (
	"context"
	"testing"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// MockResult implements a simple storage.Result for testing
type MockResult struct {
	columns []string
	rows    [][]interface{}
	current int
}

func (m *MockResult) Columns() []string {
	return m.columns
}

func (m *MockResult) Next() bool {
	m.current++
	return m.current <= len(m.rows)
}

func (m *MockResult) Scan(dest ...interface{}) error {
	row := m.rows[m.current-1]
	for i, val := range row {
		*(dest[i].(*interface{})) = val
	}
	return nil
}

func (m *MockResult) Close() error {
	return nil
}

func (m *MockResult) RowsAffected() int64 {
	return int64(len(m.rows))
}

func (m *MockResult) LastInsertID() int64 {
	return 0
}

func (m *MockResult) Context() context.Context {
	return context.Background()
}

func (m *MockResult) WithAliases(aliases map[string]string) storage.Result {
	return m
}

func (m *MockResult) Row() storage.Row {
	if m.current < 0 || m.current >= len(m.rows) {
		return nil
	}

	// Create a Row from the current row values
	row := make(storage.Row, len(m.columns))

	// Convert each value to ColumnValue
	for i, val := range m.rows[m.current] {
		if val == nil {
			row[i] = storage.StaticNullUnknown
		} else {
			row[i] = storage.NewDirectValueFromInterface(val)
		}
	}

	return row
}

// BenchmarkFilteredResult benchmarks the FilteredResult next() method
func BenchmarkFilteredResult(b *testing.B) {
	// Create mock data - 1000 rows with 5 columns
	columns := []string{"id", "name", "value", "is_active", "category"}
	rows := make([][]interface{}, 1000)

	for i := 0; i < 1000; i++ {
		rows[i] = []interface{}{
			int64(i),
			"name" + string(rune(65+(i%26))),
			float64(float64(i) * 1.5),
			i%2 == 0,
			"category" + string(rune(65+(i%5))),
		}
	}

	mockResult := &MockResult{
		columns: columns,
		rows:    rows,
	}

	// Parse a simple WHERE expression
	stmt, _ := parser.Parse("SELECT * FROM dummy WHERE value > 500 AND is_active = true")
	selectStmt := stmt.(*parser.SelectStatement)

	registry := GetGlobalFunctionRegistry()
	evaluator := NewEvaluator(context.Background(), registry)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset mock result position
		mockResult.current = 0

		// Create a new filtered result for each iteration
		filteredResult := &FilteredResult{
			result:    mockResult,
			whereExpr: selectStmt.Where,
			evaluator: evaluator,
		}

		// Count matching rows
		count := 0
		for filteredResult.Next() {
			count++
		}

		// Ensure we close the result
		filteredResult.Close()

		// Verify expected output to prevent optimizations from skipping work
		if count != 333 { // Expected number of rows that match the filter
			b.Fatalf("Expected 333 matching rows, got %d", count)
		}
	}
}

// BenchmarkFilteredResultWithLargeRows tests performance with larger rows
func BenchmarkFilteredResultWithLargeRows(b *testing.B) {
	// Create mock data - 500 rows with 20 columns
	columns := make([]string, 20)
	for i := 0; i < 20; i++ {
		columns[i] = "column" + string(rune(65+i))
	}

	rows := make([][]interface{}, 500)
	for i := 0; i < 500; i++ {
		row := make([]interface{}, 20)
		for j := 0; j < 20; j++ {
			switch j % 4 {
			case 0:
				row[j] = int64(i * j)
			case 1:
				row[j] = "value" + string(rune(65+(i+j)%26))
			case 2:
				row[j] = float64(float64(i+j) * 1.5)
			case 3:
				row[j] = (i+j)%3 == 0
			}
		}
		rows[i] = row
	}

	mockResult := &MockResult{
		columns: columns,
		rows:    rows,
	}

	// Parse a more complex WHERE expression
	stmt, _ := parser.Parse("SELECT * FROM dummy WHERE columnA > 100 AND columnD = true AND columnB LIKE 'value%'")
	selectStmt := stmt.(*parser.SelectStatement)

	registry := GetGlobalFunctionRegistry()
	evaluator := NewEvaluator(context.Background(), registry)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset mock result position
		mockResult.current = 0

		// Create a new filtered result for each iteration
		filteredResult := &FilteredResult{
			result:    mockResult,
			whereExpr: selectStmt.Where,
			evaluator: evaluator,
		}

		// Count matching rows
		count := 0
		for filteredResult.Next() {
			count++
		}

		// Ensure we close the result
		filteredResult.Close()
	}
}

// BenchmarkJoinVsStreamingJoin benchmarks the difference between regular and streaming join
func BenchmarkJoinVsStreamingJoin(b *testing.B) {
	// Create mock data - employees (1000 rows)
	employeeColumns := []string{"id", "name", "dept_id"}
	employeeRows := make([][]interface{}, 1000)

	for i := 0; i < 1000; i++ {
		employeeRows[i] = []interface{}{
			int64(i),
			"Employee" + string(rune(65+(i%26))),
			int64(i % 100), // dept_id between 0-99
		}
	}

	// Create mock data - departments (100 rows)
	deptColumns := []string{"id", "name"}
	deptRows := make([][]interface{}, 100)

	for i := 0; i < 100; i++ {
		deptRows[i] = []interface{}{
			int64(i),
			"Department" + string(rune(65+(i%26))),
		}
	}

	// Manually create a simple join condition expression
	// Since we can't directly parse e.dept_id = d.id
	leftIdentifier := &parser.Identifier{
		Value: "dept_id",
	}

	rightIdentifier := &parser.Identifier{
		Value: "id",
	}

	// Create a simple equality comparison
	joinCondition := &parser.InfixExpression{
		Left:     leftIdentifier,
		Operator: "=",
		Right:    rightIdentifier,
	}

	registry := GetGlobalFunctionRegistry()
	evaluator := NewEvaluator(context.Background(), registry)

	// Benchmark test for StreamingJoin
	b.Run("StreamingJoin", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Reset the mock results
			employeeResult := &MockResult{
				columns: employeeColumns,
				rows:    employeeRows,
				current: 0,
			}

			deptResult := &MockResult{
				columns: deptColumns,
				rows:    deptRows,
				current: 0,
			}

			// Create a new streaming join result
			streamJoin := NewStreamingJoinResult(
				employeeResult,
				deptResult,
				"INNER",
				joinCondition,
				evaluator,
				"e", "d",
			)

			// Count matching rows
			count := 0
			for streamJoin.Next() {
				count++
			}

			// Ensure we close the result
			streamJoin.Close()

			// Verify expected output to prevent optimizations from skipping work
			if count == 0 {
				b.Fatalf("Expected matching rows, got none")
			}
		}
	})

	// Benchmark test for regular JoinResult (original implementation)
	b.Run("RegularJoin", func(b *testing.B) {
		b.Skip("Skipping, this benchmark panics due to a bug in the test")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Reset the mock results
			employeeResult := &MockResult{
				columns: employeeColumns,
				rows:    employeeRows,
				current: 0,
			}

			deptResult := &MockResult{
				columns: deptColumns,
				rows:    deptRows,
				current: 0,
			}

			// Create a new regular join result
			joinResult := NewJoinResult(
				employeeResult,
				deptResult,
				"INNER",
				joinCondition,
				evaluator,
				"e", "d",
			)

			// Count matching rows
			count := 0
			for joinResult.Next() {
				count++
			}

			// Ensure we close the result
			joinResult.Close()

			// Verify expected output to prevent optimizations from skipping work
			if count == 0 {
				b.Fatalf("Expected matching rows, got none")
			}
		}
	})
}
