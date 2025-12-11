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

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/expression"
	"github.com/stoolap/stoolap-go/internal/storage/mvcc"
)

// TestColumnarIndexIsNotNull tests the fundamental behavior of IS NOT NULL
// with ColumnarIndex to identify potential issues
func TestColumnarIndexIsNotNull(t *testing.T) {
	// Create a columnar index for testing
	idx := mvcc.NewColumnarIndex(
		"test_index",
		"test_table",
		"value",
		0,
		storage.BOOLEAN,
		nil, // No version store needed
		false,
	)

	// Add rows with specific patterns
	// Every 2nd row has NULL value
	for i := 1; i <= 10; i++ {
		var value storage.ColumnValue
		if i%2 == 0 {
			value = nil // NULL
			t.Logf("Row %d: NULL value", i)
		} else {
			value = storage.NewBooleanValue(true)
			t.Logf("Row %d: true value", i)
		}

		// We'll use the row index as the rowID
		rowID := int64(i)

		// Add to index
		err := idx.Add([]storage.ColumnValue{value}, rowID, 0)
		if err != nil {
			t.Fatalf("Failed to add to index: %v", err)
		}
	}

	// Test IS NOT NULL
	nullCheckExpr := expression.NewIsNotNullExpression("value")

	// Execute the query
	result := idx.GetFilteredRowIDs(nullCheckExpr)

	// Expected: 5 non-NULL rows (1, 3, 5, 7, 9)
	t.Logf("IS NOT NULL returned %d rows: %v", len(result), result)

	expectedCount := 5
	if len(result) != expectedCount {
		t.Errorf("Expected %d results for IS NOT NULL, got %d", expectedCount, len(result))
	}

	// Verify the expected row IDs
	expectedRows := []int64{1, 3, 5, 7, 9}
	for _, expected := range expectedRows {
		found := false
		for _, actual := range result {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected row %d in result but not found", expected)
		}
	}

	// Test IS NULL
	isNullExpr := expression.NewIsNullExpression("value")

	// Execute the query
	nullResult := idx.GetFilteredRowIDs(isNullExpr)

	// Expected: 5 NULL rows (2, 4, 6, 8, 10)
	t.Logf("IS NULL returned %d rows: %v", len(nullResult), nullResult)

	expectedNullCount := 5
	if len(nullResult) != expectedNullCount {
		t.Errorf("Expected %d results for IS NULL, got %d", expectedNullCount, len(nullResult))
	}
}

// TestMultiColumnarIndexIsNotNull tests the behavior of IS NOT NULL
// with MultiColumnarIndex to identify issues with the more complex use case
func TestMultiColumnarIndexIsNotNull(t *testing.T) {
	// Create a multi-column index
	multiIdx := mvcc.NewMultiColumnarIndex(
		"test_multi_index",
		"test_table",
		[]string{"value", "active"},
		[]int{0, 1},
		[]storage.DataType{storage.FLOAT, storage.BOOLEAN},
		nil, // No version store needed
		false,
	)

	// Add 10 rows with specific patterns
	for i := 1; i <= 10; i++ {
		// For even rows, set active to NULL
		var activeValue storage.ColumnValue
		if i%2 == 0 {
			activeValue = nil // NULL
			t.Logf("Row %d: active is NULL", i)
		} else {
			activeValue = storage.NewBooleanValue(true)
			t.Logf("Row %d: active is true", i)
		}

		// All values have non-NULL float
		values := []storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5), // value column
			activeValue,                             // active column
		}

		// Add to index
		err := multiIdx.Add(values, int64(i), 0)
		if err != nil {
			t.Fatalf("Failed to add to multi-column index: %v", err)
		}
	}

	// Test: active IS NOT NULL (should return rows 1, 3, 5, 7, 9)
	nullCheckExpr := expression.NewIsNotNullExpression("active")

	// Execute query
	result := multiIdx.GetFilteredRowIDs(nullCheckExpr)

	t.Logf("Multi-column index: active IS NOT NULL returned %d rows: %v", len(result), result)

	expectedCount := 5
	if len(result) != expectedCount {
		t.Errorf("Expected %d results for multi-column IS NOT NULL, got %d", expectedCount, len(result))
	}

	// Test combined query: value > 3 AND value <= 8 AND active IS NOT NULL
	// Should return rows 5, 7 (rows in range 4-8 with non-NULL active)
	expr1 := &expression.SimpleExpression{
		Column:   "value",
		Operator: storage.GT,
		Value:    float64(3),
	}

	expr2 := &expression.SimpleExpression{
		Column:   "value",
		Operator: storage.LTE,
		Value:    float64(8),
	}

	// Create the combined AND expression
	andExpr := &expression.AndExpression{
		Expressions: []storage.Expression{expr1, expr2, nullCheckExpr},
	}

	// Execute query
	combinedResult := multiIdx.GetFilteredRowIDs(andExpr)

	t.Logf("Multi-column index: combined query returned %d rows: %v", len(combinedResult), combinedResult)

	expectedCombinedCount := 3 // Rows 5, 7 (in range 4-8, only odd rows have non-NULL active)
	if len(combinedResult) != expectedCombinedCount {
		t.Errorf("Expected %d results for combined query, got %d", expectedCombinedCount, len(combinedResult))
	}

	// Verify we got the expected rows
	expectedRows := []int64{5, 7}
	foundAll := true
	for _, expected := range expectedRows {
		found := false
		for _, actual := range combinedResult {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected row %d in combined result but not found", expected)
			foundAll = false
		}
	}

	if foundAll {
		t.Logf("All expected rows found in combined result")
	}

	// Print detailed diagnostic information
	t.Log("\nDiagnostic information:")

	// Debug: test individual range expressions
	rangeResult := multiIdx.GetFilteredRowIDs(expr1)
	t.Logf("value > 3: %d rows: %v", len(rangeResult), rangeResult)

	range2Result := multiIdx.GetFilteredRowIDs(expr2)
	t.Logf("value <= 8: %d rows: %v", len(range2Result), range2Result)

	// Debug: test range without NULL check
	rangeAndExpr := &expression.AndExpression{
		Expressions: []storage.Expression{expr1, expr2},
	}
	rangeAndResult := multiIdx.GetFilteredRowIDs(rangeAndExpr)
	t.Logf("value > 3 AND value <= 8: %d rows: %v", len(rangeAndResult), rangeAndResult)
}
