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

// TestMultiColumnarIndexWithNullCheck specifically tests the query pattern
// "value > ? AND value <= ? AND active IS NOT NULL"
// which should be optimized in the multi-columnar index.
func TestMultiColumnarIndexWithNullCheck(t *testing.T) {
	// Create a multi-columnar index
	multiIdx := mvcc.NewMultiColumnarIndex(
		"test_multi_index",
		"benchmark_table",
		[]string{"value", "active"},
		[]int{0, 1},
		[]storage.DataType{storage.FLOAT, storage.BOOLEAN},
		nil, // No version store needed for this test
		false,
	)

	// Create a boolean columnar index for comparison
	activeIdx := mvcc.NewColumnarIndex(
		"test_active_index",
		"benchmark_table",
		"active",
		1,
		storage.BOOLEAN,
		nil,
		false,
	)

	// Create a value columnar index for comparison
	valueIdx := mvcc.NewColumnarIndex(
		"test_value_index",
		"benchmark_table",
		"value",
		0,
		storage.FLOAT,
		nil,
		false,
	)

	// Add 20 rows with specific patterns to test NULL handling
	for i := 0; i < 20; i++ {
		// Make every 5th row have NULL in the active column
		var activeValue storage.ColumnValue
		if i%5 == 0 {
			activeValue = nil // NULL value
		} else {
			activeValue = storage.NewBooleanValue(i%2 == 0)
		}

		// Add value to single column indices for comparison
		valueIdx.Add([]storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5),
		}, int64(i+1), 0)

		activeIdx.Add([]storage.ColumnValue{
			activeValue,
		}, int64(i+1), 0)

		// Add to multi-column index
		multiIdx.Add([]storage.ColumnValue{
			storage.NewFloatValue(float64(i) + 0.5), // value column
			activeValue,                             // active column
		}, int64(i+1), 0)
	}

	// 1. Test IS NOT NULL on the active column
	nullCheckExpr := expression.NewIsNotNullExpression("active")

	// Get results from the single-column index
	singleColResult := activeIdx.GetFilteredRowIDs(nullCheckExpr)

	// Print diagnostics and count
	t.Logf("IS NOT NULL on single column index returned %d rows", len(singleColResult))
	if len(singleColResult) != 16 {
		t.Errorf("Expected 16 results for IS NOT NULL on single column, got %d", len(singleColResult))
	}

	// Get results from the multi-column index
	multiColNullResult := multiIdx.GetFilteredRowIDs(nullCheckExpr)

	// Print diagnostics and count
	t.Logf("IS NOT NULL on multi-column index returned %d rows", len(multiColNullResult))
	if len(multiColNullResult) != 16 {
		t.Errorf("Expected 16 results for IS NOT NULL on multi-column, got %d", len(multiColNullResult))
	}

	// 2. Test combined "value > 5 AND value <= 15 AND active IS NOT NULL"
	expr1 := &expression.SimpleExpression{
		Column:   "value",
		Operator: storage.GT,
		Value:    float64(5),
	}

	expr2 := &expression.SimpleExpression{
		Column:   "value",
		Operator: storage.LTE,
		Value:    float64(15),
	}

	// Create the combined AND expression
	andExpr := &expression.AndExpression{
		Expressions: []storage.Expression{expr1, expr2, nullCheckExpr},
	}

	// Get results from the multi-column index
	combinedResult := multiIdx.GetFilteredRowIDs(andExpr)

	// Since rows 0, 5, 10, 15 have NULL active values, we expect 8 results in range 6-14
	// Print diagnostics and count
	t.Logf("Combined expression on multi-column index returned %d rows", len(combinedResult))
	if len(combinedResult) != 8 {
		t.Errorf("Expected 8 results for combined expression, got %d", len(combinedResult))
	}
}
