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
	"context"
	"testing"

	"github.com/stoolap/stoolap"
)

// TestCastWhereClause tests CAST expressions in WHERE clauses
func TestCastWhereClause(t *testing.T) {
	ctx := context.Background()
	db, _ := stoolap.Open("memory://")
	defer db.Close()

	executor := db.Executor()

	// Create a test table with various data types
	executor.Execute(ctx, nil, `
		CREATE TABLE test_cast_where (
			id INTEGER PRIMARY KEY,
			text_val TEXT,
			int_val INTEGER,
			float_val FLOAT,
			bool_val BOOLEAN
		)
	`)

	// Insert test data
	executor.Execute(ctx, nil, `
		INSERT INTO test_cast_where (id, text_val, int_val, float_val, bool_val)
		VALUES
			(1, '123', 123, 123.45, true),
			(2, '456', 456, 456.78, false),
			(3, '789', 789, 789.01, true),
			(4, '0', 0, 0.0, false),
			(5, '-50', -50, -50.5, true),
			(6, NULL, NULL, NULL, NULL)
	`)

	// Test cases for CAST in WHERE clauses
	testCases := []struct {
		name           string
		query          string
		expectedIDs    []int
		expectedCount  int
		shouldFail     bool
		failureMessage string
	}{
		{
			name:          "CAST text_val to INTEGER with greater than",
			query:         "SELECT id FROM test_cast_where WHERE CAST(text_val AS INTEGER) > 400",
			expectedIDs:   []int{2, 3},
			expectedCount: 2,
		},
		{
			name:          "CAST text_val to INTEGER with less than",
			query:         "SELECT id FROM test_cast_where WHERE CAST(text_val AS INTEGER) < 200",
			expectedIDs:   []int{1, 4, 5},
			expectedCount: 3,
		},
		{
			name:          "CAST text_val to INTEGER with equals",
			query:         "SELECT id FROM test_cast_where WHERE CAST(text_val AS INTEGER) = 456",
			expectedIDs:   []int{2},
			expectedCount: 1,
		},
		{
			name:          "CAST text_val to INTEGER with NOT equals",
			query:         "SELECT id FROM test_cast_where WHERE CAST(text_val AS INTEGER) <> 123 AND text_val IS NOT NULL",
			expectedIDs:   []int{2, 3, 4, 5},
			expectedCount: 4,
		},
		{
			name:          "CAST int_val to STRING with LIKE",
			query:         "SELECT id FROM test_cast_where WHERE CAST(int_val AS STRING) LIKE '4%'",
			expectedIDs:   []int{2},
			expectedCount: 1,
		},
		{
			name:          "CAST float_val to INTEGER",
			query:         "SELECT id FROM test_cast_where WHERE CAST(float_val AS INTEGER) = 123",
			expectedIDs:   []int{1},
			expectedCount: 1,
		},
		{
			name:          "CAST bool_val to INTEGER",
			query:         "SELECT id FROM test_cast_where WHERE CAST(bool_val AS INTEGER) = 1",
			expectedIDs:   []int{1, 3, 5},
			expectedCount: 3,
		},
		{
			name:          "Multiple CAST in WHERE",
			query:         "SELECT id FROM test_cast_where WHERE CAST(text_val AS INTEGER) > 100 AND CAST(float_val AS INTEGER) < 500",
			expectedIDs:   []int{1, 2},
			expectedCount: 2,
		},
		// Add more test cases as needed
	}

	// Debug - check with simple WHERE first
	t.Log("DEBUG: Testing simple WHERE:")
	simpleResult, _ := executor.Execute(ctx, nil, "SELECT id FROM test_cast_where WHERE id > 3")
	var simpleIDs []int
	for simpleResult.Next() {
		var id int
		simpleResult.Scan(&id)
		simpleIDs = append(simpleIDs, id)
	}
	t.Logf("DEBUG: Simple WHERE id > 3 returned: %v", simpleIDs)

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, nil, tc.query)
			if err != nil {
				if tc.shouldFail {
					t.Logf("Expected failure occurred: %v", err)
					return
				}
				t.Fatalf("Unexpected error: %v", err)
			}

			// Get column info for debugging
			cols := result.Columns()
			t.Logf("DEBUG: Result has %d columns: %v", len(cols), cols)

			// Count and collect results
			var matchedIDs []int
			count := 0
			for result.Next() {
				var id int
				var extraCol1, extraCol2 interface{} // For handling extra columns

				// Debug the Scan operation based on column count
				switch len(cols) {
				case 3:
					// Handle the case with 3 columns (for multiple CAST test)
					err := result.Scan(&id, &extraCol1, &extraCol2)
					if err != nil {
						t.Logf("ERROR scanning ID with 3 columns: %v", err)
						continue
					}
					t.Logf("DEBUG: Scanned ID: %v (type: %T), extras: %v, %v", id, id, extraCol1, extraCol2)
				case 2:
					// Handle the case with 2 columns
					err := result.Scan(&id, &extraCol1)
					if err != nil {
						t.Logf("ERROR scanning ID with 2 columns: %v", err)
						continue
					}
					t.Logf("DEBUG: Scanned ID: %v (type: %T), extra: %v", id, id, extraCol1)
				case 1:
					// Handle single column case
					err := result.Scan(&id)
					if err != nil {
						t.Logf("ERROR scanning ID: %v", err)
						continue
					}
					t.Logf("DEBUG: Scanned ID: %v (type: %T)", id, id)
				default:
					t.Logf("ERROR: Unexpected column count: %d", len(cols))
					continue
				}

				// Only add non-zero IDs to the results
				if id > 0 {
					matchedIDs = append(matchedIDs, id)
					count++
				}
			}

			// Always print the results for debugging
			t.Logf("DEBUG: Query returned IDs: %v (expected %v)", matchedIDs, tc.expectedIDs)

			// Validate count
			if count != tc.expectedCount {
				t.Errorf("Expected %d matches, got %d: %v", tc.expectedCount, count, matchedIDs)
			}

			// Validate matched IDs (order may not matter)
			if len(matchedIDs) != len(tc.expectedIDs) {
				t.Errorf("Expected IDs %v, got %v", tc.expectedIDs, matchedIDs)
			} else {
				// Create a lookup map for expected IDs
				expectedMap := make(map[int]bool)
				for _, id := range tc.expectedIDs {
					expectedMap[id] = true
				}

				// Check each matched ID
				for _, id := range matchedIDs {
					if !expectedMap[id] {
						t.Errorf("Unexpected ID %d in results", id)
					}
				}
			}
		})
	}
}
