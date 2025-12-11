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

	"github.com/stoolap/stoolap-go/internal/parser"
)

func TestOrderByInFunctionParsing(t *testing.T) {
	// Test cases for SQL statements with ORDER BY inside function calls
	testCases := []struct {
		name     string
		sql      string
		expected bool // Whether parsing should succeed
	}{
		{
			name:     "Simple FIRST with ORDER BY",
			sql:      "SELECT FIRST(open ORDER BY time_col) FROM candles",
			expected: true,
		},
		{
			name:     "FIRST with ORDER BY ASC",
			sql:      "SELECT FIRST(open ORDER BY time_col ASC) FROM candles",
			expected: true,
		},
		{
			name:     "LAST with ORDER BY DESC",
			sql:      "SELECT LAST(close ORDER BY time_col DESC) FROM candles",
			expected: true,
		},
		{
			name:     "Combined MIN, MAX, FIRST, LAST with ORDER BY",
			sql:      "SELECT FIRST(open ORDER BY time_col), MAX(high), MIN(low), LAST(close ORDER BY time_col), SUM(volume) FROM candles GROUP BY date_col",
			expected: true,
		},
		// This test case is using non-standard SQL syntax (ORDER BY before FROM), commented out
		// {
		// 	name:     "COUNT with ORDER BY (should parse even if semantically odd)",
		// 	sql:      "SELECT COUNT(*) ORDER BY time_col FROM candles",
		// 	expected: true,
		// },
		// Using standard SQL syntax instead
		{
			name:     "COUNT with ORDER BY in standard syntax",
			sql:      "SELECT COUNT(*) FROM candles ORDER BY time_col",
			expected: true,
		},
		{
			name:     "TIME_TRUNC with GROUP BY and ordered aggregates",
			sql:      "SELECT TIME_TRUNC('15m', event_time) AS bucket, FIRST(price ORDER BY event_time) AS open, MAX(price) AS high, MIN(price) AS low, LAST(price ORDER BY event_time) AS close, SUM(volume) AS volume FROM trades GROUP BY bucket",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			stmt, err := parser.Parse(tc.sql)

			if tc.expected {
				// Should parse successfully
				if err != nil {
					t.Fatalf("Expected parsing to succeed, but got error: %v", err)
				}

				// Print statement for debugging
				t.Logf("Parsed statement: %s", stmt)

				// TODO: Add more specific assertions about the parsed structure,
				// checking for the ORDER BY expressions in the function calls
			} else {
				// Should fail to parse
				if err == nil {
					t.Fatalf("Expected parsing to fail, but it succeeded: %v", stmt)
				}
			}
		})
	}
}
