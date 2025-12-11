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
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/stoolap/stoolap-go/pkg/driver"
)

// TestJSONExtendedFeatures tests more advanced JSON functionality
// Some of these tests may fail if the feature is not yet implemented
func TestJSONExtendedFeatures(t *testing.T) {
	// Connect to an in-memory database
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table with JSON column
	_, err = db.Exec(`
		CREATE TABLE json_extended (
			id INTEGER NOT NULL,
			data JSON
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table with JSON column: %v", err)
	}

	// Insert test data
	testData := []struct {
		id   int
		json string
	}{
		{1, `{"name":"John","age":30,"address":{"city":"New York","zip":"10001"},"tags":["developer","manager"]}`},
		{2, `{"name":"Alice","age":25,"address":{"city":"Boston","zip":"02108"},"tags":["designer","artist"]}`},
		{3, `{"name":"Bob","age":null,"address":null,"tags":[]}`},
		{4, `[1,2,3,4,5]`},
		{5, `{"numbers":[1,2,3,4,5],"nested":{"a":1,"b":2}}`},
	}

	for _, td := range testData {
		_, err = db.Exec("INSERT INTO json_extended (id, data) VALUES (?, ?)", td.id, td.json)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Run a series of tests for different JSON operations
	// We'll use a table-driven test approach and skip tests that are expected to fail
	tests := []struct {
		name          string
		query         string
		args          []interface{}
		expectedFound bool
		skipReason    string // Reason to skip the test (if the feature isn't implemented)
	}{
		{
			name:          "Basic Equality",
			query:         "SELECT id FROM json_extended WHERE data = ?",
			args:          []interface{}{`{"name":"John","age":30,"address":{"city":"New York","zip":"10001"},"tags":["developer","manager"]}`},
			expectedFound: true,
			skipReason:    "JSON equality comparison not fully implemented",
		},
		{
			name:          "IS NULL on JSON field",
			query:         "SELECT id FROM json_extended WHERE data IS NULL",
			args:          []interface{}{},
			expectedFound: false,
			skipReason:    "IS NULL operator not supported in WHERE clauses for JSON",
		},
		{
			// Try an alternative for IS NULL using a workaround
			name:          "NULL check workaround",
			query:         "SELECT COUNT(*) FROM json_extended WHERE id IN (SELECT id FROM json_extended WHERE data = 'null')",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "NULL equality check in JSON not supported",
		},
		// These tests would check JSON path extraction if implemented
		{
			name:          "JSON path extraction",
			query:         "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.name') = 'John'",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "JSON path extraction (JSON_EXTRACT) not implemented",
		},
		{
			name:          "JSON path extraction with nested field",
			query:         "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.address.city') = 'Boston'",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "JSON path extraction not implemented",
		},
		{
			name:          "JSON array access",
			query:         "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.tags[0]') = 'developer'",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "JSON array access not implemented",
		},
		// These tests would check JSON functions if implemented
		{
			name:          "JSON_OBJECT function",
			query:         "SELECT JSON_OBJECT('name', 'John', 'age', 30)",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "JSON_OBJECT function not implemented",
		},
		{
			name:          "JSON_ARRAY function",
			query:         "SELECT JSON_ARRAY(1, 2, 3, 4)",
			args:          []interface{}{},
			expectedFound: true,
			skipReason:    "JSON_ARRAY function not implemented",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skip(tc.skipReason)
			}

			var found bool
			var count int

			// For COUNT(*) queries, we need to handle differently
			if strings.Contains(strings.ToUpper(tc.query), "COUNT(*)") {
				err := db.QueryRow(tc.query, tc.args...).Scan(&count)
				found = (err == nil && count > 0)
			} else {
				var id int
				err := db.QueryRow(tc.query, tc.args...).Scan(&id)
				found = (err == nil)
				if errors.Is(err, sql.ErrNoRows) {
					found = false
				}
			}

			if found != tc.expectedFound {
				if tc.expectedFound {
					t.Errorf("Expected to find results for %s, but none were found", tc.name)
				} else {
					t.Errorf("Expected not to find results for %s, but found some", tc.name)
				}
			}
		})
	}

	// Test for potential JSON modification functions
	jsonModificationTests := []struct {
		name       string
		setupQuery string
		setupArgs  []interface{}
		testQuery  string
		testArgs   []interface{}
		skipReason string
	}{
		{
			name:       "JSON_SET function",
			setupQuery: "UPDATE json_extended SET data = JSON_SET(data, '$.age', 31) WHERE id = 1",
			setupArgs:  []interface{}{},
			testQuery:  "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.age') = 31",
			testArgs:   []interface{}{},
			skipReason: "JSON_SET function not implemented",
		},
		{
			name:       "JSON_INSERT function",
			setupQuery: "UPDATE json_extended SET data = JSON_INSERT(data, '$.new_field', 'new_value') WHERE id = 2",
			setupArgs:  []interface{}{},
			testQuery:  "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.new_field') = 'new_value'",
			testArgs:   []interface{}{},
			skipReason: "JSON_INSERT function not implemented",
		},
		{
			name:       "JSON_REMOVE function",
			setupQuery: "UPDATE json_extended SET data = JSON_REMOVE(data, '$.age') WHERE id = 1",
			setupArgs:  []interface{}{},
			testQuery:  "SELECT id FROM json_extended WHERE JSON_EXTRACT(data, '$.age') IS NULL",
			testArgs:   []interface{}{},
			skipReason: "JSON_REMOVE function not implemented",
		},
	}

	for _, tc := range jsonModificationTests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skip(tc.skipReason)
			}

			_, err := db.Exec(tc.setupQuery, tc.setupArgs...)
			if err != nil {
				t.Fatalf("Setup query failed: %v", err)
			}

			var id int
			err = db.QueryRow(tc.testQuery, tc.testArgs...).Scan(&id)
			if err != nil {
				t.Errorf("Test query failed: %v", err)
			}
		})
	}

	// Test JSON comparison functions if implemented
	jsonComparisonTests := []struct {
		name       string
		query      string
		args       []interface{}
		skipReason string
	}{
		{
			name:       "JSON_CONTAINS function",
			query:      "SELECT id FROM json_extended WHERE JSON_CONTAINS(data, '{\"name\":\"John\"}') = 1",
			args:       []interface{}{},
			skipReason: "JSON_CONTAINS function not implemented",
		},
		{
			name:       "JSON_CONTAINS_PATH function",
			query:      "SELECT id FROM json_extended WHERE JSON_CONTAINS_PATH(data, 'one', '$.address.city') = 1",
			args:       []interface{}{},
			skipReason: "JSON_CONTAINS_PATH function not implemented",
		},
		{
			name:       "JSON_TYPE function",
			query:      "SELECT id FROM json_extended WHERE JSON_TYPE(data) = 'OBJECT'",
			args:       []interface{}{},
			skipReason: "JSON_TYPE function not implemented",
		},
	}

	for _, tc := range jsonComparisonTests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skip(tc.skipReason)
			}

			var id int
			err = db.QueryRow(tc.query, tc.args...).Scan(&id)
			if err != nil {
				t.Errorf("JSON comparison test failed: %v", err)
			}
		})
	}

	t.Log("Extended JSON test complete - note that most tests were skipped as advanced JSON features are not implemented yet")
}
