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
	"testing"
	"time"

	_ "github.com/stoolap/stoolap-go/pkg/driver" // Import the Stoolap driver
)

func TestInsertWithFunctions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE test_functions (
			id INTEGER PRIMARY KEY,
			name TEXT,
			created_at TIMESTAMP,
			upper_name TEXT,
			length_name INTEGER,
			concat_value TEXT,
			math_result FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		args     []interface{}
		expected map[string]interface{}
	}{
		{
			name:  "INSERT with NOW() function",
			query: "INSERT INTO test_functions (id, name, created_at) VALUES (1, 'test', NOW())",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":   int64(1),
				"name": "test",
			},
		},
		{
			name:  "INSERT with UPPER() function",
			query: "INSERT INTO test_functions (id, name, upper_name) VALUES (2, 'hello', UPPER('hello'))",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":         int64(2),
				"name":       "hello",
				"upper_name": "HELLO",
			},
		},
		{
			name:  "INSERT with LENGTH() function",
			query: "INSERT INTO test_functions (id, name, length_name) VALUES (3, 'world', LENGTH('world'))",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":          int64(3),
				"name":        "world",
				"length_name": int64(5),
			},
		},
		{
			name:  "INSERT with CONCAT() function",
			query: "INSERT INTO test_functions (id, name, concat_value) VALUES (4, 'test', CONCAT('Hello', ' ', 'World'))",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":           int64(4),
				"name":         "test",
				"concat_value": "Hello World",
			},
		},
		{
			name:  "INSERT with arithmetic expression",
			query: "INSERT INTO test_functions (id, name, math_result) VALUES (5, 'math', 10.5 * 2 + 1)",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":          int64(5),
				"name":        "math",
				"math_result": float64(22),
			},
		},
		{
			name:  "INSERT with nested functions",
			query: "INSERT INTO test_functions (id, name, upper_name) VALUES (6, 'nested', UPPER(CONCAT('hello', ' world')))",
			args:  []interface{}{},
			expected: map[string]interface{}{
				"id":         int64(6),
				"name":       "nested",
				"upper_name": "HELLO WORLD",
			},
		},
		{
			name:  "INSERT with parameters and functions",
			query: "INSERT INTO test_functions (id, name, upper_name) VALUES (?, ?, UPPER(?))",
			args:  []interface{}{7, "param", "lowercase"},
			expected: map[string]interface{}{
				"id":         int64(7),
				"name":       "param",
				"upper_name": "LOWERCASE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the INSERT
			result, err := db.Exec(tt.query, tt.args...)
			if err != nil {
				t.Fatalf("Failed to execute INSERT: %v", err)
			}

			// Check rows affected
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("Failed to get rows affected: %v", err)
			}
			if rowsAffected != 1 {
				t.Errorf("Expected 1 row affected, got %d", rowsAffected)
			}

			// Verify the inserted data
			var id int64
			var name, upperName, concatValue sql.NullString
			var createdAt sql.NullTime
			var lengthName sql.NullInt64
			var mathResult sql.NullFloat64

			err = db.QueryRow("SELECT id, name, created_at, upper_name, length_name, concat_value, math_result FROM test_functions WHERE id = ?", tt.expected["id"]).
				Scan(&id, &name, &createdAt, &upperName, &lengthName, &concatValue, &mathResult)
			if err != nil {
				t.Fatalf("Failed to query inserted row: %v", err)
			}

			// Verify expected values
			if expectedID, ok := tt.expected["id"]; ok && id != expectedID {
				t.Errorf("Expected id=%v, got %v", expectedID, id)
			}
			if expectedName, ok := tt.expected["name"]; ok && name.String != expectedName {
				t.Errorf("Expected name=%v, got %v", expectedName, name.String)
			}
			if expectedUpper, ok := tt.expected["upper_name"]; ok && upperName.String != expectedUpper {
				t.Errorf("Expected upper_name=%v, got %v", expectedUpper, upperName.String)
			}
			if expectedLength, ok := tt.expected["length_name"]; ok && lengthName.Int64 != expectedLength {
				t.Errorf("Expected length_name=%v, got %v", expectedLength, lengthName.Int64)
			}
			if expectedConcat, ok := tt.expected["concat_value"]; ok && concatValue.String != expectedConcat {
				t.Errorf("Expected concat_value=%v, got %v", expectedConcat, concatValue.String)
			}
			if expectedMath, ok := tt.expected["math_result"]; ok && mathResult.Float64 != expectedMath {
				t.Errorf("Expected math_result=%v, got %v", expectedMath, mathResult.Float64)
			}

			// Special check for NOW() function - just verify it's a recent timestamp
			if tt.name == "INSERT with NOW() function" && createdAt.Valid {
				if time.Since(createdAt.Time) > time.Minute {
					t.Errorf("Expected recent timestamp, got %v", createdAt.Time)
				}
			}
		})
	}
}

func TestUpdateWithFunctions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`
		CREATE TABLE update_test (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value INTEGER,
			updated_at TIMESTAMP,
			processed_name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec("INSERT INTO update_test (id, name, value) VALUES (1, 'hello', 10), (2, 'world', 20)")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		args     []interface{}
		checkID  int64
		expected map[string]interface{}
	}{
		{
			name:    "UPDATE with UPPER() function",
			query:   "UPDATE update_test SET processed_name = UPPER(name) WHERE id = 1",
			args:    []interface{}{},
			checkID: 1,
			expected: map[string]interface{}{
				"processed_name": "HELLO",
			},
		},
		{
			name:    "UPDATE with arithmetic expression",
			query:   "UPDATE update_test SET value = (value * 2 + 5) WHERE id = 1",
			args:    []interface{}{},
			checkID: 1,
			expected: map[string]interface{}{
				"value": int64(25), // (10 * 2) + 5
			},
		},
		{
			name:    "UPDATE with NOW() function",
			query:   "UPDATE update_test SET updated_at = NOW() WHERE id = 2",
			args:    []interface{}{},
			checkID: 2,
			expected: map[string]interface{}{
				"updated_at_recent": true, // Special flag to check if timestamp is recent
			},
		},
		{
			name:    "UPDATE with CONCAT() function",
			query:   "UPDATE update_test SET processed_name = CONCAT('Processed: ', UPPER(name)) WHERE id = 2",
			args:    []interface{}{},
			checkID: 2,
			expected: map[string]interface{}{
				"processed_name": "Processed: WORLD",
			},
		},
		{
			name:    "UPDATE with parameters and functions",
			query:   "UPDATE update_test SET processed_name = CONCAT(?, UPPER(name)) WHERE id = ?",
			args:    []interface{}{"Prefix: ", 1},
			checkID: 1,
			expected: map[string]interface{}{
				"processed_name": "Prefix: HELLO",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the UPDATE
			result, err := db.Exec(tt.query, tt.args...)
			if err != nil {
				t.Fatalf("Failed to execute UPDATE: %v", err)
			}

			// Check rows affected
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("Failed to get rows affected: %v", err)
			}
			if rowsAffected != 1 {
				t.Errorf("Expected 1 row affected, got %d", rowsAffected)
			}

			// Verify the updated data
			var id int64
			var name, processedName sql.NullString
			var value sql.NullInt64
			var updatedAt sql.NullTime

			err = db.QueryRow("SELECT id, name, value, updated_at, processed_name FROM update_test WHERE id = ?", tt.checkID).
				Scan(&id, &name, &value, &updatedAt, &processedName)
			if err != nil {
				t.Fatalf("Failed to query updated row: %v", err)
			}

			// Verify expected values
			if expectedProcessed, ok := tt.expected["processed_name"]; ok && processedName.String != expectedProcessed {
				t.Errorf("Expected processed_name=%v, got %v", expectedProcessed, processedName.String)
			}
			if expectedValue, ok := tt.expected["value"]; ok && value.Int64 != expectedValue {
				t.Errorf("Expected value=%v, got %v", expectedValue, value.Int64)
			}

			// Special check for NOW() function
			if checkRecent, ok := tt.expected["updated_at_recent"]; ok && checkRecent.(bool) && updatedAt.Valid {
				if time.Since(updatedAt.Time) > time.Minute {
					t.Errorf("Expected recent timestamp, got %v", updatedAt.Time)
				}
			}
		})
	}
}

func TestDMLCastExpressions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE cast_test (
			id INTEGER PRIMARY KEY,
			str_value TEXT,
			int_value INTEGER,
			float_value FLOAT,
			bool_value BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected map[string]interface{}
	}{
		{
			name:  "INSERT with CAST string to integer",
			query: "INSERT INTO cast_test (id, int_value) VALUES (1, CAST('123' AS INTEGER))",
			expected: map[string]interface{}{
				"id":        int64(1),
				"int_value": int64(123),
			},
		},
		{
			name:  "INSERT with CAST integer to string",
			query: "INSERT INTO cast_test (id, str_value) VALUES (2, CAST(456 AS TEXT))",
			expected: map[string]interface{}{
				"id":        int64(2),
				"str_value": "456",
			},
		},
		{
			name:  "INSERT with CAST float to integer",
			query: "INSERT INTO cast_test (id, int_value) VALUES (3, CAST(78.9 AS INTEGER))",
			expected: map[string]interface{}{
				"id":        int64(3),
				"int_value": int64(78),
			},
		},
		{
			name:  "INSERT with CAST boolean to integer",
			query: "INSERT INTO cast_test (id, int_value) VALUES (4, CAST(true AS INTEGER))",
			expected: map[string]interface{}{
				"id":        int64(4),
				"int_value": int64(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the INSERT
			_, err := db.Exec(tt.query)
			if err != nil {
				t.Fatalf("Failed to execute INSERT: %v", err)
			}

			// Verify the inserted data
			var id int64
			var strValue sql.NullString
			var intValue sql.NullInt64
			var floatValue sql.NullFloat64
			var boolValue sql.NullBool

			err = db.QueryRow("SELECT id, str_value, int_value, float_value, bool_value FROM cast_test WHERE id = ?", tt.expected["id"]).
				Scan(&id, &strValue, &intValue, &floatValue, &boolValue)
			if err != nil {
				t.Fatalf("Failed to query inserted row: %v", err)
			}

			// Verify expected values
			if expectedID, ok := tt.expected["id"]; ok && id != expectedID {
				t.Errorf("Expected id=%v, got %v", expectedID, id)
			}
			if expectedStr, ok := tt.expected["str_value"]; ok && strValue.String != expectedStr {
				t.Errorf("Expected str_value=%v, got %v", expectedStr, strValue.String)
			}
			if expectedInt, ok := tt.expected["int_value"]; ok && intValue.Int64 != expectedInt {
				t.Errorf("Expected int_value=%v, got %v", expectedInt, intValue.Int64)
			}
			if expectedFloat, ok := tt.expected["float_value"]; ok && floatValue.Float64 != expectedFloat {
				t.Errorf("Expected float_value=%v, got %v", expectedFloat, floatValue.Float64)
			}
		})
	}
}

func TestDMLCaseExpressions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE case_test (
			id INTEGER PRIMARY KEY,
			category TEXT,
			score INTEGER,
			grade TEXT,
			status TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected map[string]interface{}
	}{
		{
			name: "INSERT with simple CASE expression",
			query: `INSERT INTO case_test (id, category, grade) VALUES (1, 'A', 
				CASE 'A' 
					WHEN 'A' THEN 'Excellent'
					WHEN 'B' THEN 'Good'
					ELSE 'Average'
				END)`,
			expected: map[string]interface{}{
				"id":       int64(1),
				"category": "A",
				"grade":    "Excellent",
			},
		},
		{
			name: "INSERT with searched CASE expression",
			query: `INSERT INTO case_test (id, score, grade) VALUES (2, 85, 
				CASE 
					WHEN 85 >= 90 THEN 'A'
					WHEN 85 >= 80 THEN 'B'
					WHEN 85 >= 70 THEN 'C'
					ELSE 'F'
				END)`,
			expected: map[string]interface{}{
				"id":    int64(2),
				"score": int64(85),
				"grade": "B",
			},
		},
		{
			name: "INSERT with CASE and functions",
			query: `INSERT INTO case_test (id, category, status) VALUES (3, 'premium', 
				CASE UPPER('premium')
					WHEN 'PREMIUM' THEN 'VIP'
					WHEN 'STANDARD' THEN 'Regular'
					ELSE 'Basic'
				END)`,
			expected: map[string]interface{}{
				"id":       int64(3),
				"category": "premium",
				"status":   "VIP",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the INSERT
			_, err := db.Exec(tt.query)
			if err != nil {
				t.Fatalf("Failed to execute INSERT: %v", err)
			}

			// Verify the inserted data
			var id int64
			var category, grade, status sql.NullString
			var score sql.NullInt64

			err = db.QueryRow("SELECT id, category, score, grade, status FROM case_test WHERE id = ?", tt.expected["id"]).
				Scan(&id, &category, &score, &grade, &status)
			if err != nil {
				t.Fatalf("Failed to query inserted row: %v", err)
			}

			// Verify expected values
			if expectedID, ok := tt.expected["id"]; ok && id != expectedID {
				t.Errorf("Expected id=%v, got %v", expectedID, id)
			}
			if expectedCategory, ok := tt.expected["category"]; ok && category.String != expectedCategory {
				t.Errorf("Expected category=%v, got %v", expectedCategory, category.String)
			}
			if expectedScore, ok := tt.expected["score"]; ok && score.Int64 != expectedScore {
				t.Errorf("Expected score=%v, got %v", expectedScore, score.Int64)
			}
			if expectedGrade, ok := tt.expected["grade"]; ok && grade.String != expectedGrade {
				t.Errorf("Expected grade=%v, got %v", expectedGrade, grade.String)
			}
			if expectedStatus, ok := tt.expected["status"]; ok && status.String != expectedStatus {
				t.Errorf("Expected status=%v, got %v", expectedStatus, status.String)
			}
		})
	}
}

func TestComplexExpressions(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE complex_test (
			id INTEGER PRIMARY KEY,
			result TEXT,
			number_result FLOAT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected map[string]interface{}
	}{
		{
			name:  "INSERT with nested function calls and arithmetic",
			query: "INSERT INTO complex_test (id, result) VALUES (1, UPPER(CONCAT('result: ', CAST((10 * 2 + 5) AS TEXT))))",
			expected: map[string]interface{}{
				"id":     int64(1),
				"result": "RESULT: 25.000000", // Arithmetic returns float64, cast to text shows decimals
			},
		},
		{
			name:  "INSERT with complex arithmetic expression",
			query: "INSERT INTO complex_test (id, number_result) VALUES (2, (10.5 + 5) * 2 / 3)",
			expected: map[string]interface{}{
				"id":            int64(2),
				"number_result": float64(10.333333333333334), // (15.5 * 2) / 3
			},
		},
		{
			name: "INSERT with CASE containing function calls",
			query: `INSERT INTO complex_test (id, result) VALUES (3, 
				CASE LENGTH('hello')
					WHEN 5 THEN UPPER('correct')
					ELSE LOWER('WRONG')
				END)`,
			expected: map[string]interface{}{
				"id":     int64(3),
				"result": "CORRECT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the INSERT
			_, err := db.Exec(tt.query)
			if err != nil {
				t.Fatalf("Failed to execute INSERT: %v", err)
			}

			// Verify the inserted data
			var id int64
			var result sql.NullString
			var numberResult sql.NullFloat64

			err = db.QueryRow("SELECT id, result, number_result FROM complex_test WHERE id = ?", tt.expected["id"]).
				Scan(&id, &result, &numberResult)
			if err != nil {
				t.Fatalf("Failed to query inserted row: %v", err)
			}

			// Verify expected values
			if expectedID, ok := tt.expected["id"]; ok && id != expectedID {
				t.Errorf("Expected id=%v, got %v", expectedID, id)
			}
			if expectedResult, ok := tt.expected["result"]; ok && result.String != expectedResult {
				t.Errorf("Expected result=%v, got %v", expectedResult, result.String)
			}
			if expectedNumber, ok := tt.expected["number_result"]; ok {
				if !numberResult.Valid {
					t.Errorf("Expected number_result=%v, got NULL", expectedNumber)
				} else if absFloat(numberResult.Float64-expectedNumber.(float64)) > 0.000001 {
					t.Errorf("Expected number_result=%v, got %v", expectedNumber, numberResult.Float64)
				}
			}
		})
	}
}

// Helper function for floating point comparison
func absFloat(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
