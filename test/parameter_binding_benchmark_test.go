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
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
)

func BenchmarkParameterBinding(b *testing.B) {
	// Create a memory engine for testing
	db, err := stoolap.Open("memory://")
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer db.Close()

	// Use the Executor method from DB
	exec := db.Executor()

	// Create a test table with different data types
	createTbl := "CREATE TABLE test_params (id INTEGER, name TEXT, salary FLOAT, active BOOLEAN, hire_date DATE, meta JSON)"
	result, err := exec.Execute(context.Background(), nil, createTbl)
	if err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}
	result.Close()

	// Parameters for typical insert
	typicalParams := []driver.NamedValue{
		{Ordinal: 1, Value: 101},
		{Ordinal: 2, Value: "Jane Smith"},
		{Ordinal: 3, Value: 85000.75},
		{Ordinal: 4, Value: false},
		{Ordinal: 5, Value: time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
		{Ordinal: 6, Value: map[string]interface{}{"department": "Engineering", "level": 3}},
	}

	// Prepare the query with parameters for all columns
	query := "INSERT INTO test_params (id, name, salary, active, hire_date, meta) VALUES (?, ?, ?, ?, ?, ?)"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := exec.ExecuteWithParams(context.Background(), nil, query, typicalParams)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
		result.Close()
	}
}
