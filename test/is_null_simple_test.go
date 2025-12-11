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
)

func TestIsNullDirectExpression(t *testing.T) {
	// Create a test schema
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER},
			{Name: "name", Type: storage.TEXT},
			{Name: "optional_value", Type: storage.INTEGER},
		},
	}

	// Create a test row with NULL value
	nullRow := []storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
		storage.NewNullValue(storage.INTEGER),
	}

	// Create a test row with non-NULL value
	nonNullRow := []storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
		storage.NewIntegerValue(42),
	}

	// Test IS NULL on null value
	isNullExpr := expression.NewIsNullExpression("optional_value")
	isNullExpr = isNullExpr.PrepareForSchema(schema)

	result, err := isNullExpr.Evaluate(nullRow)
	if err != nil {
		t.Fatalf("Error evaluating IS NULL on null row: %v", err)
	}

	if !result {
		t.Errorf("Expected nullRow.optional_value IS NULL to be true, got false")
	}

	// Test IS NULL on non-null value
	result, err = isNullExpr.Evaluate(nonNullRow)
	if err != nil {
		t.Fatalf("Error evaluating IS NULL on non-null row: %v", err)
	}

	if result {
		t.Errorf("Expected nonNullRow.optional_value IS NULL to be false, got true")
	}

	// Test IS NOT NULL on null value
	isNotNullExpr := expression.NewIsNotNullExpression("optional_value")
	isNotNullExpr = isNotNullExpr.PrepareForSchema(schema)

	result, err = isNotNullExpr.Evaluate(nullRow)
	if err != nil {
		t.Fatalf("Error evaluating IS NOT NULL on null row: %v", err)
	}

	if result {
		t.Errorf("Expected nullRow.optional_value IS NOT NULL to be false, got true")
	}

	// Test IS NOT NULL on non-null value
	result, err = isNotNullExpr.Evaluate(nonNullRow)
	if err != nil {
		t.Fatalf("Error evaluating IS NOT NULL on non-null row: %v", err)
	}

	if !result {
		t.Errorf("Expected nonNullRow.optional_value IS NOT NULL to be true, got false")
	}
}
