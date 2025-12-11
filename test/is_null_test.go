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

func TestIsNullExpression(t *testing.T) {
	// Create a test schema
	schema := storage.Schema{
		TableName: "test_table",
		Columns: []storage.SchemaColumn{
			{Name: "id", Type: storage.INTEGER},
			{Name: "name", Type: storage.TEXT},
			{Name: "optional_value", Type: storage.INTEGER},
			{Name: "optional_text", Type: storage.TEXT},
		},
	}

	// Create test rows with NULL and non-NULL values
	row1 := []storage.ColumnValue{
		storage.NewIntegerValue(1),
		storage.NewStringValue("Alice"),
		storage.NewNullValue(storage.INTEGER),
		storage.NewStringValue("text1"),
	}

	row2 := []storage.ColumnValue{
		storage.NewIntegerValue(2),
		storage.NewStringValue("Bob"),
		storage.NewIntegerValue(42),
		storage.NewNullValue(storage.TEXT),
	}

	// Test IS NULL direct expression on row1
	t.Run("IS NULL on optional_value row1", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_value")
		isNullExpr = isNullExpr.PrepareForSchema(schema)

		result, err := isNullExpr.Evaluate(row1)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row1.optional_value IS NULL to be true, got false")
		}
	})

	// Test IS NOT NULL direct expression on row1
	t.Run("IS NOT NULL on optional_value row1", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_value")
		isNotNullExpr = isNotNullExpr.PrepareForSchema(schema)

		result, err := isNotNullExpr.Evaluate(row1)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row1.optional_value IS NOT NULL to be false, got true")
		}
	})

	// Test IS NULL direct expression on row2
	t.Run("IS NULL on optional_value row2", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_value")
		isNullExpr = isNullExpr.PrepareForSchema(schema)

		result, err := isNullExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row2.optional_value IS NULL to be false, got true")
		}
	})

	// Test IS NOT NULL direct expression on row2
	t.Run("IS NOT NULL on optional_value row2", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_value")
		isNotNullExpr = isNotNullExpr.PrepareForSchema(schema)

		result, err := isNotNullExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row2.optional_value IS NOT NULL to be true, got false")
		}
	})

	// Test IS NULL on optional_text with row2
	t.Run("IS NULL on optional_text row2", func(t *testing.T) {
		isNullExpr := expression.NewIsNullExpression("optional_text")
		isNullExpr = isNullExpr.PrepareForSchema(schema)

		result, err := isNullExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NULL: %v", err)
		}

		if !result {
			t.Errorf("Expected row2.optional_text IS NULL to be true, got false")
		}
	})

	// Test IS NOT NULL on optional_text with row2
	t.Run("IS NOT NULL on optional_text row2", func(t *testing.T) {
		isNotNullExpr := expression.NewIsNotNullExpression("optional_text")
		isNotNullExpr = isNotNullExpr.PrepareForSchema(schema)

		result, err := isNotNullExpr.Evaluate(row2)
		if err != nil {
			t.Fatalf("Error evaluating IS NOT NULL: %v", err)
		}

		if result {
			t.Errorf("Expected row2.optional_text IS NOT NULL to be false, got true")
		}
	})
}
