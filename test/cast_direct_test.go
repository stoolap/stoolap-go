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

	"github.com/stoolap/stoolap-go/internal/functions/scalar"
)

func TestCastDirect(t *testing.T) {
	// Create a CAST function directly
	castFunc := scalar.NewCastFunction()

	// Test cases
	tests := []struct {
		name     string
		value    interface{}
		typeName string
		expected interface{}
	}{
		{
			name:     "Cast string to integer",
			value:    "123",
			typeName: "INTEGER",
			expected: int64(123),
		},
		{
			name:     "Cast float to integer",
			value:    float64(123.456),
			typeName: "INTEGER",
			expected: int64(123),
		},
		{
			name:     "Cast bool to integer",
			value:    true,
			typeName: "INTEGER",
			expected: int64(1),
		},
		{
			name:     "Cast string to float",
			value:    "123.456",
			typeName: "FLOAT",
			expected: float64(123.456),
		},
		{
			name:     "Cast integer to string",
			value:    int64(123),
			typeName: "TEXT",
			expected: "123",
		},
		{
			name:     "Cast nil to string",
			value:    nil,
			typeName: "TEXT",
			expected: "",
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Evaluate CAST function
			result, err := castFunc.Evaluate(tt.value, tt.typeName)
			if err != nil {
				t.Fatalf("CAST evaluation failed: %v", err)
			}

			// Check result
			t.Logf("Result: %v (type: %T)", result, result)

			// Compare with expected
			switch expected := tt.expected.(type) {
			case int64:
				if value, ok := result.(int64); !ok {
					t.Errorf("Expected int64, got %T", result)
				} else if value != expected {
					t.Errorf("Expected %d, got %d", expected, value)
				}
			case float64:
				if value, ok := result.(float64); !ok {
					t.Errorf("Expected float64, got %T", result)
				} else if value != expected {
					t.Errorf("Expected %f, got %f", expected, value)
				}
			case string:
				if value, ok := result.(string); !ok {
					t.Errorf("Expected string, got %T", result)
				} else if value != expected {
					t.Errorf("Expected %q, got %q", expected, value)
				}
			default:
				t.Errorf("Unexpected type for expected value: %T", tt.expected)
			}
		})
	}
}
