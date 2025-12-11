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
package parser

import (
	"testing"

	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

func TestFunctionCallParsing(t *testing.T) {
	// Test cases for function parsing - just testing the basic function types
	tests := []struct {
		name         string
		expectedType funcregistry.FunctionType
	}{
		{"COUNT", funcregistry.AggregateFunction},
		{"SUM", funcregistry.AggregateFunction},
		{"AVG", funcregistry.AggregateFunction},
		{"MIN", funcregistry.AggregateFunction},
		{"MAX", funcregistry.AggregateFunction},
		{"UPPER", funcregistry.ScalarFunction},
		{"LOWER", funcregistry.ScalarFunction},
		{"LENGTH", funcregistry.ScalarFunction},
		{"SUBSTRING", funcregistry.ScalarFunction},
	}

	// Create a registry and register test functions
	registry := funcregistry.NewRegistry()
	registerTestFunctions(registry)

	for _, tt := range tests {
		info, err := registry.Get(tt.name)
		if err != nil {
			t.Errorf("Function %s not found in registry: %v", tt.name, err)
			continue
		}

		if info.Type != tt.expectedType {
			t.Errorf("Function %s should be type %v, got %v", tt.name, tt.expectedType, info.Type)
		}
	}
}

// Helper function to register test functions
func registerTestFunctions(registry funcregistry.Registry) {
	// Register aggregate functions
	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "COUNT",
		Type:        funcregistry.AggregateFunction,
		Description: "Counts the number of rows",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeInteger,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       0,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "SUM",
		Type:        funcregistry.AggregateFunction,
		Description: "Calculates the sum of values",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "AVG",
		Type:        funcregistry.AggregateFunction,
		Description: "Calculates the average of values",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "MIN",
		Type:        funcregistry.AggregateFunction,
		Description: "Finds the minimum value",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "MAX",
		Type:        funcregistry.AggregateFunction,
		Description: "Finds the maximum value",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	// Register scalar functions
	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "UPPER",
		Type:        funcregistry.ScalarFunction,
		Description: "Converts a string to uppercase",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "LOWER",
		Type:        funcregistry.ScalarFunction,
		Description: "Converts a string to lowercase",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "LENGTH",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the length of a string",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeInteger,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeString},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "SUBSTRING",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns a substring",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeString, funcregistry.TypeInteger, funcregistry.TypeInteger},
			MinArgs:       2,
			MaxArgs:       3,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "ABS",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the absolute value",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeNumeric,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeNumeric},
			MinArgs:       1,
			MaxArgs:       1,
		},
	})

	registry.MustRegister(funcregistry.FunctionInfo{
		Name:        "ROUND",
		Type:        funcregistry.ScalarFunction,
		Description: "Rounds a number to a specified precision",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeFloat, funcregistry.TypeInteger},
			MinArgs:       1,
			MaxArgs:       2,
		},
	})
}

func TestFunctionIsAggregate(t *testing.T) {
	// Test the IsAggregate method on FunctionCall
	registry := funcregistry.NewRegistry()
	registerTestFunctions(registry)

	// Create function calls for aggregate and non-aggregate functions
	aggregateCall := &FunctionCall{
		Token:     Token{Literal: "COUNT"},
		Function:  "COUNT",
		Arguments: []Expression{},
	}

	scalarCall := &FunctionCall{
		Token:     Token{Literal: "UPPER"},
		Function:  "UPPER",
		Arguments: []Expression{},
	}

	// Get function info from registry
	aggregateInfo, err := registry.Get("COUNT")
	if err != nil {
		t.Fatalf("Failed to get COUNT function: %v", err)
	}

	scalarInfo, err := registry.Get("UPPER")
	if err != nil {
		t.Fatalf("Failed to get UPPER function: %v", err)
	}

	// Set the function info
	aggregateCall.FunctionInfo = &aggregateInfo
	scalarCall.FunctionInfo = &scalarInfo

	// Test IsAggregate method
	if !aggregateCall.IsAggregate() {
		t.Errorf("COUNT should be identified as an aggregate function")
	}

	if scalarCall.IsAggregate() {
		t.Errorf("UPPER should not be identified as an aggregate function")
	}
}

func TestContainsAggregate(t *testing.T) {
	// Test the ContainsAggregate function
	registry := funcregistry.NewRegistry()
	registerTestFunctions(registry)

	// Create expressions with and without aggregate functions
	// 1. Simple aggregate function
	aggregateFunc := &FunctionCall{
		Token:     Token{Literal: "COUNT"},
		Function:  "COUNT",
		Arguments: []Expression{},
	}
	aggregateInfo, err := registry.Get("COUNT")
	if err != nil {
		t.Fatalf("Failed to get COUNT function: %v", err)
	}
	aggregateFunc.FunctionInfo = &aggregateInfo

	// 2. Simple scalar function
	scalarFunc := &FunctionCall{
		Token:     Token{Literal: "UPPER"},
		Function:  "UPPER",
		Arguments: []Expression{},
	}
	scalarInfo, err := registry.Get("UPPER")
	if err != nil {
		t.Fatalf("Failed to get UPPER function: %v", err)
	}
	scalarFunc.FunctionInfo = &scalarInfo

	// 3. Complex expression with aggregate: COUNT(*) + 1
	complexExpr := &InfixExpression{
		Token:    Token{Literal: "+"},
		Operator: "+",
		Left:     aggregateFunc,
		Right: &IntegerLiteral{
			Token: Token{Literal: "1"},
			Value: 1,
		},
	}

	// 4. Complex expression without aggregate: UPPER('name') + 'suffix'
	noAggregateExpr := &InfixExpression{
		Token:    Token{Literal: "+"},
		Operator: "+",
		Left:     scalarFunc,
		Right: &StringLiteral{
			Token: Token{Literal: "'suffix'"},
			Value: "suffix",
		},
	}

	// Test the function
	if !ContainsAggregate(aggregateFunc) {
		t.Errorf("COUNT function should contain aggregate")
	}

	if ContainsAggregate(scalarFunc) {
		t.Errorf("UPPER function should not contain aggregate")
	}

	if !ContainsAggregate(complexExpr) {
		t.Errorf("Expression COUNT(*) + 1 should contain aggregate")
	}

	if ContainsAggregate(noAggregateExpr) {
		t.Errorf("Expression UPPER('name') + 'suffix' should not contain aggregate")
	}
}
