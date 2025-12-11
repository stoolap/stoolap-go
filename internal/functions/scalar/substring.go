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
package scalar

import (
	"fmt"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// SubstringFunction implements the SUBSTRING function
type SubstringFunction struct{}

// Name returns the name of the function
func (f *SubstringFunction) Name() string {
	return "SUBSTRING"
}

// GetInfo returns the function information
func (f *SubstringFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "SUBSTRING",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns a substring from a string",
		Signature: funcregistry.FunctionSignature{
			ReturnType: funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{
				funcregistry.TypeAny,
				funcregistry.TypeAny,
				funcregistry.TypeAny,
			},
			MinArgs:    2,
			MaxArgs:    3,
			IsVariadic: false,
		},
	}
}

// Register registers the function with the registry
func (f *SubstringFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns a substring
func (f *SubstringFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	// Get the string
	str := ConvertToString(args[0])

	// Get the start position
	start, err := ConvertToInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid start position: %v", err)
	}

	// SQL SUBSTRING is 1-indexed
	if start <= 0 {
		start = 1
	}
	// Convert to 0-indexed for Go
	start--

	if start >= int64(len(str)) {
		return "", nil
	}

	// If length parameter is provided
	if len(args) == 3 {
		length, err := ConvertToInt64(args[2])
		if err != nil {
			return nil, fmt.Errorf("invalid length: %v", err)
		}

		if length <= 0 {
			return "", nil
		}

		end := start + length
		if end > int64(len(str)) {
			end = int64(len(str))
		}

		return str[start:end], nil
	}

	// Without length, return rest of string
	return str[start:], nil
}

// NewSubstringFunction creates a new SUBSTRING function
func NewSubstringFunction() contract.ScalarFunction {
	return &SubstringFunction{}
}

// Self-registration
func init() {
	// Register the SUBSTRING function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewSubstringFunction())
	}
}
