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
	"math"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// RoundFunction implements the ROUND function
type RoundFunction struct{}

// Name returns the name of the function
func (f *RoundFunction) Name() string {
	return "ROUND"
}

// GetInfo returns the function information
func (f *RoundFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "ROUND",
		Type:        funcregistry.ScalarFunction,
		Description: "Rounds a number to a specified number of decimal places",
		Signature: funcregistry.FunctionSignature{
			ReturnType: funcregistry.TypeFloat,
			ArgumentTypes: []funcregistry.DataType{
				funcregistry.TypeAny,
				funcregistry.TypeAny,
			},
			MinArgs:    1,
			MaxArgs:    2,
			IsVariadic: false,
		},
	}
}

// Register registers the function with the registry
func (f *RoundFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate rounds a number
func (f *RoundFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND requires 1 or 2 arguments, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil
	}

	// Get the number to round
	num, err := ConvertToFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid number to round: %v", err)
	}

	// Default to 0 decimal places if not specified
	places := 0

	// If decimal places are specified
	if len(args) == 2 && args[1] != nil {
		p, err := ConvertToInt64(args[1])
		if err != nil {
			return nil, fmt.Errorf("invalid decimal places: %v", err)
		}
		places = int(p)
	}

	// Round to specified decimal places
	shift := math.Pow(10, float64(places))
	return math.Round(num*shift) / shift, nil
}

// NewRoundFunction creates a new ROUND function
func NewRoundFunction() contract.ScalarFunction {
	return &RoundFunction{}
}

// Self-registration
func init() {
	// Register the ROUND function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewRoundFunction())
	}
}
