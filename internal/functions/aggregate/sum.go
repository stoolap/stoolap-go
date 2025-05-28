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
package aggregate

import (
	"fmt"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// SumFunction implements the SUM aggregate function
type SumFunction struct {
	sum         float64
	distinct    bool
	values      map[float64]struct{} // used for DISTINCT
	allIntegers bool                 // track if all inputs are integers
	initialized bool                 // track if we've seen any values
	intSum      int64                // separate sum for integer values
}

// Name returns the name of the function
func (f *SumFunction) Name() string {
	return "SUM"
}

// GetInfo returns the function information
func (f *SumFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "SUM",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the sum of all non-NULL values in the specified column. Returns int64 for integer inputs, float64 for floating-point inputs.",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,                          // can return either int64 or float64 based on inputs
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts numeric types
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the SUM function with the registry
func (f *SumFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the SUM calculation
func (f *SumFunction) Accumulate(value any, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (SUM ignores NULLs)
	if value == nil {
		return
	}

	// First try direct type conversion for performance
	var numericValue float64
	var isInteger bool
	var intValue int64
	var err error

	// Handle direct numeric types
	switch v := value.(type) {
	case float64:
		numericValue = v
		isInteger = false
	case float32:
		numericValue = float64(v)
		isInteger = false
	case int:
		intValue = int64(v)
		numericValue = float64(v)
		isInteger = true
	case int64:
		intValue = v
		numericValue = float64(v)
		isInteger = true
	case int32:
		intValue = int64(v)
		numericValue = float64(v)
		isInteger = true
	case Int64Convertible:
		if i64, ok := v.AsInt64(); ok {
			intValue = i64
			numericValue = float64(i64)
			isInteger = true
			err = nil
		} else {
			err = fmt.Errorf("AsInt64 method failed")
		}
	case Float64Convertible:
		if f64, ok := v.AsFloat64(); ok {
			numericValue = f64
			isInteger = false
			err = nil
		} else {
			err = fmt.Errorf("AsFloat64 method failed")
		}
	}

	if err != nil {
		// Skip non-numeric values
		return
	}

	// Initialize tracking for first value
	if !f.initialized {
		f.initialized = true
		f.allIntegers = isInteger
	} else {
		// If we see a non-integer value, update the tracking
		if !isInteger {
			f.allIntegers = false
		}
	}

	// Handle DISTINCT case
	if distinct {
		if f.values == nil {
			f.values = make(map[float64]struct{})
		}

		// Only add values we haven't seen before
		if _, exists := f.values[numericValue]; !exists {
			f.values[numericValue] = struct{}{}
			f.sum += numericValue
			if isInteger {
				f.intSum += intValue
			}
		}
	} else {
		// Regular SUM
		f.sum += numericValue
		if isInteger {
			f.intSum += intValue
		}
	}
}

// Result returns the final result of the SUM calculation
func (f *SumFunction) Result() any {
	// Return int64 for integer inputs, float64 for floating point inputs
	if f.initialized && f.allIntegers {
		return f.intSum
	}

	return f.sum
}

// Reset resets the SUM calculation
func (f *SumFunction) Reset() {
	f.sum = 0
	f.intSum = 0
	f.values = nil
	f.distinct = false
	f.allIntegers = true
	f.initialized = false
}

// NewSumFunction creates a new SUM function
func NewSumFunction() contract.AggregateFunction {
	return &SumFunction{
		sum:         0,
		intSum:      0,
		values:      make(map[float64]struct{}),
		distinct:    false,
		allIntegers: true, // Start assuming all integers until we see a float
		initialized: false,
	}
}

// Self-registration
func init() {
	// Register the SUM function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewSumFunction())
	}
}
