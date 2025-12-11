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
	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// CoalesceFunction implements the COALESCE function
type CoalesceFunction struct{}

// Name returns the name of the function
func (f *CoalesceFunction) Name() string {
	return "COALESCE"
}

// GetInfo returns the function information
func (f *CoalesceFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "COALESCE",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the first non-null value in a list",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       -1, // unlimited arguments
			IsVariadic:    true,
		},
	}
}

// Register registers the function with the registry
func (f *CoalesceFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the first non-null value
// Handles all possible NULL values and v3 types
func (f *CoalesceFunction) Evaluate(args ...interface{}) (interface{}, error) {
	for _, arg := range args {
		// Skip nil values
		if arg == nil {
			continue
		}

		// Skip values that implement IsNull() with IsNull() == true
		if nullVal, ok := arg.(interface{ IsNull() bool }); ok {
			if nullVal.IsNull() {
				continue
			}
		}

		// Skip empty strings (treated as NULL in SQL)
		if str, ok := arg.(string); ok && str == "" {
			continue
		}

		// Try to extract value from v3 storage column types

		// For string values
		if strVal, ok := arg.(interface{ AsString() (string, bool) }); ok {
			if val, ok := strVal.AsString(); ok {
				// Skip empty strings
				if val == "" {
					continue
				}
				return val, nil
			}
		}

		// For int64 values
		if intVal, ok := arg.(interface{ AsInt64() (int64, bool) }); ok {
			if val, ok := intVal.AsInt64(); ok {
				return val, nil
			}
		}

		// For float64 values
		if floatVal, ok := arg.(interface{ AsFloat64() (float64, bool) }); ok {
			if val, ok := floatVal.AsFloat64(); ok {
				return val, nil
			}
		}

		// For boolean values
		if boolVal, ok := arg.(interface{ AsBoolean() (bool, bool) }); ok {
			if val, ok := boolVal.AsBoolean(); ok {
				return val, nil
			}
		}

		// If we get here, the value is not NULL and not a v3 storage type we can extract,
		// so return it as-is
		return arg, nil
	}

	// If all arguments are null, return null
	return nil, nil
}

// NewCoalesceFunction creates a new COALESCE function
func NewCoalesceFunction() contract.ScalarFunction {
	return &CoalesceFunction{}
}

// Self-registration
func init() {
	// Register the COALESCE function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewCoalesceFunction())
	}
}
