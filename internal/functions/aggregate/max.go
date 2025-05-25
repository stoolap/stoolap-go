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
	"time"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// MaxFunction implements the MAX aggregate function
type MaxFunction struct {
	maxValue interface{}
	dataType funcregistry.DataType
	distinct bool // DISTINCT doesn't change MAX behavior, but we track it for consistency
}

// Name returns the name of the function
func (f *MaxFunction) Name() string {
	return "MAX"
}

// GetInfo returns the function information
func (f *MaxFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "MAX",
		Type:        funcregistry.AggregateFunction,
		Description: "Returns the maximum value of all non-NULL values in the specified column",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,                          // MAX returns the same type as the input
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny}, // accepts any comparable type
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the MAX function with the registry
func (f *MaxFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Accumulate adds a value to the MAX calculation
func (f *MaxFunction) Accumulate(value interface{}, distinct bool) {
	f.distinct = distinct

	// Handle NULL values (MAX ignores NULLs)
	if value == nil {
		return
	}

	// If this is the first non-NULL value, set it as the maximum
	if f.maxValue == nil {
		f.maxValue = value
		f.dataType = getDataType(value)
		return
	}

	// Compare the current value with the maximum (use isGreaterThan, which is !isLessThan && not equal)
	if !isLessThan(value, f.maxValue) && !isEqual(value, f.maxValue) {
		f.maxValue = value
	}
}

// Result returns the final result of the MAX calculation
func (f *MaxFunction) Result() interface{} {
	return f.maxValue // Returns NULL if no values were accumulated
}

// Reset resets the MAX calculation
func (f *MaxFunction) Reset() {
	f.maxValue = nil
	f.dataType = funcregistry.TypeUnknown
	f.distinct = false
}

// NewMaxFunction creates a new MAX function
func NewMaxFunction() contract.AggregateFunction {
	return &MaxFunction{
		maxValue: nil,
		dataType: funcregistry.TypeUnknown,
		distinct: false,
	}
}

// Self-registration
func init() {
	// Register the MAX function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterAggregateFunction(NewMaxFunction())
	}
}

// isEqual compares two values for equality
func isEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle time.Time specifically (common case)
	if aTime, aOk := a.(time.Time); aOk {
		if bTime, bOk := b.(time.Time); bOk {
			return aTime.Equal(bTime)
		}
		return false // time.Time != other types
	} else if _, bOk := b.(time.Time); bOk {
		return false // other types != time.Time
	}

	// Use type assertions for specific comparisons
	switch a.(type) {
	case bool:
		bBool, ok := b.(bool)
		return ok && a.(bool) == bBool
	case int:
		bInt, ok := b.(int)
		return ok && a.(int) == bInt
	case int8:
		bInt, ok := b.(int8)
		return ok && a.(int8) == bInt
	case int16:
		bInt, ok := b.(int16)
		return ok && a.(int16) == bInt
	case int32:
		bInt, ok := b.(int32)
		return ok && a.(int32) == bInt
	case int64:
		bInt, ok := b.(int64)
		return ok && a.(int64) == bInt
	case uint:
		bUint, ok := b.(uint)
		return ok && a.(uint) == bUint
	case uint8:
		bUint, ok := b.(uint8)
		return ok && a.(uint8) == bUint
	case uint16:
		bUint, ok := b.(uint16)
		return ok && a.(uint16) == bUint
	case uint32:
		bUint, ok := b.(uint32)
		return ok && a.(uint32) == bUint
	case uint64:
		bUint, ok := b.(uint64)
		return ok && a.(uint64) == bUint
	case float32:
		bFloat, ok := b.(float32)
		return ok && a.(float32) == bFloat
	case float64:
		bFloat, ok := b.(float64)
		return ok && a.(float64) == bFloat
	case string:
		bStr, ok := b.(string)
		return ok && a.(string) == bStr
	}

	// Different types or non-primitive types that are not directly comparable
	// are not equal
	return false
}
