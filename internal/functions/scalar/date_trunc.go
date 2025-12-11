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
	"strings"
	"time"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// DateTruncFunction implements the DATE_TRUNC function for scalar operations
type DateTruncFunction struct{}

// Name returns the name of the function
func (f *DateTruncFunction) Name() string {
	return "DATE_TRUNC"
}

// GetInfo returns the function information
func (f *DateTruncFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "DATE_TRUNC",
		Type:        funcregistry.ScalarFunction,
		Description: "Truncates a timestamp to the specified precision",
		Signature: funcregistry.FunctionSignature{
			ReturnType: funcregistry.TypeDateTime,
			ArgumentTypes: []funcregistry.DataType{
				funcregistry.TypeString, // Unit (year, month, day, etc.)
				funcregistry.TypeAny,    // Timestamp to truncate
			},
			MinArgs:    2,
			MaxArgs:    2,
			IsVariadic: false,
		},
	}
}

// Register registers the function with the registry
func (f *DateTruncFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate implements the scalar function interface
func (f *DateTruncFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATE_TRUNC requires exactly 2 arguments")
	}

	// Extract the unit from the first argument
	unit, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("DATE_TRUNC first argument must be a string, got %T", args[0])
	}

	// Convert unit to lowercase for case-insensitive matching
	unit = strings.ToLower(unit)

	// Extract the timestamp from the second argument
	var ts time.Time
	switch t := args[1].(type) {
	case time.Time:
		ts = t
	case string:
		// Try to parse the string as a timestamp
		parsed, err := storage.ParseTimestamp(t)
		if err != nil {
			return nil, fmt.Errorf("DATE_TRUNC could not parse timestamp: %v", err)
		}
		ts = parsed
	default:
		return nil, fmt.Errorf("DATE_TRUNC second argument must be a timestamp or string, got %T", args[1])
	}

	// Truncate based on the specified unit
	var result time.Time
	switch unit {
	case "year":
		result = time.Date(ts.Year(), 1, 1, 0, 0, 0, 0, ts.Location())
	case "month":
		result = time.Date(ts.Year(), ts.Month(), 1, 0, 0, 0, 0, ts.Location())
	case "day":
		result = time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location())
	case "hour":
		result = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), 0, 0, 0, ts.Location())
	case "minute":
		result = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), 0, 0, ts.Location())
	case "second":
		result = time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), 0, ts.Location())
	default:
		return nil, fmt.Errorf("DATE_TRUNC invalid unit: %s", unit)
	}

	return result, nil
}

// NewDateTruncFunction creates a new DATE_TRUNC function
func NewDateTruncFunction() contract.ScalarFunction {
	return &DateTruncFunction{}
}

// Self-registration
func init() {
	// Register the DATE_TRUNC function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewDateTruncFunction())
	}
}
