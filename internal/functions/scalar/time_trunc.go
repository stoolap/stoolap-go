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
	"time"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// TimeTruncFunction implements the TIME_TRUNC function for scalar operations
type TimeTruncFunction struct{}

// Name returns the name of the function
func (f *TimeTruncFunction) Name() string {
	return "TIME_TRUNC"
}

// GetInfo returns the function information
func (f *TimeTruncFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "TIME_TRUNC",
		Type:        funcregistry.ScalarFunction, // This is a scalar function, not a window function
		Description: "Truncates a time value to the specified duration (e.g., '15m', '1h', '30s')",
		Signature: funcregistry.FunctionSignature{
			ReturnType: funcregistry.TypeDateTime,
			ArgumentTypes: []funcregistry.DataType{
				funcregistry.TypeString, // Duration format (e.g., '15m', '1h', '30s')
				funcregistry.TypeAny,    // Timestamp to truncate
			},
			MinArgs:    2,
			MaxArgs:    2,
			IsVariadic: false,
		},
	}
}

// Register registers the function with the registry
func (f *TimeTruncFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// parseDuration parses duration strings like "15m", "1h30m", "30s"
func parseDuration(durationStr string) (time.Duration, error) {
	// Go's time.ParseDuration handles formats like "300ms", "-1.5h" or "2h45m"
	// It accepts: "ns", "us" (or "µs"), "ms", "s", "m", "h"
	return time.ParseDuration(durationStr)
}

// Evaluate implements the scalar function interface
func (f *TimeTruncFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("TIME_TRUNC requires exactly 2 arguments")
	}

	// Extract the duration from the first argument
	durationStr, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("TIME_TRUNC first argument must be a string, got %T", args[0])
	}

	// Parse the duration
	duration, err := parseDuration(durationStr)
	if err != nil {
		return nil, fmt.Errorf("TIME_TRUNC invalid duration: %v", err)
	}

	// Extract the timestamp from the second argument
	var ts time.Time
	switch t := args[1].(type) {
	case time.Time:
		ts = t
	case string:
		// Try to parse the string as a timestamp
		parsed, err := storage.ParseTimestamp(t)
		if err != nil {
			return nil, fmt.Errorf("TIME_TRUNC could not parse timestamp: %v", err)
		}
		ts = parsed
	default:
		return nil, fmt.Errorf("TIME_TRUNC second argument must be a timestamp or string, got %T", args[1])
	}

	// Truncate the timestamp based on the specified duration
	truncatedUnixNano := ts.UnixNano() - (ts.UnixNano() % duration.Nanoseconds())
	return time.Unix(0, truncatedUnixNano).In(ts.Location()), nil
}

// NewTimeTruncFunction creates a new TIME_TRUNC function
func NewTimeTruncFunction() contract.ScalarFunction {
	return &TimeTruncFunction{}
}

// Self-registration
func init() {
	// Register the TIME_TRUNC function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewTimeTruncFunction())
	}
}
