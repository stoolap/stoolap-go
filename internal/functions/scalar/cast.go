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
	"strconv"
	"strings"
	"time"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// CastFunction implements the CAST scalar function
type CastFunction struct{}

// Name returns the name of the function
func (f *CastFunction) Name() string {
	return "CAST"
}

// GetInfo returns the function information
func (f *CastFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "CAST",
		Type:        funcregistry.ScalarFunction,
		Description: "Converts a value from one data type to another",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeAny,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny, funcregistry.TypeString},
			MinArgs:       2,
			MaxArgs:       2,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *CastFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate performs the type conversion
func (f *CastFunction) Evaluate(args ...interface{}) (interface{}, error) {
	// Check argument count
	if len(args) != 2 {
		return nil, fmt.Errorf("CAST requires exactly 2 arguments, got %d", len(args))
	}

	// Extract the value to be cast
	value := args[0]

	// Handle NULL values - we need to handle both cases:
	// 1. In SELECT columns, NULL should be converted to appropriate zero value
	// 2. In WHERE clauses, NULL should remain NULL for comparison semantics
	if value == nil {
		// Return appropriate zero values for different target types
		targetTypeStr := strings.ToUpper(args[1].(string))
		switch targetTypeStr {
		case "STRING", "TEXT", "VARCHAR", "CHAR":
			return "", nil
		case "INT", "INTEGER":
			return int64(0), nil
		case "FLOAT", "REAL", "DOUBLE":
			return float64(0.0), nil
		case "BOOLEAN", "BOOL":
			return false, nil
		default:
			return nil, nil
		}
	}

	// The target type (as a string)
	targetType, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("second argument to CAST must be a string type name")
	}

	// Normalize the target type
	targetType = strings.ToUpper(targetType)

	// Convert based on the target type
	switch targetType {
	case "INT", "INTEGER":
		return castToInteger(value)
	case "FLOAT", "REAL", "DOUBLE":
		return castToFloat(value)
	case "STRING", "TEXT", "VARCHAR", "CHAR":
		return castToString(value)
	case "BOOLEAN", "BOOL":
		return castToBoolean(value)
	case "TIMESTAMP", "DATETIME", "DATE", "TIME":
		return castToTimestamp(value)
	case "JSON":
		return castToJSON(value)
	default:
		return nil, fmt.Errorf("unsupported cast target type: %s", targetType)
	}
}

// NewCastFunction creates a new CAST function
func NewCastFunction() contract.ScalarFunction {
	return &CastFunction{}
}

// Self-registration
func init() {
	// Register the CAST function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewCastFunction())
	}
}

// Helper functions for type conversion

func castToInteger(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		// Handle empty string as 0
		if v == "" {
			return 0, nil
		}
		// Try to parse as integer
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i, nil
		}
		// If parsing as int fails, try parsing as float
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return int64(f), nil
		}
		return 0, nil // Return 0 with no error for unparseable strings
	default:
		return 0, nil // Return 0 with no error for all other types
	}
}

func castToFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		if v == "" {
			return 0.0, nil
		}
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to FLOAT", value)
	}
}

func castToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 6, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', 6, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case time.Time:
		return v.Format(time.RFC3339), nil
	default:
		// For any other type, use fmt.Sprintf for general string conversion
		return fmt.Sprintf("%v", v), nil
	}
}

func castToBoolean(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float32:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		s := strings.ToLower(v)
		return s == "true" || s == "yes" || s == "1" || s != "" && s != "0" && s != "false" && s != "no", nil
	default:
		return false, fmt.Errorf("cannot convert %T to BOOLEAN", value)
	}
}

func castToTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		if t, err := storage.ParseTimestamp(v); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as TIMESTAMP", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to TIMESTAMP", value)
	}
}

// Added support for JSON casting
func castToJSON(value interface{}) (interface{}, error) {
	// For JSON, we need to ensure the value is valid JSON
	// For simplicity, we'll just return the value as-is for now
	// In a real implementation, you would validate and possibly transform the JSON data
	return value, nil
}
