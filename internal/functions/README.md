# SQL Function System

This package implements the SQL function system for the Stoolap database.

## Overview

The function system allows SQL queries to use standard functions like `COUNT()`, `SUM()`, `UPPER()`, etc., as well as custom user-defined functions.

### Key Components

- **Function Registry**: Central repository of all available functions
- **Parser Integration**: Functions registered in the registry are available to the SQL parser
- **Function Types**: 
  - **Scalar Functions**: Return a single value for each input row (e.g., `UPPER()`, `LOWER()`)
  - **Aggregate Functions**: Calculate a result from multiple input rows (e.g., `COUNT()`, `SUM()`)
  - **Window Functions**: Calculate a result over a window of rows (e.g., `ROW_NUMBER()`, `RANK()`)

## Self-Registration System

Functions automatically register themselves with the global registry when their package is imported, eliminating the need for manual registration.

### How It Works

1. The global registry is created during init time in the registry package
2. Each function implementation has an `init()` function that registers it with the global registry
3. The parser and executor automatically use the global registry
4. No manual registration is needed - functions become available just by being imported

### Adding a New Function

1. Copy one of the templates from the `templates` directory:
   - `template_scalar.go` for scalar functions
   - `template_aggregate.go` for aggregate functions
   - `template_window.go` for window functions (coming soon)
   
2. Create a new file in the appropriate directory:
   - `scalar` directory for scalar functions
   - `aggregate` directory for aggregate functions
   - `window` directory for window functions
   
3. Implement your function following the template:
   - Implement the required interface
   - Add the self-registration init function
   - No manual registration in registry.go is needed

4. Import your function package:
   - Either explicitly (`import _ "github.com/stoolap/stoolap-go/internal/functions/scalar/myfunc"`)
   - Or implicitly (by referencing something from the package)

## Example: Creating a New Scalar Function

```go
package myscalar

import (
	"fmt"
	
	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// MyFunction implements a custom scalar function
type MyFunction struct{}

// Name returns the name of the function
func (f *MyFunction) Name() string {
	return "MY_FUNCTION"
}

// GetInfo returns the function information
func (f *MyFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "MY_FUNCTION",
		Type:        funcregistry.ScalarFunction,
		Description: "Performs some operation on input",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeString},
			MinArgs:       1,
			MaxArgs:       1,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *MyFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate implements the function logic
func (f *MyFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MY_FUNCTION requires exactly 1 argument, got %d", len(args))
	}

	if args[0] == nil {
		return nil, nil // Handle NULL input
	}

	// Implement function logic here
	str, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("MY_FUNCTION requires a string argument, got %T", args[0])
	}
	
	// Your custom logic here
	result := "Processed: " + str
	
	return result, nil
}

// NewMyFunction creates a new instance of the function
func NewMyFunction() contract.ScalarFunction {
	return &MyFunction{}
}

// Self-registration - happens automatically when package is imported
func init() {
	// Check if global registry exists and register function
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewMyFunction())
	}
}
```

## Benefits of This Approach

- **Improved Developer Experience**: No need to manually register functions in multiple places
- **Reduced Errors**: Functions can't be used in the parser but missing from the executor
- **Self-Documenting**: Each function is fully defined in a single file
- **Easy Extension**: Just add a new function file and it's automatically available
- **Clear Organization**: Function implementations are separated by type
- **Low Overhead**: Minimal code to add a new function