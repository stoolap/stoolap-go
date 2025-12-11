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
package contract

import (
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// Function is the interface that all SQL functions must implement
type Function interface {
	// Name returns the name of the function
	Name() string

	// GetInfo returns the function information
	GetInfo() funcregistry.FunctionInfo

	// Register registers the function with the registry
	Register(registry funcregistry.Registry)
}

// AggregateFunction is the interface for aggregate functions
type AggregateFunction interface {
	Function

	// Accumulate adds a value to the aggregate calculation
	Accumulate(value interface{}, distinct bool)

	// Result returns the final result of the aggregate calculation
	Result() interface{}

	// Reset resets the aggregate calculation
	Reset()
}

// OrderedAggregateFunction extends AggregateFunction with ordering support
type OrderedAggregateFunction interface {
	AggregateFunction

	// AccumulateOrdered adds a value with an ordering key to the aggregate calculation
	AccumulateOrdered(value interface{}, orderKey interface{}, direction string, distinct bool)

	// SupportsOrdering returns true if the function supports ordering
	SupportsOrdering() bool
}

// ScalarFunction is the interface for scalar functions
type ScalarFunction interface {
	Function

	// Evaluate evaluates the function with the given arguments
	Evaluate(args ...interface{}) (interface{}, error)
}

// WindowFunction is the interface for window functions
type WindowFunction interface {
	Function

	// Process processes the window function with the given partition
	Process(partition []interface{}, orderBy []interface{}) (interface{}, error)
}

// FunctionRegistry maintains a mapping of function names to their implementations
type FunctionRegistry interface {
	// GetAggregateFunction returns an aggregate function by name
	GetAggregateFunction(name string) AggregateFunction

	// GetScalarFunction returns a scalar function by name
	GetScalarFunction(name string) ScalarFunction

	// GetWindowFunction returns a window function by name
	GetWindowFunction(name string) WindowFunction

	// RegisterAggregateFunction registers an aggregate function
	RegisterAggregateFunction(fn AggregateFunction)

	// RegisterScalarFunction registers a scalar function
	RegisterScalarFunction(fn ScalarFunction)

	// RegisterWindowFunction registers a window function
	RegisterWindowFunction(fn WindowFunction)

	// IsAggregateFunction checks if a function name is an aggregate function
	IsAggregateFunction(name string) bool

	// IsScalarFunction checks if a function name is a scalar function
	IsScalarFunction(name string) bool

	// IsWindowFunction checks if a function name is a window function
	IsWindowFunction(name string) bool

	// ParserRegistry returns the parser registry
	ParserRegistry() funcregistry.Registry
}

// SQLFunctionParameter represents a parameter to a SQL function
type SQLFunctionParameter struct {
	// Name of the parameter (optional, used for documentation)
	Name string

	// Type of the parameter
	Type funcregistry.DataType

	// Whether the parameter is optional
	Optional bool

	// Default value for the parameter (if optional)
	DefaultValue interface{}
}
