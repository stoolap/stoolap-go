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
package registry

import (
	"strings"
	"sync"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// DefaultFunctionRegistry implements the FunctionRegistry interface
type DefaultFunctionRegistry struct {
	// Parser registry for SQL parser integration
	parserRegistry funcregistry.Registry

	// Mutex for thread safety
	mu sync.RWMutex

	// Maps to store function implementations
	aggregateFunctions map[string]contract.AggregateFunction
	scalarFunctions    map[string]contract.ScalarFunction
	windowFunctions    map[string]contract.WindowFunction
}

// NewFunctionRegistry creates a new function registry
func NewFunctionRegistry(parserRegistry funcregistry.Registry) contract.FunctionRegistry {
	return &DefaultFunctionRegistry{
		parserRegistry:     parserRegistry,
		aggregateFunctions: make(map[string]contract.AggregateFunction),
		scalarFunctions:    make(map[string]contract.ScalarFunction),
		windowFunctions:    make(map[string]contract.WindowFunction),
	}
}

// Global is the global function registry that all functions register with
var Global contract.FunctionRegistry

// Initialize the global registry
func init() {
	// Create the parser registry
	parserRegistry := funcregistry.NewRegistry()

	// Create the global function registry
	Global = NewFunctionRegistry(parserRegistry)
}

func GetGlobal() contract.FunctionRegistry {
	return Global
}

// RegisterAggregateFunction registers an aggregate function
func (r *DefaultFunctionRegistry) RegisterAggregateFunction(fn contract.AggregateFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Register with the parser registry
	fn.Register(r.parserRegistry)

	// Store the function implementation
	name := strings.ToUpper(fn.Name())
	r.aggregateFunctions[name] = fn
}

// RegisterScalarFunction registers a scalar function
func (r *DefaultFunctionRegistry) RegisterScalarFunction(fn contract.ScalarFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Register with the parser registry
	fn.Register(r.parserRegistry)

	// Store the function implementation
	name := strings.ToUpper(fn.Name())
	r.scalarFunctions[name] = fn
}

// RegisterWindowFunction registers a window function
func (r *DefaultFunctionRegistry) RegisterWindowFunction(fn contract.WindowFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Register with the parser registry
	fn.Register(r.parserRegistry)

	// Store the function implementation
	name := strings.ToUpper(fn.Name())
	r.windowFunctions[name] = fn
}

// GetAggregateFunction returns an aggregate function by name
func (r *DefaultFunctionRegistry) GetAggregateFunction(name string) contract.AggregateFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Look up the function
	fn, ok := r.aggregateFunctions[name]
	if !ok {
		return nil
	}

	return fn
}

// GetScalarFunction returns a scalar function by name
func (r *DefaultFunctionRegistry) GetScalarFunction(name string) contract.ScalarFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Look up the function
	fn, ok := r.scalarFunctions[name]
	if !ok {
		return nil
	}

	return fn
}

// GetWindowFunction returns a window function by name
func (r *DefaultFunctionRegistry) GetWindowFunction(name string) contract.WindowFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	return r.windowFunctions[name]
}

// GetFunction returns a function by name and type
func (r *DefaultFunctionRegistry) GetFunction(name string) interface{} {
	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Try aggregate functions first
	if fn := r.GetAggregateFunction(name); fn != nil {
		return fn
	}

	// Try scalar functions next
	if fn := r.GetScalarFunction(name); fn != nil {
		return fn
	}

	// Try window functions last
	if fn := r.GetWindowFunction(name); fn != nil {
		return fn
	}

	return nil
}

// ParserRegistry returns the underlying parser registry
func (r *DefaultFunctionRegistry) ParserRegistry() funcregistry.Registry {
	return r.parserRegistry
}

// IsAggregateFunction checks if a function name is an aggregate function
func (r *DefaultFunctionRegistry) IsAggregateFunction(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Check if we have this function registered
	_, exists := r.aggregateFunctions[name]
	return exists
}

// IsScalarFunction checks if a function name is a scalar function
func (r *DefaultFunctionRegistry) IsScalarFunction(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Check if we have this function registered
	_, exists := r.scalarFunctions[name]
	return exists
}

// IsWindowFunction checks if a function name is a window function
func (r *DefaultFunctionRegistry) IsWindowFunction(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Function names are case-insensitive in SQL
	name = strings.ToUpper(name)

	// Check if we have this function registered
	_, exists := r.windowFunctions[name]
	return exists
}
