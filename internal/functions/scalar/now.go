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
	"time"

	"github.com/stoolap/stoolap/internal/functions/contract"
	"github.com/stoolap/stoolap/internal/functions/registry"
	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// NowFunction implements the NOW() function
type NowFunction struct{}

// Name returns the name of the function
func (f *NowFunction) Name() string {
	return "NOW"
}

// GetInfo returns the function information
func (f *NowFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "NOW",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the current date and time",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeDateTime,
			ArgumentTypes: []funcregistry.DataType{},
			MinArgs:       0,
			MaxArgs:       0,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *NowFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the current date and time
func (f *NowFunction) Evaluate(args ...interface{}) (interface{}, error) {
	return time.Now(), nil
}

// NewNowFunction creates a new NOW function
func NewNowFunction() contract.ScalarFunction {
	return &NowFunction{}
}

// Self-registration
func init() {
	// Register the NOW function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewNowFunction())
	}
}
