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
	"github.com/stoolap/stoolap-go/internal/common"
	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// VersionFunction implements the VERSION() function
// This function is commonly used by database tools and ORMs to check database compatibility
type VersionFunction struct{}

// Name returns the name of the function
func (f *VersionFunction) Name() string {
	return "VERSION"
}

// GetInfo returns the function information
func (f *VersionFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "VERSION",
		Type:        funcregistry.ScalarFunction,
		Description: "Returns the Stoolap database version string",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{},
			MinArgs:       0,
			MaxArgs:       0,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *VersionFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate returns the Stoolap version string
func (f *VersionFunction) Evaluate(args ...interface{}) (interface{}, error) {
	// Version string for Stoolap - can be updated as needed
	return common.VersionString, nil
}

// NewVersionFunction creates a new VERSION function
func NewVersionFunction() contract.ScalarFunction {
	return &VersionFunction{}
}

// Self-registration
func init() {
	// Register the VERSION function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewVersionFunction())
	}
}
