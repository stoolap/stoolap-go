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
	"strings"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// ConcatFunction implements the CONCAT function
type ConcatFunction struct{}

// Name returns the name of the function
func (f *ConcatFunction) Name() string {
	return "CONCAT"
}

// GetInfo returns the function information
func (f *ConcatFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "CONCAT",
		Type:        funcregistry.ScalarFunction,
		Description: "Concatenates strings",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny},
			MinArgs:       1,
			MaxArgs:       -1, // unlimited
			IsVariadic:    true,
		},
	}
}

// Register registers the function with the registry
func (f *ConcatFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate concatenates strings
func (f *ConcatFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}

	var sb strings.Builder
	for _, arg := range args {
		if arg != nil {
			sb.WriteString(ConvertToString(arg))
		}
	}

	return sb.String(), nil
}

// NewConcatFunction creates a new CONCAT function
func NewConcatFunction() contract.ScalarFunction {
	return &ConcatFunction{}
}

// Self-registration
func init() {
	// Register the CONCAT function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewConcatFunction())
	}
}
