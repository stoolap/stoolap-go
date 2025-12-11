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
package window

import (
	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// RowNumberFunction implements the ROW_NUMBER() window function
type RowNumberFunction struct{}

// Name returns the name of the function
func (f *RowNumberFunction) Name() string {
	return "ROW_NUMBER"
}

// GetInfo returns the function information
func (f *RowNumberFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "ROW_NUMBER",
		Type:        funcregistry.WindowFunction,
		Description: "Returns the sequential row number within the current partition",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeInteger,
			ArgumentTypes: []funcregistry.DataType{},
			MinArgs:       0,
			MaxArgs:       0,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *RowNumberFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Process returns the row number within the current partition
func (f *RowNumberFunction) Process(partition []interface{}, orderBy []interface{}) (interface{}, error) {
	// For ROW_NUMBER(), we don't use any specific column values
	// We just return the position in the partition (1-based)
	return int64(1), nil
}

// NewRowNumberFunction creates a new ROW_NUMBER function
func NewRowNumberFunction() contract.WindowFunction {
	return &RowNumberFunction{}
}

// Self-registration
func init() {
	// Register the ROW_NUMBER function with the global registry
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterWindowFunction(NewRowNumberFunction())
	}
}
