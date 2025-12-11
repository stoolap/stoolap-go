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
	"unicode"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/functions/registry"
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// CollateFunction implements the COLLATE function
// which allows custom string comparison with different collations
type CollateFunction struct{}

// Name returns the name of the function
func (f *CollateFunction) Name() string {
	return "COLLATE"
}

// GetInfo returns the function information
func (f *CollateFunction) GetInfo() funcregistry.FunctionInfo {
	return funcregistry.FunctionInfo{
		Name:        "COLLATE",
		Type:        funcregistry.ScalarFunction,
		Description: "Applies a collation to a string value for sorting and comparison",
		Signature: funcregistry.FunctionSignature{
			ReturnType:    funcregistry.TypeString,
			ArgumentTypes: []funcregistry.DataType{funcregistry.TypeAny, funcregistry.TypeString},
			MinArgs:       2,
			MaxArgs:       2,
			IsVariadic:    false,
		},
	}
}

// Register registers the function with the registry
func (f *CollateFunction) Register(registry funcregistry.Registry) {
	info := f.GetInfo()
	registry.MustRegister(info)
}

// Evaluate applies the specified collation to the string
func (f *CollateFunction) Evaluate(args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("COLLATE requires exactly 2 arguments, got %d", len(args))
	}

	// Handle null input - return nil to ensure NULL in SQL
	if args[0] == nil {
		return nil, nil
	}

	// Convert the first argument to a string
	str := ConvertToString(args[0])

	// Get the collation name
	collationName, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("COLLATE requires a string as the second argument")
	}

	// Apply the collation
	collatedStr, err := applyCollation(str, collationName)
	if err != nil {
		return nil, err
	}

	// Return the collated string
	return collatedStr, nil
}

// applyCollation applies a collation to a string
func applyCollation(str, collation string) (string, error) {
	collation = strings.ToUpper(collation)

	switch collation {
	case "BINARY":
		// Binary collation - no change
		return str, nil

	case "NOCASE", "CASE_INSENSITIVE":
		// Case-insensitive collation
		return strings.ToLower(str), nil

	case "NOACCENT", "ACCENT_INSENSITIVE":
		// Remove accents for accent-insensitive collation
		return removeAccents(str), nil

	case "NUMERIC":
		// For numeric collation, we don't modify the string here,
		// but the comparison would need to be numeric-aware.
		// Since this function is just applying a transformation,
		// we'll leave the string as is.
		return str, nil

	default:
		return "", fmt.Errorf("unsupported collation: %s", collation)
	}
}

// removeAccents removes accents from characters
func removeAccents(s string) string {
	result := []rune(s)
	for i, r := range result {
		switch {
		// Latin letters with accents
		case r >= 'À' && r <= 'Å':
			result[i] = 'A'
		case r >= 'à' && r <= 'å':
			result[i] = 'a'
		case r >= 'È' && r <= 'Ë':
			result[i] = 'E'
		case r >= 'è' && r <= 'ë':
			result[i] = 'e'
		case r >= 'Ì' && r <= 'Ï':
			result[i] = 'I'
		case r >= 'ì' && r <= 'ï':
			result[i] = 'i'
		case r >= 'Ò' && r <= 'Ö':
			result[i] = 'O'
		case r >= 'ò' && r <= 'ö':
			result[i] = 'o'
		case r >= 'Ù' && r <= 'Ü':
			result[i] = 'U'
		case r >= 'ù' && r <= 'ü':
			result[i] = 'u'
		case r == 'Ç':
			result[i] = 'C'
		case r == 'ç':
			result[i] = 'c'
		case r == 'Ñ':
			result[i] = 'N'
		case r == 'ñ':
			result[i] = 'n'
		case unicode.Is(unicode.Mn, r):
			// Remove other diacritical marks
			result[i] = 0
		}
	}

	// Filter out zero runes (removed marks)
	return strings.Map(func(r rune) rune {
		if r == 0 {
			return -1 // Will be removed
		}
		return r
	}, string(result))
}

// NewCollateFunction creates a new COLLATE function
func NewCollateFunction() contract.ScalarFunction {
	return &CollateFunction{}
}

// Self-registration
func init() {
	// Register the COLLATE function with the global registry
	// This happens automatically when the package is imported
	if registry := registry.GetGlobal(); registry != nil {
		registry.RegisterScalarFunction(NewCollateFunction())
	}
}
