//go:build ignore
// +build ignore

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
package main

import (
	"io/ioutil"
	"strings"
)

// This script helps update AST test cases to use helper functions
func main() {
	// Read the test file
	src, err := ioutil.ReadFile("ast_test.go")
	if err != nil {
		panic(err)
	}

	content := string(src)

	// Define replacements
	replacements := []struct {
		old string
		new string
	}{
		// Integer literals
		{`&IntegerLiteral{Value: `, `makeIntegerLiteral(`},
		// Float literals
		{`&FloatLiteral{Value: `, `makeFloatLiteral(`},
		// String literals
		{`&StringLiteral{Value: `, `makeStringLiteral(`},
		// Boolean literals
		{`&BooleanLiteral{Value: `, `makeBooleanLiteral(`},
		// Identifiers
		{`&Identifier{Value: `, `makeIdentifier(`},
		// Fix closing braces
		{`makeIntegerLiteral(42}`, `makeIntegerLiteral(42)`},
		{`makeFloatLiteral(3.14}`, `makeFloatLiteral(3.14)`},
	}

	// Apply replacements
	for _, r := range replacements {
		content = strings.ReplaceAll(content, r.old, r.new)
	}

	// Write back
	err = ioutil.WriteFile("ast_test.go", []byte(content), 0644)
	if err != nil {
		panic(err)
	}
}
