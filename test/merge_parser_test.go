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
package test

import (
	"testing"

	"github.com/stoolap/stoolap-go/internal/parser"
)

func TestMergeParser(t *testing.T) {
	// Create a parser
	p := parser.NewParser(parser.NewLexer("MERGE INTO target USING source ON 1=1"))

	// Parse the program without validating to avoid panics
	p.ParseProgram()

	// Look at parse errors
	t.Logf("Parse errors: %v", p.Errors())
}
