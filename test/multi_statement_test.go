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

func TestMultiStatementParsing(t *testing.T) {
	// Test a multi-statement query with transaction statements
	multiStmt := `
BEGIN TRANSACTION;
INSERT INTO users (id, name) VALUES (1, 'Alice');
COMMIT;
`

	p := parser.NewParser(parser.NewLexer(multiStmt))
	program := p.ParseProgram()

	// Check that we have multiple statements
	if len(program.Statements) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(program.Statements))
	}

	// Check the types of statements
	if len(program.Statements) >= 3 {
		_, ok1 := program.Statements[0].(*parser.BeginStatement)
		_, ok2 := program.Statements[1].(*parser.InsertStatement)
		_, ok3 := program.Statements[2].(*parser.CommitStatement)

		if !ok1 {
			t.Errorf("First statement is not a BEGIN statement, got %T", program.Statements[0])
		}

		if !ok2 {
			t.Errorf("Second statement is not an INSERT statement, got %T", program.Statements[1])
		}

		if !ok3 {
			t.Errorf("Third statement is not a COMMIT statement, got %T", program.Statements[2])
		}
	}

	// Check for any parse errors
	if len(p.Errors()) > 0 {
		t.Logf("Parse errors: %v", p.Errors())
	}
}
