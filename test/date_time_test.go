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

func TestDateLiteralParsing(t *testing.T) {
	// This test now uses the full parser to parse a SQL statement
	input := "SELECT * FROM events WHERE event_date = '2023-05-15'"
	l := parser.NewLexer(input)
	p := parser.NewParser(l)

	// Parse the full statement
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Fatalf("Parse errors: %v", p.Errors())
	}

	// Make sure we got a SELECT statement
	if len(program.Statements) == 0 {
		t.Fatalf("No statements in program")
	}

	selectStmt, ok := program.Statements[0].(*parser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SELECT statement, got %T", program.Statements[0])
	}

	// Check that we have a WHERE clause
	if selectStmt.Where == nil {
		t.Fatalf("No WHERE clause in SELECT statement")
	}

	// The WHERE clause should be an equality comparison
	infix, ok := selectStmt.Where.(*parser.InfixExpression)
	if !ok {
		t.Fatalf("Expected InfixExpression in WHERE clause, got %T", selectStmt.Where)
	}

	// The right side should be a string literal representing the date
	literal, ok := infix.Right.(*parser.StringLiteral)
	if !ok {
		t.Fatalf("Expected StringLiteral on right side of comparison, got %T", infix.Right)
	}

	// The literal should be '2023-05-15'
	if literal.Value != "2023-05-15" {
		t.Errorf("Expected date literal '2023-05-15', got '%s'", literal.Value)
	}

	// For now, we'll just verify the literal is parsed.
	// In a real implementation, this would be converted to a time.Time at execution time
	t.Logf("Successfully parsed date literal: %s", literal.Value)
}

func TestTimeLiteralParsing(t *testing.T) {
	// This test now uses the full parser to parse a SQL statement
	input := "SELECT * FROM events WHERE event_time = '14:30:00'"
	l := parser.NewLexer(input)
	p := parser.NewParser(l)

	// Parse the full statement
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Fatalf("Parse errors: %v", p.Errors())
	}

	// Make sure we got a SELECT statement
	if len(program.Statements) == 0 {
		t.Fatalf("No statements in program")
	}

	selectStmt, ok := program.Statements[0].(*parser.SelectStatement)
	if !ok {
		t.Fatalf("Expected SELECT statement, got %T", program.Statements[0])
	}

	// Check that we have a WHERE clause
	if selectStmt.Where == nil {
		t.Fatalf("No WHERE clause in SELECT statement")
	}

	// The WHERE clause should be an equality comparison
	infix, ok := selectStmt.Where.(*parser.InfixExpression)
	if !ok {
		t.Fatalf("Expected InfixExpression in WHERE clause, got %T", selectStmt.Where)
	}

	// The right side should be a string literal representing the time
	literal, ok := infix.Right.(*parser.StringLiteral)
	if !ok {
		t.Fatalf("Expected StringLiteral on right side of comparison, got %T", infix.Right)
	}

	// The literal should be '14:30:00'
	if literal.Value != "14:30:00" {
		t.Errorf("Expected time literal '14:30:00', got '%s'", literal.Value)
	}

	// For now, we'll just verify the literal is parsed.
	// In a real implementation, this would be converted to a time.Time at execution time
	t.Logf("Successfully parsed time literal: %s", literal.Value)
}
