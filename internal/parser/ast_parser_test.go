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
package parser

import (
	"testing"
)

// TestParserGeneratedAST tests AST nodes as they are created by the parser
func TestParserGeneratedAST(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Literals
		{"Integer literal", "SELECT 42", "SELECT 42"},
		{"Float literal", "SELECT 3.14", "SELECT 3.14"},
		{"String literal", "SELECT 'hello'", "SELECT 'hello'"},
		{"Boolean true", "SELECT TRUE", "SELECT TRUE"},
		{"Boolean false", "SELECT FALSE", "SELECT FALSE"},
		{"NULL literal", "SELECT NULL", "SELECT NULL"},

		// Expressions
		{"Addition", "SELECT 1 + 2", "SELECT (1 + 2)"},
		{"Subtraction", "SELECT 5 - 3", "SELECT (5 - 3)"},
		{"Multiplication", "SELECT 4 * 5", "SELECT (4 * 5)"},
		{"Division", "SELECT 10 / 2", "SELECT (10 / 2)"},
		{"String concatenation", "SELECT 'hello' || ' world'", "SELECT ('hello' || ' world')"},

		// Functions
		{"Function call", "SELECT UPPER('hello')", "SELECT UPPER('hello')"},
		{"Function with multiple args", "SELECT ROUND(3.14159, 2)", "SELECT ROUND(3.14159, 2)"},
		{"COUNT(*)", "SELECT COUNT(*)", "SELECT COUNT(*)"},

		// Aliases
		{"Column alias", "SELECT id AS user_id FROM users", "SELECT id AS user_id FROM users"},
		{"Expression alias", "SELECT price * 2 AS double_price FROM products", "SELECT (price * 2) AS double_price FROM products"},

		// Complex queries
		{"WHERE clause", "SELECT * FROM users WHERE age > 18", "SELECT * FROM users WHERE (age > 18)"},
		{"GROUP BY", "SELECT category, COUNT(*) FROM products GROUP BY category", "SELECT category, COUNT(*) FROM products GROUP BY category"},
		{"ORDER BY", "SELECT * FROM users ORDER BY name", "SELECT * FROM users ORDER BY name ASC"},
		{"LIMIT", "SELECT * FROM users LIMIT 10", "SELECT * FROM users LIMIT 10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input
			lexer := NewLexer(tt.input)
			p := NewParser(lexer)
			program := p.ParseProgram()

			// Check for parser errors
			if len(p.Errors()) > 0 {
				t.Fatalf("Parser errors: %v", p.Errors())
			}

			// Get the string representation
			if len(program.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(program.Statements))
			}

			result := program.Statements[0].String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestASTNodeProperties tests specific properties of AST nodes
func TestASTNodeProperties(t *testing.T) {
	t.Run("IntegerLiteral value", func(t *testing.T) {
		lexer := NewLexer("SELECT 42")
		p := NewParser(lexer)
		program := p.ParseProgram()

		selectStmt := program.Statements[0].(*SelectStatement)
		intLit := selectStmt.Columns[0].(*IntegerLiteral)

		if intLit.Value != 42 {
			t.Errorf("IntegerLiteral.Value = %d, want 42", intLit.Value)
		}
	})

	t.Run("Identifier value", func(t *testing.T) {
		lexer := NewLexer("SELECT user_id FROM users")
		p := NewParser(lexer)
		program := p.ParseProgram()

		selectStmt := program.Statements[0].(*SelectStatement)
		ident := selectStmt.Columns[0].(*Identifier)

		if ident.Value != "user_id" {
			t.Errorf("Identifier.Value = %q, want %q", ident.Value, "user_id")
		}
	})

	t.Run("InfixExpression operator", func(t *testing.T) {
		lexer := NewLexer("SELECT a + b FROM t")
		p := NewParser(lexer)
		program := p.ParseProgram()

		selectStmt := program.Statements[0].(*SelectStatement)
		infixExpr := selectStmt.Columns[0].(*InfixExpression)

		if infixExpr.Operator != "+" {
			t.Errorf("InfixExpression.Operator = %q, want %q", infixExpr.Operator, "+")
		}
	})
}
