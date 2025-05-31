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

	"github.com/stoolap/stoolap/internal/parser"
)

func TestStringQuoteRemoval(t *testing.T) {
	tests := []struct {
		input        string
		expected     string
		expectedType parser.TokenType
	}{
		// Single quotes create string tokens
		{"'simple string'", "simple string", parser.TokenString},
		// Double quotes now create identifier tokens (SQL standard)
		{"\"double quoted\"", "double quoted", parser.TokenIdentifier},
		{"'2023-05-15'", "2023-05-15", parser.TokenString},
		{"'14:30:00'", "14:30:00", parser.TokenString},
		// Add backtick test for completeness
		{"`backtick id`", "backtick id", parser.TokenIdentifier},
	}

	for i, tt := range tests {
		l := parser.NewLexer(tt.input)
		// For each string, we'll just check how the lexer tokenizes it
		token := l.NextToken()

		// Check the token type
		if token.Type != tt.expectedType {
			t.Fatalf("Test %d: Expected %v but got %v", i, tt.expectedType, token.Type)
		}

		// Check the literal based on token type
		if token.Type == parser.TokenString {
			// String tokens include quotes
			if token.Literal != tt.input {
				t.Errorf("Test %d: Expected literal '%s' but got '%s'", i, tt.input, token.Literal)
			}
			// Extract content without quotes
			content := ""
			if len(token.Literal) >= 2 {
				content = token.Literal[1 : len(token.Literal)-1]
			}
			if content != tt.expected {
				t.Errorf("Test %d: Expected content '%s' but got '%s'", i, tt.expected, content)
			}
		} else if token.Type == parser.TokenIdentifier {
			// Identifier tokens don't include quotes
			if token.Literal != tt.expected {
				t.Errorf("Test %d: Expected identifier '%s' but got '%s'", i, tt.expected, token.Literal)
			}
		}
	}
}
