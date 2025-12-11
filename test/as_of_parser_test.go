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

func TestAsOfParser(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "AS OF TRANSACTION with integer",
			input:    "SELECT * FROM users AS OF TRANSACTION 12345",
			expected: "SELECT * FROM users AS OF TRANSACTION 12345",
		},
		{
			name:     "AS OF TIMESTAMP with string",
			input:    "SELECT * FROM users AS OF TIMESTAMP '2024-01-01 10:00:00'",
			expected: "SELECT * FROM users AS OF TIMESTAMP '2024-01-01 10:00:00'",
		},
		{
			name:     "AS OF with table alias",
			input:    "SELECT * FROM users AS OF TRANSACTION 100 AS u",
			expected: "SELECT * FROM users AS OF TRANSACTION 100 AS u",
		},
		{
			name:     "AS OF with implicit alias",
			input:    "SELECT * FROM users AS OF TIMESTAMP '2024-01-01' u",
			expected: "SELECT * FROM users AS OF TIMESTAMP '2024-01-01' AS u",
		},
		{
			name:     "AS OF in JOIN",
			input:    "SELECT * FROM users AS OF TRANSACTION 100 u JOIN orders o ON u.id = o.user_id",
			expected: "SELECT * FROM users AS OF TRANSACTION 100 AS u INNER JOIN orders AS o ON (u.id = o.user_id)",
		},
		{
			name:    "Invalid AS OF type",
			input:   "SELECT * FROM users AS OF INVALID 100",
			wantErr: true,
		},
		{
			name:    "Missing AS OF value",
			input:   "SELECT * FROM users AS OF TRANSACTION",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := parser.NewLexer(tt.input)
			p := parser.NewParser(lexer)
			program := p.ParseProgram()

			if tt.wantErr {
				if len(p.Errors()) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(p.Errors()) > 0 {
				t.Fatalf("parser errors: %v", p.Errors())
			}

			if len(program.Statements) == 0 {
				t.Fatalf("no statements parsed")
			}

			got := program.Statements[0].String()
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}
