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

func TestEnhancedLexer_BasicTokens(t *testing.T) {
	input := `SELECT id, name FROM users WHERE age > 18 AND status = 'active';`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenKeyword, "SELECT"},
		{TokenIdentifier, "id"},
		{TokenPunctuator, ","},
		{TokenIdentifier, "name"},
		{TokenKeyword, "FROM"},
		{TokenIdentifier, "users"},
		{TokenKeyword, "WHERE"},
		{TokenIdentifier, "age"},
		{TokenOperator, ">"},
		{TokenInteger, "18"},
		{TokenKeyword, "AND"},
		{TokenIdentifier, "status"},
		{TokenOperator, "="},
		{TokenString, "'active'"},
		{TokenPunctuator, ";"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		token := lexer.NextToken()

		if token.Type != tt.expectedType {
			t.Fatalf("tests[%d] - token type wrong. expected=%s, got=%s",
				i, tt.expectedType, token.Type)
		}

		if token.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, token.Literal)
		}
	}
}

func TestEnhancedLexer_Numbers(t *testing.T) {
	input := `123 45.67 -89 -10.11 1.2e3 1.2e-3 1.2e+3 92233720368547758`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenInteger, "123"},
		{TokenFloat, "45.67"},
		{TokenInteger, "-89"},
		{TokenFloat, "-10.11"},
		{TokenFloat, "1.2e3"},
		{TokenFloat, "1.2e-3"},
		{TokenFloat, "1.2e+3"},
		{TokenInteger, "92233720368547758"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		token := lexer.NextToken()

		if token.Type != tt.expectedType {
			t.Fatalf("tests[%d] - token type wrong. expected=%s, got=%s",
				i, tt.expectedType, token.Type)
		}

		if token.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, token.Literal)
		}
	}
}

func TestEnhancedLexer_Strings(t *testing.T) {
	input := `'simple string' "double quoted" 'escaped \' quote' "column name" 'multi
line'`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenString, "'simple string'"},
		{TokenIdentifier, "double quoted"}, // Double quotes now create identifiers
		{TokenString, "'escaped \\' quote'"},
		{TokenIdentifier, "column name"}, // Double quotes now create identifiers
		{TokenString, "'multi\nline'"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		token := lexer.NextToken()

		if token.Type != tt.expectedType {
			t.Fatalf("tests[%d] - token type wrong. expected=%s, got=%s",
				i, tt.expectedType, token.Type)
		}

		if token.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, token.Literal)
		}
	}
}

func TestEnhancedLexer_Comments(t *testing.T) {
	input := `
-- Single line comment
SELECT * FROM users; # Another comment
/* 
 * Multi-line comment
 */
SELECT 1;
`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenComment, "-- Single line comment"},
		{TokenKeyword, "SELECT"},
		{TokenOperator, "*"},
		{TokenKeyword, "FROM"},
		{TokenIdentifier, "users"},
		{TokenPunctuator, ";"},
		{TokenComment, "# Another comment"},
		{TokenComment, "/* \n * Multi-line comment\n */"},
		{TokenKeyword, "SELECT"},
		{TokenInteger, "1"},
		{TokenPunctuator, ";"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		token := lexer.NextToken()

		if token.Type != tt.expectedType {
			t.Fatalf("tests[%d] - token type wrong. expected=%s, got=%s",
				i, tt.expectedType, token.Type)
		}

		if token.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, token.Literal)
		}
	}
}

func TestEnhancedLexer_Operators(t *testing.T) {
	input := `= > < >= <= <> != + - / % || -> ->>`

	lexer := NewLexer(input)

	// Test each operator individually to help diagnose issues
	token := lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "=" {
		t.Errorf("Expected OPERATOR:=, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != ">" {
		t.Errorf("Expected OPERATOR:>, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "<" {
		t.Errorf("Expected OPERATOR:<, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != ">=" {
		t.Errorf("Expected OPERATOR:>=, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "<=" {
		t.Errorf("Expected OPERATOR:<=, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "<>" {
		t.Errorf("Expected OPERATOR:<>, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "!=" {
		t.Errorf("Expected OPERATOR:!=, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "+" {
		t.Errorf("Expected OPERATOR:+, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "-" {
		t.Errorf("Expected OPERATOR:-, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "/" {
		t.Errorf("Expected OPERATOR:/, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "%" {
		t.Errorf("Expected OPERATOR:%%, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "||" {
		t.Errorf("Expected OPERATOR:||, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "->" {
		t.Errorf("Expected OPERATOR:->, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenOperator || token.Literal != "->>" {
		t.Errorf("Expected OPERATOR:->>, got %s:%q", token.Type, token.Literal)
	}

	token = lexer.NextToken()
	if token.Type != TokenEOF {
		t.Errorf("Expected EOF, got %s:%q", token.Type, token.Literal)
	}
}

func TestEnhancedLexer_Parameters(t *testing.T) {
	input := `$1 $2 $345 ?`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenParameter, "$1"},
		{TokenParameter, "$2"},
		{TokenParameter, "$345"},
		{TokenParameter, "?"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		token := lexer.NextToken()

		if token.Type != tt.expectedType {
			t.Fatalf("tests[%d] - token type wrong. expected=%s, got=%s",
				i, tt.expectedType, token.Type)
		}

		if token.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, token.Literal)
		}
	}
}

func TestEnhancedLexer_Keywords(t *testing.T) {
	input := `SELECT FROM WHERE INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE
	DROP ALTER ADD COLUMN AND OR NOT NULL PRIMARY KEY DEFAULT AS DISTINCT ORDER BY
	ASC DESC LIMIT OFFSET GROUP HAVING JOIN INNER OUTER LEFT RIGHT FULL ON USING
	CROSS NATURAL TRUE FALSE INTEGER FLOAT TEXT BOOLEAN TIMESTAMP DATE TIME JSON
	CASE WHEN THEN ELSE END BETWEEN`

	lexer := NewLexer(input)

	// All tokens should be keywords
	for {
		token := lexer.NextToken()
		if token.Type == TokenEOF {
			break
		}

		if token.Type != TokenKeyword {
			t.Fatalf("expected keyword, got %s: %q", token.Type, token.Literal)
		}
	}
}

func TestEnhancedLexer_Position(t *testing.T) {
	input := "SELECT *\nFROM users\nWHERE id = 1;"

	expectedTokens := []struct {
		tokenType TokenType
		literal   string
	}{
		{TokenKeyword, "SELECT"},
		{TokenOperator, "*"},
		{TokenKeyword, "FROM"},
		{TokenIdentifier, "users"},
		{TokenKeyword, "WHERE"},
		{TokenIdentifier, "id"},
		{TokenOperator, "="},
		{TokenInteger, "1"},
		{TokenPunctuator, ";"},
	}

	lexer := NewLexer(input)

	for i, expected := range expectedTokens {
		token := lexer.NextToken()

		// Verify token type and literal
		if token.Type != expected.tokenType || token.Literal != expected.literal {
			t.Fatalf("tests[%d] - token wrong. expected=%s:%s, got=%s:%s",
				i, expected.tokenType, expected.literal, token.Type, token.Literal)
		}

		// Just check if position info is present and reasonably valid
		if token.Position.Line < 1 || token.Position.Column < 1 {
			t.Fatalf("tests[%d] - invalid position. got=line %d, column %d",
				i, token.Position.Line, token.Position.Column)
		}
	}
}

func TestEnhancedLexer_PeekToken(t *testing.T) {
	input := "SELECT id FROM users;"

	lexer := NewLexer(input)

	// Peek the first token (SELECT)
	peekedToken := lexer.PeekToken()
	if peekedToken.Type != TokenKeyword || peekedToken.Literal != "SELECT" {
		t.Fatalf("PeekToken wrong. expected=KEYWORD:SELECT, got=%s:%s",
			peekedToken.Type, peekedToken.Literal)
	}

	// The lexer position should not have changed
	token := lexer.NextToken()
	if token.Type != TokenKeyword || token.Literal != "SELECT" {
		t.Fatalf("NextToken after peek wrong. expected=KEYWORD:SELECT, got=%s:%s",
			token.Type, token.Literal)
	}

	// Advance to "id"
	token = lexer.NextToken()
	if token.Type != TokenIdentifier || token.Literal != "id" {
		t.Fatalf("NextToken wrong. expected=IDENTIFIER:id, got=%s:%s",
			token.Type, token.Literal)
	}

	// Peek the next token (FROM)
	peekedToken = lexer.PeekToken()
	if peekedToken.Type != TokenKeyword || peekedToken.Literal != "FROM" {
		t.Fatalf("PeekToken wrong. expected=KEYWORD:FROM, got=%s:%s",
			peekedToken.Type, peekedToken.Literal)
	}

	// The actual token should still be FROM
	token = lexer.NextToken()
	if token.Type != TokenKeyword || token.Literal != "FROM" {
		t.Fatalf("NextToken after peek wrong. expected=KEYWORD:FROM, got=%s:%s",
			token.Type, token.Literal)
	}
}

func TestEnhancedLexer_PeekTokens(t *testing.T) {
	input := "SELECT id, name FROM users;"

	lexer := NewLexer(input)

	// Peek multiple tokens
	peekedTokens := lexer.PeekTokens(3)

	// Check peeked tokens
	expectedTypes := []TokenType{TokenKeyword, TokenIdentifier, TokenPunctuator}
	expectedLiterals := []string{"SELECT", "id", ","}

	for i := 0; i < 3; i++ {
		if peekedTokens[i].Type != expectedTypes[i] || peekedTokens[i].Literal != expectedLiterals[i] {
			t.Fatalf("PeekTokens[%d] wrong. expected=%s:%s, got=%s:%s",
				i, expectedTypes[i], expectedLiterals[i], peekedTokens[i].Type, peekedTokens[i].Literal)
		}
	}

	// The lexer position should not have changed
	token := lexer.NextToken()
	if token.Type != TokenKeyword || token.Literal != "SELECT" {
		t.Fatalf("NextToken after peek wrong. expected=KEYWORD:SELECT, got=%s:%s",
			token.Type, token.Literal)
	}
}

func TestEnhancedLexer_ComplexSQL(t *testing.T) {
	input := `
	-- This is a complex SQL query
	WITH sales_data AS (
		SELECT 
			product_id,
			DATE_TRUNC('month', sale_date) AS month,
			SUM(amount) AS total_sales
		FROM 
			sales
		WHERE 
			sale_date BETWEEN '2023-01-01' AND '2023-12-31'
		GROUP BY
			product_id, 
			DATE_TRUNC('month', sale_date)
	)
	SELECT 
		p.name,
		s.month,
		s.total_sales,
		RANK() OVER (PARTITION BY s.month ORDER BY s.total_sales DESC) AS rank
	FROM 
		sales_data s
	JOIN 
		products p ON p.id = s.product_id
	WHERE 
		s.total_sales > 1000.00
	ORDER BY
		s.month, rank
	LIMIT 10;
	`

	lexer := NewLexer(input)

	// Just make sure we can tokenize the whole complex query without errors
	tokenCount := 0
	hasError := false

	for {
		token := lexer.NextToken()
		if token.Type == TokenEOF {
			break
		}

		if token.Type == TokenError {
			t.Errorf("Error token encountered: %s", token.Error)
			hasError = true
		}

		tokenCount++
	}

	if hasError {
		t.Fatalf("Errors encountered while tokenizing complex SQL")
	}

	// Just a sanity check to make sure we got a reasonable number of tokens
	if tokenCount < 50 {
		t.Fatalf("Expected at least 50 tokens, got %d", tokenCount)
	}
}
