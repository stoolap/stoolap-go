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
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Position represents a position in the input
type Position struct {
	Offset int // Byte offset, starting at 0
	Line   int // Line number, starting at 1
	Column int // Column number, starting at 1
}

// String returns a string representation of the position
func (p Position) String() string {
	return fmt.Sprintf("line %d, column %d", p.Line, p.Column)
}

// TokenType represents a token type
type TokenType int

const (
	// TokenError represents a token error
	TokenError TokenType = iota
	// TokenEOF represents the end of file
	TokenEOF
	// TokenIdentifier represents an identifier
	TokenIdentifier
	// TokenKeyword represents a keyword
	TokenKeyword
	// TokenString represents a string literal
	TokenString
	// TokenInteger represents an integer number
	TokenInteger
	// TokenFloat represents a floating point number
	TokenFloat
	// TokenOperator represents an operator
	TokenOperator
	// TokenPunctuator represents a punctuator
	TokenPunctuator
	// TokenComment represents a comment
	TokenComment
	// TokenDate represents a date literal
	TokenDate
	// TokenTime represents a time literal
	TokenTime
	// TokenTimestamp represents a timestamp literal
	TokenTimestamp
	// TokenParameter represents a parameter (e.g. $1, ?)
	TokenParameter
)

// String returns the string representation of the token type
func (tt TokenType) String() string {
	switch tt {
	case TokenError:
		return "ERROR"
	case TokenEOF:
		return "EOF"
	case TokenIdentifier:
		return "IDENTIFIER"
	case TokenKeyword:
		return "KEYWORD"
	case TokenString:
		return "STRING"
	case TokenInteger:
		return "INTEGER"
	case TokenFloat:
		return "FLOAT"
	case TokenOperator:
		return "OPERATOR"
	case TokenPunctuator:
		return "PUNCTUATOR"
	case TokenComment:
		return "COMMENT"
	case TokenTimestamp:
		return "TIMESTAMP"
	case TokenParameter:
		return "PARAMETER"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", tt)
	}
}

// Token represents a token
type Token struct {
	Type     TokenType
	Literal  string
	Position Position
	Value    interface{} // Processed value, if applicable
	Error    string      // Error message, if Type is TokenError
}

// String returns a string representation of the token
func (t Token) String() string {
	if t.Type == TokenError {
		return fmt.Sprintf("%s: %s at %s", t.Type, t.Error, t.Position)
	}
	if t.Type == TokenKeyword {
		return fmt.Sprintf("%s: %s at %s", t.Type, t.Literal, t.Position)
	}
	return fmt.Sprintf("%s: '%s' at %s", t.Type, t.Literal, t.Position)
}

// Lexer represents an SQL lexer
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           rune // current char under examination
	pos          Position

	// Error reporting
	lastError string
}

// Keywords is a map of all SQL keywords (case-insensitive)
var Keywords = map[string]bool{
	"SELECT":      true,
	"FROM":        true,
	"WHERE":       true,
	"INSERT":      true,
	"INTO":        true,
	"VALUES":      true,
	"UPDATE":      true,
	"SET":         true,
	"DELETE":      true,
	"CREATE":      true,
	"TABLE":       true,
	"DROP":        true,
	"ALTER":       true,
	"ADD":         true,
	"COLUMN":      true,
	"AND":         true,
	"OR":          true,
	"NOT":         true,
	"NULL":        true,
	"PRIMARY":     true,
	"PRAGMA":      true,
	"KEY":         true,
	"DEFAULT":     true,
	"AS":          true,
	"DISTINCT":    true,
	"ORDER":       true,
	"BY":          true,
	"ASC":         true,
	"DESC":        true,
	"LIMIT":       true,
	"OFFSET":      true,
	"GROUP":       true,
	"HAVING":      true,
	"JOIN":        true,
	"INNER":       true,
	"OUTER":       true,
	"LEFT":        true,
	"RIGHT":       true,
	"FULL":        true,
	"ON":          true,
	"DUPLICATE":   true,
	"USING":       true,
	"CROSS":       true,
	"NATURAL":     true,
	"TRUE":        true,
	"FALSE":       true,
	"INTEGER":     true,
	"FLOAT":       true,
	"TEXT":        true,
	"BOOLEAN":     true,
	"BOOL":        true,
	"TIMESTAMP":   true,
	"TIMESTAMPTZ": true,
	"DATETIME":    true,
	"DATE":        true,
	"TIME":        true,
	"JSON":        true,
	"CASE":        true,
	"CAST":        true,
	"WHEN":        true,
	"THEN":        true,
	"ELSE":        true,
	"END":         true,
	"BETWEEN":     true,
	"IN":          true,
	"IS":          true,
	"LIKE":        true,
	"EXISTS":      true,
	"ALL":         true,
	"ANY":         true,
	"SOME":        true,
	"IF":          true,
	"UNION":       true,
	"INTERSECT":   true,
	"EXCEPT":      true,
	"WITH":        true,
	"UNIQUE":      true,
	"CHECK":       true,
	"FOREIGN":     true,
	"REFERENCES":  true,
	"SHOW":        true,
	"TABLES":      true,
	"INDEXES":     true,
	"CASCADE":     true,
	"RESTRICT":    true,
	"INDEX":       true,
	"COLUMNAR":    true,
	"VIEW":        true,
	"TRIGGER":     true,
	"PROCEDURE":   true,
	"FUNCTION":    true,
	"RETURNING":   true,
	"OVER":        true,
	"PARTITION":   true,
	"RANGE":       true,
	"ROWS":        true,
	"UNBOUNDED":   true,
	"BEGIN":       true,
	"TRANSACTION": true,
	"COMMIT":      true,
	"ROLLBACK":    true,
	"SAVEPOINT":   true,
	"PRECEDING":   true,
	"FOLLOWING":   true,
	"CURRENT":     true,
	"ROW":         true,
	"MODIFY":      true,
	"RENAME":      true,
	"TO":          true,
	"VARCHAR":     true,
	"CHAR":        true,
	"STRING":      true,
	"BIGINT":      true,
	"TINYINT":     true,
	"SMALLINT":    true,
	"REAL":        true,
	"DOUBLE":      true,
	"INT":         true,
	"ISOLATION":   true,
	"LEVEL":       true,
	"READ":        true,
	"COMMITTED":   true,
	"UNCOMMITTED": true,
	"INTERVAL":    true,
}

// Operators is a map of SQL operators
var Operators = map[string]bool{
	"=":   true,
	">":   true,
	"<":   true,
	">=":  true,
	"<=":  true,
	"<>":  true,
	"!=":  true,
	"+":   true,
	"-":   true,
	"*":   true,
	"/":   true,
	"%":   true,
	"||":  true, // String concatenation
	"->":  true, // JSON operator
	"->>": true, // JSON operator
	"#>":  true, // JSON path operator
	"#>>": true, // JSON path text operator
	"@>":  true, // JSON contains
	"<@":  true, // JSON contained in
	"?":   true, // JSON exists
	"?|":  true, // JSON exists any
	"?&":  true, // JSON exists all
}

// Punctuators is a map of SQL punctuators
var Punctuators = map[rune]bool{
	',': true,
	';': true,
	'(': true,
	')': true,
	'.': true,
	':': true,
	'[': true,
	']': true,
	// '*' is handled as an operator for arithmetic expressions
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input: input,
		pos: Position{
			Line:   1,
			Column: 1,
		},
	}
	l.readChar()
	return l
}

// readChar reads the next character
func (l *Lexer) readChar() {
	// Update position before changing character
	if l.ch == '\n' {
		l.pos.Line++
		l.pos.Column = 1
	} else if l.ch != 0 { // Don't increment for first character
		l.pos.Column++
	}

	if l.readPosition >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		r, size := utf8.DecodeRuneInString(l.input[l.readPosition:])
		l.ch = r
		l.position = l.readPosition
		l.readPosition += size
	}

	l.pos.Offset = l.position
}

// peekChar returns the next character without advancing the position
func (l *Lexer) peekChar() rune {
	if l.readPosition >= len(l.input) {
		return 0 // EOF
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.readPosition:])
	return r
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	// Save the starting position for the token
	pos := l.pos

	var tok Token
	tok.Position = pos

	switch {
	case l.ch == 0:
		tok.Type = TokenEOF
		tok.Literal = ""

	case l.ch == '\'':
		// String literal (single quotes)
		tok.Type = TokenString
		tok.Literal = l.readStringLiteral()

	case l.ch == '"':
		// Double-quoted identifier (SQL standard)
		tok.Type = TokenIdentifier
		tok.Literal = l.readQuotedIdentifier('"')

	case l.ch == '`':
		// Backtick-quoted identifier (MySQL style)
		tok.Type = TokenIdentifier
		tok.Literal = l.readQuotedIdentifier('`')

	case l.ch == '-' && unicode.IsDigit(l.peekChar()):
		// Negative number
		tok.Literal = l.readNumberWithSign()
		if strings.Contains(tok.Literal, ".") {
			tok.Type = TokenFloat
		} else {
			tok.Type = TokenInteger
		}

	case unicode.IsDigit(l.ch):
		// Number literal
		tok.Literal = l.readNumber()
		if strings.Contains(tok.Literal, ".") {
			tok.Type = TokenFloat
		} else {
			tok.Type = TokenInteger
		}

	case l.ch == '#' || (l.ch == '-' && l.peekChar() == '-'):
		// Single line comment
		tok.Type = TokenComment
		tok.Literal = l.readLineComment()

	case l.ch == '/' && l.peekChar() == '*':
		// Multi-line comment
		tok.Type = TokenComment
		tok.Literal = l.readBlockComment()

	case l.ch == '$' && unicode.IsDigit(l.peekChar()):
		// Parameter like $1
		tok.Type = TokenParameter
		tok.Literal = l.readParameter()

	case l.ch == '?':
		// Parameter like ?
		tok.Type = TokenParameter
		tok.Literal = "?"
		l.readChar()

	case l.ch == '*':
		// Always treat * as an operator
		// SELECT * will be handled specially by the parser
		tok.Type = TokenOperator
		tok.Literal = "*"
		l.readChar()

	case Punctuators[l.ch]:
		// Regular punctuator
		tok.Type = TokenPunctuator
		tok.Literal = string(l.ch)
		l.readChar()

	case isOperatorChar(l.ch):
		// Operator
		tok.Type = TokenOperator
		tok.Literal = l.readOperator()

	case unicode.IsLetter(l.ch) || l.ch == '_':
		// Identifier or keyword
		tok.Literal = l.readIdentifier()

		// Check if it's a keyword (case-insensitive)
		if Keywords[strings.ToUpper(tok.Literal)] {
			tok.Type = TokenKeyword
			tok.Literal = strings.ToUpper(tok.Literal)
		} else {
			tok.Type = TokenIdentifier
		}

	default:
		// Unrecognized character
		tok.Type = TokenError
		tok.Literal = string(l.ch)
		tok.Error = fmt.Sprintf("unrecognized character: %q", l.ch)
		l.readChar()
	}

	return tok
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.ch) {
		// When we skip newlines, update the line and column position
		if l.ch == '\n' {
			l.pos.Line++
			l.pos.Column = 0
		} else if l.ch == '\r' {
			// For CR+LF, we'll handle the LF separately
			if l.peekChar() == '\n' {
				l.readChar() // consume the '\r'
				// The next iteration will handle the '\n'
			} else {
				// Lone CR
				l.pos.Line++
				l.pos.Column = 0
			}
		}
		l.readChar()
	}
}

// readIdentifier reads an identifier
func (l *Lexer) readIdentifier() string {
	var result strings.Builder
	result.WriteRune(l.ch) // Include the current character

	// Advance to the next character
	l.readChar()

	// Continue reading valid identifier characters
	// Note: SQL identifiers can contain letters, digits, underscores
	for unicode.IsLetter(l.ch) || unicode.IsDigit(l.ch) || l.ch == '_' || l.ch == '$' {
		result.WriteRune(l.ch)
		l.readChar()
	}

	return result.String()
}

// readNumber reads a number (integer or float)
func (l *Lexer) readNumber() string {
	var result strings.Builder
	result.WriteRune(l.ch) // Include the current digit

	// Advance to the next character
	l.readChar()

	// Read all digits before decimal point
	for unicode.IsDigit(l.ch) {
		result.WriteRune(l.ch)
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && unicode.IsDigit(l.peekChar()) {
		result.WriteRune(l.ch) // Add the decimal point
		l.readChar()           // Move past the decimal point

		// Read all digits after decimal point
		for unicode.IsDigit(l.ch) {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Check for exponent (E or e)
	if l.ch == 'e' || l.ch == 'E' {
		result.WriteRune(l.ch) // Add the 'e' or 'E'
		l.readChar()           // Move past the 'e' or 'E'

		// Check for sign after exponent
		if l.ch == '+' || l.ch == '-' {
			result.WriteRune(l.ch) // Add the sign
			l.readChar()           // Move past the sign
		}

		// Must have at least one digit after exponent
		if !unicode.IsDigit(l.ch) {
			l.lastError = "invalid number format: exponent has no digits"
			return result.String()
		}

		// Read all digits in exponent
		for unicode.IsDigit(l.ch) {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	return result.String()
}

// readNumberWithSign reads a number that starts with a sign
func (l *Lexer) readNumberWithSign() string {
	var result strings.Builder
	result.WriteRune(l.ch) // Include the sign

	// Advance to the next character (which should be a digit)
	l.readChar()

	// Read all digits before decimal point
	for unicode.IsDigit(l.ch) {
		result.WriteRune(l.ch)
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && unicode.IsDigit(l.peekChar()) {
		result.WriteRune(l.ch) // Add the decimal point
		l.readChar()           // Move past the decimal point

		// Read all digits after decimal point
		for unicode.IsDigit(l.ch) {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Check for exponent (E or e)
	if l.ch == 'e' || l.ch == 'E' {
		result.WriteRune(l.ch) // Add the 'e' or 'E'
		l.readChar()           // Move past the 'e' or 'E'

		// Check for sign after exponent
		if l.ch == '+' || l.ch == '-' {
			result.WriteRune(l.ch) // Add the sign
			l.readChar()           // Move past the sign
		}

		// Must have at least one digit after exponent
		if !unicode.IsDigit(l.ch) {
			l.lastError = "invalid number format: exponent has no digits"
			return result.String()
		}

		// Read all digits in exponent
		for unicode.IsDigit(l.ch) {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	return result.String()
}

// readStringLiteral reads a string literal
func (l *Lexer) readStringLiteral() string {
	var result strings.Builder

	// Remember the quote character (single or double)
	quote := l.ch
	result.WriteRune(quote) // Include the opening quote

	l.readChar() // consume opening quote

	// Read until closing quote
	for l.ch != quote && l.ch != 0 {
		// Handle escape sequences
		if l.ch == '\\' {
			result.WriteRune(l.ch) // Add the backslash
			l.readChar()           // consume backslash

			// Simply add the next character, whatever it is
			if l.ch != 0 {
				result.WriteRune(l.ch)
				l.readChar()
			}
		} else {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Consume closing quote if not EOF
	if l.ch == quote {
		result.WriteRune(quote) // Include the closing quote
		l.readChar()
	} else {
		l.lastError = "unterminated string literal"
		// For unterminated strings, add a closing quote to match expectations
		result.WriteRune(quote)
	}

	return result.String()
}

// readQuotedIdentifier reads a quoted identifier (double quotes or backticks)
func (l *Lexer) readQuotedIdentifier(quote rune) string {
	var result strings.Builder

	l.readChar() // consume opening quote

	// Read until closing quote
	for l.ch != 0 {
		// Handle doubled quotes as escape (e.g., "abc""def" -> abc"def)
		if l.ch == quote && l.peekChar() == quote {
			result.WriteRune(l.ch)
			l.readChar() // consume first quote
			l.readChar() // consume second quote
		} else if l.ch == quote {
			// Found closing quote
			break
		} else {
			result.WriteRune(l.ch)
			l.readChar()
		}
	}

	// Consume closing quote if not EOF
	if l.ch == quote {
		l.readChar()
	} else {
		l.lastError = fmt.Sprintf("unterminated quoted identifier starting with %c", quote)
	}

	return result.String()
}

// readLineComment reads a single-line comment (-- or #)
func (l *Lexer) readLineComment() string {
	var result strings.Builder

	// Start with the current character (# or -)
	result.WriteRune(l.ch)

	// Skip the start of comment (-- or #)
	if l.ch == '-' && l.peekChar() == '-' {
		l.readChar()           // first -
		result.WriteRune(l.ch) // second -
		l.readChar()           // move past second -
	} else if l.ch == '#' {
		l.readChar() // move past #
	}

	// Read until end of line or EOF
	for l.ch != '\n' && l.ch != 0 {
		result.WriteRune(l.ch)
		l.readChar()
	}

	return result.String()
}

// readBlockComment reads a multi-line comment (/* ... */)
func (l *Lexer) readBlockComment() string {
	var result strings.Builder

	// Start with the opening /* sequence
	result.WriteRune(l.ch) // /
	l.readChar()
	result.WriteRune(l.ch) // *
	l.readChar()

	// Read until */ or EOF
	for !(l.ch == '*' && l.peekChar() == '/') && l.ch != 0 {
		result.WriteRune(l.ch)
		l.readChar()
	}

	// Handle closing */
	if l.ch != 0 {
		result.WriteRune(l.ch) // *
		l.readChar()
		result.WriteRune(l.ch) // /
		l.readChar()
	} else {
		l.lastError = "unterminated block comment"
	}

	return result.String()
}

// readOperator reads an operator
func (l *Lexer) readOperator() string {
	var result strings.Builder

	// Read the first character
	firstChar := l.ch
	result.WriteRune(firstChar)
	l.readChar()

	// Check for multi-character operators
	if l.ch != 0 {
		twoChars := string(firstChar) + string(l.ch)
		if Operators[twoChars] {
			result.WriteRune(l.ch) // Add the second character
			l.readChar()

			// Check for three-character operators
			if l.ch != 0 {
				threeChars := twoChars + string(l.ch)
				if Operators[threeChars] {
					result.WriteRune(l.ch) // Add the third character
					l.readChar()
					return result.String()
				}
			}
			return result.String()
		}
	}

	// Single-character operator
	return result.String()
}

// readParameter reads a parameter ($1, $2, etc.)
func (l *Lexer) readParameter() string {
	var result strings.Builder

	// Start with the $ sign
	result.WriteRune(l.ch)
	l.readChar()

	// Read all digits
	for unicode.IsDigit(l.ch) {
		result.WriteRune(l.ch)
		l.readChar()
	}

	// Validate parameter has digits
	if len(result.String()) == 1 {
		l.lastError = "parameter number expected after $"
	}

	return result.String()
}

// isOperatorChar checks if a character can be part of an operator
func isOperatorChar(ch rune) bool {
	// Don't include # as an operator character since it's used for comments
	return strings.ContainsRune("=<>!+-*/%|&^~?@:", ch)
}

// GetError returns the last error encountered
func (l *Lexer) GetError() string {
	return l.lastError
}

// PeekToken returns the next token without advancing the lexer
func (l *Lexer) PeekToken() Token {
	// Save current state
	savedPosition := l.position
	savedReadPosition := l.readPosition
	savedCh := l.ch
	savedPos := l.pos

	// Get the next token
	token := l.NextToken()

	// Restore the state
	l.position = savedPosition
	l.readPosition = savedReadPosition
	l.ch = savedCh
	l.pos = savedPos

	return token
}

// PeekTokens returns the next n tokens without advancing the lexer
func (l *Lexer) PeekTokens(n int) []Token {
	if n <= 0 {
		return []Token{}
	}

	// Save current state
	savedPosition := l.position
	savedReadPosition := l.readPosition
	savedCh := l.ch
	savedPos := l.pos

	// Get the next n tokens
	tokens := make([]Token, n)
	for i := 0; i < n; i++ {
		tokens[i] = l.NextToken()
		if tokens[i].Type == TokenEOF {
			break
		}
	}

	// Restore the state
	l.position = savedPosition
	l.readPosition = savedReadPosition
	l.ch = savedCh
	l.pos = savedPos

	return tokens
}
