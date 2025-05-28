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
	"testing"
)

// Helper functions to create AST nodes with proper tokens
func makeIntegerLiteral(value int64) *IntegerLiteral {
	return &IntegerLiteral{
		Token: Token{Type: TokenInteger, Literal: fmt.Sprintf("%d", value)},
		Value: value,
	}
}

func makeFloatLiteral(value float64) *FloatLiteral {
	return &FloatLiteral{
		Token: Token{Type: TokenFloat, Literal: fmt.Sprintf("%g", value)},
		Value: value,
	}
}

func makeStringLiteral(value string) *StringLiteral {
	return &StringLiteral{
		Token: Token{Type: TokenString, Literal: fmt.Sprintf("'%s'", value)},
		Value: value,
	}
}

func makeBooleanLiteral(value bool) *BooleanLiteral {
	literal := "FALSE"
	if value {
		literal = "TRUE"
	}
	return &BooleanLiteral{
		Token: Token{Type: TokenKeyword, Literal: literal},
		Value: value,
	}
}

func makeIdentifier(name string) *Identifier {
	return &Identifier{
		Token: Token{Type: TokenIdentifier, Literal: name},
		Value: name,
	}
}

func TestProgram(t *testing.T) {
	tests := []struct {
		name     string
		program  *Program
		expected string
	}{
		{
			name:     "Empty program",
			program:  &Program{Statements: []Statement{}},
			expected: "",
		},
		{
			name: "Program with single statement",
			program: &Program{
				Statements: []Statement{
					&SelectStatement{
						Columns: []Expression{
							makeIntegerLiteral(42),
						},
					},
				},
			},
			expected: "SELECT 42;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.program.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		ident    *Identifier
		expected string
	}{
		{
			name: "Simple identifier",
			ident: &Identifier{
				Token: Token{Type: TokenIdentifier, Literal: "column1"},
				Value: "column1",
			},
			expected: "column1",
		},
		{
			name: "Uppercase identifier",
			ident: &Identifier{
				Token: Token{Type: TokenIdentifier, Literal: "TABLE_NAME"},
				Value: "TABLE_NAME",
			},
			expected: "TABLE_NAME",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test String()
			if result := tt.ident.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}

			// Test TokenLiteral()
			if result := tt.ident.TokenLiteral(); result != tt.ident.Token.Literal {
				t.Errorf("TokenLiteral() = %q, want %q", result, tt.ident.Token.Literal)
			}
		})
	}
}

func TestQualifiedIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		qi       *QualifiedIdentifier
		expected string
	}{
		{
			name: "Table.Column",
			qi: &QualifiedIdentifier{
				Token:     Token{Type: TokenIdentifier, Literal: "users"},
				Qualifier: makeIdentifier("users"),
				Name:      makeIdentifier("id"),
			},
			expected: "users.id",
		},
		{
			name: "Database.Table",
			qi: &QualifiedIdentifier{
				Token:     Token{Type: TokenIdentifier, Literal: "mydb"},
				Qualifier: makeIdentifier("mydb"),
				Name:      makeIdentifier("orders"),
			},
			expected: "mydb.orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.qi.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestLiterals(t *testing.T) {
	t.Run("IntegerLiteral", func(t *testing.T) {
		lit := &IntegerLiteral{
			Token: Token{Type: TokenInteger, Literal: "42"},
			Value: 42,
		}
		if lit.String() != "42" {
			t.Errorf("String() = %q, want %q", lit.String(), "42")
		}
	})

	t.Run("FloatLiteral", func(t *testing.T) {
		lit := &FloatLiteral{
			Token: Token{Type: TokenFloat, Literal: "3.14"},
			Value: 3.14,
		}
		if lit.String() != "3.14" {
			t.Errorf("String() = %q, want %q", lit.String(), "3.14")
		}
	})

	t.Run("StringLiteral", func(t *testing.T) {
		lit := &StringLiteral{
			Token: Token{Type: TokenString, Literal: "'hello'"},
			Value: "hello",
		}
		if lit.String() != "'hello'" {
			t.Errorf("String() = %q, want %q", lit.String(), "'hello'")
		}
	})

	t.Run("BooleanLiteral", func(t *testing.T) {
		tests := []struct {
			value    bool
			expected string
		}{
			{true, "TRUE"},
			{false, "FALSE"},
		}

		for _, tt := range tests {
			lit := &BooleanLiteral{
				Token: Token{Type: TokenKeyword, Literal: tt.expected},
				Value: tt.value,
			}
			if lit.String() != tt.expected {
				t.Errorf("String() = %q, want %q", lit.String(), tt.expected)
			}
		}
	})

	t.Run("NullLiteral", func(t *testing.T) {
		lit := &NullLiteral{
			Token: Token{Type: TokenKeyword, Literal: "NULL"},
		}
		if lit.String() != "NULL" {
			t.Errorf("String() = %q, want %q", lit.String(), "NULL")
		}
	})
}

func TestInfixExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *InfixExpression
		expected string
	}{
		{
			name: "Addition",
			expr: &InfixExpression{
				Left:     makeIntegerLiteral(1),
				Operator: "+",
				Right:    makeIntegerLiteral(2),
			},
			expected: "(1 + 2)",
		},
		{
			name: "Comparison",
			expr: &InfixExpression{
				Left:     makeIdentifier("age"),
				Operator: ">",
				Right:    makeIntegerLiteral(18),
			},
			expected: "(age > 18)",
		},
		{
			name: "String concatenation",
			expr: &InfixExpression{
				Left:     makeStringLiteral("hello"),
				Operator: "||",
				Right:    makeStringLiteral("world"),
			},
			expected: "('hello' || 'world')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestPrefixExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *PrefixExpression
		expected string
	}{
		{
			name: "Negative number",
			expr: &PrefixExpression{
				Operator: "-",
				Right:    makeIntegerLiteral(42),
			},
			expected: "(-42)",
		},
		{
			name: "NOT expression",
			expr: &PrefixExpression{
				Operator: "NOT",
				Right:    makeBooleanLiteral(true),
			},
			expected: "(NOT TRUE)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestFunctionCall(t *testing.T) {
	tests := []struct {
		name     string
		fn       *FunctionCall
		expected string
	}{
		{
			name: "COUNT(*)",
			fn: &FunctionCall{
				Function: "COUNT",
				Arguments: []Expression{
					makeIdentifier("*"),
				},
			},
			expected: "COUNT(*)",
		},
		{
			name: "UPPER with string",
			fn: &FunctionCall{
				Function: "UPPER",
				Arguments: []Expression{
					makeStringLiteral("hello"),
				},
			},
			expected: "UPPER('hello')",
		},
		{
			name: "ROUND with two arguments",
			fn: &FunctionCall{
				Function: "ROUND",
				Arguments: []Expression{
					makeFloatLiteral(3.14159),
					makeIntegerLiteral(2),
				},
			},
			expected: "ROUND(3.14159, 2)",
		},
		{
			name: "Function with no arguments",
			fn: &FunctionCall{
				Function:  "NOW",
				Arguments: []Expression{},
			},
			expected: "NOW()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.fn.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestAliasedExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *AliasedExpression
		expected string
	}{
		{
			name: "Column with alias",
			expr: &AliasedExpression{
				Expression: makeIdentifier("first_name"),
				Alias:      makeIdentifier("name"),
			},
			expected: "first_name AS name",
		},
		{
			name: "Function with alias",
			expr: &AliasedExpression{
				Expression: &FunctionCall{
					Function: "COUNT",
					Arguments: []Expression{
						makeIdentifier("*"),
					},
				},
				Alias: makeIdentifier("total"),
			},
			expected: "COUNT(*) AS total",
		},
		{
			name: "Expression with alias",
			expr: &AliasedExpression{
				Expression: &InfixExpression{
					Left:     makeIdentifier("price"),
					Operator: "*",
					Right:    makeIntegerLiteral(2),
				},
				Alias: makeIdentifier("double_price"),
			},
			expected: "(price * 2) AS double_price",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestCastExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *CastExpression
		expected string
	}{
		{
			name: "Cast to integer",
			expr: &CastExpression{
				Expr:     makeStringLiteral("42"),
				TypeName: "INTEGER",
			},
			expected: "CAST('42' AS INTEGER)",
		},
		{
			name: "Cast to float",
			expr: &CastExpression{
				Expr:     makeIntegerLiteral(10),
				TypeName: "FLOAT",
			},
			expected: "CAST(10 AS FLOAT)",
		},
		{
			name: "Cast column to text",
			expr: &CastExpression{
				Expr:     makeIdentifier("user_id"),
				TypeName: "TEXT",
			},
			expected: "CAST(user_id AS TEXT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBetweenExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *BetweenExpression
		expected string
	}{
		{
			name: "Number between",
			expr: &BetweenExpression{
				Expr:  makeIdentifier("age"),
				Lower: makeIntegerLiteral(18),
				Upper: makeIntegerLiteral(65),
			},
			expected: "age BETWEEN 18 AND 65",
		},
		{
			name: "Date between",
			expr: &BetweenExpression{
				Expr:  makeIdentifier("created_at"),
				Lower: makeStringLiteral("2024-01-01"),
				Upper: makeStringLiteral("2024-12-31"),
			},
			expected: "created_at BETWEEN '2024-01-01' AND '2024-12-31'",
		},
		{
			name: "NOT BETWEEN",
			expr: &BetweenExpression{
				Expr:  makeIdentifier("score"),
				Lower: makeIntegerLiteral(0),
				Upper: makeIntegerLiteral(59),
				Not:   true,
			},
			expected: "score NOT BETWEEN 0 AND 59",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestInExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     *InExpression
		expected string
	}{
		{
			name: "IN with integers",
			expr: &InExpression{
				Left: makeIdentifier("id"),
				Right: &ExpressionList{
					Expressions: []Expression{
						makeIntegerLiteral(1),
						makeIntegerLiteral(2),
						makeIntegerLiteral(3),
					},
				},
			},
			expected: "id IN (1, 2, 3)",
		},
		{
			name: "NOT IN with strings",
			expr: &InExpression{
				Left: makeIdentifier("status"),
				Right: &ExpressionList{
					Expressions: []Expression{
						makeStringLiteral("pending"),
						makeStringLiteral("cancelled"),
					},
				},
				Not: true,
			},
			expected: "status NOT IN ('pending', 'cancelled')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.expr.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestCaseExpression(t *testing.T) {
	t.Run("Simple CASE", func(t *testing.T) {
		expr := &CaseExpression{
			Value: makeIdentifier("grade"),
			WhenClauses: []*WhenClause{
				{
					Condition:  makeStringLiteral("A"),
					ThenResult: makeStringLiteral("Excellent"),
				},
				{
					Condition:  makeStringLiteral("B"),
					ThenResult: makeStringLiteral("Good"),
				},
			},
			ElseValue: makeStringLiteral("Average"),
		}
		expected := "CASE grade WHEN 'A' THEN 'Excellent' WHEN 'B' THEN 'Good' ELSE 'Average' END"
		if result := expr.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})

	t.Run("Searched CASE", func(t *testing.T) {
		expr := &CaseExpression{
			WhenClauses: []*WhenClause{
				{
					Condition: &InfixExpression{
						Left:     makeIdentifier("score"),
						Operator: ">",
						Right:    makeIntegerLiteral(90),
					},
					ThenResult: makeStringLiteral("A"),
				},
				{
					Condition: &InfixExpression{
						Left:     makeIdentifier("score"),
						Operator: ">",
						Right:    makeIntegerLiteral(80),
					},
					ThenResult: makeStringLiteral("B"),
				},
			},
			ElseValue: makeStringLiteral("C"),
		}
		expected := "CASE WHEN (score > 90) THEN 'A' WHEN (score > 80) THEN 'B' ELSE 'C' END"
		if result := expr.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})
}

func TestSelectStatement(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *SelectStatement
		expected string
	}{
		{
			name: "Simple SELECT",
			stmt: &SelectStatement{
				Columns: []Expression{
					makeIdentifier("id"),
					makeIdentifier("name"),
				},
				TableExpr: makeIdentifier("users"),
			},
			expected: "SELECT id, name FROM users",
		},
		{
			name: "SELECT with WHERE",
			stmt: &SelectStatement{
				Columns: []Expression{
					makeIdentifier("*"),
				},
				TableExpr: makeIdentifier("orders"),
				Where: &InfixExpression{
					Left:     makeIdentifier("status"),
					Operator: "=",
					Right:    makeStringLiteral("active"),
				},
			},
			expected: "SELECT * FROM orders WHERE (status = 'active')",
		},
		{
			name: "SELECT with GROUP BY and HAVING",
			stmt: &SelectStatement{
				Columns: []Expression{
					makeIdentifier("category"),
					&AliasedExpression{
						Expression: &FunctionCall{
							Function: "COUNT",
							Arguments: []Expression{
								makeIdentifier("*"),
							},
						},
						Alias: makeIdentifier("count"),
					},
				},
				TableExpr: makeIdentifier("products"),
				GroupBy: []Expression{
					makeIdentifier("category"),
				},
				Having: &InfixExpression{
					Left: &FunctionCall{
						Function: "COUNT",
						Arguments: []Expression{
							makeIdentifier("*"),
						},
					},
					Operator: ">",
					Right:    makeIntegerLiteral(5),
				},
			},
			expected: "SELECT category, COUNT(*) AS count FROM products GROUP BY category HAVING (COUNT(*) > 5)",
		},
		{
			name: "SELECT with ORDER BY and LIMIT",
			stmt: &SelectStatement{
				Columns: []Expression{
					makeIdentifier("name"),
					makeIdentifier("score"),
				},
				TableExpr: makeIdentifier("players"),
				OrderBy: []OrderByExpression{
					{
						Expression: makeIdentifier("score"),
						Ascending:  false,
					},
				},
				Limit: makeIntegerLiteral(10),
			},
			expected: "SELECT name, score FROM players ORDER BY score DESC LIMIT 10",
		},
		{
			name: "SELECT DISTINCT",
			stmt: &SelectStatement{
				Distinct: true,
				Columns: []Expression{
					makeIdentifier("category"),
				},
				TableExpr: makeIdentifier("products"),
			},
			expected: "SELECT DISTINCT category FROM products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.stmt.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestInsertStatement(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *InsertStatement
		expected string
	}{
		{
			name: "Simple INSERT",
			stmt: &InsertStatement{
				TableName: makeIdentifier("users"),
				Columns: []*Identifier{
					{Value: "name"},
					{Value: "email"},
				},
				Values: [][]Expression{
					{
						makeStringLiteral("John"),
						makeStringLiteral("john@example.com"),
					},
				},
			},
			expected: "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
		},
		{
			name: "INSERT with multiple rows",
			stmt: &InsertStatement{
				TableName: makeIdentifier("products"),
				Columns: []*Identifier{
					{Value: "name"},
					{Value: "price"},
				},
				Values: [][]Expression{
					{
						makeStringLiteral("Widget"),
						makeFloatLiteral(9.99),
					},
					{
						makeStringLiteral("Gadget"),
						makeFloatLiteral(19.99),
					},
				},
			},
			expected: "INSERT INTO products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.99)",
		},
		{
			name: "INSERT without column list",
			stmt: &InsertStatement{
				TableName: makeIdentifier("logs"),
				Values: [][]Expression{
					{
						makeIntegerLiteral(1),
						makeStringLiteral("Error"),
						&FunctionCall{Function: "NOW", Arguments: []Expression{}},
					},
				},
			},
			expected: "INSERT INTO logs VALUES (1, 'Error', NOW())",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.stmt.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestCreateTableStatement(t *testing.T) {
	t.Run("Simple CREATE TABLE", func(t *testing.T) {
		stmt := &CreateTableStatement{
			TableName: makeIdentifier("users"),
			Columns: []ColumnDefinition{
				{
					Name: makeIdentifier("id"),
					Type: "INTEGER",
					Constraints: []ColumnConstraint{
						&PrimaryKeyConstraint{},
					},
				},
				{
					Name: makeIdentifier("name"),
					Type: "TEXT",
					Constraints: []ColumnConstraint{
						&NotNullConstraint{},
					},
				},
			},
		}
		expected := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
		if result := stmt.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})

	t.Run("CREATE TABLE IF NOT EXISTS", func(t *testing.T) {
		stmt := &CreateTableStatement{
			IfNotExists: true,
			TableName:   makeIdentifier("logs"),
			Columns: []ColumnDefinition{
				{
					Name: makeIdentifier("id"),
					Type: "INTEGER",
				},
				{
					Name: makeIdentifier("message"),
					Type: "TEXT",
				},
			},
		}
		expected := "CREATE TABLE IF NOT EXISTS logs (id INTEGER, message TEXT)"
		if result := stmt.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})
}

func TestCreateIndexStatement(t *testing.T) {
	t.Run("Simple CREATE INDEX", func(t *testing.T) {
		stmt := &CreateIndexStatement{
			IndexName: makeIdentifier("idx_users_email"),
			TableName: makeIdentifier("users"),
			Columns: []*Identifier{
				{Value: "email"},
			},
		}
		expected := "CREATE INDEX idx_users_email ON users (email)"
		if result := stmt.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})

	t.Run("CREATE UNIQUE INDEX IF NOT EXISTS", func(t *testing.T) {
		stmt := &CreateIndexStatement{
			IsUnique:    true,
			IfNotExists: true,
			IndexName:   makeIdentifier("idx_users_username"),
			TableName:   makeIdentifier("users"),
			Columns: []*Identifier{
				{Value: "username"},
			},
		}
		expected := "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users (username)"
		if result := stmt.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})

	t.Run("Multi-column index", func(t *testing.T) {
		stmt := &CreateIndexStatement{
			IndexName: makeIdentifier("idx_orders_composite"),
			TableName: makeIdentifier("orders"),
			Columns: []*Identifier{
				{Value: "user_id"},
				{Value: "created_at"},
			},
		}
		expected := "CREATE INDEX idx_orders_composite ON orders (user_id, created_at)"
		if result := stmt.String(); result != expected {
			t.Errorf("String() = %q, want %q", result, expected)
		}
	})
}

func TestExpressionList(t *testing.T) {
	list := &ExpressionList{
		Expressions: []Expression{
			makeIntegerLiteral(1),
			makeIntegerLiteral(2),
			makeIntegerLiteral(3),
		},
	}
	expected := "(1, 2, 3)"
	if result := list.String(); result != expected {
		t.Errorf("String() = %q, want %q", result, expected)
	}
}

func TestParameter(t *testing.T) {
	tests := []struct {
		name     string
		param    *Parameter
		expected string
	}{
		{
			name:     "Question mark parameter",
			param:    &Parameter{Token: Token{Type: TokenParameter, Literal: "?"}},
			expected: "?",
		},
		{
			name:     "Dollar parameter",
			param:    &Parameter{Token: Token{Type: TokenParameter, Literal: "$1"}},
			expected: "$1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.param.String(); result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}
