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

	"github.com/stoolap/stoolap/internal/parser/funcregistry"
)

// Node represents a node in the AST
type Node interface {
	// TokenLiteral returns the literal string of the token
	TokenLiteral() string
	// String returns a string representation of the node
	String() string
	// Position returns the position of the node in the source code
	Position() Position
}

// Statement represents a SQL statement
type Statement interface {
	Node
	// statementNode is a marker method to ensure type safety
	statementNode()
}

// Expression represents a SQL expression
type Expression interface {
	Node
	// expressionNode is a marker method to ensure type safety
	expressionNode()
}

// Program represents a SQL program
type Program struct {
	Statements []Statement
}

// TokenLiteral returns the literal string of the token
func (p *Program) TokenLiteral() string {
	if len(p.Statements) > 0 {
		return p.Statements[0].TokenLiteral()
	}
	return ""
}

// String returns a string representation of the program
func (p *Program) String() string {
	var result string
	for _, stmt := range p.Statements {
		result += stmt.String() + ";"
	}
	return result
}

// Position returns the position of the program
func (p *Program) Position() Position {
	if len(p.Statements) > 0 {
		return p.Statements[0].Position()
	}
	return Position{}
}

// Identifier represents an identifier
type Identifier struct {
	Token Token // TokenIdentifier token
	Value string
}

func (i *Identifier) expressionNode()      {}
func (i *Identifier) TokenLiteral() string { return i.Token.Literal }
func (i *Identifier) String() string       { return i.Value }
func (i *Identifier) Position() Position   { return i.Token.Position }

// QualifiedIdentifier represents a qualified identifier (e.g. table.column)
type QualifiedIdentifier struct {
	Token     Token       // The token that starts this identifier
	Qualifier *Identifier // The qualifier (e.g. table)
	Name      *Identifier // The name (e.g. column)
}

func (qi *QualifiedIdentifier) expressionNode()      {}
func (qi *QualifiedIdentifier) TokenLiteral() string { return qi.Token.Literal }
func (qi *QualifiedIdentifier) String() string {
	return fmt.Sprintf("%s.%s", qi.Qualifier.String(), qi.Name.String())
}
func (qi *QualifiedIdentifier) Position() Position { return qi.Token.Position }

// IntegerLiteral represents an integer literal
type IntegerLiteral struct {
	Token Token // TokenInteger token
	Value int64
}

func (il *IntegerLiteral) expressionNode()      {}
func (il *IntegerLiteral) TokenLiteral() string { return il.Token.Literal }
func (il *IntegerLiteral) String() string       { return il.Token.Literal }
func (il *IntegerLiteral) Position() Position   { return il.Token.Position }

// FloatLiteral represents a floating-point literal
type FloatLiteral struct {
	Token Token // TokenFloat token
	Value float64
}

func (fl *FloatLiteral) expressionNode()      {}
func (fl *FloatLiteral) TokenLiteral() string { return fl.Token.Literal }
func (fl *FloatLiteral) String() string       { return fl.Token.Literal }
func (fl *FloatLiteral) Position() Position   { return fl.Token.Position }

// StringLiteral represents a string literal
type StringLiteral struct {
	Token Token // TokenString token
	Value string
	// Optional type hint for string literals representing specific types like DATE, TIME, JSON
	TypeHint string
}

func (sl *StringLiteral) expressionNode() {}
func (sl *StringLiteral) TokenLiteral() string {
	// Lazy initialization - only build the literal string if needed
	if sl.Token.Literal == "" && sl.Value != "" {
		// Pre-allocate with 2 extra chars for the quotes
		sl.Token.Literal = "'" + sl.Value + "'"
	}
	return sl.Token.Literal
}
func (sl *StringLiteral) String() string     { return sl.TokenLiteral() }
func (sl *StringLiteral) Position() Position { return sl.Token.Position }

// BooleanLiteral represents a boolean literal
type BooleanLiteral struct {
	Token Token // TokenKeyword token (TRUE or FALSE)
	Value bool
}

func (bl *BooleanLiteral) expressionNode()      {}
func (bl *BooleanLiteral) TokenLiteral() string { return bl.Token.Literal }
func (bl *BooleanLiteral) String() string       { return bl.Token.Literal }
func (bl *BooleanLiteral) Position() Position   { return bl.Token.Position }

// NullLiteral represents a NULL literal
type NullLiteral struct {
	Token Token // TokenKeyword token (NULL)
}

func (nl *NullLiteral) expressionNode()      {}
func (nl *NullLiteral) TokenLiteral() string { return nl.Token.Literal }
func (nl *NullLiteral) String() string       { return "NULL" }
func (nl *NullLiteral) Position() Position   { return nl.Token.Position }

// Parameter represents a dynamic parameter for prepared statements
type Parameter struct {
	Token            Token  // The parameter token (? or $N)
	Name             string // The parameter name or number (empty for ?)
	Index            int    // The parameter index (1-based for positional parameters)
	Location         string // Describes where the parameter appears (e.g., "WHERE", "SELECT", "INSERT VALUES")
	StatementID      int    // Unique ID for the statement containing this parameter
	OrderInStatement int    // Position of this parameter within its parent statement
}

func (p *Parameter) expressionNode()      {}
func (p *Parameter) TokenLiteral() string { return p.Token.Literal }
func (p *Parameter) String() string {
	if p.Token.Literal != "" {
		return p.Token.Literal
	}
	if p.Name == "" {
		return "?"
	}
	return p.Name
}
func (p *Parameter) Position() Position { return p.Token.Position }

// PrefixExpression represents a prefix expression (e.g. -5, NOT condition)
type PrefixExpression struct {
	Token    Token // The prefix token, e.g. !
	Operator string
	Right    Expression
}

func (pe *PrefixExpression) expressionNode()      {}
func (pe *PrefixExpression) TokenLiteral() string { return pe.Token.Literal }
func (pe *PrefixExpression) String() string {
	// For unary minus/plus, don't add space
	if pe.Operator == "-" || pe.Operator == "+" {
		return fmt.Sprintf("(%s%s)", pe.Operator, pe.Right.String())
	}
	// For other operators like NOT, add space
	return fmt.Sprintf("(%s %s)", pe.Operator, pe.Right.String())
}
func (pe *PrefixExpression) Position() Position { return pe.Token.Position }

// InfixExpression represents an infix expression (e.g. 5 + 5, a = b)
type InfixExpression struct {
	Token    Token // The operator token, e.g. +
	Left     Expression
	Operator string
	Right    Expression
}

func (ie *InfixExpression) expressionNode()      {}
func (ie *InfixExpression) TokenLiteral() string { return ie.Token.Literal }
func (ie *InfixExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", ie.Left.String(), ie.Operator, ie.Right.String())
}
func (ie *InfixExpression) Position() Position { return ie.Token.Position }

// ListExpression represents a list of expressions (e.g., elements in IN clause)
type ListExpression struct {
	Token    Token // The opening parenthesis token
	Elements []Expression
}

func (le *ListExpression) expressionNode()      {}
func (le *ListExpression) TokenLiteral() string { return le.Token.Literal }
func (le *ListExpression) String() string {
	var out string
	elements := make([]string, 0, len(le.Elements))
	for _, e := range le.Elements {
		elements = append(elements, e.String())
	}
	out = "(" + strings.Join(elements, ", ") + ")"
	return out
}
func (le *ListExpression) Position() Position { return le.Token.Position }

// DistinctExpression represents a DISTINCT expression used in functions like COUNT(DISTINCT column)
type DistinctExpression struct {
	Token Token      // The DISTINCT token
	Expr  Expression // The expression being made distinct
}

func (de *DistinctExpression) expressionNode()      {}
func (de *DistinctExpression) TokenLiteral() string { return de.Token.Literal }
func (de *DistinctExpression) String() string {
	return fmt.Sprintf("DISTINCT %s", de.Expr.String())
}
func (de *DistinctExpression) Position() Position { return de.Token.Position }

// ExistsExpression represents an EXISTS subquery expression
type ExistsExpression struct {
	Token    Token            // The EXISTS token
	Subquery *SelectStatement // The subquery
}

func (ee *ExistsExpression) expressionNode()      {}
func (ee *ExistsExpression) TokenLiteral() string { return ee.Token.Literal }
func (ee *ExistsExpression) String() string {
	return fmt.Sprintf("EXISTS (%s)", ee.Subquery.String())
}
func (ee *ExistsExpression) Position() Position { return ee.Token.Position }

// InExpression represents an expression using the IN operator
type InExpression struct {
	Token Token      // The IN token
	Left  Expression // The expression to check
	Right Expression // Either a list of values or a subquery
	Not   bool       // If true, this is a NOT IN expression
}

func (ie *InExpression) expressionNode()      {}
func (ie *InExpression) TokenLiteral() string { return ie.Token.Literal }
func (ie *InExpression) String() string {
	if ie.Not {
		return fmt.Sprintf("%s NOT IN %s", ie.Left.String(), ie.Right.String())
	}
	return fmt.Sprintf("%s IN %s", ie.Left.String(), ie.Right.String())
}
func (ie *InExpression) Position() Position { return ie.Token.Position }

// BetweenExpression represents an expression using the BETWEEN operator
type BetweenExpression struct {
	Token Token      // The BETWEEN token
	Expr  Expression // The expression to check
	Lower Expression // The lower bound
	Upper Expression // The upper bound
	Not   bool       // If true, this is a NOT BETWEEN expression
}

func (be *BetweenExpression) expressionNode()      {}
func (be *BetweenExpression) TokenLiteral() string { return be.Token.Literal }
func (be *BetweenExpression) String() string {
	if be.Not {
		return fmt.Sprintf("%s NOT BETWEEN %s AND %s", be.Expr.String(), be.Lower.String(), be.Upper.String())
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", be.Expr.String(), be.Lower.String(), be.Upper.String())
}
func (be *BetweenExpression) Position() Position { return be.Token.Position }

// ScalarSubquery represents a subquery that returns a single value
type ScalarSubquery struct {
	Token    Token // The opening parenthesis token
	Subquery *SelectStatement
}

func (ss *ScalarSubquery) expressionNode()      {}
func (ss *ScalarSubquery) TokenLiteral() string { return ss.Token.Literal }
func (ss *ScalarSubquery) String() string {
	return fmt.Sprintf("(%s)", ss.Subquery.String())
}
func (ss *ScalarSubquery) Position() Position { return ss.Token.Position }

// ExpressionList represents a list of expressions, used in IN expressions
type ExpressionList struct {
	Token       Token        // The opening parenthesis token
	Expressions []Expression // The list of expressions
}

func (el *ExpressionList) expressionNode()      {}
func (el *ExpressionList) TokenLiteral() string { return el.Token.Literal }
func (el *ExpressionList) String() string {
	var exprStrings []string
	for _, expr := range el.Expressions {
		exprStrings = append(exprStrings, expr.String())
	}
	return fmt.Sprintf("(%s)", strings.Join(exprStrings, ", "))
}
func (el *ExpressionList) Position() Position { return el.Token.Position }

// CaseExpression represents a CASE expression
type CaseExpression struct {
	Token       Token      // The CASE token
	Value       Expression // The expression to match (for simple CASE), can be nil
	WhenClauses []*WhenClause
	ElseValue   Expression // The ELSE expression, can be nil
}

func (ce *CaseExpression) expressionNode()      {}
func (ce *CaseExpression) TokenLiteral() string { return ce.Token.Literal }
func (ce *CaseExpression) Position() Position   { return ce.Token.Position }
func (ce *CaseExpression) String() string {
	var result string
	result = "CASE"

	if ce.Value != nil {
		result += " " + ce.Value.String()
	}

	for _, when := range ce.WhenClauses {
		result += " " + when.String()
	}

	if ce.ElseValue != nil {
		result += " ELSE " + ce.ElseValue.String()
	}

	result += " END"
	return result
}

// WhenClause represents a WHEN ... THEN ... clause in a CASE expression
type WhenClause struct {
	Token      Token      // The WHEN token
	Condition  Expression // The condition to match
	ThenResult Expression // The result if the condition matches
}

func (wc *WhenClause) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", wc.Condition.String(), wc.ThenResult.String())
}

// CastExpression represents a CAST expression
type CastExpression struct {
	Token    Token      // The CAST token
	Expr     Expression // The expression to cast
	TypeName string     // The type to cast to
}

func (ce *CastExpression) expressionNode()      {}
func (ce *CastExpression) TokenLiteral() string { return ce.Token.Literal }
func (ce *CastExpression) Position() Position   { return ce.Token.Position }
func (ce *CastExpression) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", ce.Expr.String(), ce.TypeName)
}

// ToFunctionCall converts a CastExpression to a FunctionCall
// This allows CAST to be treated as a scalar function
func (ce *CastExpression) ToFunctionCall() *FunctionCall {
	// Create a string literal for the type name
	typeLiteral := &StringLiteral{
		Token: Token{
			Type:     TokenString,
			Literal:  ce.TypeName,
			Position: ce.Token.Position,
		},
		Value: ce.TypeName,
	}

	// Create the function call with the expression and type as arguments
	return &FunctionCall{
		Token:    ce.Token,
		Function: "CAST",
		Arguments: []Expression{
			ce.Expr,
			typeLiteral,
		},
	}
}

// FunctionCall represents a function call (e.g. COUNT(*), SUM(price))
type FunctionCall struct {
	Token     Token // The function name token
	Function  string
	Arguments []Expression
	// FunctionInfo contains metadata about the function, populated during validation
	FunctionInfo *funcregistry.FunctionInfo
	// IsDistinct indicates whether this is an aggregate function with DISTINCT
	IsDistinct bool
	// OrderBy holds the ORDER BY clause specification for ordered aggregate functions (FIRST, LAST)
	OrderBy []OrderByExpression
}

func (fc *FunctionCall) expressionNode()      {}
func (fc *FunctionCall) TokenLiteral() string { return fc.Token.Literal }
func (fc *FunctionCall) IsAggregate() bool {
	if fc.FunctionInfo != nil &&
		fc.FunctionInfo.Type == funcregistry.AggregateFunction {
		return true
	}
	return false
}
func (fc *FunctionCall) String() string {
	var args string

	if fc.IsDistinct && len(fc.Arguments) > 0 {
		// For DISTINCT syntax
		args = "DISTINCT "
		args += fc.Arguments[0].String()

		// Add any additional arguments
		for i := 1; i < len(fc.Arguments); i++ {
			args += ", " + fc.Arguments[i].String()
		}
	} else {
		// Regular arguments handling
		for i, arg := range fc.Arguments {
			if i > 0 {
				args += ", "
			}

			// Special handling for star argument in COUNT(*)
			if arg.String() == "*" {
				args += "*"
			} else {
				args += arg.String()
			}
		}
	}

	// Add ORDER BY if present
	if len(fc.OrderBy) > 0 {
		args += " ORDER BY "
		for i, orderBy := range fc.OrderBy {
			if i > 0 {
				args += ", "
			}
			args += orderBy.String()
		}
	}

	return fmt.Sprintf("%s(%s)", fc.Function, args)
}
func (fc *FunctionCall) Position() Position { return fc.Token.Position }

// IsWindow returns true if this function call is a window function
func (fc *FunctionCall) IsWindow() bool {
	return fc.FunctionInfo != nil &&
		fc.FunctionInfo.Type == funcregistry.WindowFunction
}

// ContainsAggregate returns true if this expression or any of its subexpressions contain an aggregate function
func ContainsAggregate(expr Expression) bool {
	switch e := expr.(type) {
	case *FunctionCall:
		return e.IsAggregate()
	case *InfixExpression:
		return ContainsAggregate(e.Left) || ContainsAggregate(e.Right)
	case *PrefixExpression:
		return ContainsAggregate(e.Right)
	case *AliasedExpression:
		return ContainsAggregate(e.Expression)
	case *DistinctExpression:
		return ContainsAggregate(e.Expr)
	default:
		return false
	}
}

// AlterTableOperation represents the type of ALTER TABLE operation
type AlterTableOperation int

const (
	// AddColumn operation adds a new column
	AddColumn AlterTableOperation = iota
	// DropColumn operation removes a column
	DropColumn
	// RenameColumn operation renames a column
	RenameColumn
	// ModifyColumn operation modifies a column definition
	ModifyColumn
	// RenameTable operation renames the table
	RenameTable
)

// AlterTableStatement represents an ALTER TABLE statement
type AlterTableStatement struct {
	Token     Token // The ALTER token
	TableName *Identifier
	Operation AlterTableOperation
	// For AddColumn and ModifyColumn operations
	ColumnDef *ColumnDefinition
	// For DropColumn, RenameColumn operations
	ColumnName *Identifier
	// For RenameColumn operation
	NewColumnName *Identifier
	// For RenameTable operation
	NewTableName *Identifier
}

func (ats *AlterTableStatement) statementNode()       {}
func (ats *AlterTableStatement) TokenLiteral() string { return ats.Token.Literal }
func (ats *AlterTableStatement) Position() Position   { return ats.Token.Position }
func (ats *AlterTableStatement) String() string {
	var result string
	result = "ALTER TABLE " + ats.TableName.String() + " "

	switch ats.Operation {
	case AddColumn:
		result += "ADD COLUMN " + ats.ColumnDef.String()
	case DropColumn:
		result += "DROP COLUMN " + ats.ColumnName.String()
	case RenameColumn:
		result += "RENAME COLUMN " + ats.ColumnName.String() + " TO " + ats.NewColumnName.String()
	case ModifyColumn:
		result += "MODIFY COLUMN " + ats.ColumnDef.String()
	case RenameTable:
		result += "RENAME TO " + ats.NewTableName.String()
	}

	return result
}

// Statement implementations

// CommonTableExpression represents a CTE in a WITH clause
type CommonTableExpression struct {
	Token       Token            // The CTE name token
	Name        *Identifier      // Name of the CTE
	ColumnNames []*Identifier    // Optional column names
	Query       *SelectStatement // The query that defines the CTE
	IsRecursive bool             // Whether this is part of a recursive WITH
}

// String returns a string representation of the CTE
func (cte *CommonTableExpression) String() string {
	var result string
	result = cte.Name.String()

	if len(cte.ColumnNames) > 0 {
		var colNames []string
		for _, col := range cte.ColumnNames {
			colNames = append(colNames, col.String())
		}
		result += fmt.Sprintf("(%s)", strings.Join(colNames, ", "))
	}

	result += fmt.Sprintf(" AS (%s)", cte.Query.String())
	return result
}

// WithClause represents a WITH clause in a SQL statement
type WithClause struct {
	Token       Token                    // The WITH token
	CTEs        []*CommonTableExpression // The CTEs defined in this WITH clause
	IsRecursive bool                     // Whether this is a recursive WITH
}

// String returns a string representation of the WITH clause
func (wc *WithClause) String() string {
	var result string

	result = "WITH "
	if wc.IsRecursive {
		result += "RECURSIVE "
	}

	var cteStrings []string
	for _, cte := range wc.CTEs {
		cteStrings = append(cteStrings, cte.String())
	}

	result += strings.Join(cteStrings, ", ")
	return result
}

// SelectStatement represents a SELECT statement
type SelectStatement struct {
	Token     Token // The SELECT token
	Distinct  bool
	Columns   []Expression
	With      *WithClause // WITH clause (Common Table Expressions)
	TableExpr Expression
	Where     Expression
	GroupBy   []Expression
	Having    Expression
	OrderBy   []OrderByExpression
	Limit     Expression
	Offset    Expression
}

func (ss *SelectStatement) statementNode()       {}
func (ss *SelectStatement) TokenLiteral() string { return ss.Token.Literal }
func (ss *SelectStatement) Position() Position   { return ss.Token.Position }
func (ss *SelectStatement) String() string {
	var result string

	// WITH clause
	if ss.With != nil {
		result = ss.With.String() + " "
	}

	result += "SELECT "

	if ss.Distinct {
		result += "DISTINCT "
	}

	// Columns
	for i, col := range ss.Columns {
		if i > 0 {
			result += ", "
		}
		result += col.String()
	}

	// FROM clause
	if ss.TableExpr != nil {
		result += " FROM " + ss.TableExpr.String()
	}

	// WHERE clause
	if ss.Where != nil {
		result += " WHERE " + ss.Where.String()
	}

	// GROUP BY clause
	if len(ss.GroupBy) > 0 {
		result += " GROUP BY "
		for i, expr := range ss.GroupBy {
			if i > 0 {
				result += ", "
			}
			result += expr.String()
		}
	}

	// HAVING clause
	if ss.Having != nil {
		result += " HAVING " + ss.Having.String()
	}

	// ORDER BY clause
	if len(ss.OrderBy) > 0 {
		result += " ORDER BY "
		for i, expr := range ss.OrderBy {
			if i > 0 {
				result += ", "
			}
			result += expr.String()
		}
	}

	// LIMIT clause
	if ss.Limit != nil {
		result += " LIMIT " + ss.Limit.String()
	}

	// OFFSET clause
	if ss.Offset != nil {
		result += " OFFSET " + ss.Offset.String()
	}

	return result
}

// OrderByExpression represents an order by expression
type OrderByExpression struct {
	Expression Expression
	Ascending  bool
}

func (o OrderByExpression) String() string {
	if o.Ascending {
		return fmt.Sprintf("%s ASC", o.Expression.String())
	}
	return fmt.Sprintf("%s DESC", o.Expression.String())
}

// TableSource represents a table source in a FROM clause
type TableSource interface {
	Expression
	tableSourceNode()
}

// SimpleTableSource represents a simple table name
type SimpleTableSource struct {
	Token Token // The table name token
	Name  *Identifier
	Alias *Identifier
}

func (sts *SimpleTableSource) expressionNode()      {}
func (sts *SimpleTableSource) tableSourceNode()     {}
func (sts *SimpleTableSource) TokenLiteral() string { return sts.Token.Literal }
func (sts *SimpleTableSource) Position() Position   { return sts.Token.Position }
func (sts *SimpleTableSource) String() string {
	if sts.Alias != nil {
		return fmt.Sprintf("%s AS %s", sts.Name.String(), sts.Alias.String())
	}
	return sts.Name.String()
}

// OnJoinCondition represents an ON condition for JOIN clause
type OnJoinCondition struct {
	Token Token // the ON token
	Expr  Expression
}

func (o *OnJoinCondition) expressionNode()      {}
func (o *OnJoinCondition) TokenLiteral() string { return o.Token.Literal }
func (o *OnJoinCondition) String() string       { return "ON " + o.Expr.String() }
func (o *OnJoinCondition) Position() Position   { return o.Token.Position }

// JoinTableSource represents a join between two tables
type JoinTableSource struct {
	Token     Token // The JOIN token
	Left      TableSource
	JoinType  string // INNER, LEFT, RIGHT, FULL, CROSS, etc.
	Right     TableSource
	Condition Expression // ON condition or USING columns
}

func (jts *JoinTableSource) expressionNode()      {}
func (jts *JoinTableSource) tableSourceNode()     {}
func (jts *JoinTableSource) TokenLiteral() string { return jts.Token.Literal }
func (jts *JoinTableSource) Position() Position   { return jts.Token.Position }
func (jts *JoinTableSource) String() string {
	var result string
	result = jts.Left.String()
	result += " " + jts.JoinType + " JOIN " + jts.Right.String()

	if jts.Condition != nil {
		result += " ON " + jts.Condition.String()
	}

	return result
}

// SubqueryTableSource represents a subquery in the FROM clause
type SubqueryTableSource struct {
	Token    Token // The opening parenthesis token
	Subquery *SelectStatement
	Alias    *Identifier
}

func (sts *SubqueryTableSource) expressionNode()      {}
func (sts *SubqueryTableSource) tableSourceNode()     {}
func (sts *SubqueryTableSource) TokenLiteral() string { return sts.Token.Literal }
func (sts *SubqueryTableSource) Position() Position   { return sts.Token.Position }
func (sts *SubqueryTableSource) String() string {
	var result string
	result = "(" + sts.Subquery.String() + ")"

	if sts.Alias != nil {
		result += " AS " + sts.Alias.String()
	}

	return result
}

// CTEReference represents a reference to a Common Table Expression
type CTEReference struct {
	Token Token       // The CTE name token
	Name  *Identifier // The CTE name (from the WITH clause)
	Alias *Identifier // Optional alias
}

func (cte *CTEReference) expressionNode()      {}
func (cte *CTEReference) tableSourceNode()     {}
func (cte *CTEReference) TokenLiteral() string { return cte.Token.Literal }
func (cte *CTEReference) Position() Position   { return cte.Token.Position }
func (cte *CTEReference) String() string {
	var result string
	result = cte.Name.String()

	if cte.Alias != nil {
		result += " AS " + cte.Alias.String()
	}

	return result
}

// AliasedExpression represents an expression with an alias
type AliasedExpression struct {
	Token      Token // The AS token or the expression token
	Expression Expression
	Alias      *Identifier
}

func (ae *AliasedExpression) expressionNode()      {}
func (ae *AliasedExpression) TokenLiteral() string { return ae.Token.Literal }
func (ae *AliasedExpression) String() string {
	if ae.Alias != nil {
		return fmt.Sprintf("%s AS %s", ae.Expression.String(), ae.Alias.String())
	}
	return ae.Expression.String()
}
func (ae *AliasedExpression) Position() Position { return ae.Token.Position }

// Other statement types

// CreateTableStatement represents a CREATE TABLE statement
type CreateTableStatement struct {
	Token       Token // The CREATE token
	TableName   *Identifier
	IfNotExists bool
	Columns     []ColumnDefinition
}

func (cts *CreateTableStatement) statementNode()       {}
func (cts *CreateTableStatement) TokenLiteral() string { return cts.Token.Literal }
func (cts *CreateTableStatement) Position() Position   { return cts.Token.Position }
func (cts *CreateTableStatement) String() string {
	var result string
	result = "CREATE TABLE "

	if cts.IfNotExists {
		result += "IF NOT EXISTS "
	}

	result += cts.TableName.String() + " ("

	for i, col := range cts.Columns {
		if i > 0 {
			result += ", "
		}
		result += col.String()
	}

	result += ")"
	return result
}

// ColumnDefinition represents a column definition in a CREATE TABLE statement
type ColumnDefinition struct {
	Name        *Identifier
	Type        string
	Constraints []ColumnConstraint
}

func (cd ColumnDefinition) String() string {
	var result string
	result = cd.Name.String() + " " + cd.Type

	for _, constraint := range cd.Constraints {
		result += " " + constraint.String()
	}

	return result
}

// ColumnConstraint represents a constraint on a column
type ColumnConstraint interface {
	String() string
}

// NotNullConstraint represents a NOT NULL constraint
type NotNullConstraint struct {
	Token Token // The NOT token
}

func (nnc *NotNullConstraint) String() string {
	return "NOT NULL"
}

// PrimaryKeyConstraint represents a PRIMARY KEY constraint
type PrimaryKeyConstraint struct {
	Token Token // The PRIMARY token
}

func (pkc *PrimaryKeyConstraint) String() string {
	return "PRIMARY KEY"
}

// DropTableStatement represents a DROP TABLE statement
type DropTableStatement struct {
	Token     Token // The DROP token
	TableName *Identifier
	IfExists  bool
}

func (dts *DropTableStatement) statementNode()       {}
func (dts *DropTableStatement) TokenLiteral() string { return dts.Token.Literal }
func (dts *DropTableStatement) Position() Position   { return dts.Token.Position }
func (dts *DropTableStatement) String() string {
	var result string
	result = "DROP TABLE "

	if dts.IfExists {
		result += "IF EXISTS "
	}

	result += dts.TableName.String()
	return result
}

// InsertStatement represents an INSERT statement
type InsertStatement struct {
	Token             Token // The INSERT token
	TableName         *Identifier
	Columns           []*Identifier
	Values            [][]Expression
	OnDuplicate       bool          // Whether there is an ON DUPLICATE KEY UPDATE clause
	UpdateColumns     []*Identifier // Columns to update on duplicate key
	UpdateExpressions []Expression  // Expressions for the values to update
}

func (is *InsertStatement) statementNode()       {}
func (is *InsertStatement) TokenLiteral() string { return is.Token.Literal }
func (is *InsertStatement) Position() Position   { return is.Token.Position }
func (is *InsertStatement) String() string {
	var result string
	result = "INSERT INTO " + is.TableName.String()

	if len(is.Columns) > 0 {
		result += " ("
		for i, col := range is.Columns {
			if i > 0 {
				result += ", "
			}
			result += col.String()
		}
		result += ")"
	}

	result += " VALUES "
	for i, row := range is.Values {
		if i > 0 {
			result += ", "
		}
		result += "("
		for j, expr := range row {
			if j > 0 {
				result += ", "
			}
			result += expr.String()
		}
		result += ")"
	}

	if is.OnDuplicate {
		result += " ON DUPLICATE KEY UPDATE "
		for i := range is.UpdateColumns {
			if i > 0 {
				result += ", "
			}
			result += is.UpdateColumns[i].String() + " = " + is.UpdateExpressions[i].String()
		}
	}

	return result
}

// MergeAction represents the type of action in a MERGE statement
type MergeAction int

const (
	// ActionUpdate represents an UPDATE action in a MERGE statement
	ActionUpdate MergeAction = iota
	// ActionInsert represents an INSERT action in a MERGE statement
	ActionInsert
	// ActionDelete represents a DELETE action in a MERGE statement
	ActionDelete
)

// MergeClause represents a WHEN [NOT] MATCHED THEN ... clause in a MERGE statement
type MergeClause struct {
	IsMatched   bool                  // true if WHEN MATCHED, false if WHEN NOT MATCHED
	Condition   Expression            // Optional AND condition
	Action      MergeAction           // UPDATE, INSERT, or DELETE
	Assignments map[string]Expression // For UPDATE action
	Columns     []*Identifier         // For INSERT action
	Values      []Expression          // For INSERT action
}

func (mc *MergeClause) String() string {
	var result string
	if mc.IsMatched {
		result = "WHEN MATCHED"
	} else {
		result = "WHEN NOT MATCHED"
	}

	if mc.Condition != nil {
		result += " AND " + mc.Condition.String()
	}

	result += " THEN "

	switch mc.Action {
	case ActionUpdate:
		result += "UPDATE SET "
		i := 0
		for col, val := range mc.Assignments {
			if i > 0 {
				result += ", "
			}
			result += col + " = " + val.String()
			i++
		}
	case ActionInsert:
		result += "INSERT "
		if len(mc.Columns) > 0 {
			result += "("
			for i, col := range mc.Columns {
				if i > 0 {
					result += ", "
				}
				result += col.String()
			}
			result += ") "
		}
		result += "VALUES ("
		for i, val := range mc.Values {
			if i > 0 {
				result += ", "
			}
			result += val.String()
		}
		result += ")"
	case ActionDelete:
		result += "DELETE"
	}

	return result
}

// MergeStatement represents a MERGE statement
type MergeStatement struct {
	Token       Token // The MERGE token
	TargetTable *Identifier
	TargetAlias *Identifier
	SourceTable Expression // Could be a table name or a subquery
	SourceAlias *Identifier
	OnCondition Expression
	Clauses     []*MergeClause
}

func (ms *MergeStatement) statementNode()       {}
func (ms *MergeStatement) TokenLiteral() string { return ms.Token.Literal }
func (ms *MergeStatement) Position() Position   { return ms.Token.Position }
func (ms *MergeStatement) String() string {
	var result string
	result = "MERGE INTO " + ms.TargetTable.String()

	if ms.TargetAlias != nil {
		result += " AS " + ms.TargetAlias.String()
	}

	result += " USING " + ms.SourceTable.String()

	if ms.SourceAlias != nil {
		result += " AS " + ms.SourceAlias.String()
	}

	result += " ON " + ms.OnCondition.String()

	for _, clause := range ms.Clauses {
		result += " " + clause.String()
	}

	return result
}

// CreateIndexStatement represents a CREATE INDEX statement
type CreateIndexStatement struct {
	Token       Token // The CREATE token
	IndexName   *Identifier
	TableName   *Identifier
	Columns     []*Identifier
	IsUnique    bool
	IfNotExists bool
}

func (cis *CreateIndexStatement) statementNode()       {}
func (cis *CreateIndexStatement) TokenLiteral() string { return cis.Token.Literal }
func (cis *CreateIndexStatement) Position() Position   { return cis.Token.Position }
func (cis *CreateIndexStatement) String() string {
	var result string
	if cis.IsUnique {
		result = "CREATE UNIQUE INDEX "
	} else {
		result = "CREATE INDEX "
	}

	if cis.IfNotExists {
		result += "IF NOT EXISTS "
	}

	result += cis.IndexName.String() + " ON " + cis.TableName.String() + " ("

	for i, col := range cis.Columns {
		if i > 0 {
			result += ", "
		}
		result += col.String()
	}

	result += ")"
	return result
}

// DropIndexStatement represents a DROP INDEX statement
type DropIndexStatement struct {
	Token     Token // The DROP token
	IndexName *Identifier
	TableName *Identifier // Optional, as some SQL dialects require it
	IfExists  bool
}

func (dis *DropIndexStatement) statementNode()       {}
func (dis *DropIndexStatement) TokenLiteral() string { return dis.Token.Literal }
func (dis *DropIndexStatement) Position() Position   { return dis.Token.Position }
func (dis *DropIndexStatement) String() string {
	var result string
	result = "DROP INDEX "

	if dis.IfExists {
		result += "IF EXISTS "
	}

	result += dis.IndexName.String()

	if dis.TableName != nil {
		result += " ON " + dis.TableName.String()
	}

	return result
}

// Transaction statements

// BeginStatement represents a BEGIN TRANSACTION statement
type BeginStatement struct {
	Token          Token  // The BEGIN token
	IsolationLevel string // Optional isolation level
}

func (bs *BeginStatement) statementNode()       {}
func (bs *BeginStatement) TokenLiteral() string { return bs.Token.Literal }
func (bs *BeginStatement) Position() Position   { return bs.Token.Position }
func (bs *BeginStatement) String() string {
	result := "BEGIN TRANSACTION"
	if bs.IsolationLevel != "" {
		result += " ISOLATION LEVEL " + bs.IsolationLevel
	}
	return result
}

// CommitStatement represents a COMMIT statement
type CommitStatement struct {
	Token Token // The COMMIT token
}

func (cs *CommitStatement) statementNode()       {}
func (cs *CommitStatement) TokenLiteral() string { return cs.Token.Literal }
func (cs *CommitStatement) Position() Position   { return cs.Token.Position }
func (cs *CommitStatement) String() string       { return "COMMIT" }

// RollbackStatement represents a ROLLBACK statement
type RollbackStatement struct {
	Token Token // The ROLLBACK token
	// Note: This could be extended to support ROLLBACK TO SAVEPOINT
	SavepointName *Identifier // Optional savepoint name (nil if not specified)
}

func (rs *RollbackStatement) statementNode()       {}
func (rs *RollbackStatement) TokenLiteral() string { return rs.Token.Literal }
func (rs *RollbackStatement) Position() Position   { return rs.Token.Position }
func (rs *RollbackStatement) String() string {
	if rs.SavepointName != nil {
		return "ROLLBACK TO SAVEPOINT " + rs.SavepointName.String()
	}
	return "ROLLBACK"
}

// SavepointStatement represents a SAVEPOINT statement
type SavepointStatement struct {
	Token         Token       // The SAVEPOINT token
	SavepointName *Identifier // Name of the savepoint
}

func (ss *SavepointStatement) statementNode()       {}
func (ss *SavepointStatement) TokenLiteral() string { return ss.Token.Literal }
func (ss *SavepointStatement) Position() Position   { return ss.Token.Position }
func (ss *SavepointStatement) String() string {
	return "SAVEPOINT " + ss.SavepointName.String()
}

// CreateColumnarIndexStatement represents a CREATE COLUMNAR INDEX statement
type CreateColumnarIndexStatement struct {
	Token       Token // The CREATE token
	TableName   *Identifier
	ColumnName  *Identifier
	IfNotExists bool
	IsUnique    bool // Whether this is a unique index
}

func (ccis *CreateColumnarIndexStatement) statementNode()       {}
func (ccis *CreateColumnarIndexStatement) TokenLiteral() string { return ccis.Token.Literal }
func (ccis *CreateColumnarIndexStatement) Position() Position   { return ccis.Token.Position }
func (ccis *CreateColumnarIndexStatement) String() string {
	var result string
	result = "CREATE "

	if ccis.IsUnique {
		result += "UNIQUE "
	}

	result += "COLUMNAR INDEX "

	if ccis.IfNotExists {
		result += "IF NOT EXISTS "
	}

	result += "ON " + ccis.TableName.String() + " (" + ccis.ColumnName.String() + ")"
	return result
}

// DropColumnarIndexStatement represents a DROP COLUMNAR INDEX statement
type DropColumnarIndexStatement struct {
	Token      Token // The DROP token
	TableName  *Identifier
	ColumnName *Identifier
	IfExists   bool
}

func (dcis *DropColumnarIndexStatement) statementNode()       {}
func (dcis *DropColumnarIndexStatement) TokenLiteral() string { return dcis.Token.Literal }
func (dcis *DropColumnarIndexStatement) Position() Position   { return dcis.Token.Position }
func (dcis *DropColumnarIndexStatement) String() string {
	var result string
	result = "DROP COLUMNAR INDEX "

	if dcis.IfExists {
		result += "IF EXISTS "
	}

	result += "ON " + dcis.TableName.String() + " (" + dcis.ColumnName.String() + ")"
	return result
}

// ViewStatement represents a CREATE VIEW statement
type CreateViewStatement struct {
	Token       Token            // The CREATE token
	ViewName    *Identifier      // Name of the view
	Query       *SelectStatement // The SELECT statement that defines the view
	IfNotExists bool
}

func (cvs *CreateViewStatement) statementNode()       {}
func (cvs *CreateViewStatement) TokenLiteral() string { return cvs.Token.Literal }
func (cvs *CreateViewStatement) Position() Position   { return cvs.Token.Position }
func (cvs *CreateViewStatement) String() string {
	var result string
	result = "CREATE VIEW "

	if cvs.IfNotExists {
		result += "IF NOT EXISTS "
	}

	result += cvs.ViewName.String() + " AS " + cvs.Query.String()
	return result
}

// DropViewStatement represents a DROP VIEW statement
type DropViewStatement struct {
	Token    Token       // The DROP token
	ViewName *Identifier // Name of the view
	IfExists bool
}

func (dvs *DropViewStatement) statementNode()       {}
func (dvs *DropViewStatement) TokenLiteral() string { return dvs.Token.Literal }
func (dvs *DropViewStatement) Position() Position   { return dvs.Token.Position }
func (dvs *DropViewStatement) String() string {
	var result string
	result = "DROP VIEW "

	if dvs.IfExists {
		result += "IF EXISTS "
	}

	result += dvs.ViewName.String()
	return result
}

// SetStatement represents a SET statement for session variables
type SetStatement struct {
	Token Token       // The SET token
	Name  *Identifier // The variable name to set
	Value Expression  // The value to set it to
}

func (ss *SetStatement) statementNode()       {}
func (ss *SetStatement) TokenLiteral() string { return ss.Token.Literal }
func (ss *SetStatement) Position() Position   { return ss.Token.Position }
func (ss *SetStatement) String() string {
	return fmt.Sprintf("SET %s = %s", ss.Name.String(), ss.Value.String())
}

// PragmaStatement represents a PRAGMA statement for database settings
type PragmaStatement struct {
	Token Token       // The PRAGMA token
	Name  *Identifier // The setting name to get or set
	Value Expression  // The value to set it to (nil for read-only pragma)
}

func (ps *PragmaStatement) statementNode()       {}
func (ps *PragmaStatement) TokenLiteral() string { return ps.Token.Literal }
func (ps *PragmaStatement) Position() Position   { return ps.Token.Position }
func (ps *PragmaStatement) String() string {
	if ps.Value != nil {
		return fmt.Sprintf("PRAGMA %s = %s", ps.Name.String(), ps.Value.String())
	}
	return fmt.Sprintf("PRAGMA %s", ps.Name.String())
}

// ExpressionStatement represents a standalone expression used as a statement
type ExpressionStatement struct {
	Token      Token // The first token of the expression
	Expression Expression
}

func (es *ExpressionStatement) statementNode()       {}
func (es *ExpressionStatement) TokenLiteral() string { return es.Token.Literal }
func (es *ExpressionStatement) Position() Position   { return es.Token.Position }
func (es *ExpressionStatement) String() string {
	if es.Expression != nil {
		return es.Expression.String()
	}
	return ""
}

// ShowTablesStatement represents a SHOW TABLES statement
type ShowTablesStatement struct {
	Token Token // The SHOW token
}

func (sts *ShowTablesStatement) statementNode()       {}
func (sts *ShowTablesStatement) TokenLiteral() string { return sts.Token.Literal }
func (sts *ShowTablesStatement) Position() Position   { return sts.Token.Position }
func (sts *ShowTablesStatement) String() string       { return "SHOW TABLES" }

// ShowCreateTableStatement represents a SHOW CREATE TABLE statement
type ShowCreateTableStatement struct {
	Token     Token       // The SHOW token
	TableName *Identifier // Name of the table
}

func (sct *ShowCreateTableStatement) statementNode()       {}
func (sct *ShowCreateTableStatement) TokenLiteral() string { return sct.Token.Literal }
func (sct *ShowCreateTableStatement) Position() Position   { return sct.Token.Position }
func (sct *ShowCreateTableStatement) String() string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", sct.TableName.String())
}

// ShowIndexesStatement represents a SHOW INDEXES FROM statement
type ShowIndexesStatement struct {
	Token     Token       // The SHOW token
	TableName *Identifier // Name of the table
}

func (sis *ShowIndexesStatement) statementNode()       {}
func (sis *ShowIndexesStatement) TokenLiteral() string { return sis.Token.Literal }
func (sis *ShowIndexesStatement) Position() Position   { return sis.Token.Position }
func (sis *ShowIndexesStatement) String() string {
	return fmt.Sprintf("SHOW INDEXES FROM %s", sis.TableName.String())
}
