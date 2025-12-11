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

// parameterInfo holds information about a parameter for testing purposes
type parameterInfo struct {
	Name             string
	Index            int
	Location         string
	StatementID      int
	OrderInStatement int
}

// extractParametersFromNode recursively extracts parameters from an AST node
func extractParametersFromNode(node parser.Node) []*parser.Parameter {
	var parameters []*parser.Parameter

	switch n := node.(type) {
	case *parser.Parameter:
		// Found a parameter
		parameters = append(parameters, n)
	case *parser.InfixExpression:
		// Check both sides of the expression
		if n.Left != nil {
			parameters = append(parameters, extractParametersFromNode(n.Left)...)
		}
		if n.Right != nil {
			parameters = append(parameters, extractParametersFromNode(n.Right)...)
		}
	case *parser.PrefixExpression:
		// Check the right side
		if n.Right != nil {
			parameters = append(parameters, extractParametersFromNode(n.Right)...)
		}
	case *parser.FunctionCall:
		// Check all arguments
		for _, arg := range n.Arguments {
			parameters = append(parameters, extractParametersFromNode(arg)...)
		}
	case *parser.AliasedExpression:
		// Check the expression
		if n.Expression != nil {
			parameters = append(parameters, extractParametersFromNode(n.Expression)...)
		}
	case *parser.ListExpression:
		// Check all elements
		for _, elem := range n.Elements {
			parameters = append(parameters, extractParametersFromNode(elem)...)
		}
	case *parser.SelectStatement:
		// Check all parts of the SELECT statement
		for _, col := range n.Columns {
			parameters = append(parameters, extractParametersFromNode(col)...)
		}
		if n.Where != nil {
			parameters = append(parameters, extractParametersFromNode(n.Where)...)
		}
		for _, group := range n.GroupBy {
			parameters = append(parameters, extractParametersFromNode(group)...)
		}
		if n.Having != nil {
			parameters = append(parameters, extractParametersFromNode(n.Having)...)
		}
		for _, order := range n.OrderBy {
			parameters = append(parameters, extractParametersFromNode(order.Expression)...)
		}
		if n.Limit != nil {
			parameters = append(parameters, extractParametersFromNode(n.Limit)...)
		}
		if n.Offset != nil {
			parameters = append(parameters, extractParametersFromNode(n.Offset)...)
		}
	case *parser.InsertStatement:
		// Check all values
		for _, row := range n.Values {
			for _, val := range row {
				parameters = append(parameters, extractParametersFromNode(val)...)
			}
		}
	case *parser.UpdateStatement:
		// Check all updates
		for _, expr := range n.Updates {
			parameters = append(parameters, extractParametersFromNode(expr)...)
		}
		if n.Where != nil {
			parameters = append(parameters, extractParametersFromNode(n.Where)...)
		}
	case *parser.DeleteStatement:
		// Check WHERE clause
		if n.Where != nil {
			parameters = append(parameters, extractParametersFromNode(n.Where)...)
		}
	}

	return parameters
}

// extractParameters extracts parameter information from a parsed program
func extractParameters(program *parser.Program) []parameterInfo {
	var result []parameterInfo

	// Extract all parameters
	for _, stmt := range program.Statements {
		params := extractParametersFromNode(stmt)
		for _, p := range params {
			info := parameterInfo{
				Name:             p.Name,
				Index:            p.Index,
				Location:         p.Location,
				StatementID:      p.StatementID,
				OrderInStatement: p.OrderInStatement,
			}
			result = append(result, info)
		}
	}

	return result
}

// Test with a simple SELECT statement
func TestParameterLocationSelectSimple(t *testing.T) {
	query := "SELECT * FROM users WHERE id = ? AND name = ?"

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 2 {
		t.Errorf("Expected 2 parameters, got %d", len(params))
	}

	// Verify parameters have the correct location
	for _, param := range params {
		if param.Location != "WHERE" {
			t.Errorf("Expected parameter location to be 'WHERE', got '%s'", param.Location)
		}
	}
}

// Test with a more complex SELECT statement
func TestParameterLocationSelectComplex(t *testing.T) {
	query := `
		SELECT * FROM users 
		WHERE age > ? AND salary < ? 
		ORDER BY last_login LIMIT ? OFFSET ?`

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 4 {
		t.Errorf("Expected 4 parameters, got %d", len(params))
	}

	// Verify parameters in different clauses have different locations
	locations := make(map[string]int)
	for _, param := range params {
		locations[param.Location]++
	}

	if locations["WHERE"] != 2 {
		t.Errorf("Expected 2 parameters in WHERE, got %d", locations["WHERE"])
	}
	if locations["LIMIT"] != 1 {
		t.Errorf("Expected 1 parameter in LIMIT, got %d", locations["LIMIT"])
	}
	if locations["OFFSET"] != 1 {
		t.Errorf("Expected 1 parameter in OFFSET, got %d", locations["OFFSET"])
	}
}

// Test with an INSERT statement
func TestParameterLocationInsert(t *testing.T) {
	query := "INSERT INTO users (name, age, email) VALUES (?, ?, ?)"

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 3 {
		t.Errorf("Expected 3 parameters, got %d", len(params))
	}

	// Verify all parameters are in the VALUES clause
	for _, param := range params {
		if param.Location != "INSERT VALUES" {
			t.Errorf("Expected parameter location to be 'INSERT VALUES', got '%s'", param.Location)
		}
	}
}

// Test with an UPDATE statement
func TestParameterLocationUpdate(t *testing.T) {
	query := "UPDATE users SET name = ?, age = ? WHERE id = ?"

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 3 {
		t.Errorf("Expected 3 parameters, got %d", len(params))
	}

	// Verify parameters in different clauses have different locations
	locations := make(map[string]int)
	for _, param := range params {
		locations[param.Location]++
	}

	if locations["UPDATE SET"] != 2 {
		t.Errorf("Expected 2 parameters in UPDATE SET, got %d", locations["UPDATE SET"])
	}
	if locations["WHERE"] != 1 {
		t.Errorf("Expected 1 parameter in WHERE, got %d", locations["WHERE"])
	}
}

// Test with multiple statements
func TestParameterLocationMultipleStatements(t *testing.T) {
	query := `
		INSERT INTO logs (user_id, action) VALUES (?, ?);
		UPDATE users SET last_login = ? WHERE id = ?;
	`

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 4 {
		t.Errorf("Expected 4 parameters, got %d", len(params))
	}

	// Verify parameters have different statement IDs
	statementIDs := make(map[int]bool)
	for _, param := range params {
		statementIDs[param.StatementID] = true
	}

	if len(statementIDs) != 2 {
		t.Errorf("Expected parameters to belong to 2 different statements, got %d", len(statementIDs))
	}
}

// Test ordering within statements
func TestParameterOrderInStatement(t *testing.T) {
	query := `
		SELECT * FROM products 
		WHERE category = ? AND price > ? AND stock > ? 
		ORDER BY name LIMIT ? OFFSET ?
	`

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Get parameters in WHERE clause
	var whereParams []parameterInfo
	for _, param := range params {
		if param.Location == "WHERE" {
			whereParams = append(whereParams, param)
		}
	}

	// Verify parameters in WHERE clause are correctly ordered
	if len(whereParams) != 3 {
		t.Errorf("Expected 3 parameters in WHERE clause, got %d", len(whereParams))
	} else {
		// Check if OrderInStatement is increasing
		for i := 1; i < len(whereParams); i++ {
			if whereParams[i].OrderInStatement <= whereParams[i-1].OrderInStatement {
				t.Errorf("Expected increasing OrderInStatement values in WHERE clause")
			}
		}
	}
}

// Test function with parameters
func TestParameterInFunction(t *testing.T) {
	query := "SELECT * FROM users WHERE SUBSTRING(name, ?, ?) = ?"

	l := parser.NewLexer(query)
	p := parser.NewParser(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Errorf("parser has %d errors for input %s", len(p.Errors()), query)
		for _, err := range p.Errors() {
			t.Errorf("parser error: %s", err)
		}
		return
	}

	// Extract parameter information
	params := extractParameters(program)

	// Print parameters for debugging
	for i, param := range params {
		t.Logf("Parameter %d: Name=%s, Index=%d, Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Name, param.Index, param.Location, param.StatementID, param.OrderInStatement)
	}

	// Verify parameter count
	if len(params) != 3 {
		t.Errorf("Expected 3 parameters, got %d", len(params))
	}

	// Verify all parameters are in the WHERE clause
	for _, param := range params {
		if param.Location != "WHERE" {
			t.Errorf("Expected parameter location to be 'WHERE', got '%s'", param.Location)
		}
	}
}
