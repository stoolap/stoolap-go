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

func TestUpdateParameters(t *testing.T) {
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

	// Print the statement as parsed
	t.Logf("Parsed statement: %s", program.Statements[0].String())

	// Extract all parameters with their location information
	var params []*parser.Parameter
	extractParams(program.Statements[0], &params)

	for i, param := range params {
		t.Logf("Parameter %d: Location=%s, StatementID=%d, OrderInStatement=%d",
			i+1, param.Location, param.StatementID, param.OrderInStatement)
	}
}

func extractParams(node parser.Node, params *[]*parser.Parameter) {
	switch n := node.(type) {
	case *parser.Parameter:
		*params = append(*params, n)
	case *parser.UpdateStatement:
		// Check the Updates map
		for _, expr := range n.Updates {
			extractParams(expr, params)
		}
		// Check the WHERE clause
		if n.Where != nil {
			extractParams(n.Where, params)
		}
	case *parser.InfixExpression:
		if n.Left != nil {
			extractParams(n.Left, params)
		}
		if n.Right != nil {
			extractParams(n.Right, params)
		}
	case *parser.PrefixExpression:
		if n.Right != nil {
			extractParams(n.Right, params)
		}
	}
}
