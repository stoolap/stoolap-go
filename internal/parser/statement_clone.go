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
	"github.com/stoolap/stoolap-go/internal/parser/funcregistry"
)

// Cloner is an interface that allows a Node to be cloned
type Cloner interface {
	Clone() Node
}

// Clone creates a minimal copy of a Statement where only Parameter nodes are duplicated.
// This is crucial for the query cache to avoid shared statement modifications.
// Without cloning Parameters, statement objects would be modified in-place when binding parameters,
// causing unexpected behavior when the same statement object is reused across different query executions.
// All other AST nodes are immutable and are shared between the original and cloned statements.
func Clone(stmt Statement) Statement {
	if stmt == nil {
		return nil
	}

	// Check if the statement implements the Cloner interface
	if cloner, ok := stmt.(Cloner); ok {
		return cloner.Clone().(Statement)
	}

	// If not, use type-specific cloning
	switch s := stmt.(type) {
	case *SelectStatement:
		return cloneSelectStatement(s)
	case *InsertStatement:
		return cloneInsertStatement(s)
	case *UpdateStatement:
		return cloneUpdateStatement(s)
	case *DeleteStatement:
		return cloneDeleteStatement(s)
	case *CreateTableStatement:
		return cloneCreateTableStatement(s)
	case *DropTableStatement:
		return cloneDropTableStatement(s)
	case *AlterTableStatement:
		return cloneAlterTableStatement(s)
	case *CreateIndexStatement:
		return cloneCreateIndexStatement(s)
	case *DropIndexStatement:
		return cloneDropIndexStatement(s)
	case *CreateViewStatement:
		return cloneCreateViewStatement(s)
	case *DropViewStatement:
		return cloneDropViewStatement(s)
	case *BeginStatement:
		return cloneBeginStatement(s)
	case *CommitStatement:
		return cloneCommitStatement(s)
	case *RollbackStatement:
		return cloneRollbackStatement(s)
	case *SavepointStatement:
		return cloneSavepointStatement(s)
	case *MergeStatement:
		return cloneMergeStatement(s)
	case *SetStatement:
		return cloneSetStatement(s)
	case *ExpressionStatement:
		return cloneExpressionStatement(s)
	default:
		// If we don't have a specific clone method for this statement type,
		// return the original. This is safer than returning nil.
		return stmt
	}
}

// CloneExpression creates a deep copy of an Expression
func CloneExpression(expr Expression) Expression {
	if expr == nil {
		return nil
	}

	// Check if the expression implements the Cloner interface
	if cloner, ok := expr.(Cloner); ok {
		return cloner.Clone().(Expression)
	}

	// If not, use type-specific cloning
	switch e := expr.(type) {
	case *Identifier:
		return cloneIdentifier(e)
	case *QualifiedIdentifier:
		return cloneQualifiedIdentifier(e)
	case *IntegerLiteral:
		return cloneIntegerLiteral(e)
	case *FloatLiteral:
		return cloneFloatLiteral(e)
	case *StringLiteral:
		return cloneStringLiteral(e)
	case *BooleanLiteral:
		return cloneBooleanLiteral(e)
	case *NullLiteral:
		return cloneNullLiteral(e)
	case *Parameter:
		return cloneParameter(e)
	case *PrefixExpression:
		return clonePrefixExpression(e)
	case *InfixExpression:
		return cloneInfixExpression(e)
	case *ListExpression:
		return cloneListExpression(e)
	case *DistinctExpression:
		return cloneDistinctExpression(e)
	case *ExistsExpression:
		return cloneExistsExpression(e)
	case *InExpression:
		return cloneInExpression(e)
	case *BetweenExpression:
		return cloneBetweenExpression(e)
	case *ScalarSubquery:
		return cloneScalarSubquery(e)
	case *ExpressionList:
		return cloneExpressionList(e)
	case *CaseExpression:
		return cloneCaseExpression(e)
	case *CastExpression:
		return cloneCastExpression(e)
	case *FunctionCall:
		return cloneFunctionCall(e)
	case *AliasedExpression:
		return cloneAliasedExpression(e)
	case *SimpleTableSource:
		return cloneSimpleTableSource(e)
	case *JoinTableSource:
		return cloneJoinTableSource(e)
	case *SubqueryTableSource:
		return cloneSubqueryTableSource(e)
	case *CTEReference:
		return cloneCTEReference(e)
	case *OnJoinCondition:
		return cloneOnJoinCondition(e)
	default:
		// If we don't have a specific clone method for this expression type,
		// return the original. This is safer than returning nil.
		return expr
	}
}

// Clone implementations for basic types

// Token is immutable, so we don't need to clone it
func cloneToken(t Token) Token {
	// All fields in Token are immutable (int, string, struct) so we can reuse them
	return t
}

// Identifier is immutable, so we can return the original
func cloneIdentifier(i *Identifier) *Identifier {
	if i == nil {
		return nil
	}
	// Since Identifier is immutable, we can just return the original
	return i
}

// QualifiedIdentifier is immutable, so we can return the original
func cloneQualifiedIdentifier(qi *QualifiedIdentifier) *QualifiedIdentifier {
	if qi == nil {
		return nil
	}
	// Since QualifiedIdentifier is immutable, we can just return the original
	return qi
}

// IntegerLiteral is immutable, so we can return the original
func cloneIntegerLiteral(il *IntegerLiteral) *IntegerLiteral {
	if il == nil {
		return nil
	}
	// Since IntegerLiteral is immutable, we can just return the original
	return il
}

// FloatLiteral is immutable, so we can return the original
func cloneFloatLiteral(fl *FloatLiteral) *FloatLiteral {
	if fl == nil {
		return nil
	}
	// Since FloatLiteral is immutable, we can just return the original
	return fl
}

// StringLiteral is immutable, so we can return the original
func cloneStringLiteral(sl *StringLiteral) *StringLiteral {
	if sl == nil {
		return nil
	}
	// Since StringLiteral is immutable, we can just return the original
	return sl
}

// BooleanLiteral is immutable, so we can return the original
func cloneBooleanLiteral(bl *BooleanLiteral) *BooleanLiteral {
	if bl == nil {
		return nil
	}
	// Since BooleanLiteral is immutable, we can just return the original
	return bl
}

// NullLiteral is immutable, so we can return the original
func cloneNullLiteral(nl *NullLiteral) *NullLiteral {
	if nl == nil {
		return nil
	}
	// Since NullLiteral is immutable, we can just return the original
	return nl
}

// cloneParameter creates a new Parameter object (immutable approach)
func cloneParameter(p *Parameter) *Parameter {
	if p == nil {
		return nil
	}

	// Create a new Parameter directly
	return &Parameter{
		Token:            cloneToken(p.Token),
		Name:             p.Name,
		Index:            p.Index,
		Location:         p.Location,
		StatementID:      p.StatementID,
		OrderInStatement: p.OrderInStatement,
	}
}

// Clone implementations for complex expressions

func clonePrefixExpression(pe *PrefixExpression) *PrefixExpression {
	if pe == nil {
		return nil
	}

	// Only clone if the right expression contains a Parameter
	// All other expression types are immutable
	right := pe.Right
	if containsParameter(right) {
		right = CloneExpression(pe.Right)
	}

	// If no parameters present, return original
	if right == pe.Right {
		return pe
	}

	// If parameters present, create a new expression with the cloned right side
	return &PrefixExpression{
		Token:    pe.Token,
		Operator: pe.Operator,
		Right:    right,
	}
}

func cloneInfixExpression(ie *InfixExpression) *InfixExpression {
	if ie == nil {
		return nil
	}

	// Only clone the sides if they contain Parameters
	// All other expression types are immutable
	left := ie.Left
	right := ie.Right

	if containsParameter(left) {
		left = CloneExpression(ie.Left)
	}

	if containsParameter(right) {
		right = CloneExpression(ie.Right)
	}

	// If no parameters present, return original
	if left == ie.Left && right == ie.Right {
		return ie
	}

	// If parameters present, create a new expression with the cloned sides
	return &InfixExpression{
		Token:    ie.Token,
		Left:     left,
		Operator: ie.Operator,
		Right:    right,
	}
}

// containsParameter checks if an expression or its children contain a Parameter node
func containsParameter(expr Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *Parameter:
		return true
	case *PrefixExpression:
		return containsParameter(e.Right)
	case *InfixExpression:
		return containsParameter(e.Left) || containsParameter(e.Right)
	case *ListExpression:
		for _, elem := range e.Elements {
			if containsParameter(elem) {
				return true
			}
		}
	case *FunctionCall:
		for _, arg := range e.Arguments {
			if containsParameter(arg) {
				return true
			}
		}
	case *AliasedExpression:
		return containsParameter(e.Expression)
	}

	return false
}

func cloneListExpression(le *ListExpression) *ListExpression {
	if le == nil {
		return nil
	}

	// Check if any element contains parameters
	hasParameters := false
	for _, e := range le.Elements {
		if containsParameter(e) {
			hasParameters = true
			break
		}
	}

	// If no parameters, return original
	if !hasParameters {
		return le
	}

	// Clone only if parameters are present
	elements := make([]Expression, len(le.Elements))
	for i, e := range le.Elements {
		if containsParameter(e) {
			elements[i] = CloneExpression(e)
		} else {
			elements[i] = e
		}
	}

	return &ListExpression{
		Token:    le.Token,
		Elements: elements,
	}
}

func cloneDistinctExpression(de *DistinctExpression) *DistinctExpression {
	if de == nil {
		return nil
	}

	// Only clone if the expression contains parameters
	if !containsParameter(de.Expr) {
		return de
	}

	return &DistinctExpression{
		Token: de.Token,
		Expr:  CloneExpression(de.Expr),
	}
}

func cloneExistsExpression(ee *ExistsExpression) *ExistsExpression {
	if ee == nil {
		return nil
	}

	// Check if subquery contains parameters
	hasParameters := false

	// Since subqueries can be complex, we need to check if there are parameters
	// in any part of the SELECT statement (WHERE, columns, etc.)
	if ee.Subquery != nil {
		// Check WHERE clause
		if containsParameter(ee.Subquery.Where) {
			hasParameters = true
		}

		// Check columns
		if !hasParameters {
			for _, col := range ee.Subquery.Columns {
				if containsParameter(col) {
					hasParameters = true
					break
				}
			}
		}

		// Check table expression
		if !hasParameters && containsParameter(ee.Subquery.TableExpr) {
			hasParameters = true
		}
	}

	// If no parameters, return original
	if !hasParameters {
		return ee
	}

	return &ExistsExpression{
		Token:    ee.Token,
		Subquery: cloneSelectStatement(ee.Subquery),
	}
}

func cloneInExpression(ie *InExpression) *InExpression {
	if ie == nil {
		return nil
	}
	return &InExpression{
		Token: cloneToken(ie.Token),
		Left:  CloneExpression(ie.Left),
		Right: CloneExpression(ie.Right),
		Not:   ie.Not,
	}
}

func cloneBetweenExpression(be *BetweenExpression) *BetweenExpression {
	if be == nil {
		return nil
	}
	return &BetweenExpression{
		Token: cloneToken(be.Token),
		Expr:  CloneExpression(be.Expr),
		Lower: CloneExpression(be.Lower),
		Upper: CloneExpression(be.Upper),
		Not:   be.Not,
	}
}

func cloneScalarSubquery(ss *ScalarSubquery) *ScalarSubquery {
	if ss == nil {
		return nil
	}
	return &ScalarSubquery{
		Token:    cloneToken(ss.Token),
		Subquery: cloneSelectStatement(ss.Subquery),
	}
}

func cloneExpressionList(el *ExpressionList) *ExpressionList {
	if el == nil {
		return nil
	}
	expressions := make([]Expression, len(el.Expressions))
	for i, e := range el.Expressions {
		expressions[i] = CloneExpression(e)
	}
	return &ExpressionList{
		Token:       cloneToken(el.Token),
		Expressions: expressions,
	}
}

func cloneWhenClause(wc *WhenClause) *WhenClause {
	if wc == nil {
		return nil
	}
	return &WhenClause{
		Token:      cloneToken(wc.Token),
		Condition:  CloneExpression(wc.Condition),
		ThenResult: CloneExpression(wc.ThenResult),
	}
}

func cloneCaseExpression(ce *CaseExpression) *CaseExpression {
	if ce == nil {
		return nil
	}
	whenClauses := make([]*WhenClause, len(ce.WhenClauses))
	for i, wc := range ce.WhenClauses {
		whenClauses[i] = cloneWhenClause(wc)
	}
	return &CaseExpression{
		Token:       cloneToken(ce.Token),
		Value:       CloneExpression(ce.Value),
		WhenClauses: whenClauses,
		ElseValue:   CloneExpression(ce.ElseValue),
	}
}

func cloneCastExpression(ce *CastExpression) *CastExpression {
	if ce == nil {
		return nil
	}
	return &CastExpression{
		Token:    cloneToken(ce.Token),
		Expr:     CloneExpression(ce.Expr),
		TypeName: ce.TypeName,
	}
}

func cloneFunctionCall(fc *FunctionCall) *FunctionCall {
	if fc == nil {
		return nil
	}
	args := make([]Expression, len(fc.Arguments))
	for i, arg := range fc.Arguments {
		args[i] = CloneExpression(arg)
	}

	orderBy := make([]OrderByExpression, len(fc.OrderBy))
	for i, ob := range fc.OrderBy {
		orderBy[i] = cloneOrderByExpression(ob)
	}

	// Create a copy of the function info if it exists
	var funcInfo *funcregistry.FunctionInfo
	if fc.FunctionInfo != nil {
		// Clone the signature
		argTypes := make([]funcregistry.DataType, len(fc.FunctionInfo.Signature.ArgumentTypes))
		copy(argTypes, fc.FunctionInfo.Signature.ArgumentTypes)

		funcInfo = &funcregistry.FunctionInfo{
			Name: fc.FunctionInfo.Name,
			Type: fc.FunctionInfo.Type,
			Signature: funcregistry.FunctionSignature{
				ReturnType:    fc.FunctionInfo.Signature.ReturnType,
				ArgumentTypes: argTypes,
				IsVariadic:    fc.FunctionInfo.Signature.IsVariadic,
				MinArgs:       fc.FunctionInfo.Signature.MinArgs,
				MaxArgs:       fc.FunctionInfo.Signature.MaxArgs,
			},
			Description: fc.FunctionInfo.Description,
		}
	}

	return &FunctionCall{
		Token:        cloneToken(fc.Token),
		Function:     fc.Function,
		Arguments:    args,
		FunctionInfo: funcInfo,
		IsDistinct:   fc.IsDistinct,
		OrderBy:      orderBy,
	}
}

func cloneAliasedExpression(ae *AliasedExpression) *AliasedExpression {
	if ae == nil {
		return nil
	}
	return &AliasedExpression{
		Token:      cloneToken(ae.Token),
		Expression: CloneExpression(ae.Expression),
		Alias:      cloneIdentifier(ae.Alias),
	}
}

// Clone implementations for table sources

func cloneSimpleTableSource(sts *SimpleTableSource) *SimpleTableSource {
	if sts == nil {
		return nil
	}
	return &SimpleTableSource{
		Token: cloneToken(sts.Token),
		Name:  cloneIdentifier(sts.Name),
		Alias: cloneIdentifier(sts.Alias),
	}
}

func cloneOnJoinCondition(o *OnJoinCondition) *OnJoinCondition {
	if o == nil {
		return nil
	}
	return &OnJoinCondition{
		Token: cloneToken(o.Token),
		Expr:  CloneExpression(o.Expr),
	}
}

func cloneJoinTableSource(jts *JoinTableSource) *JoinTableSource {
	if jts == nil {
		return nil
	}
	var left, right TableSource
	if jts.Left != nil {
		left = CloneExpression(jts.Left).(TableSource)
	}
	if jts.Right != nil {
		right = CloneExpression(jts.Right).(TableSource)
	}
	return &JoinTableSource{
		Token:     cloneToken(jts.Token),
		Left:      left,
		JoinType:  jts.JoinType,
		Right:     right,
		Condition: CloneExpression(jts.Condition),
	}
}

func cloneSubqueryTableSource(sts *SubqueryTableSource) *SubqueryTableSource {
	if sts == nil {
		return nil
	}
	return &SubqueryTableSource{
		Token:    cloneToken(sts.Token),
		Subquery: cloneSelectStatement(sts.Subquery),
		Alias:    cloneIdentifier(sts.Alias),
	}
}

func cloneCTEReference(cte *CTEReference) *CTEReference {
	if cte == nil {
		return nil
	}
	return &CTEReference{
		Token: cloneToken(cte.Token),
		Name:  cloneIdentifier(cte.Name),
		Alias: cloneIdentifier(cte.Alias),
	}
}

// Clone implementations for statements

func cloneOrderByExpression(o OrderByExpression) OrderByExpression {
	return OrderByExpression{
		Expression: CloneExpression(o.Expression),
		Ascending:  o.Ascending,
	}
}

func cloneWithClause(wc *WithClause) *WithClause {
	if wc == nil {
		return nil
	}
	ctes := make([]*CommonTableExpression, len(wc.CTEs))
	for i, cte := range wc.CTEs {
		ctes[i] = cloneCommonTableExpression(cte)
	}
	return &WithClause{
		Token:       cloneToken(wc.Token),
		CTEs:        ctes,
		IsRecursive: wc.IsRecursive,
	}
}

func cloneCommonTableExpression(cte *CommonTableExpression) *CommonTableExpression {
	if cte == nil {
		return nil
	}
	cols := make([]*Identifier, len(cte.ColumnNames))
	for i, col := range cte.ColumnNames {
		cols[i] = cloneIdentifier(col)
	}
	return &CommonTableExpression{
		Token:       cloneToken(cte.Token),
		Name:        cloneIdentifier(cte.Name),
		ColumnNames: cols,
		Query:       cloneSelectStatement(cte.Query),
		IsRecursive: cte.IsRecursive,
	}
}

func cloneSelectStatement(ss *SelectStatement) *SelectStatement {
	if ss == nil {
		return nil
	}

	// Clone columns
	columns := make([]Expression, len(ss.Columns))
	for i, col := range ss.Columns {
		columns[i] = CloneExpression(col)
	}

	// Clone GROUP BY expressions
	groupBy := make([]Expression, len(ss.GroupBy))
	for i, expr := range ss.GroupBy {
		groupBy[i] = CloneExpression(expr)
	}

	// Clone ORDER BY expressions
	orderBy := make([]OrderByExpression, len(ss.OrderBy))
	for i, ob := range ss.OrderBy {
		orderBy[i] = cloneOrderByExpression(ob)
	}

	return &SelectStatement{
		Token:     cloneToken(ss.Token),
		Distinct:  ss.Distinct,
		Columns:   columns,
		With:      cloneWithClause(ss.With),
		TableExpr: CloneExpression(ss.TableExpr),
		Where:     CloneExpression(ss.Where),
		GroupBy:   groupBy,
		Having:    CloneExpression(ss.Having),
		OrderBy:   orderBy,
		Limit:     CloneExpression(ss.Limit),
		Offset:    CloneExpression(ss.Offset),
	}
}

// createExpressionRow creates a new Expression slice - now a simple helper function
func createExpressionRow(capacity int) []Expression {
	return make([]Expression, 0, capacity)
}

func cloneInsertStatement(is *InsertStatement) *InsertStatement {
	if is == nil {
		return nil
	}

	// Clone columns
	columns := make([]*Identifier, len(is.Columns))
	for i, col := range is.Columns {
		columns[i] = cloneIdentifier(col)
	}

	// Clone values with immutable approach
	values := make([][]Expression, len(is.Values))
	for i, row := range is.Values {
		// Create a new row slice with the exact capacity needed
		rowClone := createExpressionRow(len(row))

		// Check if we need to clone any expressions in this row
		hasParameters := false
		for _, expr := range row {
			if containsParameter(expr) {
				hasParameters = true
				break
			}
		}

		// Fill the row with cloned expressions or originals
		if hasParameters {
			// Only clone expressions that contain parameters
			for _, expr := range row {
				if containsParameter(expr) {
					rowClone = append(rowClone, CloneExpression(expr))
				} else {
					// Reuse immutable expressions
					rowClone = append(rowClone, expr)
				}
			}
		} else {
			// If no parameters in this row, we can reuse all expressions
			rowClone = append(rowClone, row...)
		}

		values[i] = rowClone
	}

	return &InsertStatement{
		Token:     is.Token,     // Token is immutable, no need to clone
		TableName: is.TableName, // Identifier is immutable, no need to clone
		Columns:   columns,
		Values:    values,
	}
}

func cloneUpdateStatement(us *UpdateStatement) *UpdateStatement {
	if us == nil {
		return nil
	}

	// Clone updates map
	updates := make(map[string]Expression, len(us.Updates))
	for col, expr := range us.Updates {
		updates[col] = CloneExpression(expr)
	}

	return &UpdateStatement{
		Token:     cloneToken(us.Token),
		TableName: cloneIdentifier(us.TableName),
		Updates:   updates,
		Where:     CloneExpression(us.Where),
	}
}

func cloneDeleteStatement(ds *DeleteStatement) *DeleteStatement {
	if ds == nil {
		return nil
	}

	return &DeleteStatement{
		Token:     cloneToken(ds.Token),
		TableName: cloneIdentifier(ds.TableName),
		Where:     CloneExpression(ds.Where),
	}
}

func cloneCreateTableStatement(cts *CreateTableStatement) *CreateTableStatement {
	if cts == nil {
		return nil
	}

	// Clone column definitions
	columns := make([]ColumnDefinition, len(cts.Columns))
	for i, col := range cts.Columns {
		columns[i] = cloneColumnDefinition(col)
	}

	return &CreateTableStatement{
		Token:       cloneToken(cts.Token),
		TableName:   cloneIdentifier(cts.TableName),
		IfNotExists: cts.IfNotExists,
		Columns:     columns,
	}
}

func cloneColumnDefinition(cd ColumnDefinition) ColumnDefinition {
	// Clone constraints
	constraints := make([]ColumnConstraint, len(cd.Constraints))
	copy(constraints, cd.Constraints)

	return ColumnDefinition{
		Name:        cloneIdentifier(cd.Name),
		Type:        cd.Type,
		Constraints: constraints,
	}
}

func cloneDropTableStatement(dts *DropTableStatement) *DropTableStatement {
	if dts == nil {
		return nil
	}

	return &DropTableStatement{
		Token:     cloneToken(dts.Token),
		TableName: cloneIdentifier(dts.TableName),
		IfExists:  dts.IfExists,
	}
}

func cloneAlterTableStatement(ats *AlterTableStatement) *AlterTableStatement {
	if ats == nil {
		return nil
	}

	var columnDef *ColumnDefinition
	if ats.ColumnDef != nil {
		cd := cloneColumnDefinition(*ats.ColumnDef)
		columnDef = &cd
	}

	return &AlterTableStatement{
		Token:         cloneToken(ats.Token),
		TableName:     cloneIdentifier(ats.TableName),
		Operation:     ats.Operation,
		ColumnDef:     columnDef,
		ColumnName:    cloneIdentifier(ats.ColumnName),
		NewColumnName: cloneIdentifier(ats.NewColumnName),
		NewTableName:  cloneIdentifier(ats.NewTableName),
	}
}

func cloneCreateIndexStatement(cis *CreateIndexStatement) *CreateIndexStatement {
	if cis == nil {
		return nil
	}

	// Clone columns
	columns := make([]*Identifier, len(cis.Columns))
	for i, col := range cis.Columns {
		columns[i] = cloneIdentifier(col)
	}

	return &CreateIndexStatement{
		Token:       cloneToken(cis.Token),
		IndexName:   cloneIdentifier(cis.IndexName),
		TableName:   cloneIdentifier(cis.TableName),
		Columns:     columns,
		IsUnique:    cis.IsUnique,
		IfNotExists: cis.IfNotExists,
	}
}

func cloneDropIndexStatement(dis *DropIndexStatement) *DropIndexStatement {
	if dis == nil {
		return nil
	}

	return &DropIndexStatement{
		Token:     cloneToken(dis.Token),
		IndexName: cloneIdentifier(dis.IndexName),
		TableName: cloneIdentifier(dis.TableName),
		IfExists:  dis.IfExists,
	}
}

func cloneCreateViewStatement(cvs *CreateViewStatement) *CreateViewStatement {
	if cvs == nil {
		return nil
	}

	return &CreateViewStatement{
		Token:       cloneToken(cvs.Token),
		ViewName:    cloneIdentifier(cvs.ViewName),
		Query:       cloneSelectStatement(cvs.Query),
		IfNotExists: cvs.IfNotExists,
	}
}

func cloneDropViewStatement(dvs *DropViewStatement) *DropViewStatement {
	if dvs == nil {
		return nil
	}

	return &DropViewStatement{
		Token:    cloneToken(dvs.Token),
		ViewName: cloneIdentifier(dvs.ViewName),
		IfExists: dvs.IfExists,
	}
}

func cloneBeginStatement(bs *BeginStatement) *BeginStatement {
	if bs == nil {
		return nil
	}

	return &BeginStatement{
		Token:          cloneToken(bs.Token),
		IsolationLevel: bs.IsolationLevel,
	}
}

func cloneCommitStatement(cs *CommitStatement) *CommitStatement {
	if cs == nil {
		return nil
	}

	return &CommitStatement{
		Token: cloneToken(cs.Token),
	}
}

func cloneRollbackStatement(rs *RollbackStatement) *RollbackStatement {
	if rs == nil {
		return nil
	}

	return &RollbackStatement{
		Token:         cloneToken(rs.Token),
		SavepointName: cloneIdentifier(rs.SavepointName),
	}
}

func cloneSavepointStatement(ss *SavepointStatement) *SavepointStatement {
	if ss == nil {
		return nil
	}

	return &SavepointStatement{
		Token:         cloneToken(ss.Token),
		SavepointName: cloneIdentifier(ss.SavepointName),
	}
}

func cloneMergeClause(mc *MergeClause) *MergeClause {
	if mc == nil {
		return nil
	}

	// Clone assignments
	assignments := make(map[string]Expression, len(mc.Assignments))
	for col, expr := range mc.Assignments {
		assignments[col] = CloneExpression(expr)
	}

	// Clone columns
	columns := make([]*Identifier, len(mc.Columns))
	for i, col := range mc.Columns {
		columns[i] = cloneIdentifier(col)
	}

	// Clone values
	values := make([]Expression, len(mc.Values))
	for i, expr := range mc.Values {
		values[i] = CloneExpression(expr)
	}

	return &MergeClause{
		IsMatched:   mc.IsMatched,
		Condition:   CloneExpression(mc.Condition),
		Action:      mc.Action,
		Assignments: assignments,
		Columns:     columns,
		Values:      values,
	}
}

func cloneMergeStatement(ms *MergeStatement) *MergeStatement {
	if ms == nil {
		return nil
	}

	// Clone clauses
	clauses := make([]*MergeClause, len(ms.Clauses))
	for i, clause := range ms.Clauses {
		clauses[i] = cloneMergeClause(clause)
	}

	return &MergeStatement{
		Token:       cloneToken(ms.Token),
		TargetTable: cloneIdentifier(ms.TargetTable),
		TargetAlias: cloneIdentifier(ms.TargetAlias),
		SourceTable: CloneExpression(ms.SourceTable),
		SourceAlias: cloneIdentifier(ms.SourceAlias),
		OnCondition: CloneExpression(ms.OnCondition),
		Clauses:     clauses,
	}
}

func cloneSetStatement(ss *SetStatement) *SetStatement {
	if ss == nil {
		return nil
	}

	return &SetStatement{
		Token: cloneToken(ss.Token),
		Name:  cloneIdentifier(ss.Name),
		Value: CloneExpression(ss.Value),
	}
}

func cloneExpressionStatement(es *ExpressionStatement) *ExpressionStatement {
	if es == nil {
		return nil
	}

	return &ExpressionStatement{
		Token:      cloneToken(es.Token),
		Expression: CloneExpression(es.Expression),
	}
}
