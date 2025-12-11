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
// Package vectorized provides a vectorized execution engine for SQL queries
package vectorized

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stoolap/stoolap-go/internal/functions/contract"
	"github.com/stoolap/stoolap-go/internal/parser"
	"github.com/stoolap/stoolap-go/internal/storage"
)

// Constants for vectorized execution
const (
	// MaxBatchSize is the maximum number of rows in a batch
	// Larger batch sizes improve vector processing efficiency (CPU cache utilization)
	MaxBatchSize = 1024 * 32

	// ParallelThreshold is the threshold above which we use parallel processing
	// For small data volumes, the overhead of parallelization exceeds the benefits
	ParallelThreshold = 5000

	// OptimalBlockSize is the ideal processing block size for cache efficiency
	// This is tuned for typical CPU cache line size and SIMD register width
	// Adjusted to 64 elements to match typical L1 cache line size
	OptimalBlockSize = 64
)

// Engine implements a vectorized execution engine for SQL queries
type Engine struct {
	// Dependencies
	functionRegistry contract.FunctionRegistry
}

// NewEngine creates a new vectorized execution engine
func NewEngine(registry contract.FunctionRegistry) *Engine {
	return &Engine{
		functionRegistry: registry,
	}
}

// ExecuteQuery executes a SQL query using vectorized execution
func (e *Engine) ExecuteQuery(ctx context.Context, tx storage.Transaction, stmt *parser.SelectStatement) (storage.Result, error) {
	// Analyze the query to determine if it's suitable for vectorized execution
	if !e.isVectorizable(stmt) {
		return nil, fmt.Errorf("query not suitable for vectorized execution")
	}

	// Check for operations that benefit from vectorization
	hasComplex := e.hasComplexOperations(stmt)

	// Extract table name from the statement
	tableName, err := extractTableName(stmt)
	if err != nil {
		return nil, err
	}

	// Extract columns to fetch
	columns, err := extractColumns(stmt, tx, tableName)
	if err != nil {
		return nil, err
	}

	// For complex expressions, ensure we extract all needed columns
	// including those referenced in expressions
	if hasComplex {
		// Get table schema to fetch all column names
		table, err := tx.GetTable(tableName)
		if err != nil {
			return nil, fmt.Errorf("error getting table schema: %w", err)
		}

		// Use schema to get actual column names
		schema := table.Schema()
		allColumns := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			allColumns[i] = col.Name
		}

		// Replace columns with the complete list
		columns = allColumns
	}

	// Fetch the base data
	baseResult, err := tx.Select(tableName, columns, nil)
	if err != nil {
		return nil, err
	}

	// Convert to vectorized batch
	batch, batchColumns, err := convertResultToBatch(baseResult)
	if err != nil {
		return nil, err
	}

	// Apply WHERE filters if present
	if stmt.Where != nil {
		batch, err = e.applyFilter(batch, stmt.Where)
		if err != nil {
			return nil, err
		}
	}

	// Apply expressions in SELECT list
	batch, err = e.applySelectExpressions(batch, stmt.Columns, batchColumns)
	if err != nil {
		return nil, err
	}

	// Convert back to row-based result interface
	result := NewVectorizedResult(ctx, batch, getFinalColumnNames(stmt.Columns))

	return result, nil
}

// isVectorizable checks if a query can be executed with vectorized execution
func (e *Engine) isVectorizable(stmt *parser.SelectStatement) bool {
	// For now, simple SELECT statements without aggregations, joins, etc.
	// We'll expand this over time
	if len(stmt.GroupBy) > 0 || stmt.Having != nil {
		return false
	}

	// Check that we're selecting from a single base table
	if stmt.TableExpr == nil {
		return false
	}

	// Ensure table is a simple identifier or simple table source
	switch stmt.TableExpr.(type) {
	case *parser.Identifier, *parser.SimpleTableSource:
		// These are supported
	default:
		return false
	}

	// Look for complex operations that would benefit from vectorization
	// Examples: Arithmetic, complex WHERE clauses, etc.
	if e.hasComplexOperations(stmt) {
		return true
	}

	return true
}

// hasComplexOperations checks if the query contains operations that would
// benefit from vectorized execution like arithmetic or complex filtering
func (e *Engine) hasComplexOperations(stmt *parser.SelectStatement) bool {
	// Check for specific arithmetic operations in SELECT list that would benefit from SIMD
	hasComplex := false
	for _, expr := range stmt.Columns {
		if e.hasArithmeticExpression(expr) {
			// Found math expression in SELECT list
			hasComplex = true
			break // Found at least one, no need to check further
		}
	}

	// Also check for complex WHERE clauses
	if !hasComplex && stmt.Where != nil && e.hasArithmeticExpression(stmt.Where) {
		hasComplex = true
	}

	// Also check for filter operations that would benefit from our SIMD comparison processor
	if !hasComplex && stmt.Where != nil {
		// Look specifically for comparison operators that could be handled by SIMD
		if e.hasSimdCompatibleComparison(stmt.Where) {
			hasComplex = true
		}
	}

	return hasComplex
}

// hasSimdCompatibleComparison checks if an expression contains comparisons
// that can benefit from SIMD optimization
func (engine *Engine) hasSimdCompatibleComparison(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.InfixExpression:
		// Check for comparison operators
		switch e.Operator {
		case "=", "==", "<>", "!=", ">", ">=", "<", "<=":
			// Check if one side is a column and the other is a constant
			_, constVal, isColConst := extractColumnAndConstant(e)
			if isColConst {
				// Check if constant is numeric (which benefits from SIMD)
				switch constVal.(type) {
				case float64, int64, int:
					return true // This is a good SIMD candidate
				}
			}
		}

		// If not a direct comparison, check children recursively
		return engine.hasSimdCompatibleComparison(e.Left) || engine.hasSimdCompatibleComparison(e.Right)

	case *parser.PrefixExpression:
		// Check operand recursively
		return engine.hasSimdCompatibleComparison(e.Right)

	case *parser.AliasedExpression:
		// Check underlying expression
		return engine.hasSimdCompatibleComparison(e.Expression)
	}

	return false
}

// hasArithmeticExpression recursively checks if an expression contains arithmetic
func (engine *Engine) hasArithmeticExpression(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *parser.InfixExpression:
		// Check if this is an arithmetic operator
		if isArithmeticOperator(e.Operator) {
			return true
		}

		// If not, check children recursively
		return engine.hasArithmeticExpression(e.Left) || engine.hasArithmeticExpression(e.Right)

	case *parser.PrefixExpression:
		// Check operand recursively
		return engine.hasArithmeticExpression(e.Right)

	case *parser.AliasedExpression:
		// Check underlying expression
		return engine.hasArithmeticExpression(e.Expression)

	case *parser.FunctionCall:
		// Check if any arguments contain arithmetic
		for _, arg := range e.Arguments {
			if engine.hasArithmeticExpression(arg) {
				return true
			}
		}
	}

	return false
}

// applyFilter applies a WHERE filter to a vectorized batch
func (e *Engine) applyFilter(batch *Batch, expr parser.Expression) (*Batch, error) {
	// Save the original column names for restoring if needed
	origColumns := make([]string, len(batch.ColumnNames))
	copy(origColumns, batch.ColumnNames)

	// Create a filter processor for the expression
	processor, err := e.createFilterProcessor(expr)
	if err != nil {
		return nil, err
	}

	// Apply the filter
	result, err := processor.Process(batch)

	// Check if we lost columns in the process
	if err == nil && len(result.ColumnNames) == 0 && len(origColumns) > 0 {
		// Use our helper method to restore all column structures
		// Don't try to preserve data since we don't know the mapping
		if result.Size > 0 {
			result.RestoreColumnStructures(batch, false)
		} else {
			// Just copy column names if result is empty
			result.ColumnNames = make([]string, len(origColumns))
			copy(result.ColumnNames, origColumns)
		}
	}

	return result, err
}

// createFilterProcessor creates a filter processor for an expression
func (e *Engine) createFilterProcessor(expr parser.Expression) (Processor, error) {
	// Handle different expression types
	switch expr := expr.(type) {
	case *parser.InfixExpression:
		return e.createComparisonProcessor(expr)
	case *parser.PrefixExpression:
		if strings.ToUpper(expr.Operator) == "NOT" {
			// Create a NOT processor that inverts the result of its operand
			inner, err := e.createFilterProcessor(expr.Right)
			if err != nil {
				return nil, err
			}
			return NewNotProcessor(inner), nil
		}
	}

	// Fallback for unsupported expressions - pass through all rows
	return NewPassthroughProcessor(), nil
}

// createComparisonProcessor creates a comparison processor for an infix expression
func (e *Engine) createComparisonProcessor(expr *parser.InfixExpression) (Processor, error) {
	switch expr.Operator {
	case "=", "==", "<>", "!=", ">", ">=", "<", "<=":
		// Try to extract column name and constant value
		colName, constVal, isColConst := extractColumnAndConstant(expr)

		if isColConst {
			// Check if we can use SIMD-optimized processor
			if floatVal, ok := constVal.(float64); ok {
				return NewSIMDComparisonProcessor(colName, expr.Operator, floatVal), nil
			} else if intVal, ok := constVal.(int64); ok {
				// Also use SIMD for integer constants (converting to float)
				return NewSIMDComparisonProcessor(colName, expr.Operator, float64(intVal)), nil
			} else if intVal, ok := constVal.(int); ok {
				// Also use SIMD for integer constants (converting to float)
				return NewSIMDComparisonProcessor(colName, expr.Operator, float64(intVal)), nil
			}

			// Fall back to standard processor for other types
			return NewCompareConstantProcessor(colName, expr.Operator, constVal), nil
		} else {
			// Column to column comparison
			leftCol, rightCol, isColCol := extractColumnNames(expr)
			if isColCol {
				return NewCompareColumnsProcessor(leftCol, expr.Operator, rightCol), nil
			}
		}
	case "AND":
		// Create processors for both sides
		leftProc, err := e.createFilterProcessor(expr.Left)
		if err != nil {
			return nil, err
		}

		rightProc, err := e.createFilterProcessor(expr.Right)
		if err != nil {
			return nil, err
		}

		// Create AND processor
		return NewAndProcessor(leftProc, rightProc), nil
	case "OR":
		// Create processors for both sides
		leftProc, err := e.createFilterProcessor(expr.Left)
		if err != nil {
			return nil, err
		}

		rightProc, err := e.createFilterProcessor(expr.Right)
		if err != nil {
			return nil, err
		}

		// Create OR processor
		return NewOrProcessor(leftProc, rightProc), nil
	}

	// Fallback
	return NewPassthroughProcessor(), nil
}

// applySelectExpressions applies expressions in the SELECT list
func (e *Engine) applySelectExpressions(batch *Batch, exprs []parser.Expression, colNames []string) (*Batch, error) {
	_ = colNames // Unused in this context

	// Skip processing for empty batches - just preserve the column names
	if batch.Size == 0 {
		// Create empty result batch while preserving column names
		result := NewBatch(0)

		// Preserve column names for the SELECT list
		for _, expr := range exprs {
			var colName string

			switch e := expr.(type) {
			case *parser.Identifier:
				colName = e.Value
			case *parser.AliasedExpression:
				colName = e.Alias.Value
			default:
				colName = fmt.Sprintf("col_%d", len(result.ColumnNames)+1)
			}

			// Add to output column names
			result.ColumnNames = append(result.ColumnNames, colName)
		}

		return result, nil
	}

	// Create a new batch with the same rows but different columns
	result := NewBatch(0)
	// Set the size to match input batch
	result.Size = batch.Size

	// Add columns from the input that are referenced in the SELECT list
	for _, expr := range exprs {
		// Handle different expression types
		switch expr := expr.(type) {
		case *parser.Identifier:
			// Direct column reference
			colName := expr.Value
			if batch.HasIntColumn(colName) {
				result.AddIntColumnFrom(colName, batch)
			} else if batch.HasFloatColumn(colName) {
				result.AddFloatColumnFrom(colName, batch)
			} else if batch.HasStringColumn(colName) {
				result.AddStringColumnFrom(colName, batch)
			} else if batch.HasBoolColumn(colName) {
				result.AddBoolColumnFrom(colName, batch)
			} else if batch.HasTimeColumn(colName) {
				result.AddTimeColumnFrom(colName, batch)
			}
		case *parser.AliasedExpression:
			// Extract expression and add with alias
			aliasName := expr.Alias.Value
			switch innerExpr := expr.Expression.(type) {
			case *parser.Identifier:
				// Column alias
				colName := innerExpr.Value
				if batch.HasIntColumn(colName) {
					result.AddIntColumnFromWithName(colName, batch, aliasName)
				} else if batch.HasFloatColumn(colName) {
					result.AddFloatColumnFromWithName(colName, batch, aliasName)
				} else if batch.HasStringColumn(colName) {
					result.AddStringColumnFromWithName(colName, batch, aliasName)
				} else if batch.HasBoolColumn(colName) {
					result.AddBoolColumnFromWithName(colName, batch, aliasName)
				} else if batch.HasTimeColumn(colName) {
					result.AddTimeColumnFromWithName(colName, batch, aliasName)
				}
			case *parser.InfixExpression:
				// Arithmetic expression
				if isArithmeticOperator(innerExpr.Operator) {
					err := e.applyArithmeticExpression(batch, result, innerExpr, aliasName)
					if err != nil {
						return nil, err
					}
				}
			}
		case *parser.InfixExpression:
			// Direct arithmetic expression
			if isArithmeticOperator(expr.Operator) {
				// Generate a column name if not aliased
				colName := fmt.Sprintf("col_%d", len(result.ColumnNames)+1)
				err := e.applyArithmeticExpression(batch, result, expr, colName)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return result, nil
}

// isArithmeticOperator checks if an operator is an arithmetic operator
func isArithmeticOperator(op string) bool {
	return op == "+" || op == "-" || op == "*" || op == "/" || op == "%"
}

// applyArithmeticExpression applies an arithmetic expression to a batch
// This is a key integration point between SQL expression evaluation and
// optimized vector operations
func (e *Engine) applyArithmeticExpression(input, result *Batch, expr *parser.InfixExpression, resultColName string) error {
	// Extract column names for optimized direct access
	leftColName := getColumnName(expr.Left)
	rightColName := getColumnName(expr.Right)

	// Check if operands are constants or columns - optimized path
	leftIsConst := leftColName == ""
	rightIsConst := rightColName == ""

	// Get constant values if applicable - using optimized access paths
	var leftConst, rightConst float64

	if leftIsConst {
		switch val := expr.Left.(type) {
		case *parser.IntegerLiteral:
			leftConst = float64(val.Value)
		case *parser.FloatLiteral:
			leftConst = val.Value
		default:
			// Fall back to general extraction for non-literal expressions
			var leftCol []float64
			var err error
			leftCol, leftConst, leftIsConst, err = e.extractOperand(input, expr.Left)
			if err != nil {
				return err
			}

			// Continue with normal processing path
			return e.processArithmeticExpression(
				input, result, expr, resultColName,
				leftCol, leftConst, leftIsConst,
				nil, 0, false, // We'll extract right side later
				leftColName, rightColName)
		}
	}

	if rightIsConst {
		switch val := expr.Right.(type) {
		case *parser.IntegerLiteral:
			rightConst = float64(val.Value)
		case *parser.FloatLiteral:
			rightConst = val.Value
		default:
			// Fall back to general extraction
			var rightCol []float64
			var err error
			_, _, _, err = e.extractOperand(input, expr.Left) // Extract left side
			if err != nil {
				return err
			}

			rightCol, rightConst, rightIsConst, err = e.extractOperand(input, expr.Right)
			if err != nil {
				return err
			}

			// Continue with normal processing path
			return e.processArithmeticExpression(
				input, result, expr, resultColName,
				nil, 0, false, // Left side already handled
				rightCol, rightConst, rightIsConst,
				leftColName, rightColName)
		}
	}

	// Determine column types for direct access optimization
	leftIsInt := !leftIsConst && input.HasIntColumn(leftColName)
	leftIsFloat := !leftIsConst && input.HasFloatColumn(leftColName)
	rightIsInt := !rightIsConst && input.HasIntColumn(rightColName)
	rightIsFloat := !rightIsConst && input.HasFloatColumn(rightColName)

	// Ensure the result column exists
	if !result.HasFloatColumn(resultColName) {
		result.AddFloatColumn(resultColName)
	}

	// Get the result array for direct manipulation
	resultCol := result.FloatColumns[resultColName]

	// Handle common fast paths directly without array creation
	switch expr.Operator {
	case "+":
		if leftIsConst && rightIsInt {
			// Scalar + Int Vector
			intCol := input.IntColumns[rightColName]
			nulls := input.NullBitmaps[rightColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if nulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = leftConst + float64(intCol[j])
					}
				}
			}
			return nil

		} else if leftIsConst && rightIsFloat {
			// Scalar + Float Vector: Add using SIMD
			AddScalarFloat64SIMD(resultCol, input.FloatColumns[rightColName], leftConst, input.Size)

			// Copy nulls
			copy(result.NullBitmaps[resultColName], input.NullBitmaps[rightColName])
			return nil

		} else if leftIsInt && rightIsConst {
			// Int Vector + Scalar
			intCol := input.IntColumns[leftColName]
			nulls := input.NullBitmaps[leftColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if nulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = float64(intCol[j]) + rightConst
					}
				}
			}
			return nil

		} else if leftIsFloat && rightIsConst {
			// Float Vector + Scalar: Add using SIMD
			AddScalarFloat64SIMD(resultCol, input.FloatColumns[leftColName], rightConst, input.Size)

			// Copy nulls
			copy(result.NullBitmaps[resultColName], input.NullBitmaps[leftColName])
			return nil

		} else if leftIsInt && rightIsInt {
			// Int Vector + Int Vector
			leftCol := input.IntColumns[leftColName]
			rightCol := input.IntColumns[rightColName]
			leftNulls := input.NullBitmaps[leftColName]
			rightNulls := input.NullBitmaps[rightColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if leftNulls[j] || rightNulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = float64(leftCol[j]) + float64(rightCol[j])
					}
				}
			}
			return nil

		} else if leftIsFloat && rightIsFloat {
			// Float Vector + Float Vector: Add using SIMD
			floatAdd(
				resultCol,
				input.FloatColumns[leftColName],
				input.FloatColumns[rightColName],
				input.NullBitmaps[leftColName],
				input.NullBitmaps[rightColName],
				result.NullBitmaps[resultColName],
				input.Size,
			)
			return nil

		} else if leftIsConst && rightIsConst {
			// Constant + Constant
			sum := leftConst + rightConst

			// Set all elements to the constant result
			for i := 0; i < input.Size; i++ {
				resultCol[i] = sum
				result.NullBitmaps[resultColName][i] = false
			}
			return nil
		}

	case "-":
		if leftIsConst && rightIsInt {
			// Scalar - Int Vector
			intCol := input.IntColumns[rightColName]
			nulls := input.NullBitmaps[rightColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if nulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = leftConst - float64(intCol[j])
					}
				}
			}
			return nil

		} else if leftIsConst && rightIsFloat {
			// Scalar - Float Vector
			// Direct implementation to avoid temporary buffer
			SubScalarLeftFloat64SIMD(resultCol, input.FloatColumns[rightColName], leftConst, input.Size)

			// Copy nulls
			copy(result.NullBitmaps[resultColName], input.NullBitmaps[rightColName])
			return nil

		} else if leftIsInt && rightIsConst {
			// Int Vector - Scalar
			intCol := input.IntColumns[leftColName]
			nulls := input.NullBitmaps[leftColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if nulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = float64(intCol[j]) - rightConst
					}
				}
			}
			return nil

		} else if leftIsFloat && rightIsConst {
			// Float Vector - Scalar: Add negative constant using SIMD
			AddScalarFloat64SIMD(resultCol, input.FloatColumns[leftColName], -rightConst, input.Size)

			// Copy nulls
			copy(result.NullBitmaps[resultColName], input.NullBitmaps[leftColName])
			return nil
		}

	case "*":
		if leftIsConst && rightIsInt {
			// Scalar * Int Vector
			intCol := input.IntColumns[rightColName]
			nulls := input.NullBitmaps[rightColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block
				for j := i; j < end; j++ {
					if nulls[j] {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = leftConst * float64(intCol[j])
					}
				}
			}
			return nil

		} else if leftIsConst && rightIsFloat {
			// Scalar * Float Vector: Multiply using SIMD
			MulScalarFloat64SIMD(resultCol, input.FloatColumns[rightColName], leftConst, input.Size)

			// Copy nulls
			copy(result.NullBitmaps[resultColName], input.NullBitmaps[rightColName])
			return nil
		}

	case "/":
		if leftIsConst && rightIsInt {
			// Scalar / Int Vector
			intCol := input.IntColumns[rightColName]
			nulls := input.NullBitmaps[rightColName]

			// Process in optimized blocks
			for i := 0; i < input.Size; i += OptimalBlockSize {
				end := i + OptimalBlockSize
				if end > input.Size {
					end = input.Size
				}

				// Process this block with division by zero protection
				for j := i; j < end; j++ {
					if nulls[j] || intCol[j] == 0 {
						result.NullBitmaps[resultColName][j] = true
					} else {
						resultCol[j] = leftConst / float64(intCol[j])
					}
				}
			}
			return nil

		} else if leftIsFloat && rightIsConst {
			// Float Vector / Scalar: Multiply by reciprocal
			if rightConst == 0 {
				// Division by zero
				for i := 0; i < input.Size; i++ {
					result.NullBitmaps[resultColName][i] = true
				}
			} else {
				// Multiply by reciprocal (faster than division)
				MulScalarFloat64SIMD(resultCol, input.FloatColumns[leftColName], 1.0/rightConst, input.Size)

				// Copy nulls
				copy(result.NullBitmaps[resultColName], input.NullBitmaps[leftColName])
			}
			return nil
		}
	}

	// Fall back to general processing for unsupported cases
	leftCol, leftConst, leftIsConst, err := e.extractOperand(input, expr.Left)
	if err != nil {
		return err
	}

	rightCol, rightConst, rightIsConst, err := e.extractOperand(input, expr.Right)
	if err != nil {
		return err
	}

	return e.processArithmeticExpression(
		input, result, expr, resultColName,
		leftCol, leftConst, leftIsConst,
		rightCol, rightConst, rightIsConst,
		leftColName, rightColName)
}

// processArithmeticExpression is the fallback implementation for complex cases
// that were not handled by the optimized fast paths in applyArithmeticExpression
func (e *Engine) processArithmeticExpression(
	input, result *Batch,
	expr *parser.InfixExpression,
	resultColName string,
	leftCol []float64, leftConst float64, leftIsConst bool,
	rightCol []float64, rightConst float64, rightIsConst bool,
	leftColName, rightColName string,
) error {
	// Get the result array for direct access
	resultCol := result.FloatColumns[resultColName]

	// Use optimized SIMD operations for different arithmetic operators
	switch expr.Operator {
	case "+":
		if leftIsConst && !rightIsConst {
			// Scalar + Vector: leftConst + rightCol
			// Maps to SIMD operation in simd.go
			AddScalarFloat64SIMD(resultCol, rightCol, leftConst, input.Size)
		} else if !leftIsConst && rightIsConst {
			// Vector + Scalar: leftCol + rightConst
			AddScalarFloat64SIMD(resultCol, leftCol, rightConst, input.Size)
		} else if !leftIsConst && !rightIsConst {
			// Vector + Vector: leftCol + rightCol
			// Uses auto-vectorized implementation from simd.go
			AddFloat64SIMD(resultCol, leftCol, rightCol, input.Size)
		} else {
			// Constant + Constant: leftConst + rightConst
			// Special case optimization for constants
			resultCol[0] = leftConst + rightConst
			for i := 1; i < input.Size; i++ {
				resultCol[i] = resultCol[0]
			}
		}
	case "-":
		if leftIsConst && !rightIsConst {
			// Scalar - Vector: leftConst - rightCol
			// Direct implementation to avoid temporary buffer
			SubScalarLeftFloat64SIMD(resultCol, rightCol, leftConst, input.Size)
		} else if !leftIsConst && rightIsConst {
			// Vector - Scalar: leftCol - rightConst
			// Optimization: Use scalar addition with negative constant
			AddScalarFloat64SIMD(resultCol, leftCol, -rightConst, input.Size)
		} else if !leftIsConst && !rightIsConst {
			// Vector - Vector: leftCol - rightCol
			SubFloat64SIMD(resultCol, leftCol, rightCol, input.Size)
		} else {
			// Constant - Constant: leftConst - rightConst
			resultCol[0] = leftConst - rightConst
			for i := 1; i < input.Size; i++ {
				resultCol[i] = resultCol[0]
			}
		}
	case "*":
		if leftIsConst && !rightIsConst {
			// Scalar * Vector: leftConst * rightCol
			MulScalarFloat64SIMD(resultCol, rightCol, leftConst, input.Size)
		} else if !leftIsConst && rightIsConst {
			// Vector * Scalar: leftCol * rightConst
			MulScalarFloat64SIMD(resultCol, leftCol, rightConst, input.Size)
		} else if !leftIsConst && !rightIsConst {
			// Vector * Vector: leftCol * rightCol
			// Uses optimized implementation with loop unrolling and auto-vectorization
			MulFloat64SIMD(resultCol, leftCol, rightCol, input.Size)
		} else {
			// Constant * Constant: leftConst * rightConst
			resultCol[0] = leftConst * rightConst
			for i := 1; i < input.Size; i++ {
				resultCol[i] = resultCol[0]
			}
		}
	case "/":
		if leftIsConst && !rightIsConst {
			// Scalar / Vector: leftConst / rightCol
			// Direct implementation to avoid temporary buffer
			DivScalarLeftFloat64SIMD(resultCol, rightCol, leftConst, input.Size)
		} else if !leftIsConst && rightIsConst {
			// Vector / Scalar: leftCol / rightConst
			// Optimization: Use scalar multiplication with reciprocal (faster than division)
			MulScalarFloat64SIMD(resultCol, leftCol, 1.0/rightConst, input.Size)
		} else if !leftIsConst && !rightIsConst {
			// Vector / Vector: leftCol / rightCol
			DivFloat64SIMD(resultCol, leftCol, rightCol, input.Size)
		} else {
			// Constant / Constant: leftConst / rightConst
			resultCol[0] = leftConst / rightConst
			for i := 1; i < input.Size; i++ {
				resultCol[i] = resultCol[0]
			}
		}
	default:
		// For unsupported operations, default to a simple loop
		for i := 0; i < input.Size; i++ {
			// Set default values
			resultCol[i] = 0.0
			result.NullBitmaps[resultColName][i] = false
		}
	}

	// Set nulls for the result column
	// For now, simple rule: if any operand is null, result is null
	if !leftIsConst && !rightIsConst && leftColName != "" && rightColName != "" {
		for i := 0; i < input.Size; i++ {
			if input.NullBitmaps[leftColName][i] || input.NullBitmaps[rightColName][i] {
				result.NullBitmaps[resultColName][i] = true
			}
		}
	}

	return nil
}

// SubScalarLeftFloat64SIMD computes dst[i] = scalar - vec[i] for each element
// This is a specialized implementation to avoid creating temporary arrays
func SubScalarLeftFloat64SIMD(dst, vec []float64, scalar float64, count int) {
	// Process in blocks for better cache usage
	for i := 0; i < count; i += OptimalBlockSize {
		end := i + OptimalBlockSize
		if end > count {
			end = count
		}

		// Process this block
		for j := i; j < end; j++ {
			dst[j] = scalar - vec[j]
		}
	}
}

// DivScalarLeftFloat64SIMD computes dst[i] = scalar / vec[i] for each element
// This is a specialized implementation to avoid creating temporary arrays
func DivScalarLeftFloat64SIMD(dst, vec []float64, scalar float64, count int) {
	// Process in blocks for better cache usage
	for i := 0; i < count; i += OptimalBlockSize {
		end := i + OptimalBlockSize
		if end > count {
			end = count
		}

		// Process this block with division by zero check
		for j := i; j < end; j++ {
			if vec[j] == 0 {
				// Division by zero results in NaN
				dst[j] = 0 // We'll mark this as NULL later
			} else {
				dst[j] = scalar / vec[j]
			}
		}
	}
}

// floatAdd adds two float vectors with NULL handling
func floatAdd(dst, a, b []float64, aNulls, bNulls, dstNulls []bool, count int) {
	// Process in blocks for better cache usage
	for i := 0; i < count; i += OptimalBlockSize {
		end := i + OptimalBlockSize
		if end > count {
			end = count
		}

		// Process this block
		for j := i; j < end; j++ {
			if aNulls[j] || bNulls[j] {
				dstNulls[j] = true
			} else {
				dst[j] = a[j] + b[j]
			}
		}
	}
}

// extractOperand extracts a column or constant from an expression
func (e *Engine) extractOperand(batch *Batch, expr parser.Expression) ([]float64, float64, bool, error) {
	switch expr := expr.(type) {
	case *parser.Identifier:
		// Column reference
		colName := expr.Value

		// Create a float array with the column values
		values := make([]float64, batch.Size)

		if batch.HasIntColumn(colName) {
			// Convert int column to float
			for i := 0; i < batch.Size; i++ {
				values[i] = float64(batch.IntColumns[colName][i])
			}
		} else if batch.HasFloatColumn(colName) {
			// Use float column directly
			copy(values, batch.FloatColumns[colName])
		} else {
			return nil, 0, false, fmt.Errorf("column '%s' not found or not numeric", colName)
		}

		return values, 0, false, nil

	case *parser.IntegerLiteral:
		// Integer constant
		return nil, float64(expr.Value), true, nil

	case *parser.FloatLiteral:
		// Float constant
		return nil, expr.Value, true, nil

	default:
		return nil, 0, false, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// getColumnName extracts a column name from an expression if it's an identifier
func getColumnName(expr parser.Expression) string {
	if ident, ok := expr.(*parser.Identifier); ok {
		return ident.Value
	}
	return ""
}

// Helper functions

// extractTableName extracts the name of the table from a SELECT statement
func extractTableName(stmt *parser.SelectStatement) (string, error) {
	if stmt.TableExpr == nil {
		return "", fmt.Errorf("no table expression in SELECT")
	}

	switch expr := stmt.TableExpr.(type) {
	case *parser.Identifier:
		return expr.Value, nil
	case *parser.SimpleTableSource:
		return expr.Name.Value, nil
	default:
		return "", fmt.Errorf("unsupported table expression type")
	}
}

// extractColumns extracts the columns to fetch from a SELECT statement
func extractColumns(stmt *parser.SelectStatement, tx storage.Transaction, tableName string) ([]string, error) {
	// Handle SELECT *
	if len(stmt.Columns) == 1 {
		if ident, ok := stmt.Columns[0].(*parser.Identifier); ok && ident.Value == "*" {
			// Get all columns from the table schema
			table, err := tx.GetTable(tableName)
			if err != nil {
				return nil, err
			}

			schema := table.Schema()
			columns := make([]string, len(schema.Columns))
			for i, col := range schema.Columns {
				columns[i] = col.Name
			}
			return columns, nil
		}
	}

	// Get columns explicitly mentioned in the SELECT list
	var columns []string
	columnSet := make(map[string]bool)

	// Extract columns from the SELECT list
	for _, colExpr := range stmt.Columns {
		extractColumnsFromExpr(colExpr, &columns, columnSet)
	}

	// Extract columns from the WHERE clause
	if stmt.Where != nil {
		extractColumnsFromExpr(stmt.Where, &columns, columnSet)
	}

	return columns, nil
}

// extractColumnsFromExpr extracts column names from an expression
func extractColumnsFromExpr(expr parser.Expression, columns *[]string, columnSet map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *parser.Identifier:
		// Direct column reference
		if !columnSet[e.Value] && e.Value != "*" { // Skip * (all columns)
			*columns = append(*columns, e.Value)
			columnSet[e.Value] = true
		}
	case *parser.AliasedExpression:
		// Extract from the underlying expression
		extractColumnsFromExpr(e.Expression, columns, columnSet)
	case *parser.InfixExpression:
		// Extract from both sides
		extractColumnsFromExpr(e.Left, columns, columnSet)
		extractColumnsFromExpr(e.Right, columns, columnSet)
	case *parser.PrefixExpression:
		// Extract from the operand
		extractColumnsFromExpr(e.Right, columns, columnSet)
	case *parser.FunctionCall:
		// Extract from function arguments
		for _, arg := range e.Arguments {
			extractColumnsFromExpr(arg, columns, columnSet)
		}
	case *parser.IntegerLiteral, *parser.FloatLiteral, *parser.StringLiteral,
		*parser.BooleanLiteral, *parser.NullLiteral:
		// Constants don't contain column references, ignore them
	default:
		// Handle any other type of expression conservatively
	}
}

// extractColumnAndConstant extracts column name and constant value from a comparison
func extractColumnAndConstant(expr *parser.InfixExpression) (string, interface{}, bool) {
	// Check if left side is a column identifier
	if ident, ok := expr.Left.(*parser.Identifier); ok {
		// Check if right side is a literal
		switch lit := expr.Right.(type) {
		case *parser.IntegerLiteral:
			return ident.Value, lit.Value, true
		case *parser.FloatLiteral:
			return ident.Value, lit.Value, true
		case *parser.StringLiteral:
			return ident.Value, lit.Value, true
		case *parser.BooleanLiteral:
			return ident.Value, lit.Value, true
		case *parser.NullLiteral:
			return ident.Value, nil, true
		}
	}

	// Check if right side is a column identifier (reverse the comparison)
	if ident, ok := expr.Right.(*parser.Identifier); ok {
		// Check if left side is a literal
		switch lit := expr.Left.(type) {
		case *parser.IntegerLiteral:
			// Reverse the comparison operator
			return ident.Value, lit.Value, true
		case *parser.FloatLiteral:
			return ident.Value, lit.Value, true
		case *parser.StringLiteral:
			return ident.Value, lit.Value, true
		case *parser.BooleanLiteral:
			return ident.Value, lit.Value, true
		case *parser.NullLiteral:
			return ident.Value, nil, true
		}
	}

	return "", nil, false
}

// extractColumnNames extracts two column names from a comparison
func extractColumnNames(expr *parser.InfixExpression) (string, string, bool) {
	// Check if both sides are column identifiers
	leftIdent, leftOk := expr.Left.(*parser.Identifier)
	rightIdent, rightOk := expr.Right.(*parser.Identifier)

	if leftOk && rightOk {
		return leftIdent.Value, rightIdent.Value, true
	}

	return "", "", false
}

// convertResultToBatch converts a storage.Result to a vectorized batch
func convertResultToBatch(result storage.Result) (*Batch, []string, error) {
	// Get column names
	columns := result.Columns()

	// Create batch
	rows := 0
	batch := NewBatch(MaxBatchSize)

	// Count rows and build column structure
	colTypes := make(map[string]string)
	rowValues := make([][]interface{}, 0, MaxBatchSize)

	// Scan rows to determine types and gather data
	for result.Next() {
		// Use Row() to get storage.ColumnValue objects directly without allocation
		row := result.Row()
		if row == nil {
			continue
		}

		// Extract values and determine column types
		values := make([]interface{}, len(columns))
		for i, colValue := range row {
			// Get the value as interface directly
			if i < len(columns) && colValue != nil {
				v := colValue.AsInterface()
				values[i] = v

				// Determine column type if not already known
				if v != nil && colTypes[columns[i]] == "" {
					switch v.(type) {
					case int64, int32, int:
						colTypes[columns[i]] = "int"
					case float64, float32:
						colTypes[columns[i]] = "float"
					case string:
						colTypes[columns[i]] = "string"
					case bool:
						colTypes[columns[i]] = "bool"
					case time.Time:
						colTypes[columns[i]] = "time"
					default:
						colTypes[columns[i]] = "string" // Default to string
					}
				}
			}
		}

		rowValues = append(rowValues, values)
		rows++

		// Don't exceed batch size
		if rows >= MaxBatchSize {
			break
		}
	}

	// Resize batch to actual number of rows
	batch.Size = rows

	// Add columns to the batch based on determined types
	for _, colName := range columns {
		colType := colTypes[colName]
		switch colType {
		case "int":
			batch.AddIntColumn(colName)
		case "float":
			batch.AddFloatColumn(colName)
		case "string":
			batch.AddStringColumn(colName)
		case "bool":
			batch.AddBoolColumn(colName)
		case "time":
			batch.AddTimeColumn(colName)
		default:
			batch.AddStringColumn(colName) // Default to string
		}
	}

	// Populate the batch with data
	for rowIdx, rowValues := range rowValues {
		for colIdx, value := range rowValues {
			colName := columns[colIdx]
			if value == nil {
				batch.NullBitmaps[colName][rowIdx] = true
				continue
			}

			switch colTypes[colName] {
			case "int":
				switch v := value.(type) {
				case int:
					batch.IntColumns[colName][rowIdx] = int64(v)
				case int32:
					batch.IntColumns[colName][rowIdx] = int64(v)
				case int64:
					batch.IntColumns[colName][rowIdx] = v
				case float64:
					batch.IntColumns[colName][rowIdx] = int64(v)
				default:
					// Try to convert
					if s, ok := value.(string); ok {
						if val, err := strconv.ParseInt(s, 10, 64); err == nil {
							batch.IntColumns[colName][rowIdx] = val
						} else {
							batch.NullBitmaps[colName][rowIdx] = true
						}
					} else {
						batch.NullBitmaps[colName][rowIdx] = true
					}
				}
			case "float":
				switch v := value.(type) {
				case float64:
					batch.FloatColumns[colName][rowIdx] = v
				case float32:
					batch.FloatColumns[colName][rowIdx] = float64(v)
				case int:
					batch.FloatColumns[colName][rowIdx] = float64(v)
				case int64:
					batch.FloatColumns[colName][rowIdx] = float64(v)
				default:
					// Try to convert
					if s, ok := value.(string); ok {
						if val, err := strconv.ParseFloat(s, 64); err == nil {
							batch.FloatColumns[colName][rowIdx] = val
						} else {
							batch.NullBitmaps[colName][rowIdx] = true
						}
					} else {
						batch.NullBitmaps[colName][rowIdx] = true
					}
				}
			case "string":
				if s, ok := value.(string); ok {
					batch.StringColumns[colName][rowIdx] = s
				} else {
					// Convert other types to string
					batch.StringColumns[colName][rowIdx] = fmt.Sprint(value)
				}
			case "bool":
				if b, ok := value.(bool); ok {
					batch.BoolColumns[colName][rowIdx] = b
				} else {
					// Try to convert
					if s, ok := value.(string); ok {
						b := strings.ToLower(s) == "true" || s == "1"
						batch.BoolColumns[colName][rowIdx] = b
					} else if i, ok := value.(int64); ok {
						batch.BoolColumns[colName][rowIdx] = i != 0
					} else {
						batch.NullBitmaps[colName][rowIdx] = true
					}
				}
			case "time":
				if t, ok := value.(time.Time); ok {
					batch.TimeColumns[colName][rowIdx] = t
				} else {
					batch.NullBitmaps[colName][rowIdx] = true
				}
			}
		}
	}

	// Close the original result
	result.Close()

	return batch, columns, nil
}

// getFinalColumnNames returns the final column names for the result
func getFinalColumnNames(exprs []parser.Expression) []string {
	columns := make([]string, len(exprs))

	for i, expr := range exprs {
		switch e := expr.(type) {
		case *parser.Identifier:
			columns[i] = e.Value
		case *parser.AliasedExpression:
			columns[i] = e.Alias.Value
		default:
			columns[i] = fmt.Sprintf("column%d", i+1)
		}
	}

	return columns
}
