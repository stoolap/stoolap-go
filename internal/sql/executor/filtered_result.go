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
package executor

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// Shared object pools for various types to reduce memory allocations
var (
	// Slice pools for scan values
	scanValsPool = &sync.Pool{
		New: func() interface{} {
			sp := make([]storage.ColumnValue, 0, 64)
			return &sp
		},
	}

	// Map pools for column names
	columnNamePool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]int, 8)
		},
	}
)

// FilteredResult represents a result set that filters rows based on a WHERE clause
type FilteredResult struct {
	result    storage.Result
	whereExpr parser.Expression
	evaluator *Evaluator

	// Current row and whether it passes the filter
	currentRow   map[string]storage.ColumnValue
	currentValid bool
	closed       bool

	scanVals    []storage.ColumnValue
	columnNames []string // Column information cache

	// Evaluation statistics
	rowsScanned int
	rowsMatched int

	// Pre-compiled WHERE condition info
	whereCompiled     bool
	whereColumns      map[string]bool // Columns used in WHERE clause
	whereHasFunctions bool            // Whether WHERE contains function calls
}

// WithColumnAliases sets the column aliases on the FilteredResult's evaluator
func (f *FilteredResult) WithColumnAliases(aliases map[string]string) *FilteredResult {
	f.evaluator.WithColumnAliases(aliases)
	return f
}

// Columns returns the column names from the underlying result
func (f *FilteredResult) Columns() []string {
	if len(f.columnNames) > 0 {
		return f.columnNames
	}

	f.columnNames = f.result.Columns()
	return f.columnNames
}

// Efficient column name cache - using a regular map with RWMutex is more efficient than sync.Map
// for this specific workload with high read/low write and small dataset
var (
	columnCacheMu   sync.RWMutex
	columnNameCache = make(map[string]string, 1024) // Pre-allocate for common column names
)

// StringPoolCache pre-allocates common lowercase strings
var StringPoolCache [256]string

// ByteBufferPool provides reusable byte buffers for string lowercasing
var ByteBufferPool = sync.Pool{
	New: func() interface{} {
		buffer := make([]byte, 0, 64)
		return &buffer
	},
}

// NoAllocByteBuffer is a fixed buffer for common column names (most column names are short)
var NoAllocByteBuffer [64]byte

// asciiLowerTable is a pre-computed table for fast ASCII lowercase conversion
var asciiLowerTable [256]byte

func init() {
	// Initialize the ASCII lowercase table
	for i := 0; i < 256; i++ {
		if i >= 'A' && i <= 'Z' {
			asciiLowerTable[i] = byte(i - 'A' + 'a')
		} else {
			asciiLowerTable[i] = byte(i)
		}
	}

	// Initialize StringPoolCache for single-letter values
	for i := 0; i < 256; i++ {
		if i >= 'A' && i <= 'Z' {
			// Uppercase letters
			s := string([]byte{byte(i)})
			StringPoolCache[i] = strings.ToLower(s)
		}
	}
}

// isAllLowerASCII checks if a string is all lowercase ASCII
func isAllLowerASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 128 || ('A' <= c && c <= 'Z') {
			return false
		}
	}
	return true
}

// fastToLower is a zero-allocation lowercase function for ASCII strings
func fastToLower(s string) string {
	// Quick check for empty string or single letter
	if len(s) == 0 {
		return s
	}

	// For single character strings, use pre-computed pool
	if len(s) == 1 {
		c := s[0]
		if c < 128 {
			if 'A' <= c && c <= 'Z' {
				return StringPoolCache[c]
			}
			return s // Already lowercase or non-letter
		}
	}

	// Check if we already have it in our cache
	columnCacheMu.RLock()
	if cached, ok := columnNameCache[s]; ok {
		columnCacheMu.RUnlock()
		return cached
	}
	columnCacheMu.RUnlock()

	// Check if it's already all lowercase ASCII (avoid work if possible)
	if isAllLowerASCII(s) {
		return s
	}

	// For short strings (most column names), use stack-allocated buffer
	if len(s) <= 64 {
		// Use our fixed no-alloc buffer
		var n int
		for i := 0; i < len(s); i++ {
			NoAllocByteBuffer[n] = asciiLowerTable[s[i]]
			n++
		}

		// Convert to string (single allocation)
		result := string(NoAllocByteBuffer[:n])

		// Add to cache if it's a reasonably sized string
		if len(s) <= 32 && n <= 32 { // Avoid caching large strings
			columnCacheMu.Lock()
			// Check if it was added while we were processing
			if _, exists := columnNameCache[s]; !exists && len(columnNameCache) < 1024 {
				columnNameCache[s] = result
			}
			columnCacheMu.Unlock()
		}

		return result
	}

	// For longer strings, use the pooled buffer
	bufferPtr := ByteBufferPool.Get().(*[]byte)
	buffer := *bufferPtr

	// Ensure the buffer is large enough
	if cap(buffer) < len(s) {
		buffer = make([]byte, len(s))
	} else {
		buffer = buffer[:len(s)]
	}

	// Convert using our table
	for i := 0; i < len(s); i++ {
		buffer[i] = asciiLowerTable[s[i]]
	}

	// Convert to string (still an allocation, but we pool the buffer)
	result := string(buffer)

	// Return the buffer to the pool
	*bufferPtr = buffer
	ByteBufferPool.Put(bufferPtr)

	return result
}

func getColumnNamePool() map[string]int {
	// Get a map from the pool
	m := columnNamePool.Get().(map[string]int)

	return m
}

func putColumnNamePool(m map[string]int) {
	// Clear the map before putting it back
	clear(m)

	// Return the map to the pool
	columnNamePool.Put(m)
}

// analyzeWhereClause collects information about columns and functions used in the WHERE clause
func (f *FilteredResult) analyzeWhereClause(expr parser.Expression) {
	if expr == nil {
		f.whereCompiled = true
		return
	}

	// Initialize column map if not already done
	if f.whereColumns == nil {
		f.whereColumns = make(map[string]bool)
	}

	// Analyze the WHERE clause by traversing the expression tree
	var analyze func(parser.Expression)
	analyze = func(e parser.Expression) {
		if e == nil {
			return
		}

		switch expr := e.(type) {
		case *parser.Identifier:
			// Record used column
			f.whereColumns[expr.Value] = true

		case *parser.QualifiedIdentifier:
			// Record both qualified and unqualified column names
			f.whereColumns[expr.Name.Value] = true

			// Also record the fully qualified name for later matching
			qualifiedName := expr.Qualifier.Value + "." + expr.Name.Value
			f.whereColumns[qualifiedName] = true

			// Store lowercase versions too since our lookups might be case-insensitive
			f.whereColumns[fastToLower(expr.Name.Value)] = true
			f.whereColumns[fastToLower(qualifiedName)] = true

		case *parser.InfixExpression:
			// Analyze both sides of the expression
			analyze(expr.Left)
			analyze(expr.Right)

		case *parser.PrefixExpression:
			// Analyze operand
			analyze(expr.Right)

		case *parser.FunctionCall:
			// Record that functions are used
			f.whereHasFunctions = true
			// Analyze function arguments
			for _, arg := range expr.Arguments {
				analyze(arg)
			}

		case *parser.CastExpression:
			// Analyze the cast expression
			analyze(expr.Expr)

		case *parser.AliasedExpression:
			// Analyze the underlying expression
			analyze(expr.Expression)

		case *parser.BetweenExpression:
			analyze(expr.Expr)
			analyze(expr.Lower)
			analyze(expr.Upper)

		case *parser.CaseExpression:
			if expr.Value != nil {
				analyze(expr.Value)
			}
			for _, when := range expr.WhenClauses {
				analyze(when.Condition)
				analyze(when.ThenResult)
			}
			if expr.ElseValue != nil {
				analyze(expr.ElseValue)
			}

		case *parser.ExpressionList:
			for _, e := range expr.Expressions {
				analyze(e)
			}

			// Add other expression types as needed
		}
	}

	// Start analysis from the root of the expression
	analyze(expr)
	f.whereCompiled = true
}

func (f *FilteredResult) Next() bool {
	if f.closed {
		return false
	}

	// Initialize reusable scan arrays if needed
	if f.scanVals == nil {
		cols := f.Columns()
		colLen := len(cols)

		// Get/create scan values arrays from pool if possible
		sp := scanValsPool.Get().(*[]storage.ColumnValue)
		scanValsFromPool := *sp
		if cap(scanValsFromPool) >= colLen {
			// Reuse the slice from pool if it's large enough
			f.scanVals = scanValsFromPool[:colLen]
		} else {
			// Return too small slice to pool and create a new one
			scanValsPool.Put(&scanValsFromPool)
			f.scanVals = make([]storage.ColumnValue, colLen)
		}

		// Get map from appropriate pool based on column count
		f.currentRow = common.GetColumnValueMap(colLen)

		// Pre-compile WHERE clause info if not done yet
		if !f.whereCompiled && f.whereExpr != nil {
			f.analyzeWhereClause(f.whereExpr)
		}
	}

	// Fast path for no WHERE clause
	if f.whereExpr == nil {
		if f.result.Next() {
			f.rowsScanned++
			f.rowsMatched++
			f.currentValid = true

			// Try to use direct Row() approach first
			if row := f.result.Row(); len(row) > 0 {
				// Build a currentRow from the raw storage.Row
				// Clear the map using Go's built-in clear()
				clear(f.currentRow)

				// Populate with column values
				for i, colName := range f.columnNames {
					if i < len(row) {
						f.currentRow[colName] = row[i]

						// Handle case-insensitive lookups if needed
						if colName != "" && 'A' <= colName[0] && colName[0] <= 'Z' {
							lowerCol := fastToLower(colName)
							if lowerCol != colName {
								f.currentRow[lowerCol] = row[i]
							}
						}
					}
				}
			}

			return true
		}
		f.currentValid = false
		return false
	}

	// Try to find the next matching row
	for f.result.Next() {
		f.rowsScanned++

		// Try to get the raw storage Row first for optimal performance
		row := f.result.Row()
		var match bool
		var err error

		if len(row) > 0 {
			// Build a mapping from column names to positions
			columnMap := getColumnNamePool()
			defer putColumnNamePool(columnMap)

			for i, col := range f.columnNames {
				columnMap[col] = i

				// Also map lowercase column names for case-insensitive lookups
				if col != "" && 'A' <= col[0] && col[0] <= 'Z' {
					lowerCol := fastToLower(col)
					if lowerCol != col {
						columnMap[lowerCol] = i
					}
				}
			}

			// Evaluate the WHERE clause directly using storage.Row
			match, err = f.evaluator.EvaluateWhereClauseWithStorageRow(f.whereExpr, row, columnMap)
			if err == nil {
				if match {
					f.currentValid = true
					f.rowsMatched++

					// Populate currentRow from the raw Row
					// Clear the map first - using Go's built-in clear()
					clear(f.currentRow)

					// Populate with column values
					for i, colName := range f.columnNames {
						if i < len(row) {
							f.currentRow[colName] = row[i]

							// Handle case-insensitive lookups if needed
							if colName != "" && 'A' <= colName[0] && colName[0] <= 'Z' {
								lowerCol := fastToLower(colName)
								if lowerCol != colName {
									f.currentRow[lowerCol] = row[i]
								}
							}
						} else {
							f.currentRow[colName] = storage.StaticNullUnknown
						}
					}

					// Handle aliases if needed
					if f.evaluator != nil && f.evaluator.columnAliases != nil && len(f.evaluator.columnAliases) > 0 {
						f.processAliases()
					}

					return true
				}
				// If no match, try next row
				continue
			}
			// If there was an error with direct evaluation, fall back to Scan approach
		}

		// Clear the map using Go's built-in clear()
		clear(f.currentRow)

		// Process values - if we have analyzed the WHERE clause, only process needed columns
		if f.whereCompiled && len(f.whereColumns) > 0 && !f.whereHasFunctions {
			// Optimized path - only process columns used in WHERE clause
			for i, col := range f.columnNames {
				// Skip columns not used in WHERE clause
				if !f.whereColumns[col] {
					// For very complex queries we might need lower-case variants
					// So check that too before skipping
					lowerCol := fastToLower(col)
					if !f.whereColumns[lowerCol] {
						continue
					}
				}

				// Normal scan value handling
				val := f.scanVals[i]
				if val != nil {
					f.currentRow[col] = val
				} else {
					f.currentRow[col] = storage.StaticNullUnknown
				}

				// Handle case-insensitive lookups if needed
				if col != "" && 'A' <= col[0] && col[0] <= 'Z' {
					// Use our fast lowercase function
					lowerCol := fastToLower(col)

					if lowerCol != col {
						f.currentRow[lowerCol] = f.currentRow[col]
					}
				}
			}
		} else {
			// Standard path - process all columns
			for i, col := range f.columnNames {
				val := f.scanVals[i]
				if val != nil {
					f.currentRow[col] = val
				} else {
					f.currentRow[col] = storage.StaticNullUnknown
				}

				// Handle case-insensitive lookups
				if col != "" && 'A' <= col[0] && col[0] <= 'Z' {
					// Use our fast lowercase function
					lowerCol := fastToLower(col)

					if lowerCol != col {
						f.currentRow[lowerCol] = f.currentRow[col]
					}
				}
			}
		}

		// Handle aliases if needed - but only for columns actually used in the WHERE clause
		if f.evaluator != nil && f.evaluator.columnAliases != nil && len(f.evaluator.columnAliases) > 0 {
			f.processAliases()
		}

		// Evaluate the WHERE clause on this row
		match, err = f.evaluator.EvaluateWhereClause(f.whereExpr, f.currentRow)
		if err != nil {
			// Skip rows with evaluation errors
			continue
		}

		if match {
			f.currentValid = true
			f.rowsMatched++
			return true
		}
	}

	// No more matching rows
	f.currentValid = false
	return false
}

// AliasCacheEntry represents cached information about a column alias
type AliasCacheEntry struct {
	lowerName  string
	isFunction bool
	baseName   string
	lowerBase  string
}

// Efficient alias cache using a regular map with RWMutex
var (
	aliasEntryCacheMu sync.RWMutex
	aliasEntryCache   = make(map[string]*AliasCacheEntry, 128)
)

// hasLeftParen checks if string contains '(' without allocating
func hasLeftParen(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			return true
		}
	}
	return false
}

// findBaseName extracts the function base name without using strings.Split
func findBaseName(s string) (string, bool) {
	// Find the first '(' character
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			// Extract the base name
			return strings.TrimSpace(s[:i]), true
		}
	}
	return "", false
}

func (f *FilteredResult) processAliases() {
	// Process only aliases that matter for WHERE clause evaluation
	for alias, originalCol := range f.evaluator.columnAliases {
		// First try direct match on original column name (fastest path)
		if origVal, hasOrig := f.currentRow[originalCol]; hasOrig {
			f.currentRow[alias] = origVal
			continue
		}

		// Use cached alias information or compute it
		var entry *AliasCacheEntry

		// Check cache first
		aliasEntryCacheMu.RLock()
		cached, ok := aliasEntryCache[originalCol]
		aliasEntryCacheMu.RUnlock()

		if ok {
			entry = cached
		} else {
			// Create a new entry
			entry = &AliasCacheEntry{
				lowerName: fastToLower(originalCol),
			}

			// Check if this is a function expression
			isFunc := hasLeftParen(originalCol)
			if isFunc {
				entry.isFunction = true

				// Extract base name efficiently
				if baseName, ok := findBaseName(originalCol); ok {
					entry.baseName = baseName
					entry.lowerBase = fastToLower(baseName)
				}
			}

			// Store in cache if we have capacity
			aliasEntryCacheMu.Lock()
			if len(aliasEntryCache) < 512 { // Limit cache size
				aliasEntryCache[originalCol] = entry
			}
			aliasEntryCacheMu.Unlock()
		}

		// For complex column names or expressions, try case-insensitive match
		if origVal, hasOrig := f.currentRow[entry.lowerName]; hasOrig {
			f.currentRow[alias] = origVal
			continue
		}

		// Handle function expressions if this is one
		if entry.isFunction && entry.lowerBase != "" {
			// First try common function patterns
			if entry.lowerBase == "count" || entry.lowerBase == "sum" ||
				entry.lowerBase == "avg" || entry.lowerBase == "min" ||
				entry.lowerBase == "max" {
				// Look for common function patterns in current row
				if val, ok := f.currentRow[entry.lowerBase+"(*)"]; ok {
					f.currentRow[alias] = val
					continue
				}
			}

			// Look for columns starting with this base name - only iterate when needed
			for colName, colVal := range f.currentRow {
				// For exact function matches ("count(id)" etc.)
				if strings.HasPrefix(colName, entry.baseName) {
					f.currentRow[alias] = colVal
					break
				}

				// For case-insensitive matches, use our fast lowercase function
				// and avoid creating substrings when possible
				if len(colName) > len(entry.lowerBase) {
					colNameLower := fastToLower(colName)
					if strings.HasPrefix(colNameLower, entry.lowerBase) {
						f.currentRow[alias] = colVal
						break
					}
				}
			}
		}
	}
}

// StringPoolSize is the size of the string pool
const StringPoolSize = 256

// StringPool is a fixed-size pool of preallocated strings
var StringPool [StringPoolSize]string

// Scan copies column values from the current row into the provided variables
func (f *FilteredResult) Scan(dest ...interface{}) error {
	if !f.currentValid {
		return fmt.Errorf("no row to scan")
	}

	// Extract values in the correct order
	if len(f.columnNames) != len(dest) {
		return fmt.Errorf("column count mismatch: got %d, wanted %d", len(f.columnNames), len(dest))
	}

	// Copy values to destination
	for i, col := range f.columnNames {
		colValue, ok := f.currentRow[col]
		if !ok {
			return fmt.Errorf("column %s not found in current row", col)
		}

		// Special case for interface{} destination with NULL values
		if dest[i] == nil {
			return fmt.Errorf("destination pointer is nil")
		}

		// Use the central utility function for consistent scanning
		if err := storage.ScanColumnValueToDestination(colValue, dest[i]); err != nil {
			return fmt.Errorf("column %s: %w", col, err)
		}
	}

	return nil
}

// Close closes the result set
func (f *FilteredResult) Close() error {
	if f.closed {
		return nil
	}

	// Return the map to the appropriate pool if we have one
	if f.currentRow != nil {
		// Return to the appropriate pool based on size
		// PutColumnValueMap will clear the map for us
		capacity := len(f.columnNames)
		if capacity > 0 {
			common.PutColumnValueMap(f.currentRow, capacity)
		} else {
			// Fallback to small pool if column names not available
			common.PutColumnValueMap(f.currentRow, 8)
		}
		f.currentRow = nil
	}

	// Return scan vals array to pool if appropriate size
	if f.scanVals != nil && cap(f.scanVals) <= 64 {
		// Only return to pool if reasonable size to avoid bloating the pool
		f.scanVals = f.scanVals[:0]   // Reset slice but keep capacity
		scanValsPool.Put(&f.scanVals) // Reset slice but keep capacity
	}

	// Clear cached data
	f.columnNames = nil
	f.scanVals = nil

	f.closed = true
	return f.result.Close()
}

// RowsAffected returns the number of rows affected by the operation
func (f *FilteredResult) RowsAffected() int64 {
	return f.result.RowsAffected()
}

// LastInsertID returns the last insert ID
func (f *FilteredResult) LastInsertID() int64 {
	return 0
}

// Context returns the result's context
func (f *FilteredResult) Context() context.Context {
	if f.result != nil {
		return f.result.Context()
	}
	return context.Background()
}

// GetMetrics returns execution metrics for this result
func (f *FilteredResult) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"rows_scanned": f.rowsScanned,
		"rows_matched": f.rowsMatched,
		"filter_ratio": float64(f.rowsMatched) / float64(max(1, f.rowsScanned)),
	}
}

// WithAliases implements the storage.Result interface
func (f *FilteredResult) WithAliases(aliases map[string]string) storage.Result {
	// Create a new evaluator with these aliases
	newEvaluator := NewEvaluator(f.Context(), GetGlobalFunctionRegistry())
	newEvaluator.WithColumnAliases(aliases)

	// First try to propagate aliases to the base result if it supports them
	var baseResult storage.Result = f.result
	if aliasable, ok := f.result.(interface {
		WithAliases(map[string]string) storage.Result
	}); ok {
		baseResult = aliasable.WithAliases(aliases)
	}

	// Create a new FilteredResult with the aliased base result
	return &FilteredResult{
		result:       baseResult,
		whereExpr:    f.whereExpr,
		evaluator:    newEvaluator,
		currentValid: false,
		closed:       false,
		// Don't initialize other fields yet - they'll be created on demand
	}
}

// Row implements the storage.Result interface
// For filtered results, delegate to the underlying result's Row method
// This assumes that the rows have already been filtered by the Next method
func (f *FilteredResult) Row() storage.Row {
	if !f.currentValid {
		return nil
	}

	// Create a row from f.currentRow instead of the underlying result
	// This ensures any transformations applied by filtering (like COLLATE)
	// are properly reflected in the Row() result
	if len(f.columnNames) > 0 && len(f.currentRow) > 0 {
		row := make(storage.Row, len(f.columnNames))
		for i, colName := range f.columnNames {
			if val, ok := f.currentRow[colName]; ok {
				row[i] = val
			} else {
				row[i] = storage.StaticNullUnknown
			}
		}
		return row
	}

	// Fall back to the underlying result's Row() method if needed
	if row := f.result.Row(); row != nil {
		return row
	}

	// Fallback for results that don't support Row()
	return nil
}
