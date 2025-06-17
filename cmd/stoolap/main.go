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
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/stoolap/stoolap/internal/common"

	// Import driver
	_ "github.com/stoolap/stoolap/pkg/driver"
)

var (
	dbPath     string
	jsonOutput bool
	quiet      bool
	execute    string
	file       string
	limit      int
)

var rootCmd = &cobra.Command{
	Use:   "stoolap",
	Short: "Stoolap SQL Database CLI",
	Long: `Stoolap is a high-performance SQL database with MVCC and columnar storage.
This CLI provides an interactive interface to execute SQL queries and manage your database.`,
	Version: common.VersionMajor + "." + common.VersionMinor + "." + common.VersionPatch + "-" + common.VersionSuffix,
	RunE:    runRootCommand,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&dbPath, "db", "d", "memory://", "Database path (file://<path> or memory://)")
	rootCmd.PersistentFlags().BoolVarP(&jsonOutput, "json", "j", false, "Output results in JSON format")
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "Suppress connection messages")
	rootCmd.PersistentFlags().IntVarP(&limit, "limit", "l", 40, "Maximum number of rows to display (0 for unlimited)")
	rootCmd.Flags().StringVarP(&execute, "execute", "e", "", "Execute a single SQL statement and exit")
	rootCmd.Flags().StringVarP(&file, "file", "f", "", "Execute SQL statements from a file")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runRootCommand(cmd *cobra.Command, args []string) error {
	// Open the database
	db, err := sql.Open("stoolap", dbPath)
	if err != nil {
		return fmt.Errorf("error opening database: %v", err)
	}
	defer db.Close()

	// Ping the database to make sure it's working
	if err := db.Ping(); err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	if !quiet {
		fmt.Printf("Connected to database: %s\n", dbPath)
	}

	// Handle execute flag - run single query and exit
	if execute != "" {
		return executeQueryWithOptions(db, execute, jsonOutput, false, limit)
	}

	// Handle file flag - execute SQL from file
	if file != "" {
		return executeFromFile(db, file, jsonOutput, quiet, limit)
	}

	// Check if we're getting input from a pipe
	stat, _ := os.Stdin.Stat()
	isPipe := (stat.Mode() & os.ModeCharDevice) == 0

	if isPipe {
		return executePipedInput(db, jsonOutput)
	}

	// Interactive mode - use the improved CLI
	cli, err := NewCLI(db, jsonOutput, limit)
	if err != nil {
		return fmt.Errorf("error initializing CLI: %v", err)
	}
	defer cli.Close()

	return cli.Run()
}

func executeFromFile(db *sql.DB, filename string, jsonOutput, quiet bool, rowLimit int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentStatement strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip shell-style comment lines
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		// Skip SQL-style comment lines
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, "--") ||
			(strings.HasPrefix(trimmedLine, "/*") && strings.HasSuffix(trimmedLine, "*/")) {
			continue
		}

		// If blank line and we have a statement, execute it
		if strings.TrimSpace(line) == "" && currentStatement.Len() > 0 {
			q := strings.TrimSpace(currentStatement.String())
			currentStatement.Reset()

			if q != "" {
				statements := splitSQLStatements(q)
				for _, stmt := range statements {
					trimmedStmt := strings.TrimSpace(stmt)
					if trimmedStmt == "" {
						continue
					}

					err := executeQueryWithOptions(db, trimmedStmt, jsonOutput, quiet, rowLimit)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					}
				}
			}
		} else {
			currentStatement.WriteString(line)
			currentStatement.WriteString(" ")
		}
	}

	if scanner.Err() != nil {
		return fmt.Errorf("error reading file: %v", scanner.Err())
	}

	// Execute any remaining statement
	if currentStatement.Len() > 0 {
		q := strings.TrimSpace(currentStatement.String())
		if q != "" {
			statements := splitSQLStatements(q)
			for _, stmt := range statements {
				trimmedStmt := strings.TrimSpace(stmt)
				if trimmedStmt == "" {
					continue
				}

				err := executeQueryWithOptions(db, trimmedStmt, jsonOutput, quiet, rowLimit)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				}
			}
		}
	}

	return nil
}

func executePipedInput(db *sql.DB, jsonOutput bool) error {
	scanner := bufio.NewScanner(os.Stdin)
	var currentStatement strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip shell-style comment lines (for backward compatibility)
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}

		// Skip SQL-style comment lines
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, "--") ||
			(strings.HasPrefix(trimmedLine, "/*") && strings.HasSuffix(trimmedLine, "*/")) {
			continue
		}

		// If blank line and we have a statement, execute it
		if strings.TrimSpace(line) == "" && currentStatement.Len() > 0 {
			q := strings.TrimSpace(currentStatement.String())
			currentStatement.Reset()

			if q != "" {
				// Split the input by semicolons to handle multiple statements
				statements := splitSQLStatements(q)
				for _, stmt := range statements {
					trimmedStmt := strings.TrimSpace(stmt)
					if trimmedStmt == "" {
						continue
					}

					start := time.Now()
					err := executeQuery(db, trimmedStmt, jsonOutput)
					elapsed := time.Since(start)

					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					} else if !jsonOutput {
						fmt.Printf("Query executed in %v\n", elapsed)
					}
				}
			}
		} else {
			// Add the line to the current statement
			currentStatement.WriteString(line)
			currentStatement.WriteString(" ")
		}
	}

	if scanner.Err() != nil {
		return fmt.Errorf("error reading input: %v", scanner.Err())
	}

	// Execute any remaining statement
	if currentStatement.Len() > 0 {
		q := strings.TrimSpace(currentStatement.String())
		if q != "" {
			// Split the input by semicolons to handle multiple statements
			statements := splitSQLStatements(q)
			for _, stmt := range statements {
				trimmedStmt := strings.TrimSpace(stmt)
				if trimmedStmt == "" {
					continue
				}

				start := time.Now()
				err := executeQuery(db, trimmedStmt, jsonOutput)
				elapsed := time.Since(start)

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					// Don't exit immediately, try to process other statements
				} else if !jsonOutput {
					fmt.Printf("Query executed in %v\n", elapsed)
				}
			}
		}
	}

	return nil
}

func executeQuery(db *sql.DB, query string, jsonOutput bool) error {
	return executeQueryWithOptions(db, query, jsonOutput, false, 40)
}

func executeQueryWithOptions(db *sql.DB, query string, jsonOutput, quiet bool, rowLimit int) error {
	ctx := context.Background()

	// Handle special commands
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	switch upperQuery {
	case "HELP", "\\H", "\\?":
		printHelpMain()
		return nil
	case "EXIT", "QUIT", "\\Q":
		return fmt.Errorf("exit requested")
	}

	// Look for a special separator that indicates parameter values
	parts := strings.Split(query, " -- PARAMS: ")

	// Trim trailing semicolons and whitespace from the SQL part
	if len(parts) > 0 {
		parts[0] = strings.TrimSuffix(strings.TrimSpace(parts[0]), ";")
	}

	// Initialize parameters array
	var params []interface{}

	// If we have a parameter part
	if len(parts) > 1 {
		baseQuery := parts[0] // Already trimmed above
		paramString := strings.TrimSpace(parts[1])

		// Parse parameter values (comma-separated)
		paramValues := strings.Split(paramString, ",")
		for _, val := range paramValues {
			// Trim whitespace and add to params
			params = append(params, convertParamValue(strings.TrimSpace(val)))
		}

		// Replace the query with just the SQL part
		query = baseQuery
	}

	// Check if it's a query that returns rows (SELECT, SHOW, PRAGMA reads, WITH/CTE, etc.)
	if strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "WITH") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		(strings.HasPrefix(upperQuery, "PRAGMA") && !strings.Contains(upperQuery, "=")) {
		var rows *sql.Rows
		var err error

		if len(params) > 0 {
			// Use parameters
			rows, err = db.QueryContext(ctx, query, params...)
		} else {
			// No parameters
			rows, err = db.QueryContext(ctx, query)
		}

		if err != nil {
			return err
		}
		defer rows.Close()

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			return err
		}

		if jsonOutput {
			// Collect all rows for JSON output
			var allRows [][]interface{}

			// Create a slice of interfaces for the row values
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			// Iterate over the rows
			for rows.Next() {
				// Scan the row into the values slice
				if err := rows.Scan(scanArgs...); err != nil {
					return err
				}

				// Copy values to a new slice
				row := make([]interface{}, len(columns))
				copy(row, values)
				allRows = append(allRows, row)
			}

			if err := rows.Err(); err != nil {
				return err
			}

			// Output JSON
			result := map[string]interface{}{
				"columns": columns,
				"rows":    allRows,
				"count":   len(allRows),
			}

			jsonBytes, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %v", err)
			}

			fmt.Println(string(jsonBytes))
		} else {
			// Print the column names
			for i, column := range columns {
				if i > 0 {
					fmt.Print(" | ")
				}
				fmt.Print(column)
			}
			fmt.Println()

			// Print a separator
			for i := range columns {
				if i > 0 {
					fmt.Print("-+-")
				}
				fmt.Print("----")
			}
			fmt.Println()

			// Collect all rows first for smart truncation
			var allRows [][]string
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for rows.Next() {
				if err := rows.Scan(scanArgs...); err != nil {
					return err
				}

				// Convert row to strings
				row := make([]string, len(columns))
				for i, value := range values {
					row[i] = formatValueToString(value)
				}
				allRows = append(allRows, row)
			}

			if err := rows.Err(); err != nil {
				return err
			}

			// Display rows with smart truncation
			displayRowsWithTruncation(allRows, rowLimit, quiet)
		}
	} else {
		// Execute a non-query statement
		var result sql.Result
		var err error

		if len(params) > 0 {
			// Use parameters
			result, err = db.ExecContext(ctx, query, params...)
		} else {
			// No parameters
			result, err = db.ExecContext(ctx, query)
		}

		if err != nil {
			return err
		}

		// Print the result
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if jsonOutput {
			result := map[string]interface{}{
				"rows_affected": rowsAffected,
			}

			jsonBytes, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %v", err)
			}

			fmt.Println(string(jsonBytes))
		} else {
			if !quiet {
				fmt.Printf("%d rows affected\n", rowsAffected)
			}
		}
	}

	return nil
}

// convertParamValue tries to convert string parameter values to appropriate types
func convertParamValue(value string) interface{} {
	// Try to convert to integer
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}

	// Try to convert to float
	if f, err := strconv.ParseFloat(value, 64); err == nil && strings.Contains(value, ".") {
		return f
	}

	// Try to convert to boolean
	if b, err := strconv.ParseBool(value); err == nil && (value == "true" || value == "false") {
		return b
	}

	// Try to convert to time (ISO format)
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t
	}

	// If it's "null" or "NULL", return nil
	if strings.EqualFold(value, "null") {
		return nil
	}

	// Default to string
	return value
}

func formatValueToString(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%.1f", v)
		}
		return fmt.Sprintf("%.4g", v)
	case string:
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func displayRowsWithTruncation(allRows [][]string, rowLimit int, quiet bool) {
	totalRows := len(allRows)

	// If no limit or total rows within limit, show all
	if rowLimit <= 0 || totalRows <= rowLimit {
		for _, row := range allRows {
			for i, value := range row {
				if i > 0 {
					fmt.Print(" | ")
				}
				fmt.Print(value)
			}
			fmt.Println()
		}

		if !quiet {
			fmt.Printf("%d rows in set\n", totalRows)
		}
		return
	}

	// Smart truncation: show top half and bottom half
	topRows := rowLimit / 2
	bottomRows := rowLimit - topRows

	// Show top rows
	for i := 0; i < topRows && i < totalRows; i++ {
		row := allRows[i]
		for j, value := range row {
			if j > 0 {
				fmt.Print(" | ")
			}
			fmt.Print(value)
		}
		fmt.Println()
	}

	// Show truncation indicator with clean formatting
	if totalRows > rowLimit {
		hiddenRows := totalRows - rowLimit
		fmt.Println()
		fmt.Printf("    \033[2m... (%d more rows) ...\033[0m\n", hiddenRows)
		fmt.Println()
	}

	// Show bottom rows
	startIdx := totalRows - bottomRows
	if startIdx < topRows {
		startIdx = topRows // Avoid overlap
	}

	for i := startIdx; i < totalRows; i++ {
		row := allRows[i]
		for j, value := range row {
			if j > 0 {
				fmt.Print(" | ")
			}
			fmt.Print(value)
		}
		fmt.Println()
	}

	if !quiet {
		fmt.Printf("%d rows in set (showing %d)\n", totalRows, rowLimit)
	}
}

// splitSQLStatements splits a SQL input into multiple statements based on semicolons
// This handles semicolons within quotes and avoids splitting those incorrectly
// It also handles SQL comments (both -- and /* */ style)
func splitSQLStatements(input string) []string {
	var statements []string
	var currentStatement strings.Builder

	// Keep track of parsing state
	inSingleQuotes := false
	inDoubleQuotes := false
	inLineComment := false
	inBlockComment := false

	// Process character by character
	for i := 0; i < len(input); i++ {
		char := input[i]

		// Handle end of line comment
		if inLineComment {
			if char == '\n' {
				inLineComment = false
				currentStatement.WriteByte(char) // Keep the newline
			}
			continue // Skip characters in line comments
		}

		// Handle start of line comment
		if !inSingleQuotes && !inDoubleQuotes && !inBlockComment &&
			char == '-' && i+1 < len(input) && input[i+1] == '-' {
			inLineComment = true
			i++ // Skip the second '-'
			continue
		}

		// Handle end of block comment
		if inBlockComment {
			if char == '*' && i+1 < len(input) && input[i+1] == '/' {
				inBlockComment = false
				i++ // Skip the '/'
			}
			continue // Skip characters in block comments
		}

		// Handle start of block comment
		if !inSingleQuotes && !inDoubleQuotes &&
			char == '/' && i+1 < len(input) && input[i+1] == '*' {
			inBlockComment = true
			i++ // Skip the '*'
			continue
		}

		// Handle quotes (if not in a comment)
		if !inBlockComment && !inLineComment {
			if char == '\'' && (i == 0 || input[i-1] != '\\') {
				inSingleQuotes = !inSingleQuotes
			} else if char == '"' && (i == 0 || input[i-1] != '\\') {
				inDoubleQuotes = !inDoubleQuotes
			}
		}

		// If we find a semicolon outside of quotes and comments, it's a statement delimiter
		if char == ';' && !inSingleQuotes && !inDoubleQuotes && !inBlockComment && !inLineComment {
			statements = append(statements, currentStatement.String())
			currentStatement.Reset()
		} else {
			currentStatement.WriteByte(char)
		}
	}

	// Add any remaining statement
	if currentStatement.Len() > 0 {
		statements = append(statements, currentStatement.String())
	}

	return statements
}

// printHelpMain displays help information for piped mode
func printHelpMain() {
	fmt.Println("Stoolap SQL CLI")
	fmt.Println("")
	fmt.Println("  SQL Commands:")
	fmt.Println("    SELECT ...             Execute a SELECT query")
	fmt.Println("    INSERT ...             Insert data into a table")
	fmt.Println("    UPDATE ...             Update data in a table")
	fmt.Println("    DELETE ...             Delete data from a table")
	fmt.Println("    CREATE TABLE ...       Create a new table")
	fmt.Println("    CREATE INDEX ...       Create an index on a column")
	fmt.Println("    SHOW TABLES            List all tables")
	fmt.Println("    SHOW CREATE TABLE ...  Show CREATE TABLE statement for a table")
	fmt.Println("    SHOW INDEXES FROM ...  Show indexes for a table")
	fmt.Println("")
	fmt.Println("  Transaction Commands:")
	fmt.Println("    BEGIN                  Start a new transaction")
	fmt.Println("    COMMIT                 Commit the current transaction")
	fmt.Println("    ROLLBACK               Rollback the current transaction")
	fmt.Println("")
	fmt.Println("  Special Commands:")
	fmt.Println("    help, \\h, \\?          Show this help message")
	fmt.Println("")
}
