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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/stoolap/stoolap-go/internal/common"
)

// CLI represents an interactive command-line interface for working with the database
type CLI struct {
	db           *sql.DB
	historyFile  string
	readline     *readline.Instance
	maxTableSize int    // Max allowed table width
	timeFormat   string // Time format string for query timing
	ctx          context.Context

	// Transaction state
	tx            *sql.Tx // Current transaction (nil if not in transaction)
	inTransaction bool    // Whether we're currently in a transaction

	// Output options
	jsonOutput    bool // Whether to output results in JSON format
	isInteractive bool // Whether running in interactive mode
	limit         int  // Maximum number of rows to display

	// Multi-line query state
	currentQuery strings.Builder // Accumulates multi-line query
	inMultiLine  bool            // Whether we're in a multi-line query
}

// NewCLI creates a new CLI instance
func NewCLI(db *sql.DB, jsonOutput bool, limit int) (*CLI, error) {
	// Determine history file location (in user's home directory)
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	historyFile := homeDir + "/.stoolap_history"

	// Create readline instance with custom configuration
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[1;36m>\033[0m ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold: true, // Case-insensitive history search

		// Enable Vim-style key bindings
		VimMode: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize readline: %v", err)
	}

	// Check if we're running interactively (terminal attached)
	isInteractive := true
	if stat, err := os.Stdin.Stat(); err == nil {
		isInteractive = (stat.Mode() & os.ModeCharDevice) != 0
	}

	return &CLI{
		db:            db,
		historyFile:   historyFile,
		readline:      rl,
		maxTableSize:  120,
		timeFormat:    "15:04:05",
		ctx:           context.Background(),
		isInteractive: isInteractive,
		jsonOutput:    jsonOutput,
		limit:         limit,
	}, nil
}

// updatePrompt updates the readline prompt based on current state
func (c *CLI) updatePrompt() {
	var prompt string
	if c.inMultiLine {
		// Multi-line continuation prompt
		if c.inTransaction {
			prompt = "\033[1;33m[TXN]->\033[0m "
		} else {
			prompt = "\033[1;36m->\033[0m "
		}
	} else {
		// Normal prompt
		if c.inTransaction {
			prompt = "\033[1;33m[TXN]>\033[0m "
		} else {
			prompt = "\033[1;36m>\033[0m "
		}
	}
	c.readline.SetPrompt(prompt)
}

// Run starts the CLI
func (c *CLI) Run() error {
	if c.isInteractive {
		fmt.Printf("Stoolap v%s.%s.%s-%s\n",
			common.VersionMajor, common.VersionMinor, common.VersionPatch, common.VersionSuffix)
		fmt.Println("Enter SQL commands, 'help' for assistance, or 'exit' to quit.")
		fmt.Println("Use Up/Down arrows for history, Ctrl+R to search history.")
		if c.jsonOutput {
			fmt.Println("JSON output mode enabled.")
		}
		fmt.Println()
	}

	// Set initial prompt
	c.updatePrompt()

	// Main loop
	for {
		line, err := c.readline.Readline()
		if err != nil {
			if err == io.EOF || err == readline.ErrInterrupt {
				// If we're in a transaction, warn user
				if c.inTransaction {
					fmt.Fprintf(os.Stderr, "\nWarning: Exiting with active transaction. Rolling back...\n")
					c.rollbackTransaction()
				}
				break
			}
			return err
		}

		// Handle multi-line history items (contains escaped newlines)
		if strings.Contains(line, "\\n") {
			// This is a multi-line query from history, convert back to actual newlines
			actualQuery := strings.ReplaceAll(line, "\\n", "\n")
			lines := strings.Split(actualQuery, "\n")

			// Display the query in multi-line format
			fmt.Printf("%s\n", lines[0])
			for i := 1; i < len(lines); i++ {
				if strings.TrimSpace(lines[i]) != "" {
					fmt.Printf("\033[1;36m->\033[0m %s\n", lines[i])
				}
			}

			// Execute the complete query
			c.currentQuery.Reset()
			c.currentQuery.WriteString(actualQuery)
			c.inMultiLine = false

			fullQuery := strings.TrimSpace(c.currentQuery.String())
			// Don't add to history again, it's already there

			statements := splitSQLStatements(fullQuery)
			for _, stmt := range statements {
				trimmedStmt := strings.TrimSpace(stmt)
				if trimmedStmt == "" {
					continue
				}

				// Execute the query
				start := time.Now()
				err := c.executeQuery(trimmedStmt)
				elapsed := time.Since(start)

				if err != nil {
					fmt.Fprintf(os.Stderr, "\033[1;31mError:\033[0m %v\n", err)
				} else if c.isInteractive {
					fmt.Printf("\033[1;32mQuery executed in %v\033[0m\n", elapsed)
				}

				// Update prompt in case transaction state changed
				c.updatePrompt()
			}

			// Reset query buffer
			c.currentQuery.Reset()
			continue
		}

		// Trim whitespace
		line = strings.TrimSpace(line)

		// Handle special commands (only when not in multi-line mode)
		if !c.inMultiLine && line == "" {
			continue
		}

		if !c.inMultiLine {
			switch strings.ToLower(line) {
			case "exit", "quit", "\\q":
				return nil
			case "help", "\\h", "\\?":
				c.printHelp()
				continue
			}
		}

		// Check for transaction control statements that should execute immediately
		upperLine := strings.ToUpper(strings.TrimSpace(line))
		if upperLine == "BEGIN" || upperLine == "COMMIT" || upperLine == "ROLLBACK" ||
			strings.HasPrefix(upperLine, "BEGIN ") || strings.HasPrefix(upperLine, "COMMIT ") || strings.HasPrefix(upperLine, "ROLLBACK ") {

			// Execute transaction control statement immediately
			c.readline.SaveHistory(line)

			start := time.Now()
			err := c.executeQuery(line)
			elapsed := time.Since(start)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\033[1;31mError:\033[0m %v\n", err)
			} else if c.isInteractive {
				fmt.Printf("\033[1;32mQuery executed in %v\033[0m\n", elapsed)
			}

			// Update prompt in case transaction state changed
			c.updatePrompt()
			continue
		}

		// Add line to current query (preserve line breaks for history)
		if c.currentQuery.Len() > 0 {
			c.currentQuery.WriteString("\n")
		}
		c.currentQuery.WriteString(line)

		// Check if the query ends with semicolon
		fullQuery := strings.TrimSpace(c.currentQuery.String())
		if strings.HasSuffix(fullQuery, ";") {
			// Add the complete multi-line query to history
			// Replace actual newlines with escaped newlines to keep as single history entry
			historyEntry := strings.ReplaceAll(fullQuery, "\n", "\\n")
			c.readline.SaveHistory(historyEntry)

			// Execute the complete query
			c.inMultiLine = false
			c.updatePrompt()

			// Split and execute multiple statements if needed
			statements := splitSQLStatements(fullQuery)
			for _, stmt := range statements {
				trimmedStmt := strings.TrimSpace(stmt)
				if trimmedStmt == "" {
					continue
				}

				// Execute the query
				start := time.Now()
				err := c.executeQuery(trimmedStmt)
				elapsed := time.Since(start)

				if err != nil {
					fmt.Fprintf(os.Stderr, "\033[1;31mError:\033[0m %v\n", err)
				} else if c.isInteractive {
					fmt.Printf("\033[1;32mQuery executed in %v\033[0m\n", elapsed)
				}

				// Update prompt in case transaction state changed
				c.updatePrompt()
			}

			// Reset query buffer
			c.currentQuery.Reset()
		} else {
			// Continue multi-line input
			c.inMultiLine = true
			c.updatePrompt()
		}
	}

	return nil
}

// executeQuery executes a SQL query and displays the results
func (c *CLI) executeQuery(query string) error {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	// Handle special commands
	switch upperQuery {
	case "HELP", "\\H", "\\?":
		c.printHelp()
		return nil
	case "EXIT", "QUIT", "\\Q":
		return fmt.Errorf("exit requested")
	}

	// Handle transaction commands
	if strings.HasPrefix(upperQuery, "BEGIN") {
		return c.beginTransaction()
	} else if upperQuery == "COMMIT" {
		return c.commitTransaction()
	} else if upperQuery == "ROLLBACK" {
		return c.rollbackTransaction()
	}

	// Check if it's a query that returns rows (SELECT, SHOW, PRAGMA reads, etc.)
	if strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		(strings.HasPrefix(upperQuery, "PRAGMA") && !strings.Contains(upperQuery, "=")) {
		return c.executeReadQuery(query)
	} else {
		return c.executeWriteQuery(query)
	}
}

// beginTransaction starts a new transaction
func (c *CLI) beginTransaction() error {
	if c.inTransaction {
		return fmt.Errorf("already in a transaction")
	}

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	c.tx = tx
	c.inTransaction = true

	if c.isInteractive {
		fmt.Println("\033[1;32mTransaction started\033[0m")
	}
	return nil
}

// commitTransaction commits the current transaction
func (c *CLI) commitTransaction() error {
	if !c.inTransaction {
		return fmt.Errorf("not in a transaction")
	}

	err := c.tx.Commit()
	if err != nil {
		return err
	}

	c.tx = nil
	c.inTransaction = false

	if c.isInteractive {
		fmt.Println("\033[1;32mTransaction committed\033[0m")
	}
	return nil
}

// rollbackTransaction rolls back the current transaction
func (c *CLI) rollbackTransaction() error {
	if !c.inTransaction {
		return fmt.Errorf("not in a transaction")
	}

	err := c.tx.Rollback()
	if err != nil {
		return err
	}

	c.tx = nil
	c.inTransaction = false

	if c.isInteractive {
		fmt.Println("\033[1;33mTransaction rolled back\033[0m")
	}
	return nil
}

// executeReadQuery executes a query that returns rows (SELECT, SHOW, etc.)
func (c *CLI) executeReadQuery(query string) error {
	var rows *sql.Rows
	var err error

	// Simple spinner for long-running queries (only show if >1 second)
	var progressActive bool
	var progressDone = make(chan bool, 1)
	queryStartTime := time.Now() // Track query start time from the beginning

	if c.isInteractive {
		// Start spinner after 1 second delay
		go func() {
			select {
			case <-time.After(1 * time.Second):
				// Query is taking longer than 1 second, show spinner
				progressActive = true
				spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
				i := 0

				for progressActive {
					elapsed := time.Since(queryStartTime) // Use actual query start time
					fmt.Printf("\rExecuting query... %s [%.1fs]", spinner[i%len(spinner)], elapsed.Seconds())
					time.Sleep(100 * time.Millisecond)
					i++
				}
			case <-progressDone:
				// Query finished before 1 second, don't show spinner
				return
			}
		}()
	}

	// Execute the query using transaction if active
	if c.inTransaction {
		rows, err = c.tx.QueryContext(c.ctx, query)
	} else {
		rows, err = c.db.QueryContext(c.ctx, query)
	}

	// Stop spinner when query completes
	if c.isInteractive {
		progressActive = false
		select {
		case progressDone <- true:
		default: // Channel might be full
		}
		fmt.Print("\r\033[K") // Clear the spinner line completely
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

	// Get the column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Calculate column widths based on column names
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	// Store the rows in memory to analyze and format them nicely
	var values [][]interface{}
	var strValues [][]string

	// Create a slice of interfaces for the row values
	scanArgs := make([]interface{}, len(columns))
	for i := range scanArgs {
		scanArgs[i] = &scanArgs[i]
	}

	// Iterate over the rows
	rowCount := 0
	for rows.Next() {
		// Create a slice of interfaces for the row values
		rowValues := make([]interface{}, len(columns))
		for i := range rowValues {
			rowValues[i] = new(interface{})
		}

		// Scan the row into the values slice
		if err := rows.Scan(rowValues...); err != nil {
			return err
		}

		// Extract actual values and convert to strings
		row := make([]interface{}, len(columns))
		strRow := make([]string, len(columns))
		for i, v := range rowValues {
			val := *(v.(*interface{}))
			row[i] = val
			strValue := formatValue(val, columnTypes[i])
			strRow[i] = strValue

			// Update column width if needed
			if len(strValue) > colWidths[i] {
				colWidths[i] = len(strValue)
			}
		}

		values = append(values, row)
		strValues = append(strValues, strRow)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Handle JSON output format
	if c.jsonOutput {
		return c.outputJSON(columns, values, rowCount)
	}

	// Create and configure table
	t := c.configureTable()

	// Add headers
	headerRow := make(table.Row, len(columns))
	for i, col := range columns {
		headerRow[i] = col
	}
	t.AppendHeader(headerRow)

	// Note: Column spanning isn't directly supported, so we'll simulate it

	// Add data with smart truncation
	if c.limit > 0 && rowCount > c.limit {
		// Smart truncation: show top half and bottom half
		topRows := c.limit / 2
		bottomRows := c.limit - topRows

		// Add top rows
		for i := 0; i < topRows && i < rowCount; i++ {
			t.AppendRow(convertToTableRow(strValues[i]))
		}

		// Add truncation indication without separators
		hiddenRows := rowCount - c.limit

		// Create empty row, message row, empty row (like original)
		emptyRow := make(table.Row, len(columns))
		for i := 0; i < len(columns); i++ {
			emptyRow[i] = " " // Same space in all columns for AutoMerge
		}

		// Add empty row before message with AutoMerge for spanning
		t.AppendRow(emptyRow, table.RowConfig{AutoMerge: true})

		// Create a message that spans across columns using AutoMerge (true HTML colspan)
		message := fmt.Sprintf("... (%d more rows) ...", hiddenRows)
		truncationRow := make(table.Row, len(columns))

		// For AutoMerge to create true colspan, put the SAME content in ALL columns
		for i := 0; i < len(columns); i++ {
			truncationRow[i] = message
		}

		// Add the truncation row with horizontal auto-merge for spanning effect
		t.AppendRow(truncationRow, table.RowConfig{AutoMerge: true})

		// Add empty row after message with AutoMerge for spanning
		t.AppendRow(emptyRow, table.RowConfig{AutoMerge: true})

		// Add bottom rows
		startIdx := rowCount - bottomRows
		if startIdx < topRows {
			startIdx = topRows // Avoid overlap
		}
		for i := startIdx; i < rowCount; i++ {
			t.AppendRow(convertToTableRow(strValues[i]))
		}
	} else {
		// Add all rows
		for _, row := range strValues {
			t.AppendRow(convertToTableRow(row))
		}
	}

	// Render the table
	t.Render()

	// Print summary
	var rowText string
	if rowCount == 1 {
		rowText = "row"
	} else {
		rowText = "rows"
	}

	if c.limit > 0 && rowCount > c.limit {
		fmt.Printf("\033[1;32m%d %s in set (showing %d)\033[0m\n", rowCount, rowText, c.limit)
	} else {
		fmt.Printf("\033[1;32m%d %s in set\033[0m\n", rowCount, rowText)
	}

	return nil
}

// executeWriteQuery executes a query that doesn't return rows (INSERT, UPDATE, DELETE, etc.)
func (c *CLI) executeWriteQuery(query string) error {
	var result sql.Result
	var err error

	// Simple spinner for long-running write queries (only show if >1 second)
	var progressActive bool
	var progressDone = make(chan bool, 1)
	queryStartTime := time.Now() // Track query start time from the beginning

	if c.isInteractive {
		// Start spinner after 1 second delay
		go func() {
			select {
			case <-time.After(1 * time.Second):
				// Query is taking longer than 1 second, show spinner
				progressActive = true
				spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
				i := 0

				for progressActive {
					elapsed := time.Since(queryStartTime) // Use actual query start time
					fmt.Printf("\rExecuting query... %s [%.1fs]", spinner[i%len(spinner)], elapsed.Seconds())
					time.Sleep(100 * time.Millisecond)
					i++
				}
			case <-progressDone:
				// Query finished before 1 second, don't show spinner
				return
			}
		}()
	}

	// Execute the query using transaction if active
	if c.inTransaction {
		result, err = c.tx.ExecContext(c.ctx, query)
	} else {
		result, err = c.db.ExecContext(c.ctx, query)
	}

	// Stop spinner when query completes
	if c.isInteractive {
		progressActive = false
		select {
		case progressDone <- true:
		default: // Channel might be full
		}
		fmt.Print("\r\033[K") // Clear the spinner line completely
	}

	if err != nil {
		return err
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	// Handle JSON output format
	if c.jsonOutput {
		return c.outputWriteResultJSON(rowsAffected)
	}

	// Print result with better formatting
	var rowText string
	if rowsAffected == 1 {
		rowText = "row"
	} else {
		rowText = "rows"
	}
	fmt.Printf("\033[1;32m%d %s affected\033[0m\n", rowsAffected, rowText)

	return nil
}

// outputJSON outputs query results in JSON format
func (c *CLI) outputJSON(columns []string, values [][]interface{}, rowCount int) error {
	result := map[string]interface{}{
		"columns": columns,
		"rows":    values,
		"count":   rowCount,
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}

// outputWriteResultJSON outputs write query results in JSON format
func (c *CLI) outputWriteResultJSON(rowsAffected int64) error {
	result := map[string]interface{}{
		"rows_affected": rowsAffected,
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}

// printHelp displays help information
func (c *CLI) printHelp() {
	fmt.Println("\033[1mStoolap SQL CLI Commands:\033[0m")
	fmt.Println("")
	fmt.Println("  \033[1;33mSQL Commands:\033[0m")
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
	fmt.Println("  \033[1;33mConfiguration Commands:\033[0m")
	fmt.Println("    PRAGMA name            Get current value of configuration setting")
	fmt.Println("    PRAGMA name = value    Set configuration setting")
	fmt.Println("")
	fmt.Println("  \033[1;33mTransaction Commands:\033[0m")
	fmt.Println("    BEGIN                  Start a new transaction")
	fmt.Println("    COMMIT                 Commit the current transaction")
	fmt.Println("    ROLLBACK               Rollback the current transaction")
	fmt.Println("")
	fmt.Println("  \033[1;33mSpecial Commands:\033[0m")
	fmt.Println("    exit, quit, \\q         Exit the CLI")
	fmt.Println("    help, \\h, \\?          Show this help message")
	fmt.Println("")
	fmt.Println("  \033[1;33mKeyboard Shortcuts:\033[0m")
	fmt.Println("    Up/Down arrow keys     Navigate command history")
	fmt.Println("    Ctrl+R                 Search command history")
	fmt.Println("    Ctrl+A                 Move cursor to beginning of line")
	fmt.Println("    Ctrl+E                 Move cursor to end of line")
	fmt.Println("    Ctrl+W                 Delete word before cursor")
	fmt.Println("    Ctrl+U                 Delete from cursor to beginning of line")
	fmt.Println("    Ctrl+K                 Delete from cursor to end of line")
	fmt.Println("    Ctrl+L                 Clear screen")
	fmt.Println("")
}

// configureTable creates and configures a table writer with appropriate styling
func (c *CLI) configureTable() table.Writer {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	// Use rounded style for better visual appearance
	t.SetStyle(table.StyleRounded)

	// Configure auto-sizing
	t.SetAutoIndex(false)

	// Disable separate rows to match original table format (only header separator)
	t.Style().Options.SeparateRows = false

	// Don't use column AutoMerge as it hides duplicate data in consecutive rows
	// We only want row AutoMerge for specific truncation rows

	return t
}

// convertToTableRow converts a string slice to table.Row
func convertToTableRow(row []string) table.Row {
	tableRow := make(table.Row, len(row))
	for i, val := range row {
		tableRow[i] = val
	}
	return tableRow
}

// formatValue formats a value for display, based on its type
func formatValue(value interface{}, colType *sql.ColumnType) string {
	_ = colType // This is a placeholder for the actual column type, which can be used for more specific formatting

	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		// Check if it's potentially a JSON value
		strVal := string(v)
		if (strings.HasPrefix(strVal, "{") && strings.HasSuffix(strVal, "}")) ||
			(strings.HasPrefix(strVal, "[") && strings.HasSuffix(strVal, "]")) {
			// Format JSON more nicely for viewing
			return strVal
		}
		return strVal
	case string:
		return v
	case int64, int32, int16, int8, int:
		return fmt.Sprintf("%d", v)
	case float64:
		// Format with appropriate precision
		if v == float64(int64(v)) {
			return fmt.Sprintf("%.1f", v) // Integer value, show one decimal
		}
		return fmt.Sprintf("%.4g", v) // General format with 4 significant digits
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		// Format time/date values nicely
		t := v
		return t.Format(time.RFC3339)
	default:
		// Fall back to basic formatting
		return fmt.Sprintf("%v", v)
	}
}

// Close closes the CLI and cleans up resources
func (c *CLI) Close() error {
	if c.readline != nil {
		return c.readline.Close()
	}
	return nil
}
