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
	"fmt"
	"strings"
	"time"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// executeCreateIndex executes a CREATE INDEX statement
func (e *Executor) executeCreateIndex(tx storage.Transaction, stmt *parser.CreateIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Get the index name
	indexName := stmt.IndexName.Value

	// Check if the index already exists
	indexExists, err := e.engine.IndexExists(indexName, tableName)
	if err != nil {
		return err
	}
	if indexExists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("index %s already exists on table %s", indexName, tableName)
	}

	// Extract column names from the index definition
	columns := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = col.Value
	}

	// Determine if this is a unique index
	isUnique := stmt.IsUnique

	// Check for other indexes on the same columns with different uniqueness
	// This prevents creating both a unique and non-unique index on the same column(s)
	indexMap, err := e.engine.ListTableIndexes(tableName)
	if err != nil {
		return err
	}

	// We need to check each index to see if any of them cover the same columns
	for indexName := range indexMap {
		// Get the actual index object to examine its properties
		idx, err := e.engine.GetIndex(tableName, indexName)
		if err != nil {
			continue // Skip if we can't get the index
		}

		// Check if the index has the same columns and different uniqueness
		if hasSameColumns(columns, idx.ColumnNames()) && idx.IsUnique() != isUnique {
			if stmt.IfNotExists {
				// If IF NOT EXISTS is specified, silently ignore
				return nil
			}
			var currentType, existingType string
			if isUnique {
				currentType = "unique"
				existingType = "non-unique"
			} else {
				currentType = "non-unique"
				existingType = "unique"
			}
			return fmt.Errorf("cannot create %s index; a %s index already exists for column(s) %s on table %s",
				currentType, existingType, strings.Join(columns, ", "), tableName)
		}
	}

	// Create the index
	return tx.CreateTableIndex(tableName, indexName, columns, isUnique)
}

// executeDropIndex executes a DROP INDEX statement
func (e *Executor) executeDropIndex(tx storage.Transaction, stmt *parser.DropIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Get the index name
	indexName := stmt.IndexName.Value

	// Check if the index exists
	indexExists, err := e.engine.IndexExists(indexName, tableName)
	if err != nil {
		return err
	}
	if !indexExists {
		if stmt.IfExists {
			// If IF EXISTS is specified, don't error
			return nil
		}
		return fmt.Errorf("index %s does not exist on table %s", indexName, tableName)
	}

	// Drop the index
	return tx.DropTableIndex(tableName, indexName)
}

// executeCreateColumnarIndex executes a CREATE COLUMNAR INDEX statement
func (e *Executor) executeCreateColumnarIndex(tx storage.Transaction, stmt *parser.CreateColumnarIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Get the column name
	columnName := stmt.ColumnName.Value

	// Determine if this is a unique index
	isUnique := stmt.IsUnique

	// Generate default names for both unique and non-unique versions to check for duplicates
	uniqueIndexName := fmt.Sprintf("unique_columnar_%s_%s", tableName, columnName)
	nonUniqueIndexName := fmt.Sprintf("columnar_%s_%s", tableName, columnName)

	// Choose the appropriate name based on uniqueness
	var indexIdentifier string
	if isUnique {
		indexIdentifier = uniqueIndexName
	} else {
		indexIdentifier = nonUniqueIndexName
	}

	// First check if an index with this exact name already exists
	indexExists, err := e.engine.IndexExists(indexIdentifier, tableName)
	if err != nil {
		return err
	}

	if indexExists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("columnar index with name %s already exists on table %s", indexIdentifier, tableName)
	}

	// Now check for other indexes on the same column with different uniqueness
	// We'll use the same approach as in executeCreateIndex for consistency
	indexMap, err := e.engine.ListTableIndexes(tableName)
	if err != nil {
		return err
	}

	// We need to check each index to see if any of them cover this column with different uniqueness
	for indexName := range indexMap {
		// Skip checking the current index
		if indexName == indexIdentifier {
			continue
		}

		// Get the actual index object to examine its properties
		idx, err := e.engine.GetIndex(tableName, indexName)
		if err != nil {
			continue // Skip if we can't get the index
		}

		// For columnar indexes, we only need to check if it's on the same column
		columnNames := idx.ColumnNames()
		if len(columnNames) == 1 && columnNames[0] == columnName && idx.IsUnique() != isUnique {
			if stmt.IfNotExists {
				// If IF NOT EXISTS is specified, silently ignore
				return nil
			}
			var currentType, existingType string
			if isUnique {
				currentType = "unique"
				existingType = "non-unique"
			} else {
				currentType = "non-unique"
				existingType = "unique"
			}
			return fmt.Errorf("cannot create %s index; a %s index already exists for column %s on table %s",
				currentType, existingType, columnName, tableName)
		}
	}

	// Also check if the column name itself is used as an index name
	columnIndexExists, err := e.engine.IndexExists(columnName, tableName)
	if err != nil {
		return err
	}

	if columnIndexExists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("cannot create columnar index; an index with name %s already exists on table %s",
			columnName, tableName)
	}

	// Create the columnar index
	return tx.CreateTableColumnarIndex(tableName, columnName, isUnique, indexIdentifier)
}

// executeDropColumnarIndex executes a DROP COLUMNAR INDEX statement
func (e *Executor) executeDropColumnarIndex(tx storage.Transaction, stmt *parser.DropColumnarIndexStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Get the column name
	columnName := stmt.ColumnName.Value

	// First try to get the index to determine if it's unique
	// Try with the regular non-unique index name format
	nonUniqueIndexName := fmt.Sprintf("columnar_%s_%s", tableName, columnName)
	indexExists, err := e.engine.IndexExists(nonUniqueIndexName, tableName)
	if err != nil {
		return err
	}

	// If the non-unique index exists, drop it
	if indexExists {
		return tx.DropTableColumnarIndex(tableName, nonUniqueIndexName)
	}

	// If not found, try with the unique index name format
	uniqueIndexName := fmt.Sprintf("unique_columnar_%s_%s", tableName, columnName)
	indexExists, err = e.engine.IndexExists(uniqueIndexName, tableName)
	if err != nil {
		return err
	}

	// If the unique index exists, drop it
	if indexExists {
		return tx.DropTableColumnarIndex(tableName, uniqueIndexName)
	}

	// As a fallback, try with just the column name (for backward compatibility)
	indexExists, err = e.engine.IndexExists(columnName, tableName)
	if err != nil {
		return err
	}

	if indexExists {
		return tx.DropTableColumnarIndex(tableName, columnName)
	}

	// If we get here, the index doesn't exist under any name format
	if stmt.IfExists {
		// If IF EXISTS is specified, silently ignore
		return nil
	}
	return fmt.Errorf("columnar index for column %s not exists on table %s", columnName, tableName)
}

// executeAlterTable executes an ALTER TABLE statement
func (e *Executor) executeAlterTable(tx storage.Transaction, stmt *parser.AlterTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		return storage.ErrTableNotFound
	}

	// Process the table operation based on the operation type
	switch stmt.Operation {
	case parser.AddColumn:
		// Handle ADD COLUMN operation
		if stmt.ColumnDef == nil {
			return fmt.Errorf("missing column definition for ADD COLUMN operation")
		}

		// Convert the column definition
		dataType, err := convertDataTypeFromString(stmt.ColumnDef.Type)
		if err != nil {
			return err
		}

		// Create the storage column
		// Get the table to get a proper column implementation
		table, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		// Create the column directly on the table
		nullable := true // Default to nullable
		if stmt.ColumnDef.Constraints != nil {
			for _, constraint := range stmt.ColumnDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		return table.CreateColumn(stmt.ColumnDef.Name.Value, dataType, nullable)

	case parser.DropColumn:
		// Handle DROP COLUMN operation
		if stmt.ColumnName == nil {
			return fmt.Errorf("missing column name for DROP COLUMN operation")
		}

		// Drop the column
		return tx.DropTableColumn(tableName, stmt.ColumnName.Value)

	case parser.RenameColumn:
		// Handle RENAME COLUMN operation
		if stmt.ColumnName == nil || stmt.NewColumnName == nil {
			return fmt.Errorf("missing column names for RENAME COLUMN operation")
		}

		// Rename the column
		return tx.RenameTableColumn(tableName, stmt.ColumnName.Value, stmt.NewColumnName.Value)

	case parser.ModifyColumn:
		// Handle MODIFY COLUMN operation
		if stmt.ColumnDef == nil {
			return fmt.Errorf("missing column definition for MODIFY COLUMN operation")
		}

		// Convert the column definition
		dataType, err := convertDataTypeFromString(stmt.ColumnDef.Type)
		if err != nil {
			return err
		}

		// Get the table to modify the column
		table, err := tx.GetTable(tableName)
		if err != nil {
			return err
		}

		// First drop the old column
		err = table.DropColumn(stmt.ColumnDef.Name.Value)
		if err != nil {
			return err
		}

		// Create the column with new properties
		nullable := true // Default to nullable
		if stmt.ColumnDef.Constraints != nil {
			for _, constraint := range stmt.ColumnDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		// Modify the column
		return table.CreateColumn(stmt.ColumnDef.Name.Value, dataType, nullable)

	case parser.RenameTable:
		// Handle RENAME TABLE operation
		if stmt.NewTableName == nil {
			return fmt.Errorf("missing new table name for RENAME TABLE operation")
		}

		// Rename the table
		return tx.RenameTable(tableName, stmt.NewTableName.Value)

	default:
		return fmt.Errorf("unsupported ALTER TABLE operation: %v", stmt.Operation)
	}
}

// executeCreateTable executes a CREATE TABLE statement
func (e *Executor) executeCreateTable(tx storage.Transaction, stmt *parser.CreateTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table already exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if exists {
		if stmt.IfNotExists {
			// If IF NOT EXISTS is specified, silently ignore
			return nil
		}
		return fmt.Errorf("table %s already exists", tableName)
	}

	// Create a schema for the table
	schema := storage.Schema{
		TableName: tableName,
		Columns:   make([]storage.SchemaColumn, len(stmt.Columns)),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Fill in column definitions
	for i, colDef := range stmt.Columns {
		// Convert the column type
		dataType, err := convertDataTypeFromString(colDef.Type)
		if err != nil {
			return err
		}

		// Check for NOT NULL constraint
		nullable := true
		if colDef.Constraints != nil {
			for _, constraint := range colDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "NOT NULL") {
					nullable = false
					break
				}
			}
		}

		// Check for PRIMARY KEY constraint
		primaryKey := false
		if colDef.Constraints != nil {
			for _, constraint := range colDef.Constraints {
				if strings.Contains(strings.ToUpper(constraint.String()), "PRIMARY KEY") {
					primaryKey = true
					break
				}
			}
		}

		// Add column to schema (normalize column name to lowercase)
		schema.Columns[i] = storage.SchemaColumn{
			ID:         i,
			Name:       strings.ToLower(colDef.Name.Value),
			Type:       dataType,
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		}
	}

	// Create the table with the schema
	_, err = tx.CreateTable(tableName, schema)
	return err
}

// executeDropTable executes a DROP TABLE statement
func (e *Executor) executeDropTable(tx storage.Transaction, stmt *parser.DropTableStatement) error {
	// Get the table name
	tableName := stmt.TableName.Value

	// Check if the table exists
	exists, err := e.engine.TableExists(tableName)
	if err != nil {
		return err
	}
	if !exists {
		if stmt.IfExists {
			// If IF EXISTS is specified, don't error
			return nil
		}
		return storage.ErrTableNotFound
	}

	// Drop the table
	return tx.DropTable(tableName)
}

// hasSameColumns checks if two column name lists cover exactly the same columns
// Order doesn't matter for this comparison
func hasSameColumns(columns1, columns2 []string) bool {
	if len(columns1) != len(columns2) {
		return false
	}

	// Create maps for O(1) lookup
	set1 := make(map[string]struct{}, len(columns1))
	for _, col := range columns1 {
		set1[col] = struct{}{}
	}

	// Check if all columns in columns2 are in columns1
	for _, col := range columns2 {
		if _, exists := set1[col]; !exists {
			return false
		}
	}

	return true
}
