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
package mvcc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

var (
	ErrTransactionClosed = errors.New("transaction already closed")

	// Global pool for tables map to reduce allocations
	tablesMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]*MVCCTable)
		},
	}
)

// MVCCTransaction is an in-memory implementation of a transaction
type MVCCTransaction struct {
	id        int64
	startTime time.Time
	engine    *MVCCEngine
	tables    map[string]*MVCCTable
	active    bool
	mu        sync.RWMutex
	ctx       context.Context // Context for the transaction

	// Fast path for single table operations
	lastTableName string
	lastTable     storage.Table

	specificIsolationLevel storage.IsolationLevel
}

// ID returns the transaction ID
func (t *MVCCTransaction) ID() int64 {
	return t.id
}

func (t *MVCCTransaction) SetIsolationLevel(level storage.IsolationLevel) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// Only set the transaction-specific isolation level
	// Don't modify the engine-level setting
	t.specificIsolationLevel = level

	return nil
}

// SetContext sets the context for the transaction
func (t *MVCCTransaction) SetContext(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ctx = ctx
}

// Begin starts the transaction
// This is now a no-op for compatibility, as the transaction is fully initialized by NewMVCCTransaction or BeginTx
func (t *MVCCTransaction) Begin() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// The transaction ID and registry are now set during creation
	return nil
}

// Commit commits the transaction
func (t *MVCCTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	defer t.cleanUp()

	// First check if this is a read-only transaction BEFORE committing tables
	// since committing tables will clear their local versions
	isReadOnly := true

	// Collect all DML operations before committing tables
	type dmlOp struct {
		tableName string
		rowID     int64
		version   RowVersion
	}
	var dmlOperations []dmlOp

	// Check if any tables have modifications and collect them
	for tableName, table := range t.tables {
		if table.txnVersions != nil && table.txnVersions.localVersions.Len() > 0 {
			isReadOnly = false
			// Collect all operations from this table
			table.txnVersions.localVersions.ForEach(func(rowID int64, version RowVersion) bool {
				dmlOperations = append(dmlOperations, dmlOp{
					tableName: tableName,
					rowID:     rowID,
					version:   version,
				})
				return true
			})
		}
	}

	// Get the effective isolation level for this transaction
	effectiveIsolationLevel := t.engine.GetIsolationLevel()
	if t.specificIsolationLevel != 0 {
		effectiveIsolationLevel = t.specificIsolationLevel
	}

	if effectiveIsolationLevel == storage.SnapshotIsolation && !isReadOnly {
		// Commit each table with conflict checking
		for tableName, table := range t.tables {
			t.engine.AcquireTableLock(tableName)

			if table.txnVersions != nil {
				if err := table.Commit(); err != nil {
					t.engine.ReleaseTableLock(tableName)
					t.engine.registry.AbortTransaction(t.id)
					return err
				}
			}

			// Release lock immediately after committing each table
			t.engine.ReleaseTableLock(tableName)
		}

		// Only after all data is written, mark transaction as committed in registry
		t.engine.registry.CommitTransaction(t.id)
	} else {
		// For READ COMMITTED or read-only transactions, no serialization needed
		// But we still need to track write sequences for conflict detection by SNAPSHOT transactions

		// Collect written rows if not read-only
		var writtenRowsByTable map[string][]int64
		if !isReadOnly {
			writtenRowsByTable = make(map[string][]int64)
			for tableName, table := range t.tables {
				if table.txnVersions != nil && table.txnVersions.localVersions.Len() > 0 {
					versionStore := t.engine.versionStores[tableName]
					if versionStore != nil {
						var writtenRows []int64
						table.txnVersions.localVersions.ForEach(func(rowID int64, version RowVersion) bool {
							writtenRows = append(writtenRows, rowID)
							return true
						})
						writtenRowsByTable[tableName] = writtenRows
					}
				}
			}
		}

		// First commit all MVCC tables to merge their changes to the global version stores
		for _, table := range t.tables {
			if err := table.Commit(); err != nil {
				// If commit fails, abort the transaction
				t.engine.registry.AbortTransaction(t.id)
				return err
			}
		}

		// Only after data is written, mark transaction as committed in registry
		t.engine.registry.CommitTransaction(t.id)
	}

	// Record in WAL if persistence is enabled AND transaction modified data
	if t.engine.persistence != nil && t.engine.persistence.IsEnabled() {
		// Use the isReadOnly flag we computed BEFORE committing tables
		// At this point, most table.txnVersions will be nil because Commit() cleared them
		if !isReadOnly {
			// First record all DML operations to WAL
			for _, op := range dmlOperations {
				err := t.engine.persistence.RecordDMLOperation(t.id, op.tableName, op.rowID, op.version)
				if err != nil {
					log.Printf("Warning: Failed to record DML operation in WAL: %v", err)
				}
			}

			// Collect auto-increment values from tables that have been modified in this transaction
			var autoIncrementInfo []struct {
				TableName string
				Value     int64
			}

			// Check for any version stores that may have had their auto-increment counters updated
			for tableName, vs := range t.engine.versionStores {
				autoIncValue := vs.GetCurrentAutoIncrementValue()
				if autoIncValue > 0 {
					autoIncrementInfo = append(autoIncrementInfo, struct {
						TableName string
						Value     int64
					}{
						TableName: tableName,
						Value:     autoIncValue,
					})
				}
			}

			// Record commit with auto-increment information
			err := t.engine.persistence.RecordCommit(t.id, autoIncrementInfo...)
			if err != nil {
				log.Printf("Warning: Failed to record commit in WAL: %v\n", err)
			}
		}
	}

	t.active = false

	return nil
}

// Rollback rolls back the transaction
func (t *MVCCTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	defer t.cleanUp()

	// Mark transaction as rolled back in registry
	t.engine.registry.AbortTransaction(t.id)

	// First check if this is a read-only transaction BEFORE rolling back tables
	// since rollback will clear the local versions
	isReadOnly := true

	// Check if any tables have modifications
	for _, table := range t.tables {
		if table.txnVersions != nil && table.txnVersions.localVersions.Len() > 0 {
			isReadOnly = false
			break
		}
	}

	// Rollback all tables
	for _, table := range t.tables {
		table.Rollback()
	}

	// Record in WAL if persistence is enabled AND transaction modified data
	if t.engine.persistence != nil && t.engine.persistence.IsEnabled() {
		// Use the isReadOnly flag we computed BEFORE committing tables
		// At this point, most table.txnVersions will be nil because Commit() cleared them
		if !isReadOnly {
			err := t.engine.persistence.RecordRollback(t.id)
			if err != nil {
				log.Printf("Warning: Failed to record rollback in WAL: %v\n", err)
			}
		}
	}

	t.active = false

	return nil
}

// cleanUp cleans up the transaction resources
func (t *MVCCTransaction) cleanUp() error {
	if t.active {
		return errors.New("transaction is still active")
	}

	// Clear fast path cache
	t.lastTableName = ""
	t.lastTable = nil

	// Return the tables to the pool
	engine := t.engine
	for _, table := range t.tables {
		engine.ReturnTableToPool(table)
	}

	// Clear and return the tables map to the pool
	// Save a reference first since we'll set t.tables to nil
	tablesMap := t.tables
	t.tables = nil
	clear(tablesMap)
	tablesMapPool.Put(tablesMap)

	t.engine.registry.RemoveTransactionIsolationLevel(t.id)

	return nil
}

// AddTableColumn adds a column to a table
func (t *MVCCTransaction) AddTableColumn(tableName string, column storage.SchemaColumn) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Add the column to the table
	return table.CreateColumn(column.Name, column.Type, column.Nullable)
}

// DropTableColumn drops a column from a table
func (t *MVCCTransaction) DropTableColumn(tableName string, columnName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Drop the column from the table
	return table.DropColumn(columnName)
}

// RenameTableColumn renames a column in a table
func (t *MVCCTransaction) RenameTableColumn(tableName string, oldName, newName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Create a basic implementation for testing purposes
	tableSchema := table.Schema()

	// Check if oldName exists and newName doesn't exist
	oldColIdx := -1
	for i, col := range tableSchema.Columns {
		if col.Name == oldName {
			oldColIdx = i
		} else if col.Name == newName {
			return errors.New("column with new name already exists")
		}
	}

	if oldColIdx == -1 {
		return errors.New("column with old name not found")
	}

	// For a real implementation, we'd need more sophisticated column rename handling
	// This is a minimal implementation that modifies the schema and relies on the underlying
	// storage implementations to handle the rename

	// First create a copy of the column with the new name
	oldCol := tableSchema.Columns[oldColIdx]

	// Update the schema
	// First create a copy of the old column to preserve its values
	if err := table.CreateColumn(newName, oldCol.Type, oldCol.Nullable); err != nil {
		return err
	}

	// Drop the old column
	return table.DropColumn(oldName)
}

// ModifyTableColumn modifies a column in a table
func (t *MVCCTransaction) ModifyTableColumn(tableName string, column storage.SchemaColumn) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Create a basic implementation for testing purposes
	tableSchema := table.Schema()

	// Check if the column exists
	colName := column.Name
	colExists := false

	for _, col := range tableSchema.Columns {
		if col.Name == colName {
			colExists = true
			break
		}
	}

	if !colExists {
		return errors.New("column not found")
	}

	// For a real implementation, we'd need more sophisticated column modification handling
	// This is a simplified approach that:
	// 1. Creates a new column with the modified type
	// 2. Drops the old column

	// Create a new column with the new type
	if err := table.CreateColumn(colName+"_temp", column.Type, column.Nullable); err != nil {
		return err
	}

	// In a real implementation, we would copy the data from the old column to the new one,
	// possibly with type conversion. For this test implementation, we skip that step.

	// Drop the old column
	if err := table.DropColumn(colName); err != nil {
		return err
	}

	// Create the new column with the correct name
	if err := table.CreateColumn(colName, column.Type, column.Nullable); err != nil {
		return err
	}

	// Drop the temporary column
	return table.DropColumn(colName + "_temp")
}

// RenameTable renames a table
func (t *MVCCTransaction) RenameTable(oldName, newName string) error {
	t.mu.RLock()

	if !t.active {
		t.mu.RUnlock()
		return ErrTransactionClosed
	}

	t.mu.RUnlock()

	//TODO: Implement renaming of tables in the engine
	return nil
}

// Select performs a selection query on a table
func (t *MVCCTransaction) Select(tableName string, columnsToFetch []string, expr storage.Expression, originalColumns ...string) (storage.Result, error) {
	// We don't acquire locks here since GetTable handles its own locking

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table schema
	schema := table.Schema()

	// Find the column indices in the schema
	var columnIndices []int
	if len(columnsToFetch) > 0 {
		columnIndices = make([]int, 0, len(columnsToFetch))

		// Map column names to indices
		colMap := make(map[string]int)
		for i, col := range schema.Columns {
			colMap[col.Name] = i
		}

		// Look up each requested column
		for _, colName := range columnsToFetch {
			if idx, ok := colMap[colName]; ok {
				columnIndices = append(columnIndices, idx)
			} else {
				return nil, fmt.Errorf("column not found: %s", colName)
			}
		}
	}
	// Scan the table with the column indices and expression
	scanner, err := table.Scan(columnIndices, expr)
	if err != nil {
		return nil, err
	}

	// Return a result that wraps the scanner
	return NewTableResult(t.ctx, scanner, schema, columnIndices, originalColumns...), nil
}

// SelectWithAliases executes a SELECT query with a complex expression filter
// This allows pushing complex expressions (including AND/OR combinations) down to the storage layer
func (t *MVCCTransaction) SelectWithAliases(tableName string, columnsToFetch []string, filter storage.Expression, aliases map[string]string, originalColumns ...string) (storage.Result, error) {
	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Get the table schema
	schema := table.Schema()

	// Apply aliases to the expression if needed
	var expr storage.Expression
	if filter != nil && aliases != nil && len(aliases) > 0 {
		expr = filter.WithAliases(aliases)
	} else {
		expr = filter
	}

	// Find the column indices in the schema, resolving aliases
	var columnIndices []int
	if len(columnsToFetch) > 0 {
		columnIndices = make([]int, 0, len(columnsToFetch))

		// Map column names to indices
		colMap := make(map[string]int)
		for i, col := range schema.Columns {
			colMap[col.Name] = i
		}

		// Look up each requested column
		for _, colName := range columnsToFetch {
			// If colName is an alias, use the original column name
			actualColName := colName
			if origCol, isAlias := aliases[colName]; isAlias {
				actualColName = origCol
			}

			if idx, ok := colMap[actualColName]; ok {
				columnIndices = append(columnIndices, idx)
			} else {
				return nil, fmt.Errorf("column not found: %s", colName)
			}
		}
	}

	// Scan the table with the column indices and expression
	scanner, err := table.Scan(columnIndices, expr)
	if err != nil {
		return nil, err
	}

	// Create a base result
	baseResult := NewTableResult(t.ctx, scanner, schema, columnIndices, originalColumns...)

	// Wrap the result with alias handling
	return NewAliasedResult(baseResult, aliases), nil
}

// CreateTable creates a new table
func (t *MVCCTransaction) CreateTable(name string, schema storage.Schema) (storage.Table, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Check if table already exists
	if _, exists := t.tables[name]; exists {
		return nil, storage.ErrTableAlreadyExists
	}

	// Check if table exists in engine
	exists, err := t.engine.TableExists(name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, storage.ErrTableAlreadyExists
	}

	_, err = t.engine.CreateTable(schema)
	if err != nil {
		return nil, err
	}

	// Get the version store for this table
	versionStore, err := t.engine.GetVersionStore(name)
	if err != nil {
		return nil, err
	}

	// Create a table from the engine's pool
	mvccTable := t.engine.GetTableFromPool(t.id, versionStore)

	// Store table in transaction
	t.tables[name] = mvccTable

	return mvccTable, nil
}

// DropTable drops a table
func (t *MVCCTransaction) DropTable(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	// Check if table exists in transaction
	if table, exists := t.tables[name]; exists {
		// Return the table to the pool
		t.engine.ReturnTableToPool(table)
		delete(t.tables, name)
	}

	// Drop the table in the engine
	return t.engine.DropTable(name)
}

// GetTable returns a table wrapper with optimized memory usage
func (t *MVCCTransaction) GetTable(name string) (storage.Table, error) {
	// Quick early check if transaction is closed before acquiring any locks
	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Normalize table name for case-insensitive lookup
	name = strings.ToLower(name)

	// Fast path for repeated access to the same table (common in batch operations)
	if name == t.lastTableName && t.lastTable != nil {
		return t.lastTable, nil
	}

	// Try with a read lock to see if the table exists in the transaction's tables map
	t.mu.RLock()
	if table, exists := t.tables[name]; exists {
		// Found in the transaction's tables map, no need to cache again
		t.mu.RUnlock()

		// Update fast path cache
		t.lastTableName = name
		t.lastTable = table

		return table, nil
	}
	t.mu.RUnlock()

	// If we get here, the table doesn't exist in our transaction's tables map.
	// We need to acquire a write lock to possibly modify the tables map.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check again after getting the write lock as it might have been added
	if table, exists := t.tables[name]; exists {
		// Update fast path cache
		t.lastTableName = name
		t.lastTable = table
		return table, nil
	}

	// Check if table exists in engine (GetTableSchema already handles case-insensitive lookup)
	var schema storage.Schema
	var err error
	schema, err = t.engine.GetTableSchema(name)
	if err != nil {
		return nil, err
	}

	// Unused variable, but kept for clarity
	_ = schema

	// Get the version store for this table
	versionStore, err := t.engine.GetVersionStore(name)
	if err != nil {
		return nil, err
	}

	// Get a table from the engine's pool
	mvccTable := t.engine.GetTableFromPool(t.id, versionStore)

	// Store the table in the map for future use
	t.tables[name] = mvccTable

	// Also update the fast path cache for quicker lookups
	t.lastTableName = name
	t.lastTable = mvccTable

	return mvccTable, nil
}

// ListTables returns a list of all tables
func (t *MVCCTransaction) ListTables() ([]string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.active {
		return nil, ErrTransactionClosed
	}

	// Get list of tables from engine
	engineTables, err := t.engine.ListTables()
	if err != nil {
		return nil, err
	}

	// Create a map to track all tables (for easier deduplication)
	allTables := make(map[string]bool)

	// Add engine tables
	for _, table := range engineTables {
		allTables[table] = true
	}

	// Add tables created in this transaction
	for tableName := range t.tables {
		allTables[tableName] = true
	}

	// Convert map keys to slice
	result := make([]string, 0, len(allTables))
	for tableName := range allTables {
		result = append(result, tableName)
	}

	return result, nil
}

// CreateTableIndex creates a new index on a table
func (t *MVCCTransaction) CreateTableIndex(tableName string, indexName string, columns []string, isUnique bool) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table implements the necessary methods for indexing
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support indexing")
	}

	// Create the index
	return mvccTable.CreateIndex(indexName, columns, isUnique)
}

// DropTableIndex drops an index from a table
func (t *MVCCTransaction) DropTableIndex(tableName string, indexName string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table implements the necessary methods for indexing
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support indexing")
	}

	// Drop the index
	return mvccTable.DropIndex(indexName)
}

// CreateTableColumnarIndex creates a columnar index on a table column
// These indexes provide HTAP (Hybrid Transactional/Analytical Processing) capabilities
// If customName is provided, it will be used as the index name instead of the auto-generated one
func (t *MVCCTransaction) CreateTableColumnarIndex(tableName string, columnName string, isUnique bool, customName ...string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table is an MVCC table
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support columnar indexing")
	}

	// Create the columnar index, passing the custom name if provided
	if len(customName) > 0 && customName[0] != "" {
		return mvccTable.CreateColumnarIndex(columnName, isUnique, customName[0])
	}

	// Create with default name
	return mvccTable.CreateColumnarIndex(columnName, isUnique)
}

// DropTableColumnarIndex drops a columnar index by name
// The indexName parameter can be either the index name or the column name (for backward compatibility)
func (t *MVCCTransaction) DropTableColumnarIndex(tableName string, indexName string) error {
	if !t.active {
		return ErrTransactionClosed
	}

	// Get the table
	table, err := t.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if the table is an MVCC table
	mvccTable, ok := table.(*MVCCTable)
	if !ok {
		return errors.New("table does not support columnar indexing")
	}

	// Use MVCCTable's drop columnar index method with the index name
	return mvccTable.DropColumnarIndex(indexName)
}
