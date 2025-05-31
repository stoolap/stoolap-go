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
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// MVCCEngine is an in-memory implementation of a storage engine with MVCC
type MVCCEngine struct {
	path    string
	config  *storage.Config
	schemas map[string]storage.Schema // Table schemas
	mu      sync.RWMutex
	open    atomic.Bool

	// For storing and managing MVCC versions
	versionStores map[string]*VersionStore // Table version stores

	// Transaction registry for this engine instance
	registry *TransactionRegistry

	// Table pool specific to this engine
	tablePool sync.Pool

	// Persistence layer
	persistence *PersistenceManager

	// Flag to indicate we're loading from disk to avoid triggering redundant snapshots
	loadingFromDisk atomic.Bool

	// File lock to prevent multiple processes from accessing the same database
	fileLock *FileLock

	// Cleanup related fields
	cleanupCancel func() // Function to stop the cleanup goroutine
}

// NewMVCCEngine creates a new MVCC storage engine
func NewMVCCEngine(config *storage.Config) *MVCCEngine {
	engine := &MVCCEngine{
		path:          config.Path,
		config:        config,
		schemas:       make(map[string]storage.Schema),
		versionStores: make(map[string]*VersionStore),
		registry:      NewTransactionRegistry(),
	}

	if engine.path == "" {
		// In-memory mode
		engine.path = "memory://"
	}

	// Initialize the loadingFromDisk flag to false
	engine.loadingFromDisk.Store(false)

	// Create engine-specific table pool
	engine.tablePool = sync.Pool{
		New: func() interface{} {
			return &MVCCTable{
				engine: engine,
			}
		},
	}

	// For file:// URLs, path will be set and persistence must be enabled
	// For memory:// URLs, path will be empty and persistence will be disabled
	persistence, err := NewPersistenceManager(engine, config.Path, config.Persistence)
	if err != nil {
		if config.Path != "" && config.Persistence.Enabled {
			// This is a critical error for file:// URLs where persistence is required
			log.Printf("Error: Failed to initialize required persistence for path %s: %v\n", config.Path, err)
			// Return the persistence manager anyway, but Open() will fail later
		}
	}

	// Set the persistence manager regardless of success/failure
	// If we're in memory-only mode, persistence will be disabled in the manager
	engine.persistence = persistence

	return engine
}

// Open opens the storage engine and initializes the MVCC system
func (e *MVCCEngine) Open() error {
	// Use atomic operation to avoid potential race conditions
	if e.open.Swap(true) {
		return nil // Already open
	}

	// If we're using a persistent database, acquire a file lock to prevent multiple processes
	// from accessing the same database simultaneously
	if e.path != "" && e.config.Persistence.Enabled {
		var err error
		e.fileLock, err = AcquireFileLock(e.path)
		if err != nil {
			// If we fail to acquire the lock, restore the open flag to false
			e.open.Store(false)
			return fmt.Errorf("failed to acquire file lock: %w", err)
		}
	}

	// Start periodic transaction cleanup (every 10 minutes, clean transactions older than 30 minutes)
	// These values could be made configurable if needed
	e.cleanupCancel = e.StartPeriodicCleanup(10*time.Minute, 30*time.Minute)

	// If persistence is enabled, start it and recover data
	if e.persistence != nil && e.persistence.IsEnabled() {
		// Start persistence manager
		err := e.persistence.Start()
		if err != nil {
			// If we fail to start persistence, release the file lock and restore the open flag
			if e.fileLock != nil {
				e.fileLock.Release()
				e.fileLock = nil
			}
			e.open.Store(false)
			return fmt.Errorf("failed to start persistence: %w", err)
		}

		// Load actual data synchronously to ensure data is available immediately
		err = e.persistence.LoadDiskStores()
		if err != nil {
			log.Printf("Warning: Error during data recovery: %v\n", err)
			// Continue anyway - the database can still function with partial data
		}

		// Verify data loaded correctly
		_, err = e.ListTables()
		if err != nil {
			log.Printf("Warning: Unable to list tables after data loading: %v\n", err)
		}
	}

	return nil
}

// Close closes the storage engine and cleans up resources with robust timeout protection
func (e *MVCCEngine) Close() error {
	// Skip if already closed - use atomic operation for thread safety
	if !e.open.CompareAndSwap(true, false) {
		return nil
	}

	var shutdownErrors []string

	// First stop accepting new transactions in the registry
	if e.registry != nil {
		e.registry.StopAcceptingTransactions()
	}

	// Stop the transaction cleanup goroutine if it's running
	if e.cleanupCancel != nil {
		e.cleanupCancel()
		e.cleanupCancel = nil
	}

	// Create an overall timeout for the entire shutdown sequence
	engineShutdownTimeout := time.After(5 * time.Second)

	// First wait for any active transactions to complete or timeout
	if e.registry != nil {
		// Wait for transactions with a timeout
		txnDone := make(chan struct{})

		go func() {
			count := e.registry.WaitForActiveTransactions(2 * time.Second)
			if count > 0 {
				log.Printf("Warning: %d transactions did not complete before timeout\n", count)
				shutdownErrors = append(shutdownErrors,
					fmt.Sprintf("%d transactions did not complete before timeout", count))
			}
			close(txnDone)
		}()

		// Wait for transaction completion or global timeout
		select {
		case <-txnDone:
			// Normal completion
		case <-engineShutdownTimeout:
			log.Println("Warning: Engine shutdown timeout reached during transaction wait")
			shutdownErrors = append(shutdownErrors, "engine shutdown timeout reached during transaction wait")
			// Continue with shutdown anyway
		}
	}

	// Close persistence layer with proper timeout handling
	var persistenceErr error
	if e.persistence != nil && e.persistence.IsEnabled() {
		persistenceDone := make(chan struct{})

		go func() {
			persistenceErr = e.persistence.Stop()
			if persistenceErr != nil {
				errMsg := fmt.Sprintf("error stopping persistence: %v", persistenceErr)
				shutdownErrors = append(shutdownErrors, errMsg)
				log.Printf("Warning: %s\n", errMsg)
			}
			close(persistenceDone)
		}()

		// Wait for persistence shutdown with a shorter timeout
		select {
		case <-persistenceDone:
		case <-time.After(3 * time.Second):
			errMsg := "persistence layer shutdown timed out"
			shutdownErrors = append(shutdownErrors, errMsg)
			log.Printf("Warning: %s, forcing cleanup\n", errMsg)
		case <-engineShutdownTimeout:
			errMsg := "engine shutdown timeout reached during persistence shutdown"
			shutdownErrors = append(shutdownErrors, errMsg)
			log.Printf("Warning: %s\n", errMsg)
		}
	}

	// Cleanup all remaining resources regardless of timeout outcomes
	cleanupDone := make(chan struct{})
	go func() {
		e.cleanupResources()
		close(cleanupDone)
	}()

	// Wait for cleanup or timeout
	select {
	case <-cleanupDone:
	case <-time.After(1 * time.Second):
		errMsg := "resource cleanup timed out"
		shutdownErrors = append(shutdownErrors, errMsg)
		log.Printf("Warning: %s\n", errMsg)
	case <-engineShutdownTimeout:
		errMsg := "engine shutdown timeout reached during resource cleanup"
		shutdownErrors = append(shutdownErrors, errMsg)
		log.Printf("Warning: %s\n", errMsg)
	}

	// Release file lock if we have one
	if e.fileLock != nil {
		if err := e.fileLock.Release(); err != nil {
			errMsg := fmt.Sprintf("error releasing file lock: %v", err)
			shutdownErrors = append(shutdownErrors, errMsg)
			log.Printf("Warning: %s\n", errMsg)
		}
		e.fileLock = nil
	}

	// Return combined errors if any occurred during shutdown
	if len(shutdownErrors) > 0 {
		return fmt.Errorf("engine shutdown completed with errors: %s", strings.Join(shutdownErrors, "; "))
	}
	return nil
}

// cleanupResources releases all resources held by the engine
func (e *MVCCEngine) cleanupResources() {
	// Clean up all version stores
	for _, vs := range e.versionStores {
		if vs != nil {
			vs.Close()
		}
	}

	// Clear maps to help GC
	clear(e.schemas)
	clear(e.versionStores)
}

// BeginTransaction starts a new transaction
func (e *MVCCEngine) BeginTransaction() (storage.Transaction, error) {
	sqlLevel := sql.LevelReadCommitted
	if e.registry != nil && e.GetIsolationLevel() != storage.ReadCommitted {
		sqlLevel = sql.LevelSnapshot
	}

	return e.BeginTx(context.Background(), sqlLevel)
}

// BeginTx starts a new transaction with the provided context
func (e *MVCCEngine) BeginTx(ctx context.Context, level sql.IsolationLevel) (storage.Transaction, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	// Map SQL isolation level to storage isolation level
	var specificIsolationLevel storage.IsolationLevel
	switch level {
	case sql.LevelReadCommitted:
		specificIsolationLevel = storage.ReadCommitted
	case sql.LevelSnapshot, sql.LevelRepeatableRead:
		specificIsolationLevel = storage.SnapshotIsolation
	default:
		return nil, fmt.Errorf("unsupported isolation level: %v", level)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Get transaction ID from registry
	txnID, _ := e.registry.BeginTransaction()
	if txnID == InvalidTransactionID {
		return nil, errors.New("transaction registry is not accepting new transactions")
	}

	if e.registry.GetGlobalIsolationLevel() != specificIsolationLevel {
		e.registry.SetTransactionIsolationLevel(txnID, specificIsolationLevel)
	}

	// Get a tables map from the pool to reduce allocations
	tablesMap := tablesMapPool.Get().(map[string]*MVCCTable)

	// Create transaction and initialize it
	txn := &MVCCTransaction{
		id:        txnID,
		startTime: time.Now(),
		engine:    e,
		tables:    tablesMap,
		active:    true,
		ctx:       ctx,

		specificIsolationLevel: specificIsolationLevel,
	}

	return txn, nil
}

// Path returns the database path
func (e *MVCCEngine) Path() string {
	return e.path
}

// GetConfig returns a copy of the current engine configuration
func (e *MVCCEngine) GetConfig() storage.Config {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Return a copy of the configuration to avoid race conditions
	configCopy := *e.config
	return configCopy
}

// UpdateConfig updates the engine configuration
func (e *MVCCEngine) UpdateConfig(config storage.Config) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate that the path hasn't changed (can't change paths for an open engine)
	if config.Path != e.config.Path {
		return fmt.Errorf("cannot change database path after opening")
	}

	// Update our configuration copy
	e.config = &config

	// Apply changes to the persistence manager if it exists
	if e.persistence != nil {
		err := e.persistence.UpdateConfig(config.Persistence)
		if err != nil {
			return fmt.Errorf("failed to update persistence configuration: %v", err)
		}
	}

	return nil
}

// TableExists checks if a table exists
func (e *MVCCEngine) TableExists(name string) (bool, error) {
	if !e.open.Load() {
		return false, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Normalize to lowercase for case-insensitive lookup
	name = strings.ToLower(name)
	_, exists := e.schemas[name]
	return exists, nil
}

// validateSchema validates the schema for common errors
func validateSchema(schema storage.Schema) error {
	if schema.TableName == "" {
		return errors.New("schema missing table name")
	}

	// Check for duplicate column names
	colNames := make(map[string]bool, len(schema.Columns))
	for _, col := range schema.Columns {
		if col.Name == "" {
			return errors.New("column name cannot be empty")
		}

		if col.PrimaryKey && col.Type != storage.INTEGER {
			return fmt.Errorf("primary key column %s must be of type INTEGER", col.Name)
		}

		if colNames[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		colNames[col.Name] = true
	}

	return nil
}

// CreateTable registers a table in the engine
// This is called by the transaction when a table is committed
func (e *MVCCEngine) CreateTable(schema storage.Schema) (storage.Schema, error) {
	if !e.open.Load() {
		return storage.Schema{}, errors.New("engine is not open")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	name := schema.TableName
	originalName := name         // Keep original name for version store
	name = strings.ToLower(name) // Normalize for case-insensitive storage

	if _, exists := e.schemas[name]; exists {
		return storage.Schema{}, storage.ErrTableAlreadyExists
	}

	// Validate schema
	err := validateSchema(schema)
	if err != nil {
		return storage.Schema{}, fmt.Errorf("invalid schema: %w", err)
	}

	// Create version store for this table (use original name for the store)
	e.versionStores[name] = NewVersionStore(originalName, e)

	// Store schema with lowercase key but preserve original name in schema
	e.schemas[name] = schema

	// Create disk store if persistence is enabled
	if e.persistence != nil && e.persistence.IsEnabled() {
		// Create a disk store for the table, even if there are no snapshots yet
		vs := e.versionStores[name]
		// Pass schema directly to avoid callback to engine.GetTableSchema
		diskStore, err := NewDiskVersionStore(e.persistence.path, name, vs, schema)
		if err != nil {
			log.Printf("Warning: Failed to create disk store for table %s: %v\n", name, err)
		} else {
			// Add the disk store to the persistence manager's map
			e.persistence.mu.Lock()
			e.persistence.diskStores[name] = diskStore
			e.persistence.mu.Unlock()
		}
	}

	// Record this DDL operation in the WAL
	// Skip if we're loading from disk or if persistence is not enabled
	if !e.loadingFromDisk.Load() && e.persistence != nil && e.persistence.IsEnabled() {
		// Serialize the schema for WAL
		schemaData, err := serializeSchema(&schema)
		if err != nil {
			log.Printf("Warning: Failed to serialize schema for WAL: %v\n", err)
		} else {
			// Record DDL operation in WAL
			err = e.persistence.RecordDDLOperation(name, WALCreateTable, schemaData)
			if err != nil {
				log.Printf("Warning: Failed to record DDL operation in WAL: %v\n", err)
			}
		}
	}

	return schema, nil
}

// GetTableSchema retrieves a table's schema
func (e *MVCCEngine) GetTableSchema(tableName string) (storage.Schema, error) {
	if !e.open.Load() {
		return storage.Schema{}, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Normalize to lowercase for case-insensitive lookup
	tableName = strings.ToLower(tableName)
	schema, exists := e.schemas[tableName]
	if !exists {
		return storage.Schema{}, storage.ErrTableNotFound
	}

	return schema, nil
}

// GetVersionStore retrieves a version store for a table
func (e *MVCCEngine) GetVersionStore(tableName string) (*VersionStore, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	// Normalize to lowercase for case-insensitive lookup
	originalName := tableName
	tableName = strings.ToLower(tableName)

	// First try with a read lock to check if the version store exists
	e.mu.RLock()

	// Check if the table exists
	if _, exists := e.schemas[tableName]; !exists {
		e.mu.RUnlock()
		return nil, storage.ErrTableNotFound
	}

	// If the version store exists, return it immediately
	vs, exists := e.versionStores[tableName]
	if exists {
		e.mu.RUnlock()
		return vs, nil
	}

	// Release the read lock before acquiring the write lock
	e.mu.RUnlock()

	// Now acquire a write lock to create and store the version store
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check again after acquiring the write lock
	// Someone else might have created it while we were waiting
	if vs, exists = e.versionStores[tableName]; exists {
		return vs, nil
	}

	// Double-check that the table still exists
	if _, exists := e.schemas[tableName]; !exists {
		return nil, storage.ErrTableNotFound
	}

	// Create and store the version store (use original name for the store itself)
	vs = NewVersionStore(originalName, e)
	e.versionStores[tableName] = vs

	return vs, nil
}

// GetTableFromPool gets a table object from the engine's pool
func (e *MVCCEngine) GetTableFromPool(txnID int64, versionStore *VersionStore) *MVCCTable {
	table := e.tablePool.Get().(*MVCCTable)
	table.txnID = txnID
	table.versionStore = versionStore
	table.engine = e
	table.txnVersions = NewTransactionVersionStore(versionStore, txnID)
	return table
}

// ReturnTableToPool returns a table to the engine's pool
func (e *MVCCEngine) ReturnTableToPool(table *MVCCTable) {
	// Clean up references before returning to pool
	table.txnVersions = nil
	table.versionStore = nil
	table.txnID = 0
	e.tablePool.Put(table)
}

// GetRegistry returns the transaction registry for this engine
func (e *MVCCEngine) GetRegistry() *TransactionRegistry {
	return e.registry
}

// GetIsolationLevel returns the current isolation level for this engine
func (e *MVCCEngine) GetIsolationLevel() storage.IsolationLevel {
	if !e.open.Load() {
		return -999 // Engine not open
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.registry.GetGlobalIsolationLevel()
}

// SetIsolationLevel sets the isolation level for all transactions in this engine
func (e *MVCCEngine) SetIsolationLevel(level storage.IsolationLevel) error {
	if !e.open.Load() {
		return errors.New("engine is not open")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if level != storage.ReadCommitted && level != storage.SnapshotIsolation {
		return errors.New("invalid isolation level")
	}

	e.registry.SetGlobalIsolationLevel(level)

	return nil
}

// CleanupOldTransactions removes transactions older than the specified duration
// Returns the number of transactions removed
func (e *MVCCEngine) CleanupOldTransactions(maxAge time.Duration) int {
	return e.registry.CleanupOldTransactions(maxAge)
}

// CleanupDeletedRows removes deleted rows that are older than maxAge from all tables
// Returns the total number of rows removed
func (e *MVCCEngine) CleanupDeletedRows(maxAge time.Duration) int {
	if !e.open.Load() {
		return 0 // Engine not open
	}

	totalRemoved := 0

	// Lock for accessing version stores
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Iterate through all tables and clean up deleted rows in each
	for _, vs := range e.versionStores {
		if vs != nil {
			totalRemoved += vs.CleanupDeletedRows(maxAge)
		}
	}

	return totalRemoved
}

// CleanupOldPreviousVersions removes previous versions that are no longer needed
// Returns the total number of previous versions removed
func (e *MVCCEngine) CleanupOldPreviousVersions() int {
	if !e.open.Load() {
		return 0 // Engine not open
	}

	totalCleaned := 0

	// Lock for accessing version stores
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Iterate through all tables and clean up previous versions in each
	for _, vs := range e.versionStores {
		if vs != nil {
			totalCleaned += vs.CleanupOldPreviousVersions()
		}
	}

	return totalCleaned
}

// EvictColdData evicts cold data from memory across all tables
// This helps manage memory usage by keeping only hot data in memory
// Returns the total number of rows evicted
func (e *MVCCEngine) EvictColdData(coldPeriod time.Duration, maxRowsPerTable int) int {
	if !e.open.Load() {
		return 0 // Engine not open
	}

	totalEvicted := 0

	// Lock for accessing version stores
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Iterate through all tables and evict cold data from each
	for _, vs := range e.versionStores {
		if vs != nil {
			totalEvicted += vs.EvictColdData(coldPeriod, maxRowsPerTable)
		}
	}

	return totalEvicted
}

// StartPeriodicCleanup starts a goroutine that periodically cleans up old transactions
// and deleted rows. Returns a function that can be called to stop the cleanup process.
func (e *MVCCEngine) StartPeriodicCleanup(interval, maxAge time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Clean up old transactions
				txnCount := e.CleanupOldTransactions(maxAge)

				// Clean up deleted rows using the same maxAge parameter
				// This keeps consistency between transaction and version cleanup
				rowCount := e.CleanupDeletedRows(maxAge)

				// Clean up old previous versions that are no longer needed
				prevVersionCount := e.CleanupOldPreviousVersions()

				// Evict cold data that hasn't been accessed in 2x maxAge
				// Limit to 1000 rows per table to avoid excessive disk I/O
				// evictedCount := e.EvictColdData(2*maxAge, 1000)

				// FIXME: Check cold data eviction logic
				evictedCount := 0

				if rowCount > 0 || txnCount > 0 || evictedCount > 0 || prevVersionCount > 0 {
					/* log.Printf("Cleanup: removed %d transactions, %d deleted rows older than %s, %d previous versions, evicted %d cold rows\n",
					txnCount, rowCount, maxAge, prevVersionCount, evictedCount) */
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return cancel
}

// CreateColumn adds a new column to a table's schema (DDL operation)
func (e *MVCCEngine) CreateColumn(tableName, columnName string, columnType storage.DataType, nullable bool) error {
	if !e.open.Load() {
		return errors.New("engine is not open")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get the schema
	schema, exists := e.schemas[tableName]
	if !exists {
		return storage.ErrTableNotFound
	}

	// Check if column already exists
	for _, col := range schema.Columns {
		if col.Name == columnName {
			return fmt.Errorf("column %s already exists", columnName)
		}
	}

	// Create new column
	newColumn := storage.SchemaColumn{
		ID:         len(schema.Columns),
		Name:       columnName,
		Type:       columnType,
		Nullable:   nullable,
		PrimaryKey: false, // New columns can't be primary keys
	}

	// Add to schema
	schema.Columns = append(schema.Columns, newColumn)
	schema.UpdatedAt = time.Now()

	// Update the schema
	e.schemas[tableName] = schema

	// Record this DDL operation in the WAL
	// Skip if we're loading from disk or if persistence is not enabled
	if !e.loadingFromDisk.Load() && e.persistence != nil && e.persistence.IsEnabled() {
		// Serialize the updated schema for WAL
		schemaData, err := serializeSchema(&schema)
		if err != nil {
			log.Printf("Warning: Failed to serialize schema for WAL: %v\n", err)
		} else {
			// Record DDL operation in WAL
			err = e.persistence.RecordDDLOperation(tableName, WALAlterTable, schemaData)
			if err != nil {
				log.Printf("Warning: Failed to record alter table operation in WAL: %v\n", err)
			}
		}
	}

	return nil
}

// DropColumn removes a column from a table's schema
func (e *MVCCEngine) DropColumn(tableName, columnName string) error {
	if !e.open.Load() {
		return errors.New("engine is not open")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Get the schema
	schema, exists := e.schemas[tableName]
	if !exists {
		return storage.ErrTableNotFound
	}

	// Find the column
	columnIndex := -1
	for i, col := range schema.Columns {
		if col.Name == columnName {
			columnIndex = i
			break
		}
	}

	if columnIndex == -1 {
		return fmt.Errorf("column %s not found", columnName)
	}

	// Check if column is a primary key
	if schema.Columns[columnIndex].PrimaryKey {
		return errors.New("cannot drop primary key column")
	}

	// Remove column from schema
	schema.Columns = append(schema.Columns[:columnIndex], schema.Columns[columnIndex+1:]...)
	schema.UpdatedAt = time.Now()

	// Update column IDs
	for i := columnIndex; i < len(schema.Columns); i++ {
		schema.Columns[i].ID = i
	}

	// Update the schema
	e.schemas[tableName] = schema

	// Record this DDL operation in the WAL
	// Skip if we're loading from disk or if persistence is not enabled
	if !e.loadingFromDisk.Load() && e.persistence != nil && e.persistence.IsEnabled() {
		// Serialize the updated schema for WAL
		schemaData, err := serializeSchema(&schema)
		if err != nil {
			log.Printf("Warning: Failed to serialize schema for WAL: %v\n", err)
		} else {
			// Record DDL operation in WAL
			err = e.persistence.RecordDDLOperation(tableName, WALAlterTable, schemaData)
			if err != nil {
				log.Printf("Warning: Failed to record alter table operation in WAL: %v\n", err)
			}
		}
	}

	return nil
}

// DropTable removes a table from the engine
// This is called by the transaction when a table drop is committed
func (e *MVCCEngine) DropTable(name string) error {
	if !e.open.Load() {
		return errors.New("engine is not open")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Keep original name for logging
	originalName := name
	// Normalize to lowercase for case-insensitive lookup
	name = strings.ToLower(name)

	if _, exists := e.schemas[name]; !exists {
		return storage.ErrTableNotFound
	}

	// Close the version store
	if vs, exists := e.versionStores[name]; exists && vs != nil {
		if err := vs.Close(); err != nil {
			return fmt.Errorf("failed to close version store: %w", err)
		}
	}

	// Close and remove disk store if persistence is enabled
	if e.persistence != nil && e.persistence.IsEnabled() {
		if diskStore, exists := e.persistence.diskStores[name]; exists && diskStore != nil {
			// Close the disk store before removing it from the map
			if err := diskStore.Close(); err != nil {
				log.Printf("Warning: Failed to close disk store for table %s: %v\n", originalName, err)
			}
			delete(e.persistence.diskStores, name)

			// Remove the table's directory with all snapshots
			tableDir := filepath.Join(e.persistence.path, name)
			if err := os.RemoveAll(tableDir); err != nil {
				log.Printf("Warning: Failed to remove snapshot directory for table %s: %v\n", name, err)
			}

		}
	}

	// Record this DDL operation in the WAL before removing the schema
	// Skip if we're loading from disk, if persistence is not enabled, or if we just recreated the WAL file
	// We'll check if this is the last table and if so, skip recording the drop operation
	skipWalRecord := false

	// Check if this is the last table (before we remove it from schemas)
	if e.persistence != nil && e.persistence.IsEnabled() {
		// We need to check if this is the only table left
		tableCount := len(e.schemas)
		if tableCount == 1 { // If this is the last table (the one we're about to drop)
			skipWalRecord = true
			// Delete the WAL file and create a fresh one
			if e.persistence.wal != nil {
				// Close the WAL file
				e.persistence.wal.Close()

				// Delete the WAL directory
				walDir := filepath.Join(e.persistence.path, "wal")
				if err := os.RemoveAll(walDir); err != nil {
					log.Printf("Warning: Failed to remove WAL directory after dropping last table: %v\n", err)
				} else {
					log.Printf("Removed WAL directory after dropping last table\n")
				}

				// Create a fresh WAL manager
				walPath := filepath.Join(e.persistence.path, "wal")
				newWal, err := NewWALManager(walPath, SyncMode(e.config.Persistence.SyncMode), &e.config.Persistence)
				if err != nil {
					log.Printf("Warning: Failed to create new WAL manager after dropping last table: %v\n", err)
				} else {
					e.persistence.wal = newWal
				}
			}
		}
	}

	if !e.loadingFromDisk.Load() && e.persistence != nil && e.persistence.IsEnabled() && !skipWalRecord {
		// Record DDL operation in WAL with nil schema data since we're dropping the table
		err := e.persistence.RecordDDLOperation(name, WALDropTable, nil)
		if err != nil {
			log.Printf("Warning: Failed to record drop table operation in WAL: %v\n", err)
		}
	}

	// Remove schema and version store
	delete(e.schemas, name)
	delete(e.versionStores, name)

	return nil
}

// ListTables returns a list of all tables
func (e *MVCCEngine) ListTables() ([]string, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	tables := make([]string, 0, len(e.schemas))
	for name := range e.schemas {
		tables = append(tables, name)
	}

	return tables, nil
}

// IndexExists checks if an index exists on a table
func (e *MVCCEngine) IndexExists(indexName string, tableName string) (bool, error) {
	if !e.open.Load() {
		return false, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// First check if the table exists
	_, tableExists := e.schemas[tableName]
	if !tableExists {
		return false, storage.ErrTableNotFound
	}

	// Get the version store
	vs, exists := e.versionStores[tableName]
	if !exists {
		return false, nil
	}

	// Check for the index in the version store
	exists = vs.IndexExists(indexName)
	return exists, nil
}

// ListTableIndexes lists all indexes for a table
func (e *MVCCEngine) ListTableIndexes(tableName string) (map[string]string, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if the table exists
	_, exists := e.schemas[tableName]
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Get the version store
	vs, exists := e.versionStores[tableName]
	if !exists {
		return map[string]string{}, nil
	}

	return vs.ListIndexes(), nil
}

// GetAllIndexes returns all index objects for a table, including multi-column indexes
func (e *MVCCEngine) GetAllIndexes(tableName string) ([]storage.Index, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if the table exists
	_, exists := e.schemas[tableName]
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Get the version store
	vs, exists := e.versionStores[tableName]
	if !exists {
		return []storage.Index{}, nil
	}

	// Lock the index mutex to ensure consistent access
	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	// Create a slice to hold all index objects
	indexes := make([]storage.Index, 0, len(vs.indexes))

	// Add all indexes to the slice
	for _, index := range vs.indexes {
		indexes = append(indexes, index)
	}

	return indexes, nil
}

// GetIndex returns an index by name for a specific table
func (e *MVCCEngine) GetIndex(tableName, indexName string) (storage.Index, error) {
	if !e.open.Load() {
		return nil, errors.New("engine is not open")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if the table exists
	_, exists := e.schemas[tableName]
	if !exists {
		return nil, storage.ErrTableNotFound
	}

	// Get the version store
	vs, exists := e.versionStores[tableName]
	if !exists {
		return nil, fmt.Errorf("version store for table %s not found", tableName)
	}

	// Get all indexes
	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	// First try to find by exact index name
	for _, index := range vs.indexes {
		if index.Name() == indexName {
			return index, nil
		}
	}

	// If not found, try looking for the column name as the index name
	if index, ok := vs.indexes[indexName]; ok {
		return index, nil
	}

	return nil, fmt.Errorf("index %s not found on table %s", indexName, tableName)
}
