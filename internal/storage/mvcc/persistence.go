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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/binser"
)

// Default persistence configuration values
const (
	// Snapshot related defaults
	DefaultSnapshotInterval = 5 * time.Minute // 5 minutes between snapshots

	// Schema header size for reading
	DefaultSchemaHeaderSize = 8192 // 8KB buffer for schema reading
)

// PersistenceManager coordinates all disk operations
type PersistenceManager struct {
	path       string
	wal        *WALManager
	engine     *MVCCEngine
	meta       *PersistenceMeta
	diskStores map[string]*DiskVersionStore // Map of table names to disk version stores
	mu         sync.RWMutex                 // Protects diskStores map

	snapshotInterval    time.Duration
	keepCount           int
	enabled             bool
	cleanWalAfterReplay bool // Flag to clean up WAL after replay completes
}

// PersistenceMeta tracks metadata about persistence state
type PersistenceMeta struct {
	lastSnapshotTimeNano atomic.Int64  // Last snapshot time in Unix nanoseconds
	lastSnapshotLSN      atomic.Uint64 // LSN covered by the last snapshot
	lastWALLSN           atomic.Uint64 // Used during recovery
}

// IndexMetadata stores essential information about an index
type IndexMetadata struct {
	Name        string             // Index name
	TableName   string             // Table the index belongs to
	ColumnNames []string           // Names of the columns this index is for
	ColumnIDs   []int              // IDs of the columns in the table schema
	DataTypes   []storage.DataType // Types of data in the index
	IsUnique    bool               // Whether the index enforces uniqueness
}

// SerializeIndexMetadata converts index metadata to binary format
func SerializeIndexMetadata(index storage.Index) ([]byte, error) {
	if index == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	// Use binser for consistent binary encoding
	writer := binser.NewWriter()
	defer writer.Release()

	// Write index name and table name
	writer.WriteString(index.Name())
	writer.WriteString(index.TableName())

	// Get column names, IDs, and data types from the interface
	columnNames := index.ColumnNames()
	columnIDs := index.ColumnIDs()
	dataTypes := index.DataTypes()

	// Write column count
	writer.WriteUint16(uint16(len(columnNames)))

	// Write column names
	for _, name := range columnNames {
		writer.WriteString(name)
	}

	// Write column IDs
	for _, id := range columnIDs {
		writer.WriteInt32(int32(id))
	}

	// Write data types
	writer.WriteUint16(uint16(len(dataTypes)))
	for _, dataType := range dataTypes {
		writer.WriteUint8(uint8(dataType))
	}

	// Write unique flag
	writer.WriteBool(index.IsUnique())

	return writer.Bytes(), nil
}

// DeserializeIndexMetadata recreates index metadata from binary data
func DeserializeIndexMetadata(data []byte) (*IndexMetadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty metadata")
	}

	reader := binser.NewReader(data)
	meta := &IndexMetadata{}

	var err error

	// Read index name and table name
	meta.Name, err = reader.ReadString()
	if err != nil {
		return nil, fmt.Errorf("error reading index name: %w", err)
	}

	meta.TableName, err = reader.ReadString()
	if err != nil {
		return nil, fmt.Errorf("error reading table name: %w", err)
	}

	// Read column count
	columnCount, err := reader.ReadUint16()
	if err != nil {
		return nil, fmt.Errorf("error reading column count: %w", err)
	}

	// Read column names
	meta.ColumnNames = make([]string, columnCount)
	for i := uint16(0); i < columnCount; i++ {
		meta.ColumnNames[i], err = reader.ReadString()
		if err != nil {
			return nil, fmt.Errorf("error reading column name %d: %w", i, err)
		}
	}

	// Read column IDs
	meta.ColumnIDs = make([]int, columnCount)
	for i := uint16(0); i < columnCount; i++ {
		colID, err := reader.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("error reading column ID %d: %w", i, err)
		}
		meta.ColumnIDs[i] = int(colID)
	}

	// Read data types
	dataTypeCount, err := reader.ReadUint16()
	if err != nil {
		return nil, fmt.Errorf("error reading data type count: %w", err)
	}

	meta.DataTypes = make([]storage.DataType, dataTypeCount)
	for i := uint16(0); i < dataTypeCount; i++ {
		dataType, err := reader.ReadUint8()
		if err != nil {
			return nil, fmt.Errorf("error reading data type %d: %w", i, err)
		}
		meta.DataTypes[i] = storage.DataType(dataType)
	}

	// Read unique flag
	meta.IsUnique, err = reader.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("error reading unique flag: %w", err)
	}

	return meta, nil
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(engine *MVCCEngine, path string, config storage.PersistenceConfig) (*PersistenceManager, error) {
	// For memory:// URLs, path will be empty and config.Enabled will be false
	// For file:// URLs, path will be set and config.Enabled will be true
	if path == "" {
		// Memory-only mode
		return &PersistenceManager{
			enabled: false,
			engine:  engine,
		}, nil
	}

	// For file:// URLs, persistence must be enabled
	if !config.Enabled {
		return nil, fmt.Errorf("persistence must be enabled when using file:// scheme with path: %s", path)
	}

	// Create base directory
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create persistence directory: %w", err)
	}

	// Initialize WAL
	walPath := filepath.Join(path, "wal")
	// Create a copy of the config to pass to WALManager
	configCopy := config
	wal, err := NewWALManager(walPath, SyncMode(config.SyncMode), &configCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	// Read checkpoint file to get initial LSN value
	checkpointFile := filepath.Join(walPath, "checkpoint.meta")
	checkpointMeta, checkpointErr := ReadCheckpointMeta(checkpointFile)

	// Use checkpoint LSN as initial lastWALLSN if available
	var initialLSN uint64 = 0
	if checkpointErr == nil && checkpointMeta.LSN > 0 {
		initialLSN = checkpointMeta.LSN
	}

	// Configure WAL settings
	if config.WALFlushTrigger > 0 {
		wal.flushTrigger = uint64(config.WALFlushTrigger)
	}

	// Set maximum WAL size
	if config.WALMaxSize > 0 {
		wal.maxWALSize = uint64(config.WALMaxSize)
	} else {
		wal.maxWALSize = DefaultWALMaxSize // Default max WAL size
	}

	// Configure snapshot interval
	var snapshotInterval time.Duration
	if config.SnapshotInterval > 0 {
		snapshotInterval = time.Duration(config.SnapshotInterval) * time.Second
	} else {
		snapshotInterval = DefaultSnapshotInterval // Default snapshot interval
	}

	// Handle default keepCount for snapshots
	keepCount := config.KeepSnapshots
	if keepCount <= 0 {
		keepCount = DefaultKeepSnapshots
	}

	pm := &PersistenceManager{
		path:                path,
		wal:                 wal,
		meta:                &PersistenceMeta{},
		engine:              engine,
		enabled:             true,
		snapshotInterval:    snapshotInterval,
		keepCount:           keepCount,
		diskStores:          make(map[string]*DiskVersionStore),
		cleanWalAfterReplay: false,
	}

	pm.meta.lastWALLSN.Store(initialLSN) // Set initial LSN from checkpoint if available

	return pm, nil
}

// IsEnabled returns whether persistence is enabled
func (pm *PersistenceManager) IsEnabled() bool {
	return pm.enabled
}

// Start begins persistence operations
func (pm *PersistenceManager) Start() error {
	if !pm.enabled {
		return nil
	}

	// Initialize disk stores for all tables
	for tableName, vs := range pm.engine.versionStores {
		diskStore, err := NewDiskVersionStore(pm.path, tableName, vs)
		if err != nil {
			return fmt.Errorf("failed to create disk store for table %s: %w", tableName, err)
		}

		// Load existing snapshots
		if err := diskStore.LoadSnapshots(); err != nil {
			return fmt.Errorf("failed to load snapshots for table %s: %w", tableName, err)
		}

		pm.mu.Lock()
		pm.diskStores[tableName] = diskStore
		pm.mu.Unlock()
	}

	// Start periodic snapshot task
	go pm.runSnapshotTask()

	return nil
}

// runSnapshotTask performs periodic snapshots of all tables
func (pm *PersistenceManager) runSnapshotTask() {
	ticker := time.NewTicker(pm.snapshotInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !pm.enabled {
			return
		}

		// Take snapshots of all tables
		pm.mu.RLock()
		// Make a copy of the map to avoid holding the lock for too long
		diskStoresCopy := make(map[string]*DiskVersionStore)
		for tableName, diskStore := range pm.diskStores {
			diskStoresCopy[tableName] = diskStore
		}
		pm.mu.RUnlock()

		for tableName, diskStore := range diskStoresCopy {
			// Check if the table still exists in the engine
			pm.engine.mu.RLock()
			_, tableExists := pm.engine.schemas[tableName]
			pm.engine.mu.RUnlock()

			if !tableExists {
				// Table was dropped but diskStore wasn't properly cleaned up
				// Close and remove it from diskStores map
				if err := diskStore.Close(); err != nil {
					log.Printf("Warning: Failed to close stale disk store for table %s: %v\n", tableName, err)
				}
				pm.mu.Lock()
				delete(pm.diskStores, tableName)
				pm.mu.Unlock()

				// Remove the table's directory with all snapshots
				tableDir := filepath.Join(pm.path, tableName)
				if err := os.RemoveAll(tableDir); err != nil {
					log.Printf("Warning: Failed to remove snapshot directory for stale table %s: %v\n", tableName, err)
				}

				log.Printf("Warning: Found stale disk store for dropped table %s, removing it\n", tableName)
				continue
			}

			if err := diskStore.CreateSnapshot(); err != nil {
				log.Printf("Error: creating snapshot for table %s: %v\n", tableName, err)
				continue
			}

			// Update last snapshot time
			pm.meta.lastSnapshotTimeNano.Store(time.Now().UnixNano())

			// Clean up old snapshots if we have more than keepCount
			if pm.keepCount > 0 {
				tableDir := filepath.Join(pm.path, tableName)
				if err := pm.cleanupOldSnapshots(tableDir, pm.keepCount); err != nil {
					log.Printf("Warning: Failed to clean up old snapshots for table %s: %v\n", tableName, err)
				}
			}
		}
	}
}

// Stop halts persistence operations using a clean, fault-tolerant approach
// with multiple layers of timeout protection to prevent indefinite hanging
func (pm *PersistenceManager) Stop() error {
	if !pm.enabled {
		return nil
	}

	// Create overall completion channel with timeout
	done := make(chan struct{})
	var shutdownErr error

	// First mark WAL as not accepting new operations immediately
	// This ensures no new operations are started while shutting down
	// Do this outside the goroutine to make sure it happens even if the shutdown times out
	if pm.wal != nil {
		pm.wal.running.Store(false)
	}

	// Create a context with timeout for the entire shutdown operation
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Run shutdown sequence in a separate goroutine
	go func() {
		defer close(done)

		// Close all disk stores first
		pm.mu.RLock()
		diskStoresCopy := make(map[string]*DiskVersionStore)
		for tableName, store := range pm.diskStores {
			diskStoresCopy[tableName] = store
		}
		pm.mu.RUnlock()

		for tableName, store := range diskStoresCopy {
			// Create a separate timeout for each disk store
			storeTimeout := time.After(1 * time.Second)
			storeDone := make(chan struct{})

			go func(name string, s *DiskVersionStore) {
				defer close(storeDone)
				if err := s.Close(); err != nil {
					log.Printf("Warning: Error closing disk store for %s: %v\n", name, err)
				}
			}(tableName, store)

			// Wait for disk store closing or timeout
			select {
			case <-storeDone:
				// Store closed normally
			case <-storeTimeout:
				log.Printf("Warning: Disk store close for %s timed out\n", tableName)
			case <-ctx.Done():
				log.Println("Warning: Overall shutdown timeout reached during disk store closing")
				return
			}
		}

		// Flush WAL without holding locks for longer than necessary
		if pm.wal != nil {
			// Try to flush safely with timeout
			flushComplete := make(chan struct{})
			var flushErr error

			go func() {
				defer close(flushComplete)

				// Try to acquire lock with timeout
				lockAcquired := make(chan struct{})
				go func() {
					select {
					case <-ctx.Done():
						return
					default:
						pm.wal.mu.Lock()
						close(lockAcquired)
					}
				}()

				// Wait for lock with timeout
				select {
				case <-lockAcquired:
					// Acquired lock normally
					var bufferData []byte
					if pm.wal.walBuffer != nil && pm.wal.walBuffer.Len() > 0 {
						bufferData = pm.wal.walBuffer.Bytes()
						pm.wal.walBuffer.Reset()
					}
					pm.wal.mu.Unlock()

					// Write buffer to file without holding lock
					if len(bufferData) > 0 && pm.wal.walFile != nil {
						_, flushErr = pm.wal.walFile.Write(bufferData)
						if flushErr != nil {
							log.Printf("Error: flushing WAL buffer: %v\n", flushErr)
						} else {
							// Force sync with timeout
							syncComplete := make(chan struct{})
							go func() {
								OptimizedSync(pm.wal.walFile)
								close(syncComplete)
							}()

							select {
							case <-syncComplete:
								// Sync completed normally
							case <-time.After(500 * time.Millisecond):
								log.Println("Warning: WAL sync during flush timed out")
							case <-ctx.Done():
								log.Println("warning: Overall shutdown timeout reached during WAL sync")
								return
							}
						}
					}
				case <-time.After(500 * time.Millisecond):
					log.Println("Warning: WAL lock acquisition during flush timed out")
				case <-ctx.Done():
					log.Println("Warning: Overall shutdown timeout reached during WAL lock acquisition")
					return
				}
			}()

			// Wait for flush with timeout
			select {
			case <-flushComplete:
			case <-time.After(1 * time.Second):
				log.Println("Warning: WAL flush operation timed out")
			case <-ctx.Done():
				log.Println("Warning: Overall shutdown timeout reached during WAL flush")
				return
			}

			// Now close WAL
			walCloseDone := make(chan error)

			go func() {
				walCloseDone <- pm.wal.Close()
			}()

			// Wait for WAL close with timeout
			select {
			case err := <-walCloseDone:
				if err != nil {
					shutdownErr = fmt.Errorf("error closing WAL: %w", err)
					log.Printf("Error: closing WAL: %v\n", err)
				}
			case <-time.After(1 * time.Second):
				shutdownErr = fmt.Errorf("WAL manager close operation timed out")
				log.Println("Warning: WAL manager close operation timed out")
			case <-ctx.Done():
				shutdownErr = fmt.Errorf("overall shutdown timeout reached during WAL close")
				log.Println("Warning: Overall shutdown timeout reached during WAL close")
				return
			}
		}
	}()

	// Wait for completion with timeout
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		shutdownErr = fmt.Errorf("persistence manager shutdown timed out")
		log.Println("Warning: Persistence manager shutdown timed out, forcing exit")
	}

	// Clean up any other resources regardless of timeout
	// Clear the disk stores map to release resources
	pm.mu.Lock()
	pm.diskStores = make(map[string]*DiskVersionStore)
	pm.mu.Unlock()

	return shutdownErr
}

// findSnapshotFiles finds all snapshot files in a directory
func findSnapshotFiles(dirPath string) ([]string, error) {
	// Check if directory exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil, err
	}

	// Read directory entries
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Filter snapshot files
	var snapshotFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".bin") {
			snapshotFiles = append(snapshotFiles, entry.Name())
		}
	}

	return snapshotFiles, nil
}

// LoadData loads data from disk and replays WAL
func (pm *PersistenceManager) LoadDiskStores() error {
	if !pm.enabled {
		return nil
	}

	// Make sure the loading flag is set (should already be set from LoadSchemas)
	pm.engine.loadingFromDisk.Store(true)

	// Ensure we reset the flag when we're done, regardless of errors
	defer pm.engine.loadingFromDisk.Store(false)

	// 1. Try to load latest snapshots for all tables
	tables, err := pm.loadSnapshots()
	if err != nil {
		log.Printf("Warning: Error loading snapshots: %v. Falling back to WAL-only recovery.\n", err)
		// Create an empty tables map since we're doing WAL-only recovery
		tables = make(map[string]*storage.Schema)

		// Load schemas directly from engine
		e := pm.engine
		e.mu.RLock()
		for name, schema := range e.schemas {
			schemaCopy := schema // Make a copy to avoid reference issues
			tables[name] = &schemaCopy
		}
		e.mu.RUnlock()
	}

	// 2. Replay WAL entries
	// Get the latest snapshot timestamp
	latestSnapshotTime := pm.getLatestSnapshotTime()

	// Use the lastWALLSN from the metadata as the starting point for replay
	replayFromLSN := pm.meta.lastWALLSN.Load()

	// If we loaded a snapshot, we should only replay entries newer than the snapshot
	snapshotTimestampNano := int64(0)
	lastLSNFromSnapshot := uint64(0)

	if !latestSnapshotTime.IsZero() {
		snapshotTimestampNano = latestSnapshotTime.UnixNano()

		// Try to get the LSN corresponding to the snapshot timestamp
		// This is a heuristic to help with debugging
		lastLSNFromSnapshot = pm.meta.lastWALLSN.Load()

		// If we have a valid LSN from snapshot metadata and it's greater than our current replay point,
		// update the replay start position
		if lastLSNFromSnapshot > 0 && lastLSNFromSnapshot > replayFromLSN {
			replayFromLSN = lastLSNFromSnapshot
		}
	}

	// Start replay from the correct LSN
	lastLSN, err := pm.wal.Replay(replayFromLSN, func(entry WALEntry) error {
		// Skip entries with empty table names except for COMMIT operations
		// This handles system-level operations correctly
		if entry.TableName == "" && entry.Operation != WALCommit {
			return nil
		}

		// Apply entries only if they're newer than our snapshot
		// This ensures we don't double-apply data already in snapshots
		// Treat all operations consistently - both data and transaction ops
		if snapshotTimestampNano == 0 || entry.Timestamp > snapshotTimestampNano {
			// Apply the entry to the appropriate table
			return pm.applyWALEntry(entry, tables)
		}

		// Skip entry - it's already captured in the snapshot
		return nil
	})
	if err != nil {
		log.Printf("Warning: Error during WAL replay: %v\n", err)
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Update the last LSN
	pm.meta.lastWALLSN.Store(lastLSN)

	// Clean up WAL after replay if all tables were dropped
	if pm.cleanWalAfterReplay {
		// Check if there are no tables left
		pm.engine.mu.RLock()
		noTablesLeft := len(pm.engine.schemas) == 0
		pm.engine.mu.RUnlock()

		if noTablesLeft && pm.wal != nil {
			// Close the current WAL
			pm.wal.Close()

			// Delete the WAL directory
			walDir := filepath.Join(pm.path, "wal")
			if err := os.RemoveAll(walDir); err != nil {
				log.Printf("Warning: Failed to remove WAL directory after dropping all tables: %v\n", err)
			}

			// Create a fresh WAL manager
			walPath := filepath.Join(pm.path, "wal")
			newWal, err := NewWALManager(walPath, SyncMode(pm.engine.config.Persistence.SyncMode), &pm.engine.config.Persistence)
			if err != nil {
				log.Printf("Warning: Failed to create new WAL manager after dropping all tables: %v\n", err)
			} else {
				pm.wal = newWal
			}

			// Reset the flag
			pm.cleanWalAfterReplay = false
		}
	}

	return nil
}

// getLatestSnapshotTime returns the timestamp of the most recent snapshot
func (pm *PersistenceManager) getLatestSnapshotTime() time.Time {
	var latestTime time.Time

	// If the path doesn't exist, return zero time
	if _, err := os.Stat(pm.path); os.IsNotExist(err) {
		return latestTime
	}

	// Iterate through all tables to find the latest snapshot timestamp
	pm.mu.RLock()
	diskStoresCopy := make(map[string]*DiskVersionStore)
	for tableName, diskStore := range pm.diskStores {
		diskStoresCopy[tableName] = diskStore
	}
	pm.mu.RUnlock()

	for tableName, diskStore := range diskStoresCopy {
		// Skip tables without a disk store
		if diskStore == nil {
			continue
		}

		// Find snapshot files for this table
		tableDir := filepath.Join(pm.path, tableName)
		snapshotFiles, err := findSnapshotFiles(tableDir)
		if err != nil || len(snapshotFiles) == 0 {
			continue
		}

		// Sort snapshots by name (which includes timestamp), newest first
		sort.Sort(sort.Reverse(sort.StringSlice(snapshotFiles)))
		latestSnapshot := snapshotFiles[0]

		// Extract timestamp from filename
		// Format: snapshot-YYYYMMDD-HHMMSS.SSS.bin
		if strings.HasPrefix(latestSnapshot, "snapshot-") {
			// Extract timestamp part
			timestampPart := strings.TrimPrefix(latestSnapshot, "snapshot-")
			timestampPart = strings.TrimSuffix(timestampPart, ".bin")

			// Parse the timestamp using local timezone
			snapshotTime, err := time.ParseInLocation("20060102-150405.000", timestampPart, time.Local)
			if err == nil && snapshotTime.After(latestTime) {
				latestTime = snapshotTime
			}
		}
	}

	// If no timestamp found from disk stores, use the lastSnapshotTimeNano from metadata
	if latestTime.IsZero() {
		lastSnapshotNano := pm.meta.lastSnapshotTimeNano.Load()
		if lastSnapshotNano > 0 {
			latestTime = time.Unix(0, lastSnapshotNano)
		}
	}

	return latestTime
}

// loadSnapshots loads tables from disk using the DiskVersionStore instances
func (pm *PersistenceManager) loadSnapshots() (map[string]*storage.Schema, error) {
	tables := make(map[string]*storage.Schema)

	// If the base directory doesn't exist yet, return empty result
	if _, err := os.Stat(pm.path); os.IsNotExist(err) {
		return tables, nil
	}

	// First, scan disk for table directories with snapshots
	entries, err := os.ReadDir(pm.path)
	if err != nil {
		return tables, fmt.Errorf("failed to read tables directory: %w", err)
	}

	// Find tables on disk that have snapshots
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		tableName := entry.Name()

		// Skip if already processed
		if _, tableExists := tables[tableName]; tableExists {
			continue
		}

		// Check if this directory contains snapshots
		tableDir := filepath.Join(pm.path, tableName)
		snapshotFiles, err := findSnapshotFiles(tableDir)
		if err != nil || len(snapshotFiles) == 0 {
			continue
		}

		// Sort snapshots by name (which includes timestamp), newest first
		sort.Sort(sort.Reverse(sort.StringSlice(snapshotFiles)))
		latestSnapshot := snapshotFiles[0]

		// Read the schema from the snapshot file
		binPath := filepath.Join(pm.path, tableName, latestSnapshot)
		reader, err := NewDiskReader(binPath)
		if err != nil {
			log.Printf("Warning: Failed to create disk reader for snapshot %s: %v\n", binPath, err)
			continue
		}

		// Get schema from the snapshot
		schema := reader.GetSchema()
		reader.Close()

		if schema == nil {
			log.Printf("Warning: Failed to get schema from snapshot for table %s\n", tableName)
			continue
		}

		// Ensure schema has the correct table name
		schema.TableName = tableName

		// Create the table in the engine if it doesn't exist
		pm.engine.mu.Lock()
		vs, tableExists := pm.engine.versionStores[tableName]

		if !tableExists {
			// Table doesn't exist in engine, create it
			pm.engine.schemas[tableName] = *schema
			vs = NewVersionStore(tableName, pm.engine)
			pm.engine.versionStores[tableName] = vs
		}
		pm.engine.mu.Unlock()

		// Create disk version store
		diskStore, err := NewDiskVersionStore(pm.path, tableName, vs, *schema)
		if err != nil {
			log.Printf("Warning: Failed to create disk store for table %s: %v\n", tableName, err)
			continue
		}

		// Load snapshots
		if err := diskStore.LoadSnapshots(); err != nil {
			log.Printf("Warning: Failed to load snapshots for table %s: %v\n", tableName, err)
			continue
		}

		// Add to disk stores map
		pm.mu.Lock()
		pm.diskStores[tableName] = diskStore
		pm.mu.Unlock()
		tables[tableName] = schema
	}

	// Now handle tables that exist in memory but might not have been processed yet
	for tableName, vs := range pm.engine.versionStores {
		// Skip any tables that already have a DiskVersionStore or are already in tables map
		pm.mu.RLock()
		_, hasStore := pm.diskStores[tableName]
		pm.mu.RUnlock()
		if hasStore {
			continue
		}
		if _, hasTable := tables[tableName]; hasTable {
			continue
		}

		// Create a new disk version store
		diskStore, err := NewDiskVersionStore(pm.path, tableName, vs)
		if err != nil {
			log.Printf("Warning: Failed to create disk store for table %s: %v\n", tableName, err)
			continue
		}

		// Load snapshots for this table (may be empty)
		if err := diskStore.LoadSnapshots(); err != nil {
			log.Printf("Warning: Failed to load snapshots for table %s: %v\n", tableName, err)
			continue
		}

		// Add to disk stores map
		pm.mu.Lock()
		pm.diskStores[tableName] = diskStore
		pm.mu.Unlock()

		// Get and store the schema
		schema, err := vs.GetTableSchema()
		if err != nil {
			log.Printf("Warning: Failed to get schema for table %s: %v\n", tableName, err)
			continue
		}

		tables[tableName] = &schema
	}

	return tables, nil
}

// applyWALEntry applies a WAL entry to the in-memory structures
func (pm *PersistenceManager) applyWALEntry(entry WALEntry, tables map[string]*storage.Schema) error {
	// Optimize for commit entries
	if entry.Operation == WALCommit {
		// Register the committed transaction in the registry
		pm.engine.registry.RecoverCommittedTransaction(entry.TxnID, entry.Timestamp)

		// Check if this commit contains auto-increment values
		if len(entry.Data) > 0 {
			reader := binser.NewReader(entry.Data)

			// Read number of tables with auto-increment info
			count, err := reader.ReadUint32()
			if err == nil && count > 0 {
				// Process each table's auto-increment value
				for i := uint32(0); i < count; i++ {
					tableName, err := reader.ReadString()
					if err != nil {
						log.Printf("Warning: Error reading table name from commit record: %v\n", err)
						continue
					}

					value, err := reader.ReadInt64()
					if err != nil {
						log.Printf("Warning: Error reading auto-increment value from commit record: %v\n", err)
						continue
					}

					// Find the version store for this table and update its counter
					if vs, exists := pm.engine.versionStores[tableName]; exists {
						vs.SetAutoIncrementCounter(value)
					}
				}
			}
		}

		return nil
	}

	if entry.Operation == WALRollback {
		pm.engine.registry.RecoverAbortedTransaction(entry.TxnID)
		return nil
	}

	// Handle DDL operations
	if entry.Operation == WALCreateTable {
		if len(entry.Data) > 0 {
			// Deserialize schema data from the WAL entry
			schema, err := deserializeSchema(entry.Data)
			if err != nil {
				return fmt.Errorf("failed to deserialize schema for table %s: %w", entry.TableName, err)
			}

			// Ensure the table name matches
			if schema.TableName != entry.TableName {
				schema.TableName = entry.TableName
			}

			// Add the schema to our tables map
			tables[entry.TableName] = schema

			// Create table in engine
			pm.engine.mu.Lock()
			// Create version store for this table
			vs, exists := pm.engine.versionStores[entry.TableName]
			if !exists {
				vs = NewVersionStore(entry.TableName, pm.engine)
				pm.engine.versionStores[entry.TableName] = vs
			}
			// Store schema
			pm.engine.schemas[entry.TableName] = *schema
			pm.engine.mu.Unlock()

			// Create disk store for the table if it doesn't exist
			pm.mu.RLock()
			_, exists = pm.diskStores[entry.TableName]
			pm.mu.RUnlock()
			if !exists && pm.enabled {
				diskStore, err := NewDiskVersionStore(pm.path, entry.TableName, vs)
				if err != nil {
					log.Printf("Warning: Failed to create disk store for table %s: %v\n", entry.TableName, err)
				} else {
					// Load any existing snapshots (will be empty if none exist)
					if err := diskStore.LoadSnapshots(); err != nil {
						log.Printf("Warning: Failed to load snapshots for table %s: %v\n", entry.TableName, err)
					}
					// Add the disk store to the persistence manager's map
					pm.mu.Lock()
					pm.diskStores[entry.TableName] = diskStore
					pm.mu.Unlock()
				}
			}

			return nil
		}
	}

	// Handle index operations
	if entry.Operation == WALCreateIndex {
		if len(entry.Data) > 0 {
			// Deserialize index metadata from the WAL entry
			indexMeta, err := DeserializeIndexMetadata(entry.Data)
			if err != nil {
				return fmt.Errorf("failed to deserialize index metadata: %w", err)
			}

			// Ensure the table exists
			pm.engine.mu.RLock()
			vs, exists := pm.engine.versionStores[entry.TableName]
			pm.engine.mu.RUnlock()

			if !exists {
				return fmt.Errorf("table %s not found for index creation", entry.TableName)
			}

			_, err = vs.CreateIndex(indexMeta)
			if err != nil {
				return fmt.Errorf("failed to create index %s on table %s: %w", indexMeta.Name, entry.TableName, err)
			}

			return nil
		}
	}

	if entry.Operation == WALDropIndex {
		if len(entry.Data) > 0 {
			// The index name is stored as a simple string
			indexName := string(entry.Data)

			// Get the version store for this table
			pm.engine.mu.RLock()
			vs, exists := pm.engine.versionStores[entry.TableName]
			pm.engine.mu.RUnlock()

			if !exists {
				return fmt.Errorf("table %s not found for index drop", entry.TableName)
			}

			// Remove the index
			err := vs.RemoveIndex(indexName)
			if err != nil {
				log.Printf("Warning: WAL replay - failed to remove index '%s': %v\n", indexName, err)
				// Continue despite the error since this is just a warning
			}

			return nil
		}
	}

	// Handle table drop operation
	if entry.Operation == WALDropTable {
		// Remove from tables map
		delete(tables, entry.TableName)

		// Remove disk store if it exists
		pm.mu.Lock()
		diskStore, exists := pm.diskStores[entry.TableName]
		if exists && diskStore != nil {
			// Close the disk store before removing it from the map
			if err := diskStore.Close(); err != nil {
				log.Printf("Warning: Failed to close disk store for table %s: %v\n", entry.TableName, err)
			}
			delete(pm.diskStores, entry.TableName)
		}
		pm.mu.Unlock()

		// Remove the table's directory with all snapshots
		tableDir := filepath.Join(pm.path, entry.TableName)
		if err := os.RemoveAll(tableDir); err != nil {
			log.Printf("Warning: Failed to remove snapshot directory for table %s: %v\n", entry.TableName, err)
		}

		// Drop from engine
		pm.engine.mu.Lock()
		delete(pm.engine.schemas, entry.TableName)
		if vs, exists := pm.engine.versionStores[entry.TableName]; exists && vs != nil {
			vs.Close()
			delete(pm.engine.versionStores, entry.TableName)
		}

		// Check if this was the last table
		if len(pm.engine.schemas) == 0 {
			// Set a flag to clean up the WAL after replay is complete
			// We can't do it right now because we're in the middle of replaying the WAL
			pm.cleanWalAfterReplay = true
		}
		pm.engine.mu.Unlock()

		return nil
	}

	// Handle table alter operation
	if entry.Operation == WALAlterTable {
		if len(entry.Data) > 0 {
			// Deserialize schema data from the WAL entry
			schema, err := deserializeSchema(entry.Data)
			if err != nil {
				return fmt.Errorf("failed to deserialize schema for altered table %s: %w", entry.TableName, err)
			}

			// Update the schema in our tables map
			tables[entry.TableName] = schema

			// Update schema in engine
			pm.engine.mu.Lock()
			pm.engine.schemas[entry.TableName] = *schema
			pm.engine.mu.Unlock()

			return nil
		}
	}

	// For data operations, we need to make sure the table exists
	// This is for atomicity since snapshot might have failed
	if _, exists := tables[entry.TableName]; !exists {
		// Try to get the schema from the engine directly
		pm.engine.mu.RLock()
		schema, exists := pm.engine.schemas[entry.TableName]
		pm.engine.mu.RUnlock()

		if !exists {
			return fmt.Errorf("table %s not found", entry.TableName)
		}

		// Create a copy and add to tables map
		schemaCopy := schema
		tables[entry.TableName] = &schemaCopy
	}

	// Get the version store for this table
	vs, err := pm.engine.GetVersionStore(entry.TableName)
	if err != nil {
		return err
	}

	// Apply the operation
	switch entry.Operation {
	case WALInsert, WALUpdate:
		// Create a new row version
		row, err := deserializeRow(entry.Data)
		if err != nil {
			return err
		}

		vs.AddVersion(entry.RowID, RowVersion{
			TxnID:          entry.TxnID,
			DeletedAtTxnID: 0, // Not deleted
			Data:           row,
			RowID:          entry.RowID,
			CreateTime:     entry.Timestamp,
		})

	case WALDelete:
		// For WAL delete, we should ideally have the row data, but if not available, use nil
		row, _ := deserializeRow(entry.Data) // Ignore error, use nil if deserialization fails

		vs.AddVersion(entry.RowID, RowVersion{
			TxnID:          entry.TxnID,
			DeletedAtTxnID: entry.TxnID, // Deleted by this transaction
			Data:           row,
			RowID:          entry.RowID,
			CreateTime:     entry.Timestamp,
		})
	}

	return nil
}

// RecordDDLOperation records a DDL operation in the WAL
func (pm *PersistenceManager) RecordDDLOperation(tableName string, op WALOperationType, schemaData []byte) error {
	if !pm.enabled || pm.wal == nil {
		return nil
	}

	// Check if WAL manager is running
	if !pm.wal.running.Load() {
		return fmt.Errorf("WAL manager is not running")
	}

	// Create WAL entry - use a system transaction ID (-999) for DDL operations
	entry := WALEntry{
		TxnID:     -999, // System transaction for DDL
		TableName: tableName,
		RowID:     0, // Not applicable for DDL
		Operation: op,
		Data:      schemaData,
		Timestamp: time.Now().UnixNano(),
	}

	// Write the entry directly to the WAL - the AppendEntry method handles its own locking
	// AppendEntry and AppendEntryLocked will now handle all buffer management based on operation type and sync mode
	lsn, err := pm.wal.AppendEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to append DDL entry to WAL: %w", err)
	}

	// Update the current LSN
	pm.wal.currentLSN.Store(lsn)

	return nil
}

// RecordIndexOperation records an index creation or deletion in the WAL
func (pm *PersistenceManager) RecordIndexOperation(tableName string, op WALOperationType, indexData []byte) error {
	if !pm.enabled || pm.wal == nil {
		return nil
	}

	// Check if WAL manager is running
	if !pm.wal.running.Load() {
		return fmt.Errorf("WAL manager is not running")
	}

	// Verify operation type is valid for index operations
	if op != WALCreateIndex && op != WALDropIndex {
		return fmt.Errorf("invalid operation type for index operation: %v", op)
	}

	// Create WAL entry - use a system transaction ID (-998) for index operations
	entry := WALEntry{
		TxnID:     -998, // System transaction for index operations
		TableName: tableName,
		RowID:     0, // Not applicable for index operations
		Operation: op,
		Data:      indexData,
		Timestamp: time.Now().UnixNano(),
	}

	// Write the entry directly to the WAL
	// AppendEntry and AppendEntryLocked will now handle all buffer management based on operation type and sync mode
	lsn, err := pm.wal.AppendEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to append index operation entry to WAL: %w", err)
	}

	// Update the current LSN
	pm.wal.currentLSN.Store(lsn)

	return nil
}

// RecordDMLOperation records a DML (Data Manipulation Language) operation in the WAL
func (pm *PersistenceManager) RecordDMLOperation(txnID int64, tableName string, rowID int64, version RowVersion) error {
	if !pm.enabled || pm.wal == nil {
		return nil
	}

	// Check if WAL manager is running - avoid lock if not running
	if !pm.wal.running.Load() {
		return fmt.Errorf("WAL manager is not running")
	}

	// Acquire the lock for WAL operations
	pm.wal.mu.Lock()
	defer pm.wal.mu.Unlock()

	// Check again after acquiring the lock
	if !pm.wal.running.Load() {
		return fmt.Errorf("WAL manager shut down while waiting for lock")
	}

	// Serialize the row data without requiring schema (our current implementation doesn't need it)
	serializedData, err := serializeRow(version.Data)
	if err != nil {
		return fmt.Errorf("failed to serialize row data: %w", err)
	}

	// Determine operation type
	var opType WALOperationType
	if version.IsDeleted() {
		opType = WALDelete
	} else {
		opType = WALInsert // Default to insert for new data
	}

	// Create WAL entry
	entry := WALEntry{
		TxnID:     txnID,
		TableName: tableName,
		RowID:     rowID,
		Operation: opType,
		Data:      serializedData,
		Timestamp: time.Now().UnixNano(),
	}

	// Append entry (passing with lock already held)
	// AppendEntryLocked now handles all buffer management based on operation type and sync mode
	lsn, err := pm.wal.AppendEntryLocked(entry)
	if err != nil {
		return err
	}

	pm.meta.lastWALLSN.Store(lsn)
	return nil
}

// RecordCommit records a transaction commit in the WAL
// If autoIncrementInfo is provided, it records the table's auto-increment value in the commit
func (pm *PersistenceManager) RecordCommit(txnID int64, autoIncrementInfo ...struct {
	TableName string
	Value     int64
}) error {
	if !pm.enabled || pm.wal == nil {
		return nil
	}

	// Check if WAL manager is running before taking the lock
	if !pm.wal.running.Load() {
		log.Printf("Warning: Attempted to commit transaction %d after WAL manager shutdown\n", txnID)
		return fmt.Errorf("WAL manager is not running")
	}

	pm.wal.mu.Lock()
	defer pm.wal.mu.Unlock()

	// Check again after acquiring the lock
	if !pm.wal.running.Load() {
		log.Printf("Warning: WAL manager shut down while waiting for lock (commit txnID=%d)\n", txnID)
		return fmt.Errorf("WAL manager is not running")
	}

	// Create WAL entry for commit
	entry := WALEntry{
		TxnID:     txnID,
		Operation: WALCommit,
		Timestamp: time.Now().UnixNano(),
		TableName: "",
		RowID:     0,
		Data:      nil,
	}

	// If we have auto-increment info, serialize it into the commit data
	if len(autoIncrementInfo) > 0 {
		// Use binser for serialization
		writer := binser.NewWriter()
		defer writer.Release()

		// Serialize number of tables with auto-increment info
		writer.WriteUint32(uint32(len(autoIncrementInfo)))

		// Serialize each table's auto-increment info
		for _, info := range autoIncrementInfo {
			writer.WriteString(info.TableName)
			writer.WriteInt64(info.Value)
		}

		// Set the serialized data in the commit entry
		entry.Data = writer.Bytes()
	}

	// Append the entry with lock already held
	// AppendEntryLocked now handles all buffer management based on operation type and sync mode
	lsn, err := pm.wal.AppendEntryLocked(entry)
	if err != nil {
		log.Printf("Error: Failed to append WAL entry: %v\n", err)
		return err
	}

	pm.meta.lastWALLSN.Store(lsn)
	return nil
}

// cleanupOldSnapshots removes old snapshot files, keeping only the most recent n files
func (pm *PersistenceManager) cleanupOldSnapshots(tableDir string, keepCount int) error {
	// Find all snapshot files
	snapshotFiles, err := findSnapshotFiles(tableDir)
	if err != nil {
		return err
	}

	// If we have fewer snapshots than the keep count, nothing to do
	if len(snapshotFiles) <= keepCount {
		return nil
	}

	// Sort snapshots by name (which includes timestamp), newest first
	sort.Sort(sort.Reverse(sort.StringSlice(snapshotFiles)))

	// Keep the newest keepCount snapshots, delete the rest
	for i := keepCount; i < len(snapshotFiles); i++ {
		fileToDelete := filepath.Join(tableDir, snapshotFiles[i])

		// Also try to delete the corresponding metadata file
		metaFile := strings.TrimSuffix(fileToDelete, ".bin") + ".meta"

		// Delete both files, but don't fail if metadata doesn't exist
		if err := os.Remove(fileToDelete); err != nil {
			return fmt.Errorf("failed to delete old snapshot %s: %w", fileToDelete, err)
		}

		// Try to delete metadata file, but ignore errors
		_ = os.Remove(metaFile)
	}

	return nil
}

// RecordRollback records a transaction rollback in the WAL with comprehensive deadlock protection
func (pm *PersistenceManager) RecordRollback(txnID int64) error {
	if !pm.enabled || pm.wal == nil {
		return nil
	}

	// Check if WAL manager is running before taking the lock
	if !pm.wal.running.Load() {
		log.Printf("Warning: Attempted to rollback transaction %d after WAL manager shutdown\n", txnID)
		return fmt.Errorf("WAL manager is not running")
	}

	pm.wal.mu.Lock()
	defer pm.wal.mu.Unlock()

	// Check again after acquiring the lock
	if !pm.wal.running.Load() {
		log.Printf("Warning: WAL manager shut down while waiting for lock (rollback txnID=%d)\n", txnID)
		return fmt.Errorf("WAL manager is not running")
	}

	// Create WAL entry
	entry := WALEntry{
		TxnID:     txnID,
		Operation: WALRollback,
		Timestamp: time.Now().UnixNano(),
	}

	// AppendEntryLocked now handles all buffer management based on operation type and sync mode
	lsn, err := pm.wal.AppendEntryLocked(entry)
	if err != nil {
		log.Printf("Error: Failed to append WAL entry: %v\n", err)
		return err
	}

	// Update LSN in metadata
	pm.meta.lastWALLSN.Store(lsn)
	return nil
}

// UpdateConfig updates the persistence configuration settings
func (pm *PersistenceManager) UpdateConfig(config storage.PersistenceConfig) error {
	if !pm.enabled {
		// No-op for memory-only mode
		return nil
	}

	// Update snapshot interval
	if config.SnapshotInterval > 0 {
		pm.snapshotInterval = time.Duration(config.SnapshotInterval) * time.Second
	}

	// Update keep count
	if config.KeepSnapshots > 0 {
		pm.keepCount = config.KeepSnapshots
	}

	// Update WAL settings if the WAL manager exists
	if pm.wal != nil {
		// Update sync mode
		if config.SyncMode >= 0 && config.SyncMode <= 2 {
			pm.wal.syncMode = SyncMode(config.SyncMode)
		}

		// Update WAL flush trigger
		if config.WALFlushTrigger > 0 {
			// Update WAL flush trigger (not an atomic value in the struct)
			pm.wal.flushTrigger = uint64(config.WALFlushTrigger)
		}

		// Update commit batch size
		if config.CommitBatchSize > 0 {
			pm.wal.commitBatchSize = int32(config.CommitBatchSize)
		}

		// Update sync interval
		if config.SyncIntervalMs > 0 {
			// Convert milliseconds to nanoseconds for the internal field
			pm.wal.syncIntervalNano = int64(config.SyncIntervalMs) * 1_000_000
		}
	}

	return nil
}
