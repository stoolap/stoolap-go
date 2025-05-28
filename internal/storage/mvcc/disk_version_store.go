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
// Package mvcc implements a storage engine for multi-versioned column-based tables.
package mvcc

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
	"github.com/stoolap/stoolap/internal/storage/binser"
)

// Constants for file layout
const (
	FileHeaderSize    = 16                 // Magic bytes + version
	IndexEntrySize    = 16                 // RowID (8 bytes) + Offset (8 bytes)
	FooterSize        = 48                 // IndexOffset (8) + IndexSize (8) + RowCount (8) + TxnIDsOffset (8) + TxnIDsCount (8) + Magic (8)
	MagicBytes        = 0x5354534456534844 // "STSVSHD" (SToolaP VerSion Store HarD disk)
	FileFormatVersion = 1
	DefaultBatchSize  = 1000
	DefaultBlockSize  = 64 * 1024 // 64KB
)

// Constants for snapshot operation
const (
	// Default values
	DefaultKeepSnapshots = 5

	// Schema format
	SchemaVersion    = 1
	SchemaHeaderSize = 16

	// Default timeout values
	DefaultSnapshotIOTimeout   = 10 * time.Second
	DefaultSnapshotLockTimeout = 2 * time.Second
)

// FileHeader contains the metadata at the beginning of a disk version file
type FileHeader struct {
	Magic   uint64
	Version uint32
	Flags   uint32
}

// Footer contains metadata at the end of a disk version file
type Footer struct {
	IndexOffset  uint64
	IndexSize    uint64
	RowCount     uint64
	TxnIDsOffset uint64 // Offset where committed transaction IDs are stored
	TxnIDsCount  uint64 // Number of committed transaction IDs
	Magic        uint64
}

// DiskVersionStore manages the on-disk extension of the version store
type DiskVersionStore struct {
	mu           sync.RWMutex
	baseDir      string
	tableName    string
	schemaHash   uint64 // For schema verification
	readers      []*DiskReader
	versionStore *VersionStore // Reference to in-memory version store
}

// NewDiskVersionStore creates a new disk version store for a table
// If the schema is provided, it will be used instead of querying the version store
func NewDiskVersionStore(baseDir, tableName string, versionStore *VersionStore, providedSchema ...storage.Schema) (*DiskVersionStore, error) {
	storeDir := filepath.Join(baseDir, tableName)
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create disk version store directory: %w", err)
	}

	var schema storage.Schema
	var err error

	// Use provided schema if available (to avoid lock contention)
	if len(providedSchema) > 0 {
		schema = providedSchema[0]
	} else {
		// Otherwise get it from the version store
		schema, err = versionStore.GetTableSchema()
		if err != nil {
			return nil, fmt.Errorf("failed to get table schema: %w", err)
		}
	}

	dvs := &DiskVersionStore{
		baseDir:      baseDir,
		tableName:    tableName,
		schemaHash:   schemaHash(schema),
		versionStore: versionStore,
	}

	return dvs, nil
}

// CreateSnapshot creates a new snapshot of the table's data
func (dvs *DiskVersionStore) CreateSnapshot() error {
	timestamp := time.Now().Format("20060102-150405.000")
	filePath := filepath.Join(dvs.baseDir, dvs.tableName, fmt.Sprintf("snapshot-%s.bin", timestamp))

	// Create a new appender
	appender, err := NewDiskAppender(filePath)
	if err != nil {
		return fmt.Errorf("failed to create disk appender: %w", err)
	}
	defer appender.Close()

	// Get schema for the table
	schema, err := dvs.versionStore.GetTableSchema()
	if err != nil {
		appender.Fail()
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	// Write schema to file
	if err := appender.WriteSchema(&schema); err != nil {
		appender.Fail()
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Process data in batches without locking the whole table
	batch := make([]RowVersion, 0, DefaultBatchSize)

	// Map to track processed rowIDs
	processedRowIDs := fastmap.NewInt64Map[struct{}](1000)

	var lastErr error

	// Use ForEach to avoid locking the whole table
	dvs.versionStore.versions.ForEach(func(rowID int64, version *RowVersion) bool {
		// Verify that map key matches the RowID inside the version
		if rowID != version.RowID {
			log.Printf("WARNING: Version store RowID (%d) doesn't match RowVersion.RowID (%d)\n",
				rowID, version.RowID)
			return true // Continue processing
		}

		// Record that we've processed this rowID
		processedRowIDs.Put(rowID, struct{}{})

		if version.IsDeleted() {
			// Skip deleted versions
			return true
		}

		// Copy the version to avoid concurrent modification issues
		versionCopy := *version

		versionCopy.TxnID = -1 // Set to -1 to indicate it's a snapshot version

		// Add to batch
		batch = append(batch, versionCopy)

		// Process batch when full
		if len(batch) >= DefaultBatchSize {
			if err := appender.AppendBatch(batch); err != nil {
				log.Printf("Error: appending batch: %v\n", err)
				lastErr = err
				return false // Stop iteration on error
			}
			batch = batch[:0] // Reset batch
		}

		return true
	})

	if lastErr != nil {
		appender.Fail()
		return fmt.Errorf("failed to process append batch: %w", lastErr)
	}

	// Process any remaining rows
	if len(batch) > 0 {
		if err := appender.AppendBatch(batch); err != nil {
			appender.Fail()
			return fmt.Errorf("failed to append final batch: %w", err)
		}
	}

	// Now check if we have previous disk snapshots to merge
	dvs.mu.RLock()
	hasReaders := len(dvs.readers) > 0
	var lastReader *DiskReader
	if hasReaders {
		lastReader = dvs.readers[len(dvs.readers)-1]
	}
	dvs.mu.RUnlock()

	if hasReaders && lastReader != nil {
		batch = batch[:0] // Reset batch

		// Get the last reader
		r := lastReader

		// Iterate through all entries in the index
		r.index.ForEach(func(rowID int64, offset int64) bool {
			// Skip if this row was already processed from memory
			if processedRowIDs.Has(rowID) {
				return true
			}

			// Read length prefix using pre-allocated buffer
			if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
				return true // Continue on error
			}
			rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

			// Get a buffer from the pool
			buffer := common.GetBufferPool()
			defer common.PutBufferPool(buffer)

			// Ensure buffer has enough capacity
			if cap(buffer.B) < int(rowLen) {
				buffer.B = make([]byte, rowLen)
			} else {
				buffer.B = buffer.B[:rowLen]
			}

			// Read row data
			if _, err := r.file.ReadAt(buffer.B, offset+4); err != nil {
				return true // Continue on error
			}

			// Deserialize row version
			version, err := deserializeRowVersion(buffer.B)
			if err != nil {
				return true // Continue on error
			}

			if version.IsDeleted() {
				// Skip deleted versions
				return true
			}

			// Copy the version with snapshot TxnID
			versionCopy := version
			versionCopy.TxnID = -1 // Mark as snapshot version

			// Add to batch
			batch = append(batch, versionCopy)

			// Process batch when full
			if len(batch) >= DefaultBatchSize {
				if err := appender.AppendBatch(batch); err != nil {
					log.Printf("Error: appending disk batch: %v\n", err)
					lastErr = err
					return false // Stop iteration on error
				}
				batch = batch[:0] // Reset batch
			}

			return true
		})

		if lastErr != nil {
			appender.Fail()
			return fmt.Errorf("failed to process appending disk batch: %w", lastErr)
		}

		// Process any remaining rows from disk
		if len(batch) > 0 {
			if err := appender.AppendBatch(batch); err != nil {
				appender.Fail()
				return fmt.Errorf("failed to append final disk batch: %w", err)
			}
		}
	}

	// Finalize the snapshot file
	if err := appender.Finalize(); err != nil {
		appender.Fail()
		return fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	// Create metadata file
	metaPath := strings.TrimSuffix(filePath, ".bin") + ".meta"
	if err := dvs.writeMetadata(metaPath, &schema); err != nil {
		appender.Fail()
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Add reader for the new snapshot
	reader, err := NewDiskReader(filePath)
	if err != nil {
		appender.Fail()
		return fmt.Errorf("failed to create disk reader: %w", err)
	}

	// Add to readers list and transfer loaded rowIDs tracking
	dvs.mu.Lock()

	// Transfer loaded rowIDs tracking from previous reader to the new one
	if len(dvs.readers) > 0 {
		previousReader := dvs.readers[len(dvs.readers)-1]

		// Iterate through all loaded rowIDs and transfer them to the new reader
		// This ensures we don't reload rows that were already loaded into memory
		previousReader.mu.Lock()
		for rowID := range previousReader.LoadedRowIDs.Keys() {
			reader.LoadedRowIDs.Put(rowID, struct{}{})
		}
		previousReader.mu.Unlock()
	}

	// Manage readers list according to keepCount
	if len(dvs.readers) > 0 && len(dvs.readers) >= dvs.versionStore.engine.persistence.keepCount {
		// Remove the oldest reader if we exceed the limit
		oldestReader := dvs.readers[0]
		oldestReader.Close()
		dvs.readers = dvs.readers[1:]
	}

	// Add the new reader to the list
	dvs.readers = append(dvs.readers, reader)
	dvs.mu.Unlock()

	// Access the engine persistence to create a checkpoint and truncate WAL
	// Only create a checkpoint if we have access to the persistence and WAL managers
	if dvs.versionStore.engine != nil &&
		dvs.versionStore.engine.persistence != nil &&
		dvs.versionStore.engine.persistence.wal != nil {

		// Get the current LSN from the WAL
		currentLSN := dvs.versionStore.engine.persistence.wal.currentLSN.Load()

		// Create the checkpoint with the current LSN
		dvs.versionStore.engine.persistence.wal.createConsistentCheckpoint(currentLSN, true)

		// Update persistence metadata with the snapshot information
		dvs.versionStore.engine.persistence.meta.lastWALLSN.Store(currentLSN)
		dvs.versionStore.engine.persistence.meta.lastSnapshotLSN.Store(currentLSN)
		dvs.versionStore.engine.persistence.meta.lastSnapshotTimeNano.Store(time.Now().UnixNano())

		// Truncate the WAL file using the checkpoint LSN
		// This removes entries that are already captured in the snapshot
		err := dvs.versionStore.engine.persistence.wal.TruncateWAL(currentLSN)
		if err != nil {
			log.Printf("Warning: Failed to truncate WAL after checkpoint: %v\n", err)
			// Continue despite error since this is not critical
		}
	}

	return nil
}

// writeMetadata writes metadata about the snapshot
func (dvs *DiskVersionStore) writeMetadata(path string, schema *storage.Schema) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := binser.NewWriter()
	defer writer.Release()

	// Write format version
	writer.WriteUint32(FileFormatVersion)

	// Write timestamp
	writer.WriteInt64(time.Now().UnixNano())

	// Write schema hash
	writer.WriteUint64(dvs.schemaHash)

	// Write schema itself (for verification)
	schemaBytes, err := serializeSchema(schema)
	if err != nil {
		return err
	}
	writer.WriteBytes(schemaBytes)

	// Write auto-increment counter value
	writer.WriteInt64(dvs.versionStore.GetCurrentAutoIncrementValue())

	// Save indexes metadata
	// First collect all index data under the read lock to minimize lock time
	var indexDataList [][]byte
	dvs.versionStore.indexMutex.RLock()
	// Store the number of indexes
	indexCount := len(dvs.versionStore.indexes)

	// Collect indexes in a deterministic order using a map to prevent duplicate indexes
	indexMap := make(map[string]storage.Index)
	indexNames := make([]string, 0, indexCount)

	// Collect all indexes with details and ensure we have unique instances
	for name, index := range dvs.versionStore.indexes {
		// Store in our map and track the index names in order
		indexMap[name] = index
		indexNames = append(indexNames, name)
	}

	// Now create an ordered list of indexes from our map to ensure consistent ordering
	var indexes []storage.Index
	for _, name := range indexNames {
		indexes = append(indexes, indexMap[name])
	}

	dvs.versionStore.indexMutex.RUnlock()

	// Write index count
	writer.WriteUint16(uint16(indexCount))

	// Create a map to keep track of the indexes by name for verification
	indexSerialData := make(map[string][]byte)

	// Process each index to create serialized data
	for _, colIndex := range indexes {
		indexData, err := SerializeIndexMetadata(colIndex)
		if err != nil {
			return fmt.Errorf("failed to serialize index metadata for %s: %w", colIndex.Name(), err)
		}

		// Store in our map
		indexSerialData[colIndex.Name()] = indexData

		// Create deep copy of the data to prevent any reference issues
		dataCopy := make([]byte, len(indexData))
		copy(dataCopy, indexData)

		// Add the copy to our list
		indexDataList = append(indexDataList, dataCopy)
	}

	// Verify the arrays have the same length
	if len(indexDataList) != len(indexes) {
		return fmt.Errorf("mismatch between indexDataList (%d) and indexes (%d)",
			len(indexDataList), len(indexes))
	}

	// Now write each index's serialized data to the file
	for _, indexData := range indexDataList {
		// Write a separator marker (byte value of 0xFF)
		writer.WriteByte(0xFF)

		// Write data length and content
		writer.WriteUint32(uint32(len(indexData)))

		for _, b := range indexData {
			writer.WriteByte(b)
		}
	}

	_, err = file.Write(writer.Bytes())
	if err != nil {
		return err
	}

	return file.Sync()
}

// LoadSnapshots loads the most recent snapshot for the table
func (dvs *DiskVersionStore) LoadSnapshots() error {
	tableDir := filepath.Join(dvs.baseDir, dvs.tableName)

	// Check if directory exists
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return nil // No snapshots yet
	}

	// Read directory
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshots directory: %w", err)
	}

	var snapshotFiles []string
	var metadataFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			if strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".bin") {
				snapshotFiles = append(snapshotFiles, filepath.Join(tableDir, entry.Name()))
			} else if strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".meta") {
				metadataFiles = append(metadataFiles, filepath.Join(tableDir, entry.Name()))
			}
		}
	}

	// Sort by name (which includes timestamp)
	sort.Strings(snapshotFiles)
	sort.Strings(metadataFiles)

	if len(snapshotFiles) == 0 {
		return nil // No snapshots available
	}

	// Get transaction registry from version store's engine
	registry := dvs.versionStore.engine.GetRegistry()
	if registry == nil {
		return fmt.Errorf("transaction registry not found")
	}

	// Load metadata from the newest file
	if len(metadataFiles) > 0 {
		// Get the most recent metadata file
		metaFile := metadataFiles[len(metadataFiles)-1]
		err := dvs.loadMetadataFile(metaFile)
		if err != nil {
			log.Printf("Warning: Failed to load metadata %s: %v\n", metaFile, err)
			// Continue despite the error, as we might still be able to load the snapshot
		}
	}

	// Only load the most recent snapshot
	var reader *DiskReader
	var loadErr error

	// Try to load the newest snapshot first
	newestSnapshot := snapshotFiles[len(snapshotFiles)-1]
	reader, loadErr = NewDiskReader(newestSnapshot)

	// If the newest snapshot fails, try older ones in reverse order
	if loadErr != nil {
		log.Printf("Warning: Failed to load newest snapshot %s: %v\n", newestSnapshot, loadErr)

		// Try older snapshots as fallback, from newest to oldest
		for i := len(snapshotFiles) - 2; i >= 0; i-- {
			reader, loadErr = NewDiskReader(snapshotFiles[i])
			if loadErr == nil {
				log.Printf("Successfully loaded fallback snapshot %s\n", snapshotFiles[i])
				break
			}
			log.Printf("Warning: Failed to load fallback snapshot %s: %v\n", snapshotFiles[i], loadErr)
		}
	}

	// If we managed to load any snapshot, add it to readers
	if reader != nil && loadErr == nil {
		dvs.mu.Lock()
		dvs.readers = append(dvs.readers, reader)
		dvs.mu.Unlock()

		// After loading the snapshot, build indexes directly with data from the reader
		// This is critical to ensure that unique constraints are enforced properly
		// but we do it in a memory-efficient way by not loading all rows to memory first
		/* dvs.versionStore.indexMutex.RLock()
		hasIndexes := len(dvs.versionStore.indexes) > 0
		dvs.versionStore.indexMutex.RUnlock()

		if len(dvs.readers) > 0 && hasIndexes {
			// Get all rows from the newest reader without loading to memory
			newestReader := dvs.readers[len(dvs.readers)-1]
			// GetAllRows returns a map of rowID -> RowVersion without marking them as loaded in memory
			allRows := newestReader.GetAllRows()

			rowCount := 0

			// Build indexes with data directly from disk
			dvs.versionStore.indexMutex.RLock()
			for _, index := range dvs.versionStore.indexes {
				// Get column IDs for this index
				columnIDs := index.ColumnIDs()

				// Add each row's values to the index
				for rowID, version := range allRows {
					if !version.IsDeleted() {
						// Create values array for each row
						values := make([]storage.ColumnValue, len(columnIDs))

						// Fill values array with data from the row
						for i, columnID := range columnIDs {
							if int(columnID) < len(version.Data) {
								values[i] = version.Data[columnID]
							} else {
								// Column doesn't exist, treat as NULL
								values[i] = nil
							}
						}

						// Add values to the index
						err := index.Add(values, rowID, 0)
						if err != nil {
							// Log error but continue processing
							log.Printf("Error adding row %d to index %s: %v\n", rowID, index.Name(), err)
						}

						rowCount++
					}
				}
			}
			dvs.versionStore.indexMutex.RUnlock()
		} */

		return nil
	}

	// If all snapshots failed to load, return the most recent error
	if loadErr != nil {
		return fmt.Errorf("failed to load any snapshot: %w", loadErr)
	}

	return nil
}

// loadMetadataFile loads and processes a metadata file, including recreating indexes
func (dvs *DiskVersionStore) loadMetadataFile(path string) error {
	// Open the metadata file
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file stats to determine size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get metadata file stats: %w", err)
	}

	// Get a buffer from the pool for the file data
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Ensure buffer has enough capacity
	fileSize := fileInfo.Size()
	if cap(buffer.B) < int(fileSize) {
		buffer.B = make([]byte, fileSize)
	} else {
		buffer.B = buffer.B[:fileSize]
	}

	// Read the entire file into the buffer
	_, err = file.Read(buffer.B)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	reader := binser.NewReader(buffer.B)

	// Read format version
	version, err := reader.ReadUint32()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != FileFormatVersion {
		return fmt.Errorf("incompatible file format version: %d", version)
	}

	// Read timestamp
	_, err = reader.ReadInt64() // Timestamp, not used here
	if err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read schema hash
	schemaHash, err := reader.ReadUint64()
	if err != nil {
		return fmt.Errorf("failed to read schema hash: %w", err)
	}
	if schemaHash != dvs.schemaHash {
		return fmt.Errorf("schema hash mismatch: %d != %d", schemaHash, dvs.schemaHash)
	}

	// Skip schema bytes
	schemaBytes, err := reader.ReadBytes()
	if err != nil {
		return fmt.Errorf("failed to read schema bytes: %w", err)
	}
	if len(schemaBytes) == 0 {
		return fmt.Errorf("empty schema bytes")
	}

	// Read auto-increment counter value
	autoIncValue, err := reader.ReadInt64()
	if err != nil {
		// For backward compatibility with older snapshots
		log.Printf("Info: Auto-increment value not found in snapshot, using default\n")
	} else if autoIncValue > 0 {
		// Update the version store's auto-increment counter if the snapshot value is higher
		dvs.versionStore.SetAutoIncrementCounter(autoIncValue)
	}

	// Read index count
	indexCount, err := reader.ReadUint16()
	if err != nil {
		return fmt.Errorf("failed to read index count: %w", err)
	}

	// Process each index
	var createdIndexes []storage.Index

	// First read all index metadata
	var indexMetaList []*IndexMetadata

	for i := uint16(0); i < indexCount; i++ {
		// Always expect and read a separator marker
		marker, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read separator marker for index %d: %w", i, err)
		}
		if marker != 0xFF {
			return fmt.Errorf("invalid separator marker for index %d: expected 0xFF, got %X", i, marker)
		}

		// Read index data length with its type marker
		indexLen, err := reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read index data length for index %d: %w", i, err)
		}

		// Additional sanity check for data length
		if indexLen == 0 || indexLen > 10*1024*1024 { // 10MB max size as sanity check
			return fmt.Errorf("invalid index data length: %d", indexLen)
		}

		// Read index data directly (without using ReadBytes that would expect a TypeBytes marker)
		// Get a buffer from the pool
		indexBuffer := common.GetBufferPool()
		defer common.PutBufferPool(indexBuffer)

		// Ensure buffer has enough capacity
		if cap(indexBuffer.B) < int(indexLen) {
			indexBuffer.B = make([]byte, indexLen)
		} else {
			indexBuffer.B = indexBuffer.B[:indexLen]
		}

		// Check if we have enough bytes remaining
		if reader.RemainingBytes() < int(indexLen) {
			return fmt.Errorf("not enough data remaining for index %d: need %d bytes, have %d bytes",
				i, indexLen, reader.RemainingBytes())
		}

		// Read each byte manually
		for j := uint32(0); j < indexLen; j++ {
			b, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("failed to read byte %d of index data for index %d: %w", j, i, err)
			}
			indexBuffer.B[j] = b
		}

		// Deserialize index metadata
		if len(indexBuffer.B) < 10 {
			return fmt.Errorf("index data too small for index %d: %d bytes", i, len(indexBuffer.B))
		}

		// Use debug version for detailed logging
		indexMeta, err := DeserializeIndexMetadata(indexBuffer.B)
		if err != nil {
			return fmt.Errorf("failed to deserialize index metadata for index %d: %w", i, err)
		}

		// Add to our list for processing after reading all metadata
		indexMetaList = append(indexMetaList, indexMeta)
	}

	// Now process each index metadata and create only non-existing indexes
	for _, indexMeta := range indexMetaList {
		// Check if this index already exists
		exists := false
		dvs.versionStore.indexMutex.RLock()

		// First check if an index with this name already exists
		// This is critical for custom-named indexes
		for _, idx := range dvs.versionStore.indexes {
			if idx.Name() == indexMeta.Name {
				exists = true
				break
			}
		}

		// Only if not found by name, check if the column already has an index
		if !exists && len(indexMeta.ColumnNames) > 0 {
			if _, ok := dvs.versionStore.indexes[indexMeta.Name]; ok {
				exists = true
			}
		}

		dvs.versionStore.indexMutex.RUnlock()

		// Skip if the index already exists
		if exists {
			continue
		}

		// Create the index using the standardized CreateIndex method
		idx, err := dvs.versionStore.CreateIndex(indexMeta)
		if err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}

		// Add to list of created indexes
		createdIndexes = append(createdIndexes, idx)
	}

	// Build indexes using optimized Build method
	if len(createdIndexes) > 0 {
		// Use the index's Build method which is optimized for bulk loading
		for _, colIndex := range createdIndexes {
			// Verify the index was added to the indexes map
			dvs.versionStore.indexMutex.RLock()
			var found bool
			for _, idx := range dvs.versionStore.indexes {
				if idx.Name() == colIndex.Name() {
					found = true
					break
				}
			}
			dvs.versionStore.indexMutex.RUnlock()

			if !found {
				// This is a crucial fix: manually add the index to the indexes map if it's missing
				dvs.versionStore.indexMutex.Lock()
				dvs.versionStore.indexes[colIndex.Name()] = colIndex
				dvs.versionStore.indexMutex.Unlock()
			}
		}
	}

	return nil
}

// QuickCheckRowExists checks if the rowID exists in any disk snapshot without reading the full row
// This is a fast path for existence checks, used by HasRowIDWithQuickCheck
func (dvs *DiskVersionStore) QuickCheckRowExists(rowID int64) bool {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	if len(dvs.readers) == 0 {
		return false // No snapshots available
	}

	// Check each reader from newest to oldest
	neweastReader := dvs.readers[len(dvs.readers)-1]

	// Check if the rowID exists in the in-memory version store
	if neweastReader.LoadedRowIDs.Has(rowID) {
		return false
	}

	// Check if the rowID exists in the index without reading the row
	if neweastReader.index != nil {
		_, found := neweastReader.index.Search(rowID)
		if found {
			return true
		}
	}

	return false
}

// GetVersionFromDisk tries to find a row version on disk
func (dvs *DiskVersionStore) GetVersionFromDisk(rowID int64) (RowVersion, bool) {
	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	if len(dvs.readers) == 0 {
		return RowVersion{}, false // No snapshots available
	}

	// Check each reader from newest to oldest
	neweastReader := dvs.readers[len(dvs.readers)-1]

	version, found := neweastReader.GetRow(rowID)
	if found {
		return version, true
	}

	return RowVersion{}, false
}

// GetVersionsBatch retrieves multiple row versions by their IDs
func (dvs *DiskVersionStore) GetVersionsBatch(rowIDs []int64) map[int64]RowVersion {
	if len(rowIDs) == 0 {
		return nil
	}

	dvs.mu.RLock()
	defer dvs.mu.RUnlock()

	if len(dvs.readers) == 0 {
		return nil
	}

	result := make(map[int64]RowVersion, len(rowIDs))

	// Check each reader from newest to oldest
	reader := dvs.readers[len(dvs.readers)-1]

	// Get rows from this reader
	versions := reader.GetRowBatch(rowIDs)

	// Add found versions to the result
	for id, version := range versions {
		result[id] = version
	}

	return result
}

// Close closes the disk version store and all its readers
func (dvs *DiskVersionStore) Close() error {
	dvs.mu.Lock()
	defer dvs.mu.Unlock()

	var lastErr error
	for _, reader := range dvs.readers {
		if err := reader.Close(); err != nil {
			lastErr = err
		}
	}

	// Clear readers to release memory
	dvs.readers = nil

	return lastErr
}

// Helper functions for serialization

// serializeSchema serializes a schema to binary format
func serializeSchema(schema *storage.Schema) ([]byte, error) {
	// Get a buffer from the pool for the writer
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Create a new writer
	writer := binser.NewWriter()
	defer writer.Release()

	// Write table name
	writer.WriteString(schema.TableName)

	// Write number of columns
	writer.WriteUint16(uint16(len(schema.Columns)))

	// Write each column
	for _, col := range schema.Columns {
		writer.WriteString(col.Name)
		writer.WriteUint8(uint8(col.Type))
		writer.WriteBool(col.Nullable)
		writer.WriteBool(col.PrimaryKey)
	}

	// Write timestamps
	writer.WriteInt64(schema.CreatedAt.UnixNano())
	writer.WriteInt64(schema.UpdatedAt.UnixNano())

	// Create a copy of the writer's buffer to return
	writerBytes := writer.Bytes()
	result := make([]byte, len(writerBytes))
	copy(result, writerBytes)

	return result, nil
}

// deserializeSchema deserializes a schema from binary data
func deserializeSchema(data []byte) (*storage.Schema, error) {
	reader := binser.NewReader(data)

	schema := &storage.Schema{}

	// Read table name
	tableName, err := reader.ReadString()
	if err != nil {
		return nil, err
	}
	schema.TableName = tableName

	// Read column count
	colCount, err := reader.ReadUint16()
	if err != nil {
		return nil, err
	}

	// Read columns
	schema.Columns = make([]storage.SchemaColumn, colCount)
	for i := 0; i < int(colCount); i++ {
		// Read column name
		colName, err := reader.ReadString()
		if err != nil {
			return nil, err
		}

		// Read column type
		colType, err := reader.ReadUint8()
		if err != nil {
			return nil, err
		}

		// Read nullable flag
		nullable, err := reader.ReadBool()
		if err != nil {
			return nil, err
		}

		// Read primary key flag
		primaryKey, err := reader.ReadBool()
		if err != nil {
			return nil, err
		}

		schema.Columns[i] = storage.SchemaColumn{
			ID:         i,
			Name:       colName,
			Type:       storage.DataType(colType),
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		}
	}

	// Read timestamps
	createdAt, err := reader.ReadInt64()
	if err != nil {
		return nil, err
	}
	schema.CreatedAt = time.Unix(0, createdAt)

	updatedAt, err := reader.ReadInt64()
	if err != nil {
		return nil, err
	}
	schema.UpdatedAt = time.Unix(0, updatedAt)

	return schema, nil
}

// serializeRow serializes a row to binary format
func serializeRow(row storage.Row) ([]byte, error) {
	// Get a buffer from the pool
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Make sure we have capacity for the header (column count) and some data
	// We'll grow as needed
	initialCap := 256
	if cap(buffer.B) < initialCap {
		buffer.B = make([]byte, 0, initialCap)
	} else {
		buffer.B = buffer.B[:0]
	}

	// Add column count
	buffer.B = append(buffer.B, byte(len(row)>>8), byte(len(row)))

	// Add each column
	for _, col := range row {
		if col == nil || col.IsNull() {
			// Write NULL marker
			buffer.B = append(buffer.B, binser.TypeNull)
			continue
		}

		// Write the column type
		buffer.B = append(buffer.B, binser.TypeUint8)
		buffer.B = append(buffer.B, byte(col.Type()))

		// Write value based on type
		switch col.Type() {
		case storage.TypeInteger:
			v, _ := col.AsInt64()
			buffer.B = append(buffer.B, binser.TypeInt64)
			// Allocate space for the int64 value
			buffer.B = append(buffer.B, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buffer.B[len(buffer.B)-8:], uint64(v))

		case storage.TypeFloat:
			v, _ := col.AsFloat64()
			buffer.B = append(buffer.B, binser.TypeFloat64)
			// Allocate space for the float64 value
			buffer.B = append(buffer.B, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buffer.B[len(buffer.B)-8:], math.Float64bits(v))

		case storage.TypeString:
			v, _ := col.AsString()
			buffer.B = append(buffer.B, binser.TypeString)
			// Allocate space for string length (uint32)
			buffer.B = append(buffer.B, 0, 0, 0, 0)
			// Write the length
			binary.LittleEndian.PutUint32(buffer.B[len(buffer.B)-4:], uint32(len(v)))
			// Append the string data
			buffer.B = append(buffer.B, v...)

		case storage.TypeBoolean:
			v, _ := col.AsBoolean()
			buffer.B = append(buffer.B, binser.TypeBool)
			if v {
				buffer.B = append(buffer.B, 1)
			} else {
				buffer.B = append(buffer.B, 0)
			}

		case storage.TypeTimestamp:
			v, _ := col.AsTimestamp()
			buffer.B = append(buffer.B, binser.TypeTime)
			// Allocate space for the time value (int64 nanoseconds)
			buffer.B = append(buffer.B, 0, 0, 0, 0, 0, 0, 0, 0)
			// Write directly to the buffer at the correct position
			binary.LittleEndian.PutUint64(buffer.B[len(buffer.B)-8:], uint64(v.UnixNano()))

		case storage.TypeJSON:
			v, _ := col.AsJSON()
			buffer.B = append(buffer.B, binser.TypeString)
			// Allocate space for JSON string length (uint32)
			buffer.B = append(buffer.B, 0, 0, 0, 0)
			// Write the length
			binary.LittleEndian.PutUint32(buffer.B[len(buffer.B)-4:], uint32(len(v)))
			// Append the JSON string data
			buffer.B = append(buffer.B, v...)

		default:
			return nil, fmt.Errorf("unsupported type: %v", col.Type())
		}
	}

	// Create a copy of the buffer to return
	// This is necessary since the buffer will be returned to the pool
	result := make([]byte, len(buffer.B))
	copy(result, buffer.B)

	return result, nil
}

// serializeRowVersion serializes a row version to binary format
// This reuses the same approach as in the snapshotter for consistency
func serializeRowVersion(version RowVersion) ([]byte, error) {
	// Get a buffer from the pool
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Create a new writer
	writer := binser.NewWriter()
	defer writer.Release()

	// Write basic fields
	writer.WriteInt64(version.RowID)
	writer.WriteInt64(version.TxnID)
	writer.WriteInt64(version.CreateTime)
	writer.WriteInt64(version.DeletedAtTxnID)

	// Write row data if not deleted
	if !version.IsDeleted() && version.Data != nil {
		rowData, err := serializeRow(version.Data)
		if err != nil {
			return nil, err
		}
		writer.WriteBytes(rowData)
	} else {
		// Empty data
		writer.WriteBytes(nil)
	}

	// Copy writer output to our result
	writerBytes := writer.Bytes()
	result := make([]byte, len(writerBytes))
	copy(result, writerBytes)

	return result, nil
}

func deserializeRow(data []byte) (storage.Row, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid row data: too short")
	}

	// Read column count
	colCount := int(uint16(data[0])<<8 | uint16(data[1]))
	pos := 2

	// Create row with pre-allocated capacity
	row := make(storage.Row, colCount)

	// Read each column
	for i := 0; i < colCount; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of data at column %d", i)
		}

		// Read type marker
		marker := data[pos]
		pos++

		if marker == binser.TypeNull {
			// NULL value
			row[i] = storage.StaticNullUnknown
			continue
		}

		if marker == binser.TypeUint8 {
			// Our custom format - read column type
			if pos >= len(data) {
				return nil, fmt.Errorf("unexpected end of data while reading column type for column %d", i)
			}
			colType := storage.DataType(data[pos])
			pos++

			// Bounds checking helper function
			ensureSpace := func(needed int, description string) error {
				if pos+needed > len(data) {
					return fmt.Errorf("unexpected end of data while reading %s for column %d", description, i)
				}
				return nil
			}

			// Read value based on type
			switch colType {
			case storage.TypeInteger:
				if err := ensureSpace(9, "integer"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeInt64 {
					return nil, fmt.Errorf("invalid integer format for column %d", i)
				}
				pos++
				val := int64(binary.LittleEndian.Uint64(data[pos:]))
				row[i] = storage.NewIntegerValue(val)
				pos += 8

			case storage.TypeFloat:
				if err := ensureSpace(9, "float"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeFloat64 {
					return nil, fmt.Errorf("invalid float format for column %d", i)
				}
				pos++
				bits := binary.LittleEndian.Uint64(data[pos:])
				val := math.Float64frombits(bits)
				row[i] = storage.NewFloatValue(val)
				pos += 8

			case storage.TypeString:
				if err := ensureSpace(1, "string marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeString {
					return nil, fmt.Errorf("invalid string format for column %d", i)
				}
				pos++
				if err := ensureSpace(4, "string length"); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if err := ensureSpace(int(length), "string data"); err != nil {
					return nil, err
				}
				val := string(data[pos : pos+int(length)])
				row[i] = storage.NewStringValue(val)
				pos += int(length)

			case storage.TypeBoolean:
				if err := ensureSpace(1, "boolean marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeBool {
					return nil, fmt.Errorf("invalid boolean format for column %d", i)
				}
				pos++
				if err := ensureSpace(1, "boolean value"); err != nil {
					return nil, err
				}
				val := data[pos] != 0
				row[i] = storage.NewBooleanValue(val)
				pos++

			case storage.TypeTimestamp:
				if err := ensureSpace(1, "time marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeTime {
					return nil, fmt.Errorf("invalid time format for column %d", i)
				}
				pos++
				if err := ensureSpace(8, "time value"); err != nil {
					return nil, err
				}
				nanos := int64(binary.LittleEndian.Uint64(data[pos:]))
				val := time.Unix(0, nanos)
				pos += 8

				row[i] = storage.NewTimestampValue(val)

			case storage.TypeJSON:
				if err := ensureSpace(1, "JSON marker"); err != nil {
					return nil, err
				}
				if data[pos] != binser.TypeString {
					return nil, fmt.Errorf("invalid JSON format for column %d", i)
				}
				pos++
				if err := ensureSpace(4, "JSON length"); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(data[pos:])
				pos += 4
				if err := ensureSpace(int(length), "JSON data"); err != nil {
					return nil, err
				}
				val := string(data[pos : pos+int(length)])
				row[i] = storage.NewJSONValue(val)
				pos += int(length)

			default:
				return nil, fmt.Errorf("unsupported column type: %d for column %d", colType, i)
			}
		} else {
			// Legacy format - return a clear error
			return nil, fmt.Errorf("legacy format not supported in deserializeRow for column %d", i)
		}
	}

	return row, nil
}

// deserializeRowVersion deserializes a row version from binary data
// This reuses the same approach as in the snapshotter for consistency
func deserializeRowVersion(data []byte) (RowVersion, error) {
	reader := binser.NewReader(data)

	var version RowVersion

	// Read basic fields
	rowID, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.RowID = rowID

	txnID, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.TxnID = txnID

	createTime, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.CreateTime = createTime

	deletedAtTxnID, err := reader.ReadInt64()
	if err != nil {
		return version, err
	}
	version.DeletedAtTxnID = deletedAtTxnID

	// Read row data if not deleted
	rowData, err := reader.ReadBytes()
	if err != nil {
		return version, err
	}

	if version.DeletedAtTxnID == 0 && len(rowData) > 0 {
		version.Data, err = deserializeRow(rowData)
		if err != nil {
			return version, err
		}
	}

	return version, nil
}

// schemaHash computes a hash of the schema for validation
func schemaHash(schema interface{}) uint64 {
	var schemaToHash storage.Schema

	// Handle pointer or value
	switch s := schema.(type) {
	case storage.Schema:
		schemaToHash = s
	case *storage.Schema:
		if s != nil {
			schemaToHash = *s
		} else {
			return 0 // Nil schema
		}
	default:
		return 0 // Unknown type
	}

	var hash uint64 = 14695981039346656037 // FNV offset basis

	// Hash table name
	for _, c := range schemaToHash.TableName {
		hash ^= uint64(c)
		hash *= 1099511628211 // FNV prime
	}

	// Hash columns
	for _, col := range schemaToHash.Columns {
		// Hash column name
		for _, c := range col.Name {
			hash ^= uint64(c)
			hash *= 1099511628211
		}

		// Hash column type
		hash ^= uint64(col.Type)
		hash *= 1099511628211

		// Hash nullable flag
		if col.Nullable {
			hash ^= 1
		}
		hash *= 1099511628211

		// Hash primary key flag
		if col.PrimaryKey {
			hash ^= 1
		}
		hash *= 1099511628211
	}

	return hash
}
