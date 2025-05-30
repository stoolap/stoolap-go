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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// RowVersion represents a specific version of a row with complete data
type RowVersion struct {
	TxnID          int64       // Transaction that created this version
	DeletedAtTxnID int64       // Transaction that deleted this version (0 if not deleted)
	Data           storage.Row // Complete row data, not just a reference
	RowID          int64       // Row identifier (replaces string primary key)
	CreateTime     int64       // Timestamp when this version was created

	// Previous version - only kept in memory for active transactions
	// This field is NOT persisted to disk snapshots
	PrevVersion *RowVersion `json:"-"` // Pointer to previous version
}

func (rv *RowVersion) String() string {
	return fmt.Sprintf("RowVersion{TxnID: %d, DeletedAtTxnID: %d, RowID: %d, CreateTime: %d}", rv.TxnID, rv.DeletedAtTxnID, rv.RowID, rv.CreateTime)
}

// IsDeleted returns true if this version has been marked as deleted
func (rv *RowVersion) IsDeleted() bool {
	return rv.DeletedAtTxnID != 0
}

// VersionStore tracks the latest committed version of each row for a table
// Simplified to keep only one version per row (the latest committed version)
type VersionStore struct {
	versions  *fastmap.SegmentInt64Map[*RowVersion] // Using high-performance concurrent map
	tableName string                                // The name of the table this store belongs to

	indexes    map[string]storage.Index
	indexMutex sync.RWMutex

	closed atomic.Bool // Whether this store has been closed - using atomic for better performance

	// Auto-increment counter for tables without explicit PK
	// Start at 1 for better interoperability with other databases
	autoIncrementCounter atomic.Int64

	// Reference to the engine that owns this version store
	engine *MVCCEngine // Engine that owns this version store

	// Hot/Cold data management
	accessTimes *fastmap.SegmentInt64Map[int64] // Maps rowID -> last access timestamp

	// Dirty write prevention: track which transaction has uncommitted changes to each row
	uncommittedWrites *fastmap.SegmentInt64Map[int64] // Maps rowID -> txnID
}

// NewVersionStore creates a new version store
func NewVersionStore(tableName string, engine *MVCCEngine) *VersionStore {
	vs := &VersionStore{
		versions:          fastmap.NewSegmentInt64Map[*RowVersion](8, 1000), // Start with reasonable capacity
		tableName:         tableName,
		indexes:           make(map[string]storage.Index),
		engine:            engine,
		accessTimes:       fastmap.NewSegmentInt64Map[int64](8, 1000), // Initialize access times tracking
		uncommittedWrites: fastmap.NewSegmentInt64Map[int64](8, 1000), // Initialize uncommitted writes tracking
	}
	// Initialize atomic.Bool to false (not closed)
	vs.closed.Store(false)

	// Initialize auto-increment counter to 0
	// We'll use 1 as the first ID (incrementing before use)
	vs.autoIncrementCounter.Store(0)

	return vs
}

// GetNextAutoIncrementID returns the next available auto-increment ID
// This is used both for primary key columns with auto-increment and
// for generating synthetic keys for tables without a primary key
func (vs *VersionStore) GetNextAutoIncrementID() int64 {
	return vs.autoIncrementCounter.Add(1)
}

// SetAutoIncrementCounter sets the auto-increment counter to a specific value
// but only if the current value is lower, to prevent assigning duplicate IDs
// Returns true if the value was updated, false if no update was needed
// This is used during recovery from snapshots or WAL
func (vs *VersionStore) SetAutoIncrementCounter(value int64) bool {
	// We need to ensure the counter only goes forward, never backward
	// Keep trying to update until either we succeed or determine our stored
	// value is already higher than the requested value
	for {
		current := vs.autoIncrementCounter.Load()
		if current >= value {
			// Current value is already higher or equal, no need to update
			return false
		}

		// Try to update - will only succeed if no other thread modified it
		if vs.autoIncrementCounter.CompareAndSwap(current, value) {
			// Successfully updated
			return true
		}

		// If we get here, another thread updated the counter between our load and CAS
		// Loop and try again with the new current value
	}
}

// GetCurrentAutoIncrementValue returns the current auto-increment value
// without incrementing it
func (vs *VersionStore) GetCurrentAutoIncrementValue() int64 {
	return vs.autoIncrementCounter.Load()
}

// AddVersion adds or replaces the version for a row
// Only keeps the latest committed version per row
func (vs *VersionStore) AddVersion(rowID int64, version RowVersion) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return // Skip version update if closed
	}

	// For better efficiency, we can reuse existing version objects
	rv, exists := vs.versions.Get(rowID)
	if !exists {
		// Check again if closed after the potentially expensive lookup
		if vs.closed.Load() {
			return
		}

		// Create a new RowVersion and store a pointer to it
		newVersion := &RowVersion{
			TxnID:          version.TxnID,
			DeletedAtTxnID: version.DeletedAtTxnID,
			Data:           version.Data,
			RowID:          version.RowID,
			CreateTime:     version.CreateTime,
			PrevVersion:    nil, // First version has no previous
		}
		vs.versions.Set(rowID, newVersion)

		// Update columnar indexes with the new version
		vs.UpdateColumnarIndexes(rowID, version)
	} else {
		// Store old deleted status for index updates
		oldIsDeleted := rv.IsDeleted()

		// For updates, keep the previous version for MVCC
		// For deletes, we still update the current version
		if version.DeletedAtTxnID != 0 && !rv.IsDeleted() {
			// This is a delete operation - just mark current version as deleted
			newVersion := &RowVersion{
				TxnID:          rv.TxnID, // Keep the original TxnID
				DeletedAtTxnID: version.DeletedAtTxnID,
				RowID:          version.RowID,
				CreateTime:     rv.CreateTime,  // Keep original create time
				Data:           rv.Data,        // Keep the data for deleted rows
				PrevVersion:    rv.PrevVersion, // Keep existing previous version
			}
			vs.versions.Set(rowID, newVersion)
		} else {
			// This is an update operation - create new version with previous
			newVersion := &RowVersion{
				TxnID:          version.TxnID, // Use the new TxnID
				DeletedAtTxnID: version.DeletedAtTxnID,
				RowID:          version.RowID,
				CreateTime:     version.CreateTime,
				Data:           version.Data,
				PrevVersion:    rv, // Link to previous version
			}
			vs.versions.Set(rowID, newVersion)
		}

		// Update columnar indexes
		// First check if there are any indexes to update
		vs.indexMutex.RLock()
		hasIndexes := len(vs.indexes) > 0
		vs.indexMutex.RUnlock()

		if hasIndexes {
			// If the row was previously not deleted but is now deleted,
			// we need to remove it from all indexes
			if !oldIsDeleted && version.IsDeleted() {
				vs.UpdateColumnarIndexes(rowID, version)
			} else if oldIsDeleted && !version.IsDeleted() {
				vs.UpdateColumnarIndexes(rowID, version)
			} else {
				// Always update indexes as we can't directly compare slices
				vs.UpdateColumnarIndexes(rowID, version)
			}
		}
	}
}

// QuickCheckRowExistence is a fast check if a row might exist
// This is optimized for the critical path in Insert operation
// Returns false if the row definitely doesn't exist
func (vs *VersionStore) QuickCheckRowExistence(rowID int64) bool {
	// Check if the version store is closed
	if vs.closed.Load() {
		return false
	}

	// Check in-memory store first - no lock needed with haxmap
	if vs.versions.Has(rowID) {
		return true
	}

	// If not in memory and persistence is enabled, check disk store
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Quick index-only check in disk store
			return diskStore.QuickCheckRowExists(rowID)
		}
	}

	return false
}

// GetVisibleVersion gets the latest visible version of a row
func (vs *VersionStore) GetVisibleVersion(rowID int64, txnID int64) (RowVersion, bool) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return RowVersion{}, false
	}

	// Check if the row exists in memory - haxmap is concurrency-safe
	versionPtr, exists := vs.versions.Get(rowID)
	if exists {
		// Check current version first
		// fmt.Printf("[GetVisibleVersion] Row %d: checking version TxnID=%d for viewer TxnID=%d\n", rowID, versionPtr.TxnID, txnID)
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			// Check if the row has been deleted and if the deletion is visible
			if versionPtr.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(versionPtr.DeletedAtTxnID, txnID) {
				// The deletion is visible to this transaction, so the row is not visible
				return RowVersion{}, false
			}
			// Return a copy of the version by value
			// fmt.Printf("[GetVisibleVersion] Row %d: version IS visible, returning data\n", rowID)
			return *versionPtr, true
		}

		// Current version not visible, check previous version if it exists
		// fmt.Printf("[GetVisibleVersion] Row %d: current version NOT visible\n", rowID)
		if versionPtr.PrevVersion != nil {
			prev := versionPtr.PrevVersion
			// fmt.Printf("[GetVisibleVersion] Row %d: checking prev version TxnID=%d\n", rowID, prev.TxnID)
			if vs.engine.registry.IsVisible(prev.TxnID, txnID) {
				// Check if previous version was deleted
				if prev.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(prev.DeletedAtTxnID, txnID) {
					// Previous version was deleted and deletion is visible
					return RowVersion{}, false
				}
				// Return a copy of the previous version
				// fmt.Printf("[GetVisibleVersion] Row %d: prev version IS visible, returning data\n", rowID)
				return *prev, true
			}
		}

		// Neither current nor previous version is visible
		return RowVersion{}, false
	}

	// If not in memory and persistence is enabled, check disk store
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Check if the row exists in disk store
			if version, found := diskStore.GetVersionFromDisk(rowID); found {
				// Cache the version in memory for future access
				vs.AddVersion(rowID, version)

				// Track access time for rows loaded from disk
				vs.accessTimes.Set(rowID, GetFastTimestamp())

				return version, true
			}
		}
	}

	return RowVersion{}, false
}

// IterateVisibleVersions iterates through visible versions for the given rowIDs
// and calls the provided callback function for each one, avoiding any map allocation
func (vs *VersionStore) IterateVisibleVersions(rowIDs []int64, txnID int64,
	callback func(rowID int64, version RowVersion) bool) {

	// Check if the version store is closed
	if vs.closed.Load() {
		return
	}

	// Early validation of parameters
	if callback == nil || len(rowIDs) == 0 {
		return
	}

	// Keep track of rowIDs not found in memory to check in disk store
	var notFoundIDs []int64

	// No lock needed with haxmap - it's concurrency-safe
	for _, rowID := range rowIDs {
		// Check again if closed during the iteration
		if vs.closed.Load() {
			return
		}

		versionPtr, exists := vs.versions.Get(rowID)
		if !exists {
			// Keep track of IDs not found in memory for disk lookup
			notFoundIDs = append(notFoundIDs, rowID)
			continue
		}

		// Check visibility
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			// Check if the row has been deleted and if the deletion is visible
			if versionPtr.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(versionPtr.DeletedAtTxnID, txnID) {
				// The deletion is visible to this transaction, skip this row
				return
			}
			// Call the callback with the rowID and a copy of the version
			if !callback(rowID, *versionPtr) {
				// Stop iteration if callback returns false
				return
			}
		} else if versionPtr.PrevVersion != nil {
			// Current version not visible, check previous version
			prev := versionPtr.PrevVersion
			if vs.engine.registry.IsVisible(prev.TxnID, txnID) {
				// Check if previous version was deleted
				if prev.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(prev.DeletedAtTxnID, txnID) {
					// Previous version was deleted and deletion is visible
					continue
				}
				// Call the callback with the previous version
				if !callback(rowID, *prev) {
					// Stop iteration if callback returns false
					return
				}
			}
		}
	}

	// If there are IDs not found in memory and persistence is enabled, check disk store
	if len(notFoundIDs) > 0 && vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Process each rowID not found in memory
			for _, rowID := range notFoundIDs {
				// Check if closed during disk operations
				if vs.closed.Load() {
					return
				}

				// Check if the row exists in disk store
				if version, found := diskStore.GetVersionFromDisk(rowID); found {
					// Cache the version in memory for future access
					vs.AddVersion(rowID, version)

					// Track access time for this row loaded from disk
					vs.accessTimes.Set(rowID, GetFastTimestamp())

					// Call the callback
					if !callback(rowID, version) {
						// Stop iteration if callback returns false
						return
					}
				}
			}
		}
	}
}

// GetVisibleVersionsByIDs retrieves visible versions for the given rowIDs
// This is an optimized batch version of GetVisibleVersion using fastmap for high performance
func (vs *VersionStore) GetVisibleVersionsByIDs(rowIDs []int64, txnID int64) *fastmap.Int64Map[*RowVersion] {
	// Check if the version store is closed
	if vs.closed.Load() || vs.versions == nil {
		// Return empty map
		return &fastmap.Int64Map[*RowVersion]{}
	}

	// Early validation of parameters
	if len(rowIDs) == 0 {
		return &fastmap.Int64Map[*RowVersion]{}
	}

	result := GetVisibleVersionMap()

	// Track IDs not found in memory for disk lookup
	var notFoundIDs []int64

	// Process in batches to optimize memory access patterns
	const batchSize = 100
	for i := 0; i < len(rowIDs); i += batchSize {
		// Check again if closed during batch processing
		if vs.closed.Load() {
			break
		}

		end := i + batchSize
		if end > len(rowIDs) {
			end = len(rowIDs)
		}

		// Process this batch
		for j := i; j < end; j++ {
			rowID := rowIDs[j]

			// No lock needed with haxmap - it's concurrency-safe
			versionPtr, exists := vs.versions.Get(rowID)
			if !exists {
				// Track IDs not found for later disk lookup
				notFoundIDs = append(notFoundIDs, rowID)
				continue
			}

			// Check visibility
			if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
				// Check if the row has been deleted and if the deletion is visible
				if versionPtr.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(versionPtr.DeletedAtTxnID, txnID) {
					// The deletion is visible to this transaction, skip this row
					continue
				}
				// Add the visible version to result
				result.Put(rowID, versionPtr)
			} else if versionPtr.PrevVersion != nil {
				// Current version not visible, check previous version
				prev := versionPtr.PrevVersion
				if vs.engine.registry.IsVisible(prev.TxnID, txnID) {
					// Check if previous version was deleted
					if prev.DeletedAtTxnID != 0 && vs.engine.registry.IsVisible(prev.DeletedAtTxnID, txnID) {
						// Previous version was deleted and deletion is visible
						continue
					}
					// Add the previous version to result
					result.Put(rowID, prev)
				}
			}
		}
	}

	// If persistence is enabled and we have IDs not found in memory,
	// check the disk store for those IDs
	if len(notFoundIDs) > 0 && vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists {
			// Check if closed again
			if vs.closed.Load() {
				ReturnVisibleVersionMap(result)
				return &fastmap.Int64Map[*RowVersion]{}
			}

			// For optimization, if there are many IDs, use batch retrieval
			if len(notFoundIDs) > 10 {
				// Get versions from disk in batch
				diskVersions := diskStore.GetVersionsBatch(notFoundIDs)

				// Process each disk version
				for rowID, version := range diskVersions {
					if !version.IsDeleted() {
						// Cache the version in memory for future access
						vs.AddVersion(rowID, version)

						// Track access time for disk-loaded row
						vs.accessTimes.Set(rowID, GetFastTimestamp())

						// Get the cached version pointer from memory to ensure consistency with the pool
						if versionPtr, exists := vs.versions.Get(rowID); exists {
							result.Put(rowID, versionPtr)
						}
					}
				}
			} else {
				// For smaller sets, process individually (which can be faster for few IDs)
				for _, rowID := range notFoundIDs {
					// Check if the row exists in disk store
					if version, found := diskStore.GetVersionFromDisk(rowID); found {
						if !version.IsDeleted() {
							// Cache the version in memory for future access
							vs.AddVersion(rowID, version)

							// Track access time for disk-loaded row
							vs.accessTimes.Set(rowID, GetFastTimestamp())

							// Get the cached version from memory
							if versionPtr, exists := vs.versions.Get(rowID); exists {
								result.Put(rowID, versionPtr)
							}
						}
					}
				}
			}
		}
	}

	SIMDSortInt64s(rowIDs) // Sort rowIDs for consistent order in result

	return result
}

// Pool for version maps used in visible version retrieval
var visibleVersionMapPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[*RowVersion](1000) // Start with reasonable capacity
	},
}

// GetVisibleVersionMap gets a version map from the pool
func GetVisibleVersionMap() *fastmap.Int64Map[*RowVersion] {
	m := visibleVersionMapPool.Get().(*fastmap.Int64Map[*RowVersion])

	return m
}

// ReturnVisibleVersionMap returns a version map to the pool
func ReturnVisibleVersionMap(m *fastmap.Int64Map[*RowVersion]) {
	if m == nil {
		return
	}

	m.Clear()
	visibleVersionMapPool.Put(m)
}

// We'll manage transaction timestamps directly in the registry without caching

// GetAllVisibleVersions gets all visible versions for a scan operation
// This is an optimized version that reduces allocations and properly respects snapshot isolation
func (vs *VersionStore) GetAllVisibleVersions(txnID int64) *fastmap.Int64Map[*RowVersion] {
	// Check if the version store is closed or versions is nil
	if vs.closed.Load() || vs.versions == nil {
		// Return empty map
		return &fastmap.Int64Map[*RowVersion]{}
	}

	// Check if closed after the sampling
	if vs.closed.Load() {
		return &fastmap.Int64Map[*RowVersion]{}
	}

	result := GetVisibleVersionMap()

	// Get the isolation level for this transaction
	isolationLevel := vs.engine.registry.GetIsolationLevel(txnID)

	// For bulk operations in READ COMMITTED, optimize the common case
	if isolationLevel == storage.ReadCommitted {
		vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
			// Check if closed during iteration
			if vs.closed.Load() {
				return false
			}

			// Skip if it's owned by the current txn (likely being deleted)
			if versionPtr.TxnID == txnID {
				return true
			}

			// Check deletion visibility - if deleted by a visible transaction (but not current txn), skip it
			if versionPtr.DeletedAtTxnID != 0 && versionPtr.DeletedAtTxnID != txnID && vs.engine.registry.IsVisible(versionPtr.DeletedAtTxnID, txnID) {
				return true
			}

			// Skip if it's not committed (only for other txns)
			if !vs.engine.registry.IsDirectlyVisible(versionPtr.TxnID) {
				return true
			}

			// Visible row - add a copy to result
			result.Put(rowID, versionPtr)

			return true
		})

		// Check if closed after processing in-memory versions
		if vs.closed.Load() {
			ReturnVisibleVersionMap(result)
			return &fastmap.Int64Map[*RowVersion]{}
		}

		// Only check disk if persistence is enabled
		if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
			// Get the disk store for this table
			if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
				// For bulk operations, process the most recent snapshot efficiently using ForEach
				// to avoid unnecessary allocations of the entire map
				newestReader := diskStore.readers[len(diskStore.readers)-1]

				newestReader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
					// Skip deleted rows - they shouldn't be in snapshots
					if diskVersion.DeletedAtTxnID != 0 {
						return true // Continue iteration
					}

					// All rows from disk snapshots have TxnID = -1 and are always visible
					// Cache in memory for future use
					vs.AddVersion(rowID, diskVersion)

					// Track access time for disk-loaded row
					vs.accessTimes.Set(rowID, GetFastTimestamp())

					// Get the newly cached version for consistency
					if versionPtr, exists := vs.versions.Get(rowID); exists {
						result.Put(rowID, versionPtr)
					}

					return true // Continue iteration
				})
			}
		}

		return result
	}

	// Fall back to standard visibility rules for SNAPSHOT isolation
	vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
		// Check if closed during iteration
		if vs.closed.Load() {
			return false
		}

		// No need to track rowIDs separately - we'll check result map directly

		// Check for visibility based on isolation level rules
		if vs.engine.registry.IsVisible(versionPtr.TxnID, txnID) {
			// For deleted rows, check if the deletion is visible
			if versionPtr.IsDeleted() {
				// If the deletion is visible to this transaction, skip this row
				// But if current transaction deleted it, include it (so txn can see its own deletions)
				deletionVisible := vs.engine.registry.IsVisible(versionPtr.DeletedAtTxnID, txnID)
				if versionPtr.DeletedAtTxnID != txnID && deletionVisible {
					// The deletion is visible to this transaction, skip this row
					return true
				}
				// If deletion is NOT visible or done by current txn, include the row
			}
			result.Put(rowID, versionPtr)
		} else if versionPtr.PrevVersion != nil {
			// Current version not visible, check previous version
			prev := versionPtr.PrevVersion
			if vs.engine.registry.IsVisible(prev.TxnID, txnID) {
				// Check if previous version was deleted
				if prev.IsDeleted() {
					deletionVisible := vs.engine.registry.IsVisible(prev.DeletedAtTxnID, txnID)
					if prev.DeletedAtTxnID != txnID && deletionVisible {
						// Previous version was deleted and deletion is visible
						return true
					}
				}
				// Include the previous version
				result.Put(rowID, prev)
			}
		}

		return true
	})

	// Final check if closed after in-memory processing
	if vs.closed.Load() {
		ReturnVisibleVersionMap(result)
		return &fastmap.Int64Map[*RowVersion]{}
	}

	// For SNAPSHOT isolation, process disk versions
	if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		// Get the disk store for this table
		if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
			// For snapshot, all rows from disk snapshots have TxnID = -1 and are always visible
			// Start with the most recent snapshot
			newestReader := diskStore.readers[len(diskStore.readers)-1]

			// Use ForEach for memory-efficient iteration without allocating the entire map
			newestReader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
				// Skip deleted versions - they shouldn't be in snapshots
				if diskVersion.DeletedAtTxnID != 0 {
					return true // Continue iteration
				}

				// Cache in memory for future use
				vs.AddVersion(rowID, diskVersion)

				// Track access time for disk-loaded row
				vs.accessTimes.Set(rowID, GetFastTimestamp())

				// Get the newly cached version for consistency
				if versionPtr, exists := vs.versions.Get(rowID); exists {
					result.Put(rowID, versionPtr)
				}

				return true // Continue iteration
			})
		}
	}

	return result
}

// WriteSetEntry tracks a write operation with the version read
type WriteSetEntry struct {
	ReadVersion    *RowVersion // Version when first read (nil if row didn't exist)
	ReadVersionSeq int64       // Sequence number when read
}

// TransactionVersionStore holds changes specific to a transaction
type TransactionVersionStore struct {
	localVersions *fastmap.Int64Map[RowVersion] // RowID -> local version
	parentStore   *VersionStore                 // Reference to the shared store
	txnID         int64                         // This transaction's ID
	fromPool      bool                          // Whether this object came from the pool

	// Write-set tracking for conflict detection
	writeSet *fastmap.Int64Map[WriteSetEntry] // RowID -> write set entry
}

// Pool for TransactionVersionStore objects
var transactionVersionStorePool = sync.Pool{
	New: func() interface{} {
		return &TransactionVersionStore{
			localVersions: fastmap.NewInt64Map[RowVersion](100),    // Start with reasonable capacity
			writeSet:      fastmap.NewInt64Map[WriteSetEntry](100), // Start with reasonable capacity
		}
	},
}

// NewTransactionVersionStore creates a transaction-local version store
func NewTransactionVersionStore(
	parentStore *VersionStore,
	txnID int64) *TransactionVersionStore {

	// Get an object from the pool
	tvs := transactionVersionStorePool.Get().(*TransactionVersionStore)

	// Initialize or clear the maps
	if tvs.localVersions == nil {
		tvs.localVersions = fastmap.NewInt64Map[RowVersion](100)
	} else {
		tvs.localVersions.Clear()
	}

	if tvs.writeSet == nil {
		tvs.writeSet = fastmap.NewInt64Map[WriteSetEntry](100)
	} else {
		tvs.writeSet.Clear()
	}

	// Set the fields
	tvs.parentStore = parentStore
	tvs.txnID = txnID
	tvs.fromPool = true

	return tvs
}

// Put adds or updates a row in the transaction's local store
func (tvs *TransactionVersionStore) Put(rowID int64, data storage.Row, isDelete bool) error {
	// First check if we already have a local version of this row
	// If we do, we've already claimed it
	if !tvs.localVersions.Has(rowID) {
		// Check if this row exists in the parent store
		var rowExists bool
		if tvs.parentStore != nil {
			_, rowExists = tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID)
		}

		// Only claim the row if it exists (for updates/deletes)
		// New rows (inserts) don't need to be claimed
		if rowExists {
			if err := tvs.ClaimRowForUpdate(rowID); err != nil {
				// Another transaction has uncommitted changes to this row
				return err
			}
		}

		// Track in write-set if not already tracked
		// Note: We might already have tracked this from a previous read in Get()
		if !tvs.writeSet.Has(rowID) {
			// Get the current version from parent store for conflict detection
			var readVersion *RowVersion
			if tvs.parentStore != nil {
				if version, exists := tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID); exists {
					versionCopy := version
					readVersion = &versionCopy
				}
			}

			// Track this write with the version we read
			entry := WriteSetEntry{
				ReadVersion:    readVersion,
				ReadVersionSeq: tvs.parentStore.engine.registry.GetCurrentSequence(),
			}
			tvs.writeSet.Put(rowID, entry)
		}
	}

	// Create a row version directly
	rv := RowVersion{
		TxnID:          tvs.txnID,
		DeletedAtTxnID: 0, // Will be set during delete operations
		Data:           data,
		RowID:          rowID,
		CreateTime:     GetFastTimestamp(),
	}

	// If this is a delete operation, set the DeletedAtTxnID
	if isDelete {
		rv.DeletedAtTxnID = tvs.txnID
	}

	// Store by value in the local versions map
	tvs.localVersions.Put(rowID, rv)
	return nil
}

// PutRowsBatch efficiently adds multiple rows with different data values
// This is optimized for batch insert operations
func (tvs *TransactionVersionStore) PutRowsBatch(rowIDs []int64, rows []storage.Row, isDelete bool) error {
	// Get current sequence for conflict detection
	currentSeq := tvs.parentStore.engine.registry.GetCurrentSequence()

	// First, try to claim all rows that exist atomically
	// We need to check/claim all rows before modifying any
	rowsToClaimMap := make(map[int64]bool)
	for _, rowID := range rowIDs {
		if !tvs.localVersions.Has(rowID) {
			// Check if this row exists in the parent store
			var rowExists bool
			if tvs.parentStore != nil {
				_, rowExists = tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID)
			}

			// Only claim the row if it exists (for updates/deletes)
			if rowExists {
				rowsToClaimMap[rowID] = true
				if err := tvs.ClaimRowForUpdate(rowID); err != nil {
					// Another transaction has uncommitted changes to this row
					// Release any rows we already claimed
					for claimedRowID := range rowsToClaimMap {
						if claimedRowID == rowID {
							break // Don't release rows we haven't claimed yet
						}
						tvs.ReleaseRowClaim(claimedRowID)
					}
					return err
				}
			}

			// Track in write-set if not already tracked
			// Note: We might already have tracked this from a previous read in Get()
			if !tvs.writeSet.Has(rowID) {
				// Get the current version from parent store for conflict detection
				var readVersion *RowVersion
				if tvs.parentStore != nil {
					if version, exists := tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID); exists {
						versionCopy := version
						readVersion = &versionCopy
					}
				}

				// Track this write with the version we read
				entry := WriteSetEntry{
					ReadVersion:    readVersion,
					ReadVersionSeq: currentSeq,
				}
				tvs.writeSet.Put(rowID, entry)
			}
		}
	}

	// Get a single timestamp for all versions to ensure consistency
	// and avoid multiple system calls
	now := GetFastTimestamp()
	deletedAtTxnID := int64(0)
	if isDelete {
		deletedAtTxnID = tvs.txnID
	}

	// Add all rows with the same timestamp
	for i, rowID := range rowIDs {
		// Create a row version with the data for this row
		rv := RowVersion{
			TxnID:          tvs.txnID,
			DeletedAtTxnID: deletedAtTxnID,
			Data:           rows[i],
			RowID:          rowID,
			CreateTime:     now,
		}
		tvs.localVersions.Put(rowID, rv)
	}

	return nil
}

// ReleaseTransactionVersionStore returns a TransactionVersionStore to the pool
func ReleaseTransactionVersionStore(tvs *TransactionVersionStore) {
	if tvs == nil || !tvs.fromPool {
		return
	}

	// Clear fields to prevent memory leaks
	tvs.localVersions.Clear()
	tvs.writeSet.Clear()
	tvs.parentStore = nil
	tvs.txnID = 0
	tvs.fromPool = false

	// Put back in the pool
	transactionVersionStorePool.Put(tvs)
}

// Rollback aborts the transaction and releases resources
func (tvs *TransactionVersionStore) Rollback() {
	// Release all claimed rows
	tvs.ReleaseAllClaims()

	// During rollback, we just need to release resources
	// No need to merge changes to parent store as we're aborting
	if tvs.fromPool {
		// Return this object to the pool
		ReleaseTransactionVersionStore(tvs)
	} else {
		// For backward compatibility with existing code
		tvs.localVersions = nil
	}
}

// HasLocallySeen checks if this rowID has been seen in this transaction
// This is a fast path optimization to avoid the expensive Get operation
func (tvs *TransactionVersionStore) HasLocallySeen(rowID int64) bool {
	return tvs.localVersions.Has(rowID)
}

// Get retrieves a row by its row ID
func (tvs *TransactionVersionStore) Get(rowID int64) (storage.Row, bool) {
	// First check local versions
	if localVersion, exists := tvs.localVersions.Get(rowID); exists {
		if localVersion.IsDeleted() {
			return nil, false
		}
		return localVersion.Data, true
	}

	// If not in local store, check parent store with visibility rules
	if tvs.parentStore != nil {
		if version, exists := tvs.parentStore.GetVisibleVersion(rowID, tvs.txnID); exists {
			// Track this read in the write-set for conflict detection
			// We only track if we're going to potentially write to this row later
			if !tvs.writeSet.Has(rowID) {
				// Store the version we read
				versionCopy := version
				entry := WriteSetEntry{
					ReadVersion:    &versionCopy,
					ReadVersionSeq: tvs.parentStore.engine.registry.GetCurrentSequence(),
				}
				tvs.writeSet.Put(rowID, entry)
			}

			// IMPORTANT: In SNAPSHOT isolation, a deleted row that is visible to this
			// transaction should still be returned. The deletion visibility is already
			// handled by GetVisibleVersion - if it returned a deleted row, it means
			// this transaction should see the row as it existed before deletion.
			// Only in READ COMMITTED mode should we hide deleted rows immediately.
			if version.IsDeleted() {
				// Check isolation level to determine behavior
				isolationLevel := tvs.parentStore.engine.registry.GetIsolationLevel(tvs.txnID)
				if isolationLevel == storage.SnapshotIsolation {
					// In SNAPSHOT isolation, return the deleted row if it's visible
					// The caller will need to handle the deleted status appropriately
					return version.Data, true
				}
				// In READ COMMITTED, hide deleted rows
				return nil, false
			}
			return version.Data, true
		}

		// Track that we read a non-existent row
		if !tvs.writeSet.Has(rowID) {
			entry := WriteSetEntry{
				ReadVersion:    nil, // Row didn't exist when we read
				ReadVersionSeq: tvs.parentStore.engine.registry.GetCurrentSequence(),
			}
			tvs.writeSet.Put(rowID, entry)
		}
	}

	return nil, false
}

// Pool for row maps to reduce allocations
var rowMapPool = sync.Pool{
	New: func() interface{} {
		return fastmap.NewInt64Map[storage.Row](1000)
	},
}

// GetRowMap gets a map from the pool or creates a new one
func GetRowMap() *fastmap.Int64Map[storage.Row] {
	m := rowMapPool.Get().(*fastmap.Int64Map[storage.Row])

	return m
}

// ReturnRowMap returns a map to the pool
func PutRowMap(m *fastmap.Int64Map[storage.Row]) {
	if m == nil {
		return
	}

	m.Clear()

	rowMapPool.Put(m)
}

// GetAllVisibleRows retrieves all rows visible to this transaction
// This version implements zero-copy semantics where possible to reduce allocations
// and uses optimized batch processing for disk data with caching
func (tvs *TransactionVersionStore) GetAllVisibleRows() *fastmap.Int64Map[storage.Row] {
	// Get a preallocated map from the pool
	result := GetRowMap()

	// Get globally visible versions directly from the parent store
	if tvs.parentStore != nil {
		vs := tvs.parentStore
		txnID := tvs.txnID
		registry := tvs.parentStore.engine.registry

		vs.versions.ForEach(func(rowID int64, versionPtr *RowVersion) bool {
			// Check visibility
			if registry.IsVisible(versionPtr.TxnID, txnID) {
				// For deleted rows, check if the deletion is visible
				if versionPtr.IsDeleted() {
					// If the deletion is visible, skip this row
					if registry.IsVisible(versionPtr.DeletedAtTxnID, txnID) {
						return true
					}
					// If deletion is NOT visible, include the row
				}
				// Add directly to result
				result.Put(rowID, versionPtr.Data)
			} else if versionPtr.PrevVersion != nil {
				// Current version not visible, check previous version
				prev := versionPtr.PrevVersion
				if registry.IsVisible(prev.TxnID, txnID) {
					// Check if there's a deletion we need to consider
					if prev.IsDeleted() {
						// If the deletion is visible, skip this row
						if registry.IsVisible(prev.DeletedAtTxnID, txnID) {
							return true
						}
					}
					// Previous version is visible and not deleted (or deletion not visible)
					result.Put(rowID, prev.Data)
				}
			}

			return true
		})

		// Check for disk-stored rows if persistence is enabled
		if vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
			// Get the disk store for this table
			if diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]; exists && len(diskStore.readers) > 0 {
				reader := diskStore.readers[len(diskStore.readers)-1]

				reader.ForEach(func(rowID int64, diskVersion RowVersion) bool {
					// Skip deleted rows - they shouldn't be in snapshots
					if diskVersion.DeletedAtTxnID != 0 {
						return true // Continue iteration
					}

					// All rows from disk snapshots have TxnID = -1 and are always visible
					result.Put(rowID, diskVersion.Data)

					// Cache in memory for future use
					vs.AddVersion(rowID, diskVersion)

					// Track access time for disk-loaded row
					vs.accessTimes.Set(rowID, GetFastTimestamp())

					return true // Continue iteration
				})
			}
		}
	}

	// Process local versions (these take precedence)
	tvs.localVersions.ForEach(func(rowID int64, version RowVersion) bool {
		if version.IsDeleted() {
			// If deleted locally, remove from result
			result.Del(rowID)
		} else {
			// Local versions must be copied since they may be modified during transaction
			result.Put(rowID, version.Data)
		}

		return true // Continue iteration
	})

	return result
}

// DetectConflicts checks for write-write conflicts with other transactions
// Returns an error if any row in the write-set was modified after this transaction began
func (tvs *TransactionVersionStore) DetectConflicts() error {
	if tvs.parentStore == nil || tvs.writeSet == nil {
		return nil
	}

	// Get this transaction's begin sequence for comparison
	txnBeginSeq := tvs.parentStore.engine.registry.GetTransactionBeginSeq(tvs.txnID)

	// Check each write in our write-set
	var conflictErr error
	tvs.writeSet.ForEach(func(rowID int64, entry WriteSetEntry) bool {
		// Get the current version from parent store
		currentVersion, exists := tvs.parentStore.versions.Get(rowID)

		// Case 1: We're creating a new row (entry.ReadVersion == nil)
		if entry.ReadVersion == nil {
			// If a row now exists that didn't exist when we read, that's a conflict
			if exists {
				// Check if this version was created after our transaction began
				commitSeq, committed := tvs.parentStore.engine.registry.GetCommitTimestamp(currentVersion.TxnID)
				if committed && commitSeq > txnBeginSeq {
					conflictErr = fmt.Errorf("write-write conflict: row %d was created by transaction %d after this transaction began", rowID, currentVersion.TxnID)
					return false // Stop iteration
				}
			}
			return true // Continue
		}

		// Case 2: We're updating an existing row
		if !exists {
			// Row was deleted by another transaction
			conflictErr = fmt.Errorf("write-write conflict: row %d was deleted after this transaction read it", rowID)
			return false
		}

		// Check if the version changed
		if currentVersion.TxnID != entry.ReadVersion.TxnID {
			// The row was modified by another transaction
			conflictErr = fmt.Errorf("write-write conflict: row %d was modified by transaction %d after this transaction read it", rowID, currentVersion.TxnID)
			return false
		}

		return true // Continue checking other rows
	})

	return conflictErr
}

// Commit merges local changes into the parent version store
func (tvs *TransactionVersionStore) Commit() error {
	// Add all local versions to the parent store
	if tvs.parentStore != nil {
		tvs.localVersions.ForEach(func(rowID int64, version RowVersion) bool {
			tvs.parentStore.AddVersion(rowID, version)
			return true // Continue iteration
		})
	}

	// Release all claims after adding versions
	// This must be done here because the table will clear the reference after commit
	tvs.ReleaseAllClaims()

	// If from pool, return it after commit
	if tvs.fromPool {
		ReleaseTransactionVersionStore(tvs)
	} else {
		// For backward compatibility with existing code
		tvs.localVersions = nil
	}

	return nil
}

// ClaimRowForUpdate attempts to claim a row for update by this transaction
// Returns error if another transaction has uncommitted changes to this row
func (tvs *TransactionVersionStore) ClaimRowForUpdate(rowID int64) error {
	// Try to claim the row atomically
	existingTxn, inserted := tvs.parentStore.uncommittedWrites.PutIfNotExists(rowID, tvs.txnID)

	if !inserted && existingTxn != tvs.txnID {
		// Another transaction has uncommitted changes to this row
		return fmt.Errorf("row is being modified by another transaction")
	}

	// Successfully claimed (either newly claimed or already owned by us)
	return nil
}

// ReleaseRowClaim releases the claim on a row (used during rollback)
func (tvs *TransactionVersionStore) ReleaseRowClaim(rowID int64) {
	// Only release if we own it
	if txnID, exists := tvs.parentStore.uncommittedWrites.Get(rowID); exists && txnID == tvs.txnID {
		tvs.parentStore.uncommittedWrites.Del(rowID)
	}
}

// ReleaseAllClaims releases all row claims for this transaction
func (tvs *TransactionVersionStore) ReleaseAllClaims() {
	// If we don't have a parent store, nothing to do
	if tvs.parentStore == nil || tvs.parentStore.uncommittedWrites == nil {
		return
	}

	// Iterate through all uncommitted writes and remove those belonging to this transaction
	toDelete := make([]int64, 0)

	tvs.parentStore.uncommittedWrites.ForEach(func(rowID, txnID int64) bool {
		if txnID == tvs.txnID {
			toDelete = append(toDelete, rowID)
		}
		return true
	})

	// Delete all claims for this transaction
	for _, rowID := range toDelete {
		tvs.parentStore.uncommittedWrites.Del(rowID)
	}
}

// CreateColumnarIndex creates a columnar index for a specific column
func (vs *VersionStore) CreateColumnarIndex(tableName string, columnName string, columnID int,
	dataType storage.DataType, isUnique bool, customName string) (storage.Index, error) {

	// Check if the version store is closed using atomic operation
	if vs.closed.Load() {
		return nil, errors.New("version store is closed")
	}

	// Generate index name early so we can check if it already exists
	indexName := customName
	if indexName == "" {
		// Generate default name if custom name is not provided
		if isUnique {
			indexName = fmt.Sprintf("unique_columnar_%s_%s", tableName, columnName)
		} else {
			indexName = fmt.Sprintf("columnar_%s_%s", tableName, columnName)
		}
	}

	// First check with a read lock to see if the index already exists
	vs.indexMutex.RLock()

	// Check for existing index by name
	indexExists := false
	for _, idx := range vs.indexes {
		if idx.Name() == indexName {
			indexExists = true
			break
		}
	}
	vs.indexMutex.RUnlock()

	if indexExists {
		return nil, fmt.Errorf("columnar index with name %s already exists", indexName)
	}

	// Use the btree implementation with the isUnique parameter
	index := NewColumnarIndex(indexName, tableName, columnName, columnID, dataType, vs, isUnique)

	// Build the index from existing data
	// This is done outside of lock to avoid holding the lock during expensive operations
	err := index.Build()
	if err != nil {
		// Close the index to clean up any resources
		index.Close()
		return nil, err
	}

	// Check again if the store was closed during our potentially long build operation
	if vs.closed.Load() {
		// Close the index to clean up any resources
		index.Close()
		return nil, errors.New("version store is closed")
	}

	// Now acquire the lock to update the map
	vs.indexMutex.Lock()
	defer vs.indexMutex.Unlock()

	// Check again if the index already exists by name
	// Someone else might have created it while we were building
	for _, existingIndex := range vs.indexes {
		if existingIndex.Name() == indexName {
			// Close the index to clean up any resources, since we won't be using it
			index.Close()
			return nil, fmt.Errorf("columnar index with name %s already exists", indexName)
		}
	}

	// One final check if the version store was closed while we were waiting for the lock
	if vs.closed.Load() {
		// Close the index to clean up any resources
		index.Close()
		return nil, errors.New("version store is closed")
	}

	// Store in the map using the index name as the key
	vs.indexes[indexName] = index

	return index, nil
}

// GetColumnarIndex retrieves a columnar index by identifier (index name)
func (vs *VersionStore) GetColumnarIndex(indexIdentifier string) (storage.Index, error) {
	// Check if the version store is closed using atomic operation
	if vs.closed.Load() {
		return nil, errors.New("version store is closed")
	}

	// Acquire read lock to access the indexes map
	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	// First try direct map lookup by name (the key might be the index name)
	if index, exists := vs.indexes[indexIdentifier]; exists {
		return index, nil
	}

	// If not found, search for index with the given name
	for _, index := range vs.indexes {
		if index.Name() == indexIdentifier {
			return index, nil
		}
	}

	return nil, fmt.Errorf("index %s not found", indexIdentifier)
}

// UpdateAccessTime records the current time as the last access time for a row
// This is used to identify hot/cold data for memory management
func (vs *VersionStore) UpdateAccessTime(rowID int64) {
	// Only track if persistence is enabled (otherwise we're memory-only and don't need this)
	if vs.engine != nil && vs.engine.persistence != nil && vs.engine.persistence.IsEnabled() {
		vs.accessTimes.Set(rowID, GetFastTimestamp())
	}
}

// canSafelyRemove checks if a deleted row can be safely removed from memory
// without violating transaction isolation guarantees
func (vs *VersionStore) canSafelyRemove(version *RowVersion) bool {
	// If no engine or registry, can't check visibility
	if vs.engine == nil || vs.engine.registry == nil {
		return false
	}

	// Get all active transactions
	activeTransactions := make([]int64, 0)
	vs.engine.registry.activeTransactions.ForEach(func(txnID int64, beginTS int64) bool {
		activeTransactions = append(activeTransactions, txnID)
		return true
	})

	// Check if any active transaction can see this deleted row
	for _, txnID := range activeTransactions {
		// Check if this transaction can see the row version
		if vs.engine.registry.IsVisible(version.TxnID, txnID) {
			// Now check if the deletion is NOT visible to this transaction
			// If deletion is not visible, the row is still visible to this transaction
			if version.DeletedAtTxnID == 0 || !vs.engine.registry.IsVisible(version.DeletedAtTxnID, txnID) {
				// An active transaction can still see this row (either not deleted or deletion not visible)
				return false
			}
		}
	}

	// Also check if the deleting transaction is still active
	if version.DeletedAtTxnID != 0 && vs.engine.registry.activeTransactions.Has(version.DeletedAtTxnID) {
		// The transaction that deleted this row is still active
		return false
	}

	return true
}

// CleanupOldPreviousVersions removes previous versions that are no longer needed by any active transaction
func (vs *VersionStore) CleanupOldPreviousVersions() int {
	// Check if the version store is closed
	if vs.closed.Load() {
		return 0
	}

	cleaned := 0

	// Get all active transaction IDs to check visibility
	activeTransactions := make([]int64, 0)
	vs.engine.registry.activeTransactions.ForEach(func(txnID int64, beginTS int64) bool {
		activeTransactions = append(activeTransactions, txnID)
		return true
	})

	// If no active transactions, we can clean all previous versions
	if len(activeTransactions) == 0 {
		vs.versions.ForEach(func(rowID int64, version *RowVersion) bool {
			if version.PrevVersion != nil {
				version.PrevVersion = nil
				cleaned++
			}
			return true
		})
		return cleaned
	}

	// Check each row to see if we can discard its previous version
	vs.versions.ForEach(func(rowID int64, version *RowVersion) bool {
		if version.PrevVersion == nil {
			return true // No previous version to clean
		}

		canDiscard := true
		for _, txnID := range activeTransactions {
			// If this transaction can see the previous version but not the current version,
			// we need to keep the previous version
			if vs.engine.registry.IsVisible(version.PrevVersion.TxnID, txnID) &&
				!vs.engine.registry.IsVisible(version.TxnID, txnID) {
				canDiscard = false
				break
			}
		}

		if canDiscard {
			version.PrevVersion = nil
			cleaned++
		}

		return true
	})

	return cleaned
}

// CleanupDeletedRows removes deleted rows that are older than the specified retention period
// This helps prevent memory leaks from accumulated deleted rows
func (vs *VersionStore) CleanupDeletedRows(retentionPeriod time.Duration) int {
	// Check if the version store is closed
	if vs.closed.Load() {
		return 0 // Skip cleanup if closed
	}

	// Current time for comparison
	now := time.Now().UnixNano()
	cutoffTime := now - retentionPeriod.Nanoseconds()

	var rowsToDelete []int64

	// First pass: identify deleted rows older than the retention period that are safe to remove
	vs.versions.ForEach(func(rowID int64, version *RowVersion) bool {
		// CRITICAL: Only process rows that are actually deleted
		if version != nil && version.IsDeleted() && version.CreateTime < cutoffTime {
			// Check if any active transaction can still see this row
			if vs.canSafelyRemove(version) {
				rowsToDelete = append(rowsToDelete, rowID)
			}
		}
		return true // Continue iteration
	})

	// Second pass: remove the identified rows
	for _, rowID := range rowsToDelete {
		// Get the version to extract column values for index removal
		if versionPtr, exists := vs.versions.Get(rowID); exists {
			// Remove from all indexes before deleting the version
			vs.indexMutex.RLock()
			for _, index := range vs.indexes {
				// Get the column IDs for this index
				columnIDs := index.ColumnIDs()

				// Extract column values based on index structure
				var values []storage.ColumnValue
				if len(columnIDs) == 1 {
					// Single-column index
					columnID := columnIDs[0]
					if columnID < len(versionPtr.Data) {
						values = []storage.ColumnValue{versionPtr.Data[columnID]}
					} else {
						values = []storage.ColumnValue{nil}
					}
				} else if len(columnIDs) > 1 {
					// Multi-column index
					values = make([]storage.ColumnValue, len(columnIDs))
					for i, columnID := range columnIDs {
						if columnID < len(versionPtr.Data) {
							values[i] = versionPtr.Data[columnID]
						} else {
							values[i] = nil
						}
					}
				}

				// Remove from index
				index.Remove(values, rowID, 0)
			}
			vs.indexMutex.RUnlock()
		}

		// Now remove from version store
		vs.versions.Del(rowID)
		// Also remove from access times tracking
		vs.accessTimes.Del(rowID)
	}

	return len(rowsToDelete)
}

// EvictColdData removes rows that haven't been accessed for longer than the specified period
// but only if they are already stored on disk (so they can be loaded again if needed)
// This helps manage memory usage by keeping only hot data in memory
func (vs *VersionStore) EvictColdData(coldPeriod time.Duration, maxRowsToEvict int) int {
	// Check if the version store is closed
	if vs.closed.Load() {
		return 0 // Skip cleanup if closed
	}

	// Skip if persistence is not enabled
	if vs.engine == nil || vs.engine.persistence == nil || !vs.engine.persistence.IsEnabled() {
		return 0 // Memory-only mode, no eviction
	}

	// Get the disk store for this table
	diskStore, exists := vs.engine.persistence.diskStores[vs.tableName]
	if !exists || diskStore == nil {
		return 0 // No disk store, cannot evict
	}

	// Current time for comparison
	now := GetFastTimestamp()
	cutoffTime := now - coldPeriod.Nanoseconds()

	var coldRows []int64

	// First pass: identify cold rows (not accessed recently)
	vs.accessTimes.ForEach(func(rowID int64, lastAccess int64) bool {
		// Skip if we've already found enough rows to evict
		if len(coldRows) >= maxRowsToEvict {
			return false // Stop iteration
		}

		// Check if this row is cold (not accessed recently)
		if lastAccess < cutoffTime {
			// Only evict if not deleted (deleted rows should be handled by CleanupDeletedRows)
			if versionPtr, exists := vs.versions.Get(rowID); exists && !versionPtr.IsDeleted() {
				// Only evict if we have a disk version that can be reloaded
				if diskStore.QuickCheckRowExists(rowID) {
					coldRows = append(coldRows, rowID)
				}
			}
		}

		return true // Continue iteration
	})

	// Second pass: evict the identified cold rows
	for _, rowID := range coldRows {
		// Remove from memory version store
		vs.versions.Del(rowID)

		// Remove from access times tracking
		vs.accessTimes.Del(rowID)

		// This allows the row to be loaded again from disk when it's next accessed
		if len(diskStore.readers) > 0 {
			// Only update the newest reader, which is the one we load from
			newestReader := diskStore.readers[len(diskStore.readers)-1]
			if newestReader.LoadedRowIDs != nil {
				newestReader.mu.Lock()
				newestReader.LoadedRowIDs.Del(rowID)
				newestReader.mu.Unlock()
			}
		}
	}

	return len(coldRows)
}

// Close releases resources associated with this version store
func (vs *VersionStore) Close() error {
	// Use atomic CompareAndSwap to ensure only one goroutine will do the actual closing
	// This is a more efficient replacement for the mutex-based approach
	if !vs.closed.CompareAndSwap(false, true) {
		// If already closed or another goroutine is closing it, return early
		return nil
	}

	// At this point we're the only goroutine that will execute the cleanup code
	// because we successfully changed the state from false to true

	// Clear all columnar indexes - still need a lock for map access
	vs.indexMutex.Lock()
	for name, index := range vs.indexes {
		if index != nil {
			// Call any cleanup needed for the index
			if closeableIndex, ok := index.(interface{ Close() error }); ok {
				_ = closeableIndex.Close() // Ignore errors during cleanup
			}
		}
		delete(vs.indexes, name)
	}
	vs.indexMutex.Unlock()

	// Clear all versions to release memory
	vs.versions = nil

	// The closed state is already set to true from the CompareAndSwap above
	return nil
}

// IndexExists checks if an index exists for this table
func (vs *VersionStore) IndexExists(indexName string) bool {
	// If the store is closed, no indexes exist
	if vs.closed.Load() {
		return false
	}

	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	// Check if the index exists by name
	_, exists := vs.indexes[indexName]
	return exists
}

// ListIndexes returns all indexes for this table
// Since indexes are now stored by name, this returns a map of index names to their primary column name
func (vs *VersionStore) ListIndexes() map[string]string {
	// If the store is closed, return empty list
	if vs.closed.Load() {
		return map[string]string{}
	}

	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	indexes := make(map[string]string, len(vs.indexes))
	for indexName, index := range vs.indexes {
		// Get the first column name as the primary column (for backward compatibility)
		columnNames := index.ColumnNames()
		if len(columnNames) > 0 {
			indexes[indexName] = columnNames[0]
		} else {
			indexes[indexName] = "" // Should never happen, but just in case
		}
	}

	return indexes
}

// GetTableSchema returns the schema for this table
func (vs *VersionStore) GetTableSchema() (storage.Schema, error) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return storage.Schema{}, errors.New("version store is closed")
	}

	// Use the engine reference that was provided during initialization
	if vs.engine == nil {
		return storage.Schema{}, errors.New("engine not available")
	}

	// Get the schema from the engine
	return vs.engine.GetTableSchema(vs.tableName)
}

// AddIndex adds an index to the version store
func (vs *VersionStore) AddIndex(index storage.Index) error {
	// Check if the version store is closed
	if vs.closed.Load() {
		return errors.New("version store is closed")
	}

	// Check if index is nil
	if index == nil {
		return errors.New("index cannot be nil")
	}

	// Get the index name
	indexName := index.Name()

	vs.indexMutex.Lock()
	defer vs.indexMutex.Unlock()

	// Check if an index with this name already exists
	for _, existing := range vs.indexes {
		if existing.Name() == indexName {
			return fmt.Errorf("index %s already exists", indexName)
		}
	}

	// Get the column names
	columnNames := index.ColumnNames()
	if len(columnNames) == 0 {
		return fmt.Errorf("index must have at least one column")
	}

	// Store the index by its name for consistent lookup across both single-column and multi-column indexes
	vs.indexes[indexName] = index

	return nil
}

// CreateIndex creates an index with the given properties
func (vs *VersionStore) CreateIndex(meta *IndexMetadata) (storage.Index, error) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return nil, errors.New("version store is closed")
	}

	// Validate required fields
	if meta == nil {
		return nil, errors.New("index metadata cannot be nil")
	}

	if meta.Name == "" {
		return nil, errors.New("index name cannot be empty")
	}

	if len(meta.ColumnNames) == 0 || len(meta.ColumnIDs) == 0 || len(meta.DataTypes) == 0 {
		return nil, errors.New("index must specify at least one column")
	}

	if len(meta.ColumnNames) != len(meta.ColumnIDs) || len(meta.ColumnNames) != len(meta.DataTypes) {
		return nil, errors.New("column names, IDs, and types must have the same length")
	}

	// First check with a read lock to see if an index with this name already exists
	vs.indexMutex.RLock()
	for _, existing := range vs.indexes {
		if existing.Name() == meta.Name {
			vs.indexMutex.RUnlock()
			return nil, fmt.Errorf("index with name %s already exists", meta.Name)
		}
	}

	// Release the read lock before proceeding
	vs.indexMutex.RUnlock()

	// We now check for existing indexes by name instead of by column

	// Create the appropriate type of index based on the number of columns
	var index storage.Index
	var err error

	if len(meta.ColumnNames) == 1 {
		// Create a single column index
		index = NewColumnarIndex(
			meta.Name,
			meta.TableName,
			meta.ColumnNames[0],
			meta.ColumnIDs[0],
			meta.DataTypes[0],
			vs,
			meta.IsUnique,
		)
	} else {
		// Create a multi-column index
		index = NewMultiColumnarIndex(
			meta.Name,
			meta.TableName,
			meta.ColumnNames,
			meta.ColumnIDs,
			meta.DataTypes,
			vs,
			meta.IsUnique,
		)
	}

	// Build the index
	err = index.Build()
	if err != nil {
		// Clean up the index
		if closeableIndex, ok := index.(interface{ Close() error }); ok {
			_ = closeableIndex.Close() // Ignore errors during cleanup
		}
		return nil, fmt.Errorf("failed to build index: %w", err)
	}

	// Add the index to our map
	err = vs.AddIndex(index)
	if err != nil {
		// Clean up the index
		if closeableIndex, ok := index.(interface{ Close() error }); ok {
			_ = closeableIndex.Close() // Ignore errors during cleanup
		}
		return nil, err
	}

	return index, nil
}

// RemoveIndex removes an index from the version store
func (vs *VersionStore) RemoveIndex(indexName string) error {
	// Check if the version store is closed
	if vs.closed.Load() {
		return errors.New("version store is closed")
	}

	vs.indexMutex.Lock()
	defer vs.indexMutex.Unlock()

	// Check if the index exists directly by name
	index, exists := vs.indexes[indexName]
	if exists {
		// Close the index if it has a Close method
		if closeableIndex, ok := index.(interface{ Close() error }); ok {
			_ = closeableIndex.Close() // Ignore errors during cleanup
		}

		// Remove from the map
		delete(vs.indexes, indexName)
		return nil
	}

	return fmt.Errorf("index %s not found", indexName)
}

// UpdateColumnarIndexes updates all columnar indexes with a new row version
func (vs *VersionStore) UpdateColumnarIndexes(rowID int64, version RowVersion) {
	// Check if the version store is closed
	if vs.closed.Load() {
		return // Skip index updates if closed
	}

	vs.indexMutex.RLock()
	defer vs.indexMutex.RUnlock()

	// If there are no columnar indexes, we can skip this
	if len(vs.indexes) == 0 {
		return
	}

	// Since we're only updating indexes for a single row, avoid the batch overhead
	// and just call Add/Remove directly on each index
	if version.IsDeleted() {
		// Remove from all indexes
		for _, index := range vs.indexes {
			// Get the column IDs for this index
			columnIDs := index.ColumnIDs()

			// Extract column values based on index structure
			var values []storage.ColumnValue
			if len(columnIDs) == 1 {
				// Single-column index
				columnID := columnIDs[0]
				if columnID < len(version.Data) {
					values = []storage.ColumnValue{version.Data[columnID]}
				} else {
					values = []storage.ColumnValue{nil}
				}
			} else if len(columnIDs) > 1 {
				// Multi-column index
				values = make([]storage.ColumnValue, len(columnIDs))
				for i, columnID := range columnIDs {
					if columnID < len(version.Data) {
						values[i] = version.Data[columnID]
					} else {
						values[i] = nil
					}
				}
			}

			// Remove from index
			index.Remove(values, rowID, 0)
		}
		return
	}

	// For non-deleted rows, update all relevant indexes
	for _, index := range vs.indexes {
		// Get the column IDs for this index
		columnIDs := index.ColumnIDs()

		// Extract column values based on index structure
		var values []storage.ColumnValue
		if len(columnIDs) == 1 {
			// Single-column index
			columnID := columnIDs[0]
			if columnID < len(version.Data) {
				values = []storage.ColumnValue{version.Data[columnID]}
			} else {
				values = []storage.ColumnValue{nil}
			}
		} else if len(columnIDs) > 1 {
			// Multi-column index
			values = make([]storage.ColumnValue, len(columnIDs))
			for i, columnID := range columnIDs {
				if columnID < len(version.Data) {
					values[i] = version.Data[columnID]
				} else {
					values[i] = nil
				}
			}
		}

		// Add to index
		index.Add(values, rowID, 0)
	}
}
