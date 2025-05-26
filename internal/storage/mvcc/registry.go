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
	"sync"
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

const (
	// InvalidTransactionID is returned when the registry is not accepting new transactions
	InvalidTransactionID = -999999999
)

// TransactionRegistry manages transaction states and visibility rules
// Lock-free implementation using our optimized SegmentInt64Map for optimal performance and concurrency
type TransactionRegistry struct {
	nextTxnID             atomic.Int64
	activeTransactions    *fastmap.SegmentInt64Map[int64] // txnID -> begin sequence number
	committedTransactions *fastmap.SegmentInt64Map[int64] // txnID -> commit sequence number
	isolationLevel        storage.IsolationLevel
	accepting             atomic.Bool  // Flag to control if new transactions are accepted
	nextSequence          atomic.Int64 // Single monotonic sequence for both begin and commit

	mu sync.RWMutex // RWMutex for additional safety in some operations
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry() *TransactionRegistry {
	reg := &TransactionRegistry{
		activeTransactions:    fastmap.NewSegmentInt64Map[int64](8, 1000),
		committedTransactions: fastmap.NewSegmentInt64Map[int64](8, 1000),
		isolationLevel:        storage.ReadCommitted, // Default isolation level
	}
	reg.accepting.Store(true) // Start accepting transactions by default
	return reg
}

// SetIsolationLevel sets the isolation level for this registry
func (r *TransactionRegistry) SetIsolationLevel(level storage.IsolationLevel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.isolationLevel = level
}

// GetIsolationLevel returns the current isolation level
func (r *TransactionRegistry) GetIsolationLevel() storage.IsolationLevel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.isolationLevel
}

// BeginTransaction starts a new transaction
func (r *TransactionRegistry) BeginTransaction() (txnID int64, beginTS int64) {
	// Check if we're accepting new transactions
	if !r.accepting.Load() {
		// Return InvalidTransactionID to indicate error condition
		return InvalidTransactionID, 0
	}

	// Generate a new transaction ID atomically
	txnID = r.nextTxnID.Add(1)
	// Use monotonic sequence instead of wall-clock time
	beginSeq := r.nextSequence.Add(1)

	// Record the transaction (thread-safe with SegmentInt64Map)
	r.activeTransactions.Set(txnID, beginSeq)

	return txnID, beginSeq
}

// CommitTransaction commits a transaction
func (r *TransactionRegistry) CommitTransaction(txnID int64) int64 {
	// Use monotonic sequence instead of wall-clock time
	// This ensures consistent ordering even with clock skew or low timer resolution
	commitSeq := r.nextSequence.Add(1)

	// First remove from active transactions to avoid deadlock
	// when a cleanup operation is running concurrently
	r.activeTransactions.Del(txnID)

	// Then add to committed transactions (storing the commit sequence)
	r.committedTransactions.Set(txnID, commitSeq)

	return commitSeq
}

// RecoverCommittedTransaction recreates a committed transaction during recovery
func (r *TransactionRegistry) RecoverCommittedTransaction(txnID int64, commitTS int64) {
	// During recovery, we directly add the transaction to the committed map
	// with its original commit timestamp
	r.committedTransactions.Set(txnID, commitTS)

	// Also update nextTxnID if necessary to ensure new transactions get unique IDs
	for {
		current := r.nextTxnID.Load()
		if txnID >= current {
			if r.nextTxnID.CompareAndSwap(current, txnID+1) {
				break
			}
			// If CAS failed, another thread updated it, try again
		} else {
			// Current ID is already higher, nothing to do
			break
		}
	}
}

// RecoverAbortedTransaction records an aborted transaction during recovery
func (r *TransactionRegistry) RecoverAbortedTransaction(txnID int64) {
	// We don't need to explicitly track aborted transactions
	// But we do need to update nextTxnID to avoid ID conflicts
	for {
		current := r.nextTxnID.Load()
		if txnID >= current {
			if r.nextTxnID.CompareAndSwap(current, txnID+1) {
				break
			}
			// If CAS failed, another thread updated it, try again
		} else {
			// Current ID is already higher, nothing to do
			break
		}
	}
}

// AbortTransaction marks a transaction as aborted
func (r *TransactionRegistry) AbortTransaction(txnID int64) {
	// Lock-free delete with SegmentInt64Map
	r.activeTransactions.Del(txnID)
	// No entry in committedTransactions means it was aborted
}

// GetCommitTimestamp gets the commit timestamp for a transaction
func (r *TransactionRegistry) GetCommitTimestamp(txnID int64) (int64, bool) {
	// Thread-safe get with SegmentInt64Map
	return r.committedTransactions.Get(txnID)
}

// GetTransactionBeginTime gets the begin timestamp for a transaction
func (r *TransactionRegistry) GetTransactionBeginSeq(txnID int64) int64 {
	// Thread-safe get with SegmentInt64Map
	beginSeq, exists := r.activeTransactions.Get(txnID)
	if exists {
		return beginSeq
	}
	// If not active, return 0 (transaction may have already committed/aborted)
	return 0
}

// IsDirectlyVisible is an optimized version that only checks common cases
// for better performance in bulk operations. It only returns true for
// already committed transactions (in ReadCommitted mode).
func (r *TransactionRegistry) IsDirectlyVisible(versionTxnID int64) bool {
	// Special case for recovery transactions with ID = -1
	// These are always visible to everyone
	if versionTxnID == -1 {
		return true
	}

	// Fast path for ReadCommitted isolation level (the default)
	// where any committed transaction is visible to all other transactions
	r.mu.RLock()
	isolationLevel := r.isolationLevel
	r.mu.RUnlock()

	if isolationLevel == storage.ReadCommitted {
		// Thread-safe check with SegmentInt64Map
		// This is a hot path that benefits from being as fast as possible
		return r.committedTransactions.Has(versionTxnID)
	}

	// For other isolation levels, we need full visibility check
	return false
}

// IsVisible determines if a row version is visible to a transaction
func (r *TransactionRegistry) IsVisible(versionTxnID int64, viewerTxnID int64) bool {
	// Special case for recovery transactions with ID = -1
	// These are always visible to everyone
	if versionTxnID == -1 {
		return true
	}

	// Ultra-fast path for reading own writes
	// This optimization helps performance for update/delete operations that read then write
	if versionTxnID == viewerTxnID {
		// Current transaction's own changes are always visible
		return true
	}

	// Fast path for common READ COMMITTED level (most databases default to this)
	r.mu.RLock()
	isolationLevel := r.isolationLevel
	r.mu.RUnlock()

	if isolationLevel == storage.ReadCommitted {
		// In READ COMMITTED, only committed transactions are visible
		// This delegation is inlinable and very efficient
		return r.IsDirectlyVisible(versionTxnID)
	}

	// For SNAPSHOT isolation, we need full visibility check
	// All operations are thread-safe with SegmentInt64Map

	// Transaction can only see committed changes from other transactions
	// Lock-free access via SegmentInt64Map
	commitTS, committed := r.committedTransactions.Get(versionTxnID)
	if !committed {
		// Not committed, definitely not visible
		return false
	}

	// For Snapshot Isolation, version must be committed before viewer began
	// Lock-free access via SegmentInt64Map
	viewerBeginTS, viewerActive := r.activeTransactions.Get(viewerTxnID)
	if !viewerActive {
		// Viewer transaction isn't active, use its commit time
		// Lock-free access via SegmentInt64Map
		viewerBeginTS, committed = r.committedTransactions.Get(viewerTxnID)
		if !committed {
			// If viewer isn't committed or active, it must be aborted
			return false
		}
	}

	// Version must have been committed before or at the same time viewer began
	// This timestamp comparison is the core of snapshot isolation
	return commitTS <= viewerBeginTS
}

// CleanupOldTransactions removes committed transactions older than maxAge
// This helps prevent memory leaks from accumulating transaction records
// IMPORTANT: In READ COMMITTED mode, we cannot clean up committed transactions
// because data visibility depends on the transaction being in committedTransactions.
// Only SNAPSHOT isolation can safely clean up old transactions.
func (r *TransactionRegistry) CleanupOldTransactions(maxAge time.Duration) int {
	// In READ COMMITTED mode, we cannot clean up committed transactions
	// because IsDirectlyVisible checks if the transaction exists in committedTransactions
	r.mu.RLock()
	isolationLevel := r.isolationLevel
	r.mu.RUnlock()

	if isolationLevel == storage.ReadCommitted {
		return 0
	}

	// Calculate the cutoff timestamp for old transactions
	cutoffTime := time.Now().Add(-maxAge).UnixNano()

	// Get the list of all active transaction IDs to avoid cleaning up
	// transactions that might still be needed for visibility checks
	var activeSet map[int64]struct{}

	// If we're in snapshot isolation mode, we need to preserve transactions
	// that might still be visible to active transactions
	if isolationLevel == storage.SnapshotIsolation {
		activeSet = make(map[int64]struct{})

		// Collect all active transaction IDs into our map
		// This is done in a separate loop to avoid nested locks
		txnIDs := make([]int64, 0, 100)
		r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
			txnIDs = append(txnIDs, txnID)
			return true
		})

		// Now populate our set without holding any locks
		for _, txnID := range txnIDs {
			activeSet[txnID] = struct{}{}
		}
	}

	// Track how many transactions we remove
	removed := 0

	// First collect transactions to remove to avoid modifying during iteration
	txnsToRemove := make([]int64, 0, 100)

	// Find transactions to remove
	r.committedTransactions.ForEach(func(txnID, commitTS int64) bool {
		// NEVER clean up negative transaction IDs
		// These are special transactions (like recovery) that must remain visible
		if txnID < 0 {
			return true
		}

		// Skip transactions that are still active
		if isolationLevel == storage.SnapshotIsolation {
			if _, isActive := activeSet[txnID]; isActive {
				return true
			}
		}

		// Only mark transactions older than the cutoff
		if commitTS < cutoffTime {
			txnsToRemove = append(txnsToRemove, txnID)
		}
		return true
	})

	// Now perform the actual deletions outside the iteration loop
	for _, txnID := range txnsToRemove {
		r.committedTransactions.Del(txnID)
		removed++
	}

	return removed
}

// WaitForActiveTransactions waits for all active transactions to complete with timeout
// Returns the number of transactions that were still active after timeout
func (r *TransactionRegistry) WaitForActiveTransactions(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)

	for {
		// Check if we've reached the timeout
		if time.Now().After(deadline) {
			break
		}

		// Count active transactions - SegmentInt64Map is already thread-safe
		activeCount := 0
		r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
			activeCount++
			return true
		})

		// If no active transactions, we're done
		if activeCount == 0 {
			return 0
		}

		// Wait a short time before checking again
		time.Sleep(10 * time.Millisecond)
	}

	// Return the number of transactions still active after timeout
	activeCount := 0
	r.activeTransactions.ForEach(func(txnID, beginTS int64) bool {
		activeCount++
		return true
	})

	return activeCount
}

// StopAcceptingTransactions stops the registry from accepting new transactions
func (r *TransactionRegistry) StopAcceptingTransactions() {
	r.accepting.Store(false)
}
