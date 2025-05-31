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
	nextTxnID                 atomic.Int64
	activeTransactions        *fastmap.SegmentInt64Map[int64] // txnID -> begin sequence number
	committedTransactions     *fastmap.SegmentInt64Map[int64] // txnID -> commit sequence number
	committingTransactions    *fastmap.SegmentInt64Map[int64] // txnID -> commit sequence number (in committing state)
	globalIsolationLevel      storage.IsolationLevel
	transactionIsolationLevel map[int64](storage.IsolationLevel) // Per-transaction isolation level
	accepting                 atomic.Bool                        // Flag to control if new transactions are accepted
	nextSequence              atomic.Int64                       // Single monotonic sequence for both begin and commit

	mu sync.RWMutex // RWMutex for additional safety in some operations

	commitCond *sync.Cond
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry() *TransactionRegistry {
	reg := &TransactionRegistry{
		activeTransactions:        fastmap.NewSegmentInt64Map[int64](8, 1000),
		committedTransactions:     fastmap.NewSegmentInt64Map[int64](8, 1000),
		committingTransactions:    fastmap.NewSegmentInt64Map[int64](8, 1000),
		transactionIsolationLevel: make(map[int64]storage.IsolationLevel, 100), // Preallocate for 100 transactions
		commitCond:                sync.NewCond(&sync.Mutex{}),
	}
	reg.accepting.Store(true) // Start accepting transactions by default
	return reg
}

// SetTransactionIsolationLevel sets the isolation level for new transactions
func (r *TransactionRegistry) SetTransactionIsolationLevel(txnID int64, level storage.IsolationLevel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Update the transaction-specific isolation level
	r.transactionIsolationLevel[txnID] = level
}

// RemoveTransactionIsolationLevel removes the isolation level for a transaction
func (r *TransactionRegistry) RemoveTransactionIsolationLevel(txnID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove the transaction-specific isolation level
	delete(r.transactionIsolationLevel, txnID)
}

// SetGlobalIsolationLevel sets the isolation level for this registry
func (r *TransactionRegistry) SetGlobalIsolationLevel(level storage.IsolationLevel) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.globalIsolationLevel = level
}

// GetGlobalIsolationLevel returns the current global isolation level
func (r *TransactionRegistry) GetGlobalIsolationLevel() storage.IsolationLevel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.globalIsolationLevel
}

// GetIsolationLevel returns the current isolation level
func (r *TransactionRegistry) GetIsolationLevel(txnID int64) storage.IsolationLevel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if level, ok := r.transactionIsolationLevel[txnID]; ok {
		// If transaction-specific level is set, use that
		return level
	}

	return r.globalIsolationLevel
}

// BeginTransaction starts a new transaction
func (r *TransactionRegistry) BeginTransaction() (txnID int64, beginSeq int64) {
	// Check if we're accepting new transactions
	if !r.accepting.Load() {
		// Return InvalidTransactionID to indicate error condition
		return InvalidTransactionID, 0
	}

	r.commitCond.L.Lock() // Lock to ensure no committing transactions are in progress
	defer r.commitCond.L.Unlock()

	for r.committingTransactions.Len() != 0 {
		// Wait for all committing transactions to finish
		r.commitCond.Wait()
	}

	// Generate a new transaction ID atomically
	txnID = r.nextTxnID.Add(1)
	// Use monotonic sequence
	beginSeq = r.nextSequence.Add(1)

	// Record the transaction (thread-safe with SegmentInt64Map)
	r.activeTransactions.Set(txnID, beginSeq)

	return txnID, beginSeq
}

// StartCommit begins the commit process for a transaction
// This puts the transaction into "committing" state - it's no longer active
// but not yet fully committed. Changes won't be visible to other transactions yet.
func (r *TransactionRegistry) StartCommit(txnID int64) int64 {
	r.commitCond.L.Lock()
	defer r.commitCond.L.Unlock()

	// Get the commit sequence
	commitSeq := r.nextSequence.Add(1)

	// Move from active to committing state
	r.activeTransactions.Del(txnID)
	r.committingTransactions.Set(txnID, commitSeq)

	return commitSeq
}

// CompleteCommit completes the commit process for a transaction
// This moves the transaction from "committing" to "committed" state
// making all its changes atomically visible to other transactions
func (r *TransactionRegistry) CompleteCommit(txnID int64) {
	r.commitCond.L.Lock() // Lock to ensure no committing transactions are in progress
	// Get the commit sequence from committing state
	commitSeq, exists := r.committingTransactions.Get(txnID)
	if !exists {
		// Transaction not in committing state - this is an error
		// but we'll handle it gracefully by using current sequence
		commitSeq = r.nextSequence.Load()
	}

	// Move from committing to committed state
	r.committingTransactions.Del(txnID)
	r.committedTransactions.Set(txnID, commitSeq)
	r.commitCond.L.Unlock()

	r.commitCond.Broadcast() // Notify any waiting threads that commit is done
}

// CommitTransaction commits a transaction for read-only transactions
func (r *TransactionRegistry) CommitTransaction(txnID int64) int64 {
	// This ensures consistent ordering with atomic sequence
	commitSeq := r.nextSequence.Add(1)

	// First remove from active transactions to avoid deadlock
	// when a cleanup operation is running concurrently
	r.activeTransactions.Del(txnID)

	// Then add to committed transactions (storing the commit sequence)
	r.committedTransactions.Set(txnID, commitSeq)

	return commitSeq
}

// RecoverCommittedTransaction recreates a committed transaction during recovery
func (r *TransactionRegistry) RecoverCommittedTransaction(txnID int64, commitSeq int64) {
	// During recovery, we directly add the transaction to the committed map
	r.committedTransactions.Set(txnID, commitSeq)

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
	// Remove from whatever state it's in
	r.activeTransactions.Del(txnID)

	// Check if we need to notify
	if r.committingTransactions.Has(txnID) {
		r.commitCond.L.Lock()
		r.committingTransactions.Del(txnID)
		r.commitCond.L.Unlock()

		// Notify any waiting threads that commit is done
		r.commitCond.Broadcast()
	}
}

// GetCommitTimestamp gets the commit sequence for a transaction
func (r *TransactionRegistry) GetCommitSequence(txnID int64) (int64, bool) {
	// Thread-safe get with SegmentInt64Map
	return r.committedTransactions.Get(txnID)
}

// GetTransactionBeginTime gets the begin sequence for a transaction
func (r *TransactionRegistry) GetTransactionBeginSequence(txnID int64) int64 {
	// Thread-safe get with SegmentInt64Map
	beginSeq, exists := r.activeTransactions.Get(txnID)
	if exists {
		return beginSeq
	}
	// If not active, return 0 (transaction may have already committed/aborted)
	return 0
}

// GetCurrentSequence returns the current sequence number
func (r *TransactionRegistry) GetCurrentSequence() int64 {
	return r.nextSequence.Load()
}

// IsDirectlyVisible is an optimized version that only checks common cases
// for better performance in bulk operations. It only returns true for
// already committed transactions (in ReadCommitted mode).
// Note: This method assumes the caller has already verified the isolation level
func (r *TransactionRegistry) IsDirectlyVisible(versionTxnID int64) bool {
	// Special case for recovery transactions with ID = -1
	// These are always visible to everyone
	if versionTxnID == -1 {
		return true
	}

	// Check if transaction is in committing state - these are NOT visible
	if r.committingTransactions.Has(versionTxnID) {
		return false
	}

	// In READ COMMITTED mode, only FULLY committed transactions are visible
	return r.committedTransactions.Has(versionTxnID)
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

	if r.GetIsolationLevel(viewerTxnID) == storage.ReadCommitted {
		// In READ COMMITTED, only committed transactions are visible
		// This delegation is inlinable and very efficient
		return r.IsDirectlyVisible(versionTxnID)
	}

	// Check if transaction is in committing state - these are NOT visible
	if r.committingTransactions.Has(versionTxnID) {
		return false
	}

	// Transaction can only see committed changes from other transactions
	commitSeq, committed := r.committedTransactions.Get(versionTxnID)
	if !committed {
		// Not committed, definitely not visible
		return false
	}

	// For Snapshot Isolation, version must be committed before viewer began
	viewerBeginSeq, viewerActive := r.activeTransactions.Get(viewerTxnID)
	if !viewerActive {
		return false // Viewer transaction is not active, cannot see anything
	}

	return commitSeq <= viewerBeginSeq
}

// CleanupOldTransactions removes committed transactions older than maxAge
// This helps prevent memory leaks from accumulating transaction records
// IMPORTANT: In READ COMMITTED mode, we cannot clean up committed transactions
// because data visibility depends on the transaction being in committedTransactions.
// Only SNAPSHOT isolation can safely clean up old transactions.
func (r *TransactionRegistry) CleanupOldTransactions(maxAge time.Duration) int {
	// In READ COMMITTED mode, we cannot clean up committed transactions
	// because IsDirectlyVisible checks if the transaction exists in committedTransactions
	isolationLevel := r.GetGlobalIsolationLevel() // Retrieve global isolation level explicitly

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
		r.activeTransactions.ForEach(func(txnID, beginSeq int64) bool {
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
	r.committedTransactions.ForEach(func(txnID, commitSeq int64) bool {
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
		if commitSeq < cutoffTime {
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
		r.activeTransactions.ForEach(func(txnID, beginSeq int64) bool {
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
	r.activeTransactions.ForEach(func(txnID, beginSeq int64) bool {
		activeCount++
		return true
	})

	return activeCount
}

// StopAcceptingTransactions stops the registry from accepting new transactions
func (r *TransactionRegistry) StopAcceptingTransactions() {
	r.accepting.Store(false)
}
