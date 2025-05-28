---
title: MVCC Implementation
category: Architecture
order: 1
---

# MVCC Implementation

This document provides a detailed explanation of Stoolap's Multi-Version Concurrency Control (MVCC) implementation, which enables transaction isolation with minimal locking.

## MVCC Overview

Multi-Version Concurrency Control (MVCC) is a concurrency control method used by Stoolap to provide transaction isolation. The key principles are:

1. Maintain the latest committed version of each row (single-version design)
2. Track deletion status with transaction IDs for proper visibility
3. Each transaction has a consistent view based on visibility rules
4. Reads never block writes, and writes never block reads
5. Implement optimistic concurrency control for conflict detection in SNAPSHOT isolation

## Design Philosophy

Stoolap implements a **single-version MVCC** design, which differs from traditional multi-version systems:

- **Single Version Per Row**: Only the latest committed version is kept in memory
- **Deletion Tracking**: Deleted rows are marked with `DeletedAtTxnID` rather than removed
- **Transaction ID Preservation**: Updates preserve the original creator's transaction ID
- **Memory Efficiency**: Reduced memory footprint compared to full multi-version systems
- **Simplified Garbage Collection**: Deleted rows can be cleaned up after visibility expires

## Core Components

### Transaction Registry

- Manages transaction lifecycle and state tracking
- Assigns unique transaction IDs using atomic counters
- Tracks active and committed transactions with monotonic sequences
- Supports per-transaction isolation levels without race conditions
- Implements visibility rules for both READ COMMITTED and SNAPSHOT isolation

### Version Store

- Maintains single latest version of each row
- Tracks both creation (`TxnID`) and deletion (`DeletedAtTxnID`) transaction IDs
- Implements efficient concurrent access using segment maps
- Manages columnar and bitmap indexes
- Provides visibility-aware iteration and retrieval

### Row Version Structure

```go
type RowVersion struct {
    TxnID          int64       // Transaction that created this version
    DeletedAtTxnID int64       // Transaction that deleted this version (0 if not deleted)
    Data           storage.Row // Complete row data
    RowID          int64       // Row identifier
    CreateTime     int64       // Timestamp when created
}
```

## Transaction IDs and Timestamps

Stoolap uses monotonic sequences instead of wall-clock timestamps to avoid platform-specific timing issues:

- **Transaction ID**: Unique identifier assigned atomically
- **Begin Sequence**: Monotonic sequence when transaction starts
- **Commit Sequence**: Monotonic sequence when transaction commits
- **Write Sequences**: Track when rows were last modified for conflict detection

This approach solves Windows' 15.6ms timer resolution issue and ensures consistent ordering.

## Isolation Levels

### READ COMMITTED (Default)

- Transactions see committed changes immediately
- No global locks for commits - high concurrency
- Each statement sees the latest committed data
- Suitable for most OLTP workloads

Implementation:
```go
// In READ COMMITTED, only check if transaction is committed
func IsDirectlyVisible(versionTxnID int64) bool {
    return r.committedTransactions.Has(versionTxnID)
}
```

### SNAPSHOT Isolation

- Transactions see a consistent snapshot from when they started
- Write-write conflict detection prevents lost updates
- Serialized commits via global mutex ensure correctness
- Lower throughput but stronger consistency guarantees

Implementation:
```go
// In SNAPSHOT, check if version was committed before viewer began
func IsVisible(versionTxnID, viewerTxnID int64) bool {
    commitTS, committed := r.committedTransactions.Get(versionTxnID)
    if !committed {
        return false
    }
    viewerBeginTS := r.GetTransactionBeginSeq(viewerTxnID)
    return commitTS <= viewerBeginTS
}
```

## Visibility Rules

### Row Visibility

A row is visible to a transaction if:
1. The row was created by a visible transaction, AND
2. The row was NOT deleted, OR the deletion is not visible

### Deletion Visibility

With `DeletedAtTxnID` tracking:
```go
// Check if row is visible considering deletion
if versionPtr.DeletedAtTxnID != 0 {
    // Check if deletion is visible
    deletionVisible := registry.IsVisible(versionPtr.DeletedAtTxnID, txnID)
    if versionPtr.DeletedAtTxnID != txnID && deletionVisible {
        // Deletion is visible, skip this row
        return false
    }
}
// Row is visible (not deleted or deletion not visible)
return true
```

### Transaction-Specific Isolation

Each transaction maintains its own isolation level:
```go
// Set isolation level for specific transaction
registry.SetTransactionIsolationLevel(txnID, level)

// Get isolation level for visibility checks
isolationLevel := registry.GetIsolationLevel(txnID)
```

## Concurrency Control

### SNAPSHOT Isolation Conflicts

Write-write conflict detection during commit:

```go
// Check for conflicts before commit
if versionStore.CheckWriteConflict(writtenRows, beginSeq) {
    return errors.New("transaction aborted due to write-write conflict")
}

// Set write sequences after successful commit
versionStore.SetWriteSequences(writtenRows, commitSeq)
```

### Commit Synchronization

SNAPSHOT commits are serialized to prevent race conditions:
1. Acquire global commit mutex
2. Check for write-write conflicts
3. Generate commit sequence
4. Apply changes to version stores
5. Set write sequences atomically
6. Mark transaction as committed
7. Release mutex

## Single-Version Design Implications

### Updates

When a row is updated:
- The new data replaces the old data
- The original `TxnID` is preserved (non-standard MVCC behavior)
- Previous versions are not accessible
- This limits true point-in-time queries

### Deletions

When a row is deleted:
- The row remains in the version store
- `DeletedAtTxnID` is set to the deleting transaction's ID
- Data is preserved for transactions that need visibility
- Garbage collection eventually removes invisible deleted rows

### Limitations

1. **No Historical Versions**: Cannot query past states beyond current snapshot
2. **Update Visibility**: Updates immediately replace data for all future viewers
3. **No Savepoints**: Transaction savepoints not supported
4. **Limited Isolation Levels**: Only READ COMMITTED and SNAPSHOT

## Performance Optimizations

### Lock-Free Data Structures

- `SegmentInt64Map`: High-performance concurrent maps
- Atomic operations for counters and flags
- Minimal mutex usage in hot paths

### Object Pooling

- Transaction objects
- Table objects  
- Version maps
- Reduces GC pressure in high-throughput scenarios

### Optimized Visibility Checks

- Fast path for own-transaction visibility
- Direct visibility check for READ COMMITTED
- Batch processing for bulk operations

### Memory Management

- Single version reduces memory footprint
- Efficient row representation
- Periodic cleanup of deleted rows
- Cold data eviction to disk

## Garbage Collection

Deleted rows are cleaned up based on:
1. Retention period (age-based)
2. Transaction visibility (no active transaction can see them)
3. Safety checks to prevent removing visible data

```go
func canSafelyRemove(version *RowVersion) bool {
    // Check if any active transaction can see this deleted row
    for _, txnID := range activeTransactions {
        if registry.IsVisible(version.TxnID, txnID) {
            if !registry.IsVisible(version.DeletedAtTxnID, txnID) {
                // Row still visible to this transaction
                return false
            }
        }
    }
    return true
}
```

## Key Implementation Files

- **registry.go**: Transaction registry and visibility rules
- **version_store.go**: Row version storage and management
- **transaction.go**: Transaction implementation and conflict detection
- **engine.go**: MVCC engine coordinating all components
- **table.go**: Table operations with MVCC support

## Best Practices

1. **Choose Appropriate Isolation**: Use READ COMMITTED unless you need snapshot consistency
2. **Keep Transactions Short**: Long transactions delay garbage collection
3. **Handle Conflicts**: Implement retry logic for SNAPSHOT conflicts
4. **Monitor Deleted Rows**: Ensure garbage collection keeps up with deletions
5. **Batch Operations**: Group related changes in single transactions

## Future Improvements

Potential enhancements to the current design:
1. **Multi-Version Storage**: Keep multiple versions for better SNAPSHOT performance
2. **Row-Level Locking**: Replace global mutex with fine-grained locks
3. **Additional Isolation Levels**: REPEATABLE READ, SERIALIZABLE
4. **Savepoint Support**: Transaction savepoints for partial rollbacks
5. **Historical Queries**: Time-travel queries with version retention