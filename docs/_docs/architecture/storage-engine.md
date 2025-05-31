---
title: Storage Engine
category: Architecture
order: 1
---

# Storage Engine

This document provides a detailed overview of Stoolap's storage engine, including its design principles, components, and how data is stored and retrieved.

## Storage Engine Design

Stoolap's storage engine is designed with the following principles:

- **HTAP Architecture** - Combines row-based storage with columnar indexing for hybrid workloads
- **Memory-optimized** - Prioritizes in-memory performance with optional persistence
- **MVCC-based** - Uses multi-version concurrency control for transaction isolation
- **Version-organized** - Tracks different versions of rows for transaction isolation
- **Type-specialized** - Uses different strategies for different data types
- **Index-accelerated** - Multiple index types to optimize different query patterns

## Storage Components

### Table Structure

Tables in Stoolap are composed of:

- **Metadata** - Schema information, column definitions, and indexes
- **Row Data** - The primary data storage, organized by row
- **Version Store** - Tracks row versions for MVCC
- **Columnar Indexes** - Optional column-oriented indexes for analytical queries
- **Transaction Manager** - Manages transaction state and visibility

### Data Types

Stoolap supports a variety of data types, each with optimized storage:

- **Int64** - Optimized for 64-bit integers
- **Float64** - Optimized for 64-bit floating-point numbers
- **String** - Optimized for variable-length string data
- **Bool** - Optimized for boolean values
- **Timestamp** - Optimized for datetime values
- **JSON** - Optimized for JSON documents
- **NULL values** - Efficiently tracked for any data type

### Version Management

Stoolap tracks different versions of data for transaction isolation:

- Each change creates a new version rather than overwriting
- Versions are associated with transaction IDs
- Visibility rules determine which versions each transaction can see
- Old versions are garbage collected when no longer needed

## Data Storage Format

### In-Memory Format

In memory, data is stored with these characteristics:

- **Row-based primary storage** - Records are stored as coherent rows
- **Version chains** - Linked versions for MVCC
- **Columnar indexes** - Column-oriented organization for analytical access
- **Type-specific structures** - Optimized for different data types
- **Bitmap filters** - Fast filtering for lookups

### On-Disk Format

When persistence is enabled, data is stored on disk with:

- **Binary serialization** - Compact binary format for storage
- **Row files** - Primary data in row format
- **Index files** - Columnar index data
- **Metadata files** - Schema and index information
- **WAL files** - Write-ahead log for durability
- **Snapshot files** - Point-in-time table snapshots

## MVCC Implementation

The storage engine uses MVCC to provide transaction isolation:

- **Full Version Chains** - Unlimited version history per row linked via pointers
- **Transaction IDs** - Each version is associated with a transaction ID
- **Visibility Rules** - Traverse version chains to find visible versions
- **Lock-Free Design** - No table-level locks, optimistic concurrency control
- **Automatic Cleanup** - Old versions garbage collected when no longer needed

For more details, see the [MVCC Implementation](mvcc-implementation.md) and [Transaction Isolation](transaction-isolation.md) documentation.

## Data Access Paths

### OLTP Access Path

For transactional operations:

1. The storage engine uses row-based access for efficiency
2. Indexes help locate specific rows quickly
3. Visibility rules are applied to ensure transaction isolation
4. Point operations are optimized for low latency

### OLAP Access Path

For analytical operations:

1. Columnar indexes are used to minimize data access
2. Batch processing is used for vectorized execution
3. Filter pushdown optimizes query execution
4. Parallel scanning improves throughput

## Data Modification

### Insert Operations

When data is inserted:

1. Values are validated against column types
2. A new row version is created with the current transaction ID
3. The row is added to the primary row storage
4. Columnar indexes are updated
5. The operation is recorded in the WAL (if enabled)

### Update Operations

When data is updated:

1. The existing row is located via indexes or scan
2. A new version is created with updated values
3. The new version's `prev` pointer links to the current version
4. The version chain grows backward in time
5. Indexes are updated to reflect the changes
6. The operation is recorded in the WAL (if enabled)

### Delete Operations

When data is deleted:

1. The existing row is located via indexes or scan
2. A new version is created with `DeletedAtTxnID` set
3. The deletion version links to the previous version
4. Data is preserved in the deletion version
5. Indexes are updated to reflect the deletion
6. The operation is recorded in the WAL (if enabled)

## Persistence and Recovery

When persistence is enabled:

### Write-Ahead Logging (WAL)

1. All modifications are recorded in the WAL before being applied
2. WAL entries include transaction ID, operation type, and data
3. WAL is flushed to disk based on sync mode configuration
4. This ensures durability in case of crashes

### Snapshots

1. Periodically, consistent snapshots of tables are created
2. Snapshots contain only the latest version of each row
3. Version chains are rebuilt from WAL replay during recovery
4. Multiple snapshots may be retained for safety
5. Snapshots accelerate recovery compared to replaying the entire WAL

### Recovery Process

After a crash, recovery proceeds as follows:

1. The latest valid snapshot is loaded for each table
2. WAL entries after the snapshot are replayed
3. Transaction state is reconstructed
4. Incomplete transactions are rolled back

## Memory Management

The storage engine includes several memory optimization techniques:

### Buffer Pool

- Reusable memory buffers reduce allocation overhead
- Buffers are managed in pools by size categories
- Used for both in-memory operations and disk I/O

### Value Pool

- Specialized object pooling for common data types
- Reduces garbage collection pressure
- Particularly beneficial for string and structural data

### Segment Maps

- Efficient concurrent data structures
- Optimized for different access patterns
- Specialized implementations for common key types (Int64)

## Implementation Details

Core storage engine components:

- **storage.go** - Storage interfaces and factory functions
- **column.go** - Column type implementations
- **mvcc/** - MVCC implementation components
  - **engine.go** - MVCC storage engine
  - **table.go** - Table implementation with row-based storage
  - **transaction.go** - Transaction management
  - **version_store.go** - Version tracking for rows
  - **columnar_index.go** - Columnar indexing for analytics
  - **scanner.go** - Data scanning
  - **disk_version_store.go** - Persistence implementation
  - **wal_manager.go** - Write-ahead logging
- **compression/** - Data compression implementations
- **expression/** - Storage-level expression evaluation

## HTAP Performance Characteristics

### OLTP Performance

- **Fast Point Operations** - Row-based storage optimizes transaction processing
- **Low Latency** - Designed for quick response time
- **High Concurrency** - MVCC enables simultaneous access
- **Efficient Writes** - Optimized for insert and update operations

### OLAP Performance

- **Columnar Scanning** - Columnar indexes optimize analytical queries
- **Predicate Pushdown** - Filtering at the lowest level
- **Vectorized Processing** - Batch operations for high throughput
- **Parallel Execution** - Multiple cores utilized efficiently

### Hybrid Benefits

- **No ETL** - Data immediately available for both workloads
- **Real-time Analytics** - Query live transactional data
- **Consistent View** - Transaction isolation across all operations
- **Unified Management** - Single system for all database operations