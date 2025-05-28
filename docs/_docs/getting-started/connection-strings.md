---
title: Connection String Reference
category: Getting Started
order: 1
---

# Connection String Reference

This document provides information about Stoolap connection string formats and their usage.

## Connection String Basics

Stoolap connection strings follow a URL-like format:

```
scheme://[path]
```

Where:
- `scheme` specifies the storage engine type
- `path` provides location information for persistent storage (optional for memory storage)

## MVCC Storage Engine

Stoolap uses a single MVCC (Multi-Version Concurrency Control) storage engine that supports two modes:

### In-Memory Mode (memory://)

```
memory://
```

MVCC engine with in-memory storage:
- All data stored in RAM for maximum performance
- Full MVCC transaction isolation and concurrency control
- Data is lost when the process terminates
- No persistence between sessions

Example:
```
memory://
```

### Persistent Mode (file://)

```
file:///path/to/data
```

MVCC engine with disk persistence:
- Same MVCC features as memory mode
- Data persisted to disk for durability
- Snapshot-based persistence for analytical workloads
- WAL (Write-Ahead Logging) for crash recovery
- Transaction isolation with optimistic concurrency control

Examples:
```
file:///data/mydb
file:///Users/username/stoolap/data
file:///C:/stoolap/data
```

### Configuration Options

The persistent mode supports additional configuration through query parameters:

```
file:///path/to/data?sync_mode=full&snapshot_interval=60
```

Available options:
- `sync_mode`: WAL synchronization mode (none, normal, full)
- `snapshot_interval`: Time between snapshots in seconds  
- `keep_snapshots`: Number of snapshots to keep per table

## Usage Examples

### Using the New Simplified API (Recommended)

Stoolap now provides a simplified API that doesn't require the `database/sql` package:

```go
package main

import (
    "context"
    "log"
    
    "github.com/stoolap/stoolap"
)

func main() {
    // Open database using the simplified API
    db, err := stoolap.Open("memory://")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Execute queries directly
    ctx := context.Background()
    _, err = db.Exec(ctx, "CREATE TABLE users (id INT, name TEXT)")
    if err != nil {
        log.Fatal(err)
    }
    
    // Query data with parameters
    rows, err := db.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    // Use transactions
    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }
    
    _, err = tx.ExecContext(ctx, "INSERT INTO users VALUES (?, ?)", 1, "Alice")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }
}
```

### Command Line Example

```bash
# Start CLI with in-memory database
stoolap -db memory://

# Start with file-based persistent storage
stoolap -db file:///data/mydb

# Start with persistent MVCC engine
stoolap -db file:///data/mydb
```

### Using Standard database/sql Package

You can also use Stoolap with Go's standard `database/sql` package:

#### In-Memory Database

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
    // Connect to an in-memory database
    db, err := sql.Open("stoolap", "memory://")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use the database...
}
```

#### File-Based Persistent Database

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
    // Connect to a file-based database
    db, err := sql.Open("stoolap", "file:///path/to/database")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use the database...
}
```

#### Persistent MVCC Database with Configuration

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
    // Connect with persistence and custom configuration
    connStr := "file:///path/to/database?sync_mode=full&snapshot_interval=30"
    db, err := sql.Open("stoolap", connStr)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use the database with full MVCC transaction support...
}
```

## PRAGMA Configuration

You can also configure the database using PRAGMA commands after connection (alternative to query parameters):

```go
// Open database
db, err := sql.Open("stoolap", "file:///data/mydb")
if err != nil {
    log.Fatal(err)
}

// Configure database settings
_, err = db.Exec("PRAGMA snapshot_interval = 60")
if err != nil {
    log.Fatal(err)
}

// Configure WAL synchronization mode  
_, err = db.Exec("PRAGMA sync_mode = 2")
if err != nil {
    log.Fatal(err)
}
```

See the [PRAGMA Commands](pragma-commands) documentation for more details on available configuration options.