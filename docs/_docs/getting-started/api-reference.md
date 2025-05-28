---
title: API Reference
category: Getting Started
order: 4
---

# Stoolap API Reference

This document provides a comprehensive reference for the Stoolap API, including the new simplified API and the standard database/sql driver.

## Simplified API (stoolap package)

The simplified API provides direct access to Stoolap functionality without requiring the `database/sql` package.

### Opening a Database

```go
import "github.com/stoolap/stoolap"

// Open an in-memory database
db, err := stoolap.Open("memory://")

// Open a persistent database
db, err := stoolap.Open("file:///path/to/database")
```

**Important**: Only ONE engine instance can exist per DSN for the entire application lifetime. Subsequent calls to `Open()` with the same DSN return the existing instance.

### Database Methods

#### Basic Operations

```go
// Execute a query that doesn't return rows
result, err := db.Exec(ctx, "CREATE TABLE users (id INT, name TEXT)")

// Query data
rows, err := db.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
defer rows.Close()

// Query single row
row := db.QueryRow(ctx, "SELECT name FROM users WHERE id = ?", 1)
var name string
err := row.Scan(&name)

// Close the database
err := db.Close()
```

#### Transactions

```go
// Begin a transaction with default isolation level (READ COMMITTED)
tx, err := db.Begin()

// Begin a transaction with specific isolation level
import "database/sql"

opts := &sql.TxOptions{
    Isolation: sql.LevelSnapshot,
}
tx, err := db.BeginTx(ctx, opts)

// Execute queries in transaction
_, err = tx.ExecContext(ctx, "INSERT INTO users VALUES (?, ?)", 1, "Alice")
_, err = tx.ExecContext(ctx, "UPDATE users SET name = ? WHERE id = ?", "Bob", 1)

// Commit or rollback
err = tx.Commit()
// or
err = tx.Rollback()
```

#### Prepared Statements

```go
// Prepare a statement
stmt, err := db.Prepare("INSERT INTO users VALUES (?, ?)")
defer stmt.Close()

// Execute prepared statement
_, err = stmt.ExecContext(ctx, 1, "Alice")
_, err = stmt.ExecContext(ctx, 2, "Bob")
```

#### Advanced Features

```go
// Get the underlying storage engine
engine := db.Engine()

// Get the SQL executor (for advanced usage)
executor := db.Executor()

// Enable vectorized execution mode
executor.EnableVectorizedMode()

// Check if vectorized mode is enabled
isEnabled := executor.IsVectorizedModeEnabled()

// Get default isolation level
level := executor.GetDefaultIsolationLevel()
```

### Working with Results

#### Rows Interface

```go
rows, err := db.Query(ctx, "SELECT id, name FROM users")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Get column names
columns := rows.Columns()

// Iterate through results
for rows.Next() {
    var id int
    var name string
    err := rows.Scan(&id, &name)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User: %d - %s\n", id, name)
}
```

#### Row Interface

```go
row := db.QueryRow(ctx, "SELECT COUNT(*) FROM users")
var count int
err := row.Scan(&count)
if err == sql.ErrNoRows {
    fmt.Println("No rows found")
} else if err != nil {
    log.Fatal(err)
}
```

## Standard database/sql Driver

Stoolap also provides a standard Go database/sql driver for compatibility.

### Basic Usage

```go
import (
    "database/sql"
    _ "github.com/stoolap/stoolap/pkg/driver"
)

// Open database
db, err := sql.Open("stoolap", "memory://")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Use standard database/sql methods
_, err = db.Exec("CREATE TABLE users (id INT, name TEXT)")
rows, err := db.Query("SELECT * FROM users")
```

## Connection Strings

Both APIs support the same connection string formats:

- `memory://` - In-memory database
- `file:///path/to/db` - Persistent database
- `file:///path/to/db?sync_mode=full&snapshot_interval=60` - With options

## Transaction Isolation Levels

Stoolap supports two isolation levels:

1. **READ COMMITTED** (default)
   - No read locks
   - High concurrency
   - Suitable for most OLTP workloads

2. **SNAPSHOT**
   - Write-write conflict detection
   - Prevents lost updates
   - Serialized commits for correctness

## Error Handling

Common errors you might encounter:

```go
import "database/sql"

// Check for no rows
err := row.Scan(&value)
if err == sql.ErrNoRows {
    // Handle no rows case
}

// Check for constraint violations
if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
    // Handle unique constraint violation
}

// Check for transaction conflicts (SNAPSHOT isolation)
if err != nil && strings.Contains(err.Error(), "write-write conflict") {
    // Retry transaction
}
```

## Best Practices

1. **Always close resources**: Use `defer` to ensure rows, statements, and transactions are closed
2. **Use contexts**: Pass contexts for cancellation and timeout support
3. **Handle conflicts**: When using SNAPSHOT isolation, be prepared to retry on conflicts
4. **Singleton pattern**: Remember that only one engine instance exists per DSN
5. **Parameter binding**: Always use parameter placeholders (?) to prevent SQL injection

## Example: Complete Application

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/stoolap/stoolap"
)

func main() {
    // Open database
    db, err := stoolap.Open("memory://")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    ctx := context.Background()
    
    // Create schema
    _, err = db.Exec(ctx, `
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert data in a transaction
    tx, err := db.Begin()
    if err != nil {
        log.Fatal(err)
    }
    
    stmt, err := tx.Prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)")
    if err != nil {
        tx.Rollback()
        log.Fatal(err)
    }
    
    users := []struct {
        id    int
        name  string
        email string
    }{
        {1, "Alice", "alice@example.com"},
        {2, "Bob", "bob@example.com"},
        {3, "Charlie", "charlie@example.com"},
    }
    
    for _, u := range users {
        _, err = stmt.ExecContext(ctx, u.id, u.name, u.email)
        if err != nil {
            tx.Rollback()
            log.Fatal(err)
        }
    }
    
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }
    
    // Query data
    rows, err := db.Query(ctx, "SELECT id, name, email FROM users ORDER BY id")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    fmt.Println("Users:")
    for rows.Next() {
        var id int
        var name, email string
        err := rows.Scan(&id, &name, &email)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("  %d: %s <%s>\n", id, name, email)
    }
}
```