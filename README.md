# stoolap-go

Go driver for [Stoolap](https://github.com/stoolap/stoolap), a high-performance embedded SQL database built in pure Rust with MVCC transactions, cost-based optimizer, time-travel queries, parallel execution, and native vector search.

Two driver implementations:

| | CGO Driver | WASM Driver |
|---|---|---|
| **Package** | `github.com/stoolap/stoolap-go` | `github.com/stoolap/stoolap-go/wasm` |
| **CGO required** | Yes | No (pure Go) |
| **Dependencies** | Shared library (.dylib/.so/.dll) | Single .wasm file (5 MB) |
| **Threading** | Full (parallel queries) | Single-threaded |
| **File persistence** | Yes | Yes (via WASI) |
| **Cross-compile** | Needs C toolchain per target | Anywhere Go compiles |
| **Best for** | Production, max throughput | Portability, zero dependencies |

Both drivers provide:
- **Direct API** for maximum performance and control
- **`database/sql`** driver for standard Go database access

## CGO Driver

### Requirements

- Go 1.24+
- CGO enabled (`CGO_ENABLED=1`, the default)

### Installation

```bash
go get github.com/stoolap/stoolap-go
```

Prebuilt shared libraries for macOS (arm64), Linux (x64), and Windows (x64) are bundled
in the module. No extra downloads or environment variables needed — just `go get` and build.

The compiled Go binary dynamically links against `libstoolap`. For deployment, place the
shared library next to your executable or in a system library path.

#### Other Platforms

For platforms without a bundled library (e.g. Linux arm64, macOS x64), download from the
[releases page](https://github.com/stoolap/stoolap-go/releases) or build from source,
then build with the `stoolap_use_lib` tag:

```bash
export LIBRARY_PATH=/path/to/stoolap/target/release
go build -tags stoolap_use_lib ./...
```

## WASM Driver (Pure Go, Zero CGO)

The WASM driver runs the Stoolap engine as a WebAssembly module inside your Go process
using [wazero](https://wazero.io/). No CGO, no shared libraries, no platform-specific
binaries. One 5 MB `.wasm` file works on every OS and architecture.

### Installation

```bash
go get github.com/stoolap/stoolap-go/wasm
```

### Quick Start (WASM)

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/stoolap/stoolap-go/wasm"
)

func main() {
    ctx := context.Background()

    // Load the WASM binary (5 MB, built from the Stoolap Rust engine)
    wasmBytes, _ := os.ReadFile("stoolap.wasm")
    engine, _ := wasm.NewEngine(ctx, wasmBytes)

    db, _ := engine.OpenMemory(ctx)
    defer db.Close()

    db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    rows, _ := db.Query(ctx, "SELECT id, name FROM users ORDER BY id")
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        rows.Scan(&id, &name)
        fmt.Printf("id=%d name=%s\n", id, name)
    }
}
```

### database/sql (WASM)

```go
import (
    "context"
    "database/sql"
    "os"

    "github.com/stoolap/stoolap-go/wasm"
)

func main() {
    ctx := context.Background()
    wasmBytes, _ := os.ReadFile("stoolap.wasm")
    wasm.SetWASM(ctx, wasmBytes)

    db, _ := sql.Open("stoolap-wasm", "memory://")
    defer db.Close()
    // Use standard database/sql API...
}
```

### File Persistence (WASM)

```go
engine, _ := wasm.NewEngineWithFS(ctx, wasmBytes, "/path/to/data")
db, _ := engine.Open(ctx, "file:///data/mydb")
```

**Note:** WASM does not support threads, so background maintenance tasks (MVCC cleanup,
snapshots, volume freezing) do not run automatically. Use manual maintenance commands
periodically:

```go
db.Exec(ctx, "VACUUM")              // Clean deleted rows, old versions, compact indexes
db.Exec(ctx, "PRAGMA snapshot")     // Force a snapshot to disk
db.Exec(ctx, "ANALYZE my_table")    // Update optimizer statistics
```

For production file-based workloads with automatic background maintenance, use the CGO driver.

### Building the WASM Binary from Source

Requires: Rust toolchain, `wasm32-wasip1` target, and [binaryen](https://github.com/WebAssembly/binaryen) (for `wasm-opt`).

```bash
# Install WASI target
rustup target add wasm32-wasip1

# Build (from the stoolap engine repo)
cd stoolap
cargo build --profile max --target wasm32-wasip1 --features ffi --no-default-features

# Optimize (31 MB -> 5 MB)
wasm-opt -Oz target/wasm32-wasip1/max/stoolap.wasm -o stoolap.wasm
```

A prebuilt `stoolap.wasm` is included in the module and available on the
[releases page](https://github.com/stoolap/stoolap-go/releases).

## Quick Start (CGO)

### Direct API

```go
package main

import (
    "context"
    "fmt"

    stoolap "github.com/stoolap/stoolap-go"
)

func main() {
    db, err := stoolap.Open("memory://")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    ctx := context.Background()

    db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    db.Exec(ctx, "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    rows, _ := db.Query(ctx, "SELECT id, name, age FROM users ORDER BY id")
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        var age int64
        rows.Scan(&id, &name, &age)
        fmt.Printf("id=%d name=%s age=%d\n", id, name, age)
    }
}
```

### database/sql Driver

```go
package main

import (
    "context"
    "database/sql"
    "fmt"

    _ "github.com/stoolap/stoolap-go/pkg/driver"
)

func main() {
    db, err := sql.Open("stoolap", "memory://")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    ctx := context.Background()

    db.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    db.ExecContext(ctx, "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")

    rows, _ := db.QueryContext(ctx, "SELECT id, name, age FROM users ORDER BY id")
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        var age int64
        rows.Scan(&id, &name, &age)
        fmt.Printf("id=%d name=%s age=%d\n", id, name, age)
    }
}
```

## Connection Strings (DSN)

| DSN | Description |
|-----|-------------|
| `memory://` | In-memory database (unique, isolated instance) |
| `memory://mydb` | Named in-memory database (same name shares the engine) |
| `file:///path/to/db` | File-based persistent database |
| `file:///path/to/db?sync_mode=full` | File-based with configuration options |

## Direct API Reference

### Package Functions

```go
// Version returns the stoolap library version string.
func Version() string

// Open opens a database connection with the given DSN.
func Open(dsn string) (*DB, error)
```

### DB

```go
// Close closes the database connection and releases resources.
func (db *DB) Close() error

// Clone creates a cloned handle for multi-threaded use.
// The clone shares the underlying engine but has its own executor and error state.
func (db *DB) Clone() (*DB, error)

// Exec executes a query without returning any rows.
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error)

// ExecContext executes a query with parameters.
func (db *DB) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error)

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string) (Rows, error)

// QueryContext executes a query that returns rows with parameters.
func (db *DB) QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error)

// QueryRow executes a query that is expected to return at most one row.
func (db *DB) QueryRow(ctx context.Context, query string, args ...driver.NamedValue) Row

// Begin starts a new transaction with default isolation level (Read Committed).
func (db *DB) Begin() (Tx, error)

// BeginTx starts a new transaction with options.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)

// Prepare creates a prepared statement.
func (db *DB) Prepare(query string) (Stmt, error)

// PrepareContext creates a prepared statement with context.
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error)
```

### Rows

```go
type Rows interface {
    // Next advances to the next row. Returns false when done.
    Next() bool

    // Scan reads the current row's columns into dest.
    Scan(dest ...any) error

    // Close closes the result set and frees resources.
    Close() error

    // Columns returns the column names.
    Columns() []string

    // FetchAll fetches all remaining rows into a packed binary buffer.
    // Useful for bulk data transfer, avoiding per-row overhead.
    // The caller should still call Close() after FetchAll.
    FetchAll() ([]byte, error)
}
```

### Row

```go
type Row interface {
    // Scan reads the row's columns into dest.
    // Returns sql.ErrNoRows if the query returned no rows.
    Scan(dest ...any) error
}
```

### Tx

```go
type Tx interface {
    // Commit commits the transaction.
    Commit() error

    // Rollback rolls back the transaction.
    Rollback() error

    // ExecContext executes a query within the transaction.
    ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error)

    // QueryContext executes a query within the transaction that returns rows.
    QueryContext(ctx context.Context, query string, args ...driver.NamedValue) (Rows, error)

    // Prepare creates a prepared statement bound to the transaction.
    // The statement executes within the transaction (uses stoolap_tx_stmt_exec/query),
    // ensuring operations participate in the transaction's commit/rollback.
    Prepare(query string) (Stmt, error)

    // ID returns the transaction ID.
    ID() int64
}
```

### Stmt

```go
type Stmt interface {
    // ExecContext executes the prepared statement with parameters.
    ExecContext(ctx context.Context, args ...driver.NamedValue) (sql.Result, error)

    // QueryContext executes the prepared statement as a query with parameters.
    QueryContext(ctx context.Context, args ...driver.NamedValue) (Rows, error)

    // SQL returns the SQL text of the prepared statement.
    SQL() string

    // Close destroys the prepared statement and frees resources.
    Close() error
}
```

## Usage Examples

### Parameters

Use positional parameters `$1`, `$2`, etc.:

```go
ctx := context.Background()

db.ExecContext(ctx, "INSERT INTO users VALUES ($1, $2, $3)",
    driver.NamedValue{Ordinal: 1, Value: int64(1)},
    driver.NamedValue{Ordinal: 2, Value: "Alice"},
    driver.NamedValue{Ordinal: 3, Value: int64(30)},
)

row := db.QueryRow(ctx, "SELECT name FROM users WHERE id = $1",
    driver.NamedValue{Ordinal: 1, Value: int64(1)},
)
var name string
row.Scan(&name)
```

### Transactions

```go
tx, err := db.Begin()
if err != nil {
    panic(err)
}

tx.ExecContext(ctx, "INSERT INTO users VALUES ($1, $2)",
    driver.NamedValue{Ordinal: 1, Value: int64(1)},
    driver.NamedValue{Ordinal: 2, Value: "Alice"},
)
tx.ExecContext(ctx, "INSERT INTO users VALUES ($1, $2)",
    driver.NamedValue{Ordinal: 1, Value: int64(2)},
    driver.NamedValue{Ordinal: 2, Value: "Bob"},
)

if err := tx.Commit(); err != nil {
    panic(err)
}
```

### Snapshot Isolation

Stoolap supports two isolation levels:

| Level | Description |
|-------|-------------|
| Read Committed (default) | Each statement sees data committed before it started |
| Snapshot | The transaction sees a consistent snapshot from when it began |

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSnapshot,
})
if err != nil {
    panic(err)
}
defer tx.Rollback()

// All reads within this transaction see the same snapshot
rows, _ := tx.QueryContext(ctx, "SELECT * FROM users")
// ...
tx.Commit()
```

### Prepared Statements

Parse once, execute many times:

```go
stmt, err := db.Prepare("INSERT INTO users VALUES ($1, $2)")
if err != nil {
    panic(err)
}
defer stmt.Close()

for i := int64(1); i <= 1000; i++ {
    stmt.ExecContext(ctx,
        driver.NamedValue{Ordinal: 1, Value: i},
        driver.NamedValue{Ordinal: 2, Value: "User"},
    )
}
```

### Prepared Statements in Transactions

For transactional atomicity with parse-once performance, prepare statements via `Tx.Prepare()`. This uses `stoolap_tx_stmt_exec`/`stoolap_tx_stmt_query` internally, ensuring all operations participate in the transaction's commit/rollback.

```go
stmt, err := db.Prepare("INSERT INTO orders VALUES ($1, $2, $3)")
if err != nil {
    panic(err)
}
defer stmt.Close()

tx, err := db.Begin()
if err != nil {
    panic(err)
}

txStmt, err := tx.Prepare("INSERT INTO orders VALUES ($1, $2, $3)")
if err != nil {
    tx.Rollback()
    panic(err)
}
defer txStmt.Close()

for i := int64(0); i < 1000; i++ {
    txStmt.ExecContext(ctx,
        driver.NamedValue{Ordinal: 1, Value: i},
        driver.NamedValue{Ordinal: 2, Value: int64(1)},
        driver.NamedValue{Ordinal: 3, Value: 99.99},
    )
}

tx.Commit() // all 1000 rows committed atomically
```

### NULL Handling

Use `sql.Null*` types for nullable columns:

```go
var (
    name  sql.NullString
    age   sql.NullInt64
    score sql.NullFloat64
    active sql.NullBool
    ts    sql.NullTime
)
row := db.QueryRow(ctx, "SELECT name, age, score, active, created_at FROM users WHERE id = $1",
    driver.NamedValue{Ordinal: 1, Value: int64(1)},
)
row.Scan(&name, &age, &score, &active, &ts)

if name.Valid {
    fmt.Println("Name:", name.String)
} else {
    fmt.Println("Name is NULL")
}
```

### Scanning into `any`

```go
rows, _ := db.Query(ctx, "SELECT id, name, age FROM users")
defer rows.Close()

for rows.Next() {
    var id, name, age any
    rows.Scan(&id, &name, &age)
    fmt.Printf("id=%v name=%v age=%v\n", id, name, age)
}
```

### JSON

```go
db.Exec(ctx, "CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSON)")
db.Exec(ctx, `INSERT INTO docs VALUES (1, '{"name":"Alice","age":30}')`)

var data string
db.QueryRow(ctx, "SELECT data FROM docs WHERE id = 1").Scan(&data)
// data = `{"name":"Alice","age":30}`
```

### VECTOR (BLOB)

Vectors are stored as packed little-endian f32 bytes:

```go
import (
    "encoding/binary"
    "math"
)

db.Exec(ctx, "CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(3))")

// Encode a float32 vector to bytes
vec := []float32{1.0, 2.0, 3.0}
buf := make([]byte, len(vec)*4)
for i, f := range vec {
    binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
}

db.ExecContext(ctx, "INSERT INTO vectors VALUES ($1, $2)",
    driver.NamedValue{Ordinal: 1, Value: int64(1)},
    driver.NamedValue{Ordinal: 2, Value: buf},
)

// Read back
var blob []byte
db.QueryRow(ctx, "SELECT embedding FROM vectors WHERE id = 1").Scan(&blob)

// Decode packed f32 bytes back to float32 slice
result := make([]float32, len(blob)/4)
for i := range result {
    result[i] = math.Float32frombits(binary.LittleEndian.Uint32(blob[i*4:]))
}
```

### Bulk Fetch

`FetchAll()` fetches all remaining rows into a single packed binary buffer, avoiding per-row FFI overhead. Useful for bulk data transfer in language bindings or high-throughput pipelines.

```go
rows, _ := db.Query(ctx, "SELECT id, name, age FROM users")
defer rows.Close()

buf, err := rows.FetchAll()
if err != nil {
    panic(err)
}
// buf contains all rows in packed binary format
// See the Stoolap C API docs for the binary format specification
```

### Cloning for Concurrency

A single `DB` handle must not be used from multiple goroutines simultaneously. Use `Clone()` to create per-goroutine handles that share the underlying engine:

```go
db, _ := stoolap.Open("memory://mydb")
defer db.Close()

db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

var wg sync.WaitGroup
for i := 0; i < 4; i++ {
    wg.Add(1)
    go func(workerID int) {
        defer wg.Done()

        clone, _ := db.Clone()
        defer clone.Close()

        // Each goroutine uses its own cloned handle
        clone.Exec(ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, 'worker-%d')", workerID, workerID))
    }(i)
}
wg.Wait()
```

The `database/sql` driver handles this automatically: each connection in the pool gets its own cloned handle via `stoolap_clone()`.

## Type Mapping

| SQL Type | Go Type | Nullable Go Type |
|----------|---------|------------------|
| INTEGER | `int64`, `int`, `int32` | `sql.NullInt64` |
| FLOAT | `float64`, `float32` | `sql.NullFloat64` |
| TEXT | `string` | `sql.NullString` |
| BOOLEAN | `bool` | `sql.NullBool` |
| TIMESTAMP | `time.Time` | `sql.NullTime` |
| JSON | `string` | `sql.NullString` |
| VECTOR/BLOB | `[]byte` | `[]byte` (nil for NULL) |

Scan supports type coercion: INTEGER columns can scan into `*string`, FLOAT into `*int64`, etc.

## database/sql Driver

The driver is registered as `"stoolap"` and implements the following `database/sql/driver` interfaces:

| Interface | Description |
|-----------|-------------|
| `driver.Driver` | Basic driver |
| `driver.DriverContext` | Connector-based driver |
| `driver.Connector` | Connection factory with pooling |
| `driver.Conn` | Connection |
| `driver.ConnBeginTx` | Transaction with isolation levels |
| `driver.ExecerContext` | Direct exec (bypasses prepare) |
| `driver.QueryerContext` | Direct query (bypasses prepare) |
| `driver.ConnPrepareContext` | Prepared statements |
| `driver.Pinger` | Connection health check |
| `driver.SessionResetter` | Session reset on pool return |
| `driver.Validator` | Connection validation |
| `driver.Tx` | Transaction commit/rollback |
| `driver.Stmt` | Prepared statement |
| `driver.StmtExecContext` | Prepared exec with context |
| `driver.StmtQueryContext` | Prepared query with context |
| `driver.Rows` | Result set iteration |

## Thread Safety

- **Direct API**: A single `DB` handle must not be shared across goroutines. Use `Clone()` for per-goroutine handles.
- **`database/sql`**: Thread-safe by default. The connection pool creates cloned handles automatically.
- **Tx, Stmt, Rows**: Must remain on the goroutine that created them.

## Project Structure

```
stoolap-go/
  stoolap.go              Direct API (DB, Rows, Row, Stmt, Tx)
  stoolap_test.go         Direct API tests
  lib/
    darwin_arm64/          Prebuilt shared library (separate Go module)
    linux_amd64/           Prebuilt shared library (separate Go module)
    windows_amd64/         Prebuilt shared library (separate Go module)
  internal/
    cs/
      cs.go               cgo bindings to libstoolap C API
      cs_test.go          Low-level C bindings tests
      stoolap.h           C header (copied from stoolap)
  pkg/
    driver/
      driver.go           database/sql driver ("stoolap")
      conn.go             driver.Conn implementation
      stmt.go             driver.Stmt implementation
      rows.go             driver.Rows implementation
      transaction.go      driver.Tx implementation
      helpers.go          Value conversion helpers
      driver_test.go      database/sql integration tests
  wasm/                   Pure Go WASM driver (separate Go module)
    engine.go             WASM runtime, arena allocator, FFI wrappers
    stoolap.go            Direct API (DB, Rows, Stmt, Tx)
    driver.go             database/sql driver ("stoolap-wasm")
    stoolap.wasm          Prebuilt WASM binary (5 MB)
    stoolap_test.go       Tests (80%+ coverage)
    go.mod
  example/
    benchmark/            Stoolap CGO vs SQLite CGO benchmark
    wasm_benchmark/       Stoolap WASM vs SQLite WASM benchmark
  go.mod
  LICENSE
  README.md
```

## Testing

```bash
# CGO driver
go test -v ./...

# WASM driver
cd wasm && go test -v ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
