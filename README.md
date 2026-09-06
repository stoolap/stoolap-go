# stoolap-go

[![Go Report Card](https://goreportcard.com/badge/github.com/stoolap/stoolap-go)](https://goreportcard.com/report/github.com/stoolap/stoolap-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/stoolap/stoolap-go.svg)](https://pkg.go.dev/github.com/stoolap/stoolap-go)

Go driver for [Stoolap](https://github.com/stoolap/stoolap), a high-performance embedded SQL database built in pure Rust with MVCC transactions, columnar storage, cost-based optimizer, time-travel queries, parallel execution, and native vector search.

Two driver implementations:

| | Native Driver | WASM Driver |
|---|---|---|
| **Package** | `github.com/stoolap/stoolap-go` | `github.com/stoolap/stoolap-go/wasm` |
| **CGO required** | No (`CGO_ENABLED=0` works) | No (pure Go) |
| **Dependencies** | Shared library (.dylib/.so/.dll) | Single .wasm file (5 MB) |
| **Threading** | Full (parallel queries) | Single-threaded |
| **File persistence** | Yes | Yes (via WASI) |
| **Cross-compile** | Anywhere Go compiles | Anywhere Go compiles |
| **Best for** | Production, max throughput | Portability, zero native dependencies |

Both drivers provide:
- **Direct API** for maximum performance and control
- **`database/sql`** driver for standard Go database access

## Native Driver

The native driver loads `libstoolap` at runtime via direct ABI calls. No C compiler or CGO needed.

### Requirements

- Go 1.24+

Go 1.27 and later reject the `runtime.asmcgocall` linkname the native driver uses. Until a linkname-free path ships, build with the linker check disabled:

```bash
go build -ldflags=-checklinkname=0 ./...
# or set it once for every command
export GOFLAGS=-ldflags=-checklinkname=0
```

### Installation

```bash
go get github.com/stoolap/stoolap-go
```

Prebuilt shared libraries for macOS (arm64), Linux (x64), and Windows (x64) are bundled in the module. No extra downloads or environment variables needed.

### Quick Start

#### Direct API

```go
package main

import (
    "context"
    "fmt"

    "github.com/stoolap/stoolap-go"
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
        var id, age int64
        var name string
        rows.Scan(&id, &name, &age)
        fmt.Printf("id=%d name=%s age=%d\n", id, name, age)
    }
}
```

#### database/sql

```go
package main

import (
    "context"
    "database/sql"
    "fmt"

    _ "github.com/stoolap/stoolap-go"
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
        var id, age int64
        var name string
        rows.Scan(&id, &name, &age)
        fmt.Printf("id=%d name=%s age=%d\n", id, name, age)
    }
}
```

#### Other Platforms

For platforms without a bundled library (e.g. Linux arm64, macOS x64), download from the
[releases page](https://github.com/stoolap/stoolap-go/releases) or build from source,
then set the `STOOLAP_LIB` environment variable:

```bash
export STOOLAP_LIB=/path/to/libstoolap.dylib
```

## WASM Driver (Pure Go, Zero Native Dependencies)

The WASM driver runs the Stoolap engine as a WebAssembly module inside your Go process
using [wazero](https://wazero.io/). No shared libraries, no platform-specific binaries.
One 5 MB `.wasm` file works on every OS and architecture.

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

**Note:** WASM does not support threads, so automatic checkpoint and background compaction
do not run. Use manual maintenance commands periodically:

```go
db.Exec(ctx, "PRAGMA checkpoint")
db.Exec(ctx, "VACUUM")
db.Exec(ctx, "PRAGMA snapshot")
db.Exec(ctx, "ANALYZE my_table")
```

For production file-based workloads with automatic background maintenance, use the native driver.

### Building the WASM Binary from Source

Requires: Rust toolchain, `wasm32-wasip1` target, and [binaryen](https://github.com/WebAssembly/binaryen) (for `wasm-opt`).

```bash
rustup target add wasm32-wasip1
cd stoolap
cargo build --profile max --target wasm32-wasip1 --features ffi --no-default-features
wasm-opt -Oz target/wasm32-wasip1/max/stoolap.wasm -o stoolap.wasm
```

A prebuilt `stoolap.wasm` is included in the module and available on the
[releases page](https://github.com/stoolap/stoolap-go/releases).

## Connection Strings (DSN)

| DSN | Description |
|-----|-------------|
| `memory://` | In-memory database (unique, isolated instance) |
| `memory://mydb` | Named in-memory database (same name shares the engine) |
| `file:///path/to/db` | File-based persistent database |
| `file:///path/to/db?sync_mode=full` | File-based with configuration options |

### File DSN Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `sync_mode` | `normal` | `none`, `normal` (fsync every 1s), `full` (fsync every write) |
| `checkpoint_interval` | `60` | Seconds between checkpoint cycles |
| `compact_threshold` | `4` | Sub-target volumes per table before compaction |
| `target_volume_rows` | `1048576` | Target rows per cold volume |
| `checkpoint_on_close` | `on` | Seal hot rows on shutdown |
| `compression` | `on` | LZ4 compression for WAL and cold volumes |

## Direct API Reference

### DB

```go
func Open(dsn string) (*DB, error)
func OpenMemory() (*DB, error)
func (db *DB) Close() error
func (db *DB) Clone() (*DB, error)
func (db *DB) Exec(ctx context.Context, query string) (sql.Result, error)
func (db *DB) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error)
func (db *DB) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error)
func (db *DB) Query(ctx context.Context, query string) (*Rows, error)
func (db *DB) QueryParams(ctx context.Context, query string, args []any) (*Rows, error)
func (db *DB) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error)
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error)
func (db *DB) Begin(ctx context.Context) (*Tx, error)
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
```

### Rows

```go
func (r *Rows) Next() bool
func (r *Rows) Scan(dest ...any) error
func (r *Rows) Columns() []string
func (r *Rows) Close() error
func (r *Rows) FetchAll() ([][]any, error)
```

### Stmt

```go
func (s *Stmt) ExecContext(ctx context.Context, args []any) (sql.Result, error)
func (s *Stmt) ExecBatch(ctx context.Context, rows [][]any) (sql.Result, error)
func (s *Stmt) QueryContext(ctx context.Context, args []any) (*Rows, error)
func (s *Stmt) Close() error
```

`ExecBatch` runs the statement once per row inside a single transaction with one native call. Every row must have the same number of parameters. If any row fails the whole batch is rolled back.

```go
stmt, _ := db.Prepare(ctx, "INSERT INTO users VALUES ($1, $2, $3)")
res, err := stmt.ExecBatch(ctx, [][]any{
    {int64(1), "Alice", int64(30)},
    {int64(2), "Bob", int64(25)},
})
```

### Tx

```go
func (tx *Tx) Exec(ctx context.Context, query string) (sql.Result, error)
func (tx *Tx) ExecParams(ctx context.Context, query string, args []any) (sql.Result, error)
func (tx *Tx) ExecNamed(ctx context.Context, query string, args []sql.NamedArg) (sql.Result, error)
func (tx *Tx) Query(ctx context.Context, query string) (*Rows, error)
func (tx *Tx) QueryParams(ctx context.Context, query string, args []any) (*Rows, error)
func (tx *Tx) QueryNamed(ctx context.Context, query string, args []sql.NamedArg) (*Rows, error)
func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error
```

## Parameters

Use positional parameters `$1`, `$2`, etc. with the direct API:

```go
db.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3)",
    []any{int64(1), "Alice", int64(30)})

rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE id = $1",
    []any{int64(1)})
```

With `database/sql`, use `?` placeholders as usual:

```go
db.ExecContext(ctx, "INSERT INTO users VALUES (?, ?, ?)", 1, "Alice", 30)
```

Named parameters use `:name` placeholders. A name may appear more than once in the query.

```go
db.ExecNamed(ctx, "INSERT INTO users VALUES (:id, :name, :age)", []sql.NamedArg{
    sql.Named("id", int64(1)), sql.Named("name", "Alice"), sql.Named("age", int64(30)),
})
rows, _ := db.QueryNamed(ctx, "SELECT * FROM users WHERE age > :min", []sql.NamedArg{sql.Named("min", int64(18))})
```

With `database/sql`, pass `sql.Named` values. Named and positional arguments cannot be mixed in one call, and prepared statements accept positional arguments only.

```go
db.ExecContext(ctx, "INSERT INTO users VALUES (:id, :name, :age)",
    sql.Named("id", 1), sql.Named("name", "Alice"), sql.Named("age", 30))
```

## Transactions

```go
tx, _ := db.Begin(ctx)

tx.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2)", []any{int64(1), "Alice"})
tx.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2)", []any{int64(2), "Bob"})

tx.Commit()
```

### Snapshot Isolation

| Level | Description |
|-------|-------------|
| Read Committed (default) | Each statement sees data committed before it started |
| Snapshot | Transaction sees a consistent snapshot from when it began |

```go
tx, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
defer tx.Rollback()

rows, _ := tx.Query(ctx, "SELECT * FROM users")
// All reads see the same snapshot
tx.Commit()
```

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

## Error Handling

All errors returned by the stoolap engine are typed as `*stoolap.Error`, which carries a categorized error code:

```go
var stErr *stoolap.Error
if errors.As(err, &stErr) {
    switch stErr.Code() {
    case stoolap.ErrUniqueConstraint:
        // handle duplicate
    case stoolap.ErrPrimaryKeyConstraint:
        // handle primary key violation
    case stoolap.ErrTableNotFound:
        // handle missing table
    }
}
```

Use `IsConstraintViolation()` to check for any constraint error (unique, primary key, not null, check, foreign key):

```go
if errors.As(err, &stErr) && stErr.IsConstraintViolation() {
    // handle any constraint violation
}
```

| ErrorCode | Description |
|-----------|-------------|
| `ErrGeneral` | Uncategorized error |
| `ErrUniqueConstraint` | Unique index violation |
| `ErrPrimaryKeyConstraint` | Primary key violation |
| `ErrNotNullConstraint` | NOT NULL violation |
| `ErrCheckConstraint` | CHECK constraint violation |
| `ErrForeignKeyViolation` | Foreign key violation |
| `ErrTableNotFound` | Table does not exist |
| `ErrTableExists` | Table already exists |

## Thread Safety

- **Direct API**: A single `DB` handle must not be shared across goroutines. Use `Clone()` for per-goroutine handles.
- **`database/sql`**: Thread-safe by default. The connection pool creates cloned handles automatically.
- **Tx, Stmt, Rows**: Must remain on the goroutine that created them.

## Project Structure

```
stoolap-go/
  stoolap.go              Direct API (DB, Rows, Stmt, Tx)
  driver.go               database/sql driver ("stoolap")
  lib.go                  Library loading, symbols, string helpers
  abi_*.go/s              Platform ABI calls (arm64, amd64, windows)
  dlopen_*.go/s           Platform-specific library loading
  stoolap_test.go         Tests
  bench_test.go           Benchmarks
  lib/
    darwin_arm64/          Prebuilt shared library (Go module)
    linux_amd64/           Prebuilt shared library (Go module)
    windows_amd64/         Prebuilt shared library (Go module)
  wasm/                   WASM driver (separate Go module)
    engine.go             WASM runtime, FFI wrappers
    stoolap.go            Direct API
    driver.go             database/sql driver ("stoolap-wasm")
    stoolap.wasm          Prebuilt WASM binary (5 MB)
    go.mod
  go.mod
  LICENSE
  README.md
```

## Testing

```bash
# Native driver
go test -v -race ./...

# WASM driver
cd wasm && go test -v ./...
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
