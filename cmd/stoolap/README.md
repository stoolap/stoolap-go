# Stoolap CLI

The official command-line interface for Stoolap - a high-performance embedded SQL database with HTAP capabilities.

## Installation

```bash
go install github.com/stoolap/stoolap/cmd/stoolap@latest
```

## Usage

### Interactive Mode

Start an interactive SQL session:

```bash
# In-memory database
stoolap

# Persistent database
stoolap /path/to/database.db
```

### Non-Interactive Mode

Execute SQL from files or stdin:

```bash
# Execute SQL from file
stoolap /path/to/database.db < script.sql

# Execute single query
echo "SELECT * FROM users" | stoolap database.db

# Execute SQL script
cat schema.sql data.sql | stoolap database.db
```

### Command Options

- First argument: Database path (optional, defaults to in-memory)
  - Use absolute or relative file paths for persistent storage
  - Omit for in-memory database

## Interactive Commands

When in interactive mode, the following commands are available:

- `.help` - Show help message
- `.tables` - List all tables
- `.schema [table]` - Show CREATE statements
- `.quit` or `.exit` - Exit the CLI
- `Ctrl+D` - Exit (Unix/Linux/macOS)
- `Ctrl+Z` then Enter - Exit (Windows)

## SQL Features

### Creating Tables

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
```

### Transactions

```sql
-- Start transaction with isolation level
BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;

INSERT INTO accounts (id, balance) VALUES (1, 1000);
INSERT INTO accounts (id, balance) VALUES (2, 2000);

-- Transfer money
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

COMMIT;

-- Or rollback
BEGIN;
DELETE FROM important_data;
ROLLBACK;
```

### Supported Transaction Isolation Levels

- `READ COMMITTED` (default) - High concurrency, no global locks
- `SNAPSHOT` - Write-write conflict detection, prevents lost updates

### Data Types

- **Integer types**: `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT` - All stored as 64-bit integers
- **Floating point**: `FLOAT`, `REAL`, `DOUBLE` - 64-bit floating point numbers
- **Text types**: `TEXT`, `VARCHAR`, `CHAR`, `STRING` - UTF-8 strings
- **Boolean**: `BOOLEAN`, `BOOL` - True/false values
- **Date/Time**: `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIME`, `DATE`, `TIME` - Date and time values
- **JSON**: `JSON` - JSON data type with native support

### SQL Functions

#### Scalar Functions
- String: `LOWER()`, `UPPER()`, `LENGTH()`, `SUBSTRING()`, `CONCAT()`
- Numeric: `ABS()`, `ROUND()`, `FLOOR()`, `CEILING()`
- Date/Time: `NOW()`, `DATE_TRUNC()`, `TIME_TRUNC()`
- Other: `COALESCE()`, `CAST()`, `VERSION()`

#### Aggregate Functions
- `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
- `FIRST()`, `LAST()`

#### Window Functions
- `ROW_NUMBER() OVER (...)`

### Joins

```sql
SELECT u.name, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE o.status = 'completed';

-- Also supports LEFT JOIN, CROSS JOIN
```

### Parameter Binding

```sql
-- Uses ? as placeholder
SELECT * FROM users WHERE id = ? AND status = ?;
```

## Examples

### Basic Usage

```bash
# Create a new database and add data
$ stoolap mydb.db
Connected to database: mydb.db
stoolap> CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL);
stoolap> INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99);
stoolap> SELECT * FROM products WHERE price < 15;
id | name   | price
---|--------|------
1  | Widget | 9.99

stoolap> .quit
```

### Batch Processing

```bash
# schema.sql
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    level TEXT,
    message TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

# Load schema
$ stoolap logs.db < schema.sql

# Import data
$ cat log_data.sql | stoolap logs.db
```

### Transaction Example

```bash
$ stoolap accounts.db
stoolap> BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT;
stoolap> UPDATE accounts SET balance = balance * 1.05; -- 5% interest
stoolap> SELECT SUM(balance) FROM accounts; -- Verify total
stoolap> COMMIT;
```

## Performance Tips

1. **Use Transactions** - Group multiple operations in transactions for better performance
2. **Create Indexes** - Add indexes on columns used in WHERE clauses
3. **Use Prepared Statements** - When executing similar queries repeatedly
4. **Choose Right Isolation** - Use READ COMMITTED for high concurrency OLTP

## Limitations

- No stored procedures or triggers
- Limited subquery support in DML operations
- No views (yet)
- No foreign key constraints (yet)

## Troubleshooting

### Database Locked
If you see "database is locked" errors, ensure no other process has the database open.

### Performance Issues
- Check if you have appropriate indexes
- Use EXPLAIN to understand query execution (when available)
- Consider using READ COMMITTED isolation for better concurrency

### Memory Usage
For large datasets, use a persistent database instead of in-memory mode.

## Development

To run from source:
```bash
cd cmd/stoolap
go run . /path/to/database.db
```

To run tests:
```bash
go test ./...
```

## See Also

- [Stoolap Documentation](https://stoolap.io/docs)
- [SQL Commands Reference](https://stoolap.io/docs/sql-commands)
- [stoolap-pgserver](../stoolap-pgserver/README.md) - PostgreSQL wire protocol server