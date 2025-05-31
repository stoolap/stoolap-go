# Stoolap PostgreSQL Wire Protocol Server

This server provides PostgreSQL wire protocol compatibility for Stoolap, allowing any PostgreSQL client or driver to connect to a Stoolap database.

## Installation

```bash
go install github.com/stoolap/stoolap/cmd/stoolap-pgserver@latest
```

## Usage

### Start with in-memory database:
```bash
stoolap-pgserver
```

### Start with persistent database:
```bash
stoolap-pgserver -d file:///path/to/database.db
```

### Options:
- `-d, --database`: Database path (default: `memory://`)
- `-b, --bind`: Bind address (default: `:5432`)
- `-v, --verbose`: Verbose logging

### Help:
```bash
stoolap-pgserver --help
```

## Connecting

### Using psql:
```bash
psql -h localhost -p 5432 -d stoolap
```

### Transaction example:
```sql
-- Standard PostgreSQL isolation levels
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
INSERT INTO accounts (id, balance) VALUES (1, 1000);
INSERT INTO accounts (id, balance) VALUES (2, 2000);
COMMIT;

-- REPEATABLE READ or SERIALIZABLE (mapped to SNAPSHOT in Stoolap)
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- Simple transaction
BEGIN;
DELETE FROM accounts WHERE id = 3;
ROLLBACK;
```

Note: The PostgreSQL command-line client (psql) validates SQL syntax and rejects SNAPSHOT as it's not a standard PostgreSQL isolation level. When using psql, use REPEATABLE READ or SERIALIZABLE instead, which Stoolap maps to SNAPSHOT isolation. Other clients that don't pre-validate SQL (like some programming language drivers) can use SNAPSHOT directly.

### Using Python (psycopg2):
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="stoolap"
)

cur = conn.cursor()
cur.execute("SELECT * FROM users")
rows = cur.fetchall()
```

### Using Node.js (pg):
```javascript
const { Client } = require('pg')

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'stoolap'
})

await client.connect()
const res = await client.query('SELECT * FROM users')
console.log(res.rows)
```

## Features

- Transaction support (BEGIN/COMMIT/ROLLBACK)
- Transaction isolation levels:
  - READ COMMITTED (default)
  - SNAPSHOT (with write-write conflict detection)
- Read-only transactions
- Proper transaction state management

## Current Limitations

- No SSL/TLS support
- No authentication (accepts all connections)
- Limited prepared statement support
- No COPY protocol support yet
- Some PostgreSQL-specific SQL features may not work
- Transaction isolation levels limited to:
  - READ COMMITTED and SNAPSHOT only
  - REPEATABLE READ, SERIALIZABLE map to SNAPSHOT
  - No savepoints or nested transactions

## Development

To run from source:
```bash
cd cmd/stoolap-pgserver
go run . -v
```