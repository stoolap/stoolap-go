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
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DataType represents a column data type
type DataType int

const (
	// NULL represents an NULL data type, This is mostly used for unknown values
	NULL DataType = iota
	// INTEGER represents an integer data type
	INTEGER
	// FLOAT represents a floating point data type
	FLOAT
	// TEXT represents a string data type
	TEXT
	// BOOLEAN represents a boolean data type
	BOOLEAN
	// TIMESTAMP represents a timestamp data type
	TIMESTAMP
	// JSON represents a JSON data type
	JSON
)

// String returns a string representation of the DataType
func (dt DataType) String() string {
	switch dt {
	case NULL:
		return "NULL"
	case INTEGER:
		return "INTEGER"
	case FLOAT:
		return "FLOAT"
	case TEXT:
		return "TEXT"
	case BOOLEAN:
		return "BOOLEAN"
	case TIMESTAMP:
		return "TIMESTAMP"
	case JSON:
		return "JSON"
	default:
		return fmt.Sprintf("DataType(%d)", dt)
	}
}

// Operator represents a comparison operator
type Operator int

const (
	// EQ represents equality (=)
	EQ Operator = iota
	// NE represents inequality (!=)
	NE
	// GT represents greater than (>)
	GT
	// GTE represents greater than or equal (>=)
	GTE
	// LT represents less than (<)
	LT
	// LTE represents less than or equal (<=)
	LTE
	// LIKE represents pattern matching
	LIKE
	// IN represents value in a set
	IN
	// NOTIN represents value not in a set
	NOTIN
	// ISNULL represents NULL check
	ISNULL
	// ISNOTNULL represents NOT NULL check
	ISNOTNULL
)

// String returns a string representation of the Operator
func (op Operator) String() string {
	switch op {
	case EQ:
		return "="
	case NE:
		return "!="
	case GT:
		return ">"
	case GTE:
		return ">="
	case LT:
		return "<"
	case LTE:
		return "<="
	case LIKE:
		return "LIKE"
	case IN:
		return "IN"
	case NOTIN:
		return "NOT IN"
	case ISNULL:
		return "IS NULL"
	case ISNOTNULL:
		return "IS NOT NULL"
	default:
		return fmt.Sprintf("Operator(%d)", op)
	}
}

// Constants for easier reference in tests
const (
	TypeNull      = NULL
	TypeInteger   = INTEGER
	TypeFloat     = FLOAT
	TypeString    = TEXT
	TypeBoolean   = BOOLEAN
	TypeTimestamp = TIMESTAMP
	TypeJSON      = JSON
)

// These constants are deprecated - use ones at the top of the file
// Kept for backward compatibility

// Result represents a query result
type Result interface {
	// Columns returns the column names in the result
	// Note: If aliases are set, this should return the aliased column names
	Columns() []string
	// Next moves the cursor to the next row
	Next() bool
	// Scan scans the current row into the specified variables
	Scan(dest ...interface{}) error
	// Row returns the current row directly without copying
	// This is a high-performance method to access raw column values
	// The returned Row is valid until the next call to Next or Close
	Row() Row
	// Close closes the result set
	Close() error
	// Context returns the result's context
	Context() context.Context
	// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or DELETE
	RowsAffected() int64
	// LastInsertID returns the last inserted ID for an INSERT operation
	LastInsertID() int64
	// WithAliases sets column aliases for this result
	// The map keys are alias names, values are original column names
	WithAliases(aliases map[string]string) Result
}

// SchemaColumn represents a column in a table schema
type SchemaColumn struct {
	ID         int      // Unique identifier for the column
	Name       string   // Column name
	Type       DataType // Data type
	Nullable   bool     // Whether the column can be NULL
	PrimaryKey bool     // Whether this column is part of the primary key
}

// Schema represents the structure of a table
type Schema struct {
	TableName string         // Name of the table
	Columns   []SchemaColumn // Columns in the table
	CreatedAt time.Time      // Creation timestamp
	UpdatedAt time.Time      // Last update timestamp
}

// ColumnValue represents a single value in a column
type ColumnValue interface {
	Type() DataType
	IsNull() bool
	AsInt64() (int64, bool)
	AsFloat64() (float64, bool)
	AsBoolean() (bool, bool)
	AsString() (string, bool)
	AsTimestamp() (time.Time, bool)
	AsJSON() (string, bool)
	AsInterface() interface{} // Returns the underlying value as an interface{}

	Equals(other ColumnValue) bool

	// Compare compares two values and returns:
	// -1 if v < other
	// 0 if v == other
	// 1 if v > other
	// error if the comparison is not possible
	Compare(other ColumnValue) (int, error)
}

// Row represents a single row of data
type Row []ColumnValue

// Expression represents a boolean expression that can be evaluated against a row
type Expression interface {
	// Evaluate evaluates the expression against a row
	Evaluate(row Row) (bool, error)

	// EvaluateFast is an optimized version of Evaluate that sacrifices error handling for speed
	// It returns true if the expression matches, false otherwise
	EvaluateFast(row Row) bool

	// WithAliases sets column aliases for this expression
	// This allows the expression to resolve alias references to original column names
	WithAliases(aliases map[string]string) Expression

	// PrepareForSchema optimizes the expression for a given schema
	// This allows the expression to quickly find columns by index instead of by name
	PrepareForSchema(schema Schema) Expression
}

// Table represents a database table
type Table interface {
	Name() string
	Schema() Schema
	CreateColumn(name string, columnType DataType, nullable bool) error
	DropColumn(name string) error
	Insert(row Row) error
	InsertBatch(rows []Row) error
	Update(where Expression, setter func(Row) (Row, bool)) (int, error)
	Delete(where Expression) (int, error)
	Scan(columnIndices []int, where Expression) (Scanner, error)
	Close() error
}

// TableScanner provides an iterator over rows in a table
type Scanner interface {
	Next() bool
	Row() Row
	Err() error
	Close() error
}

// IndexType represents the type of index
type IndexType string

const (
	// BitmapIndex is for low-cardinality columns (< 5% cardinality)
	BitmapIndex IndexType = "bitmap"
	// BTreeIndex is for high-cardinality columns (> 5% cardinality)
	BTreeIndex IndexType = "btree"
	// ColumnIndex is for columnar storage
	ColumnarIndex IndexType = "columnar" // For columnar storage
)

// IndexEntry represents a result from an index lookup
type IndexEntry struct {
	RowID int64 // Row ID in the table
	RefID int64 // Reference ID in the index
}

// Index represents an abstract index for a column
type Index interface {
	// Name returns the name of the index
	Name() string

	// TableName returns the name of the table this index belongs to
	TableName() string

	// Build builds the index
	Build() error

	// Add adds a values to the index with the given row IDs
	Add(values []ColumnValue, rowID int64, refID int64) error

	// AddBatch adds multiple entries to the index in a single batch operation
	// The map key is the rowID and the value is the column values
	AddBatch(entries map[int64][]ColumnValue) error

	// Remove removes a values from the index
	Remove(values []ColumnValue, rowID int64, refID int64) error

	// RemoveBatch removes multiple entries from the index in a single batch operation
	// The map key is the rowID and the value is the column values
	RemoveBatch(entries map[int64][]ColumnValue) error

	// ColumnIDs returns the column IDs for this index
	ColumnIDs() []int // Returns the column ID for this index

	// ColumnNames returns the column names for this index
	ColumnNames() []string // Returns the column names for this index

	// DataTypes returns the data types for this index
	DataTypes() []DataType // Returns the data types for this index

	// Type returns the type of index
	IndexType() IndexType // Returns the type of index (e.g., BTree, Bitmap)

	// IsUnique returns true if this is a unique index
	IsUnique() bool // Returns true if this is a unique index

	// Find finds all pairs where the column equals the given values
	Find(values []ColumnValue) ([]IndexEntry, error)

	// FindRange finds all pairs where the column is in the given range
	FindRange(min, max []ColumnValue, minInclusive, maxInclusive bool) ([]IndexEntry, error)

	// FindWithOperator finds all pairs where the column matches the given operator and values
	FindWithOperator(op Operator, values []ColumnValue) ([]IndexEntry, error)

	// GetRowIDsEqual returns row IDs with the given values
	GetRowIDsEqual(values []ColumnValue) []int64

	// GetRowIDsInRange returns row IDs with values in the given range
	GetRowIDsInRange(minValue, maxValue []ColumnValue, includeMin, includeMax bool) []int64

	// GetFilteredRowIDs returns row IDs that match the given expression
	GetFilteredRowIDs(expr Expression) []int64

	// Close releases resources held by the index
	Close() error
}

// Transaction represents a database transaction
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	ID() int64
	SetIsolationLevel(level IsolationLevel) error
	CreateTable(name string, schema Schema) (Table, error)
	DropTable(name string) error
	GetTable(name string) (Table, error)
	ListTables() ([]string, error)
	CreateTableIndex(tableName string, indexName string, columns []string, isUnique bool) error
	DropTableIndex(tableName string, indexName string) error
	// Columnar index operations
	CreateTableColumnarIndex(tableName string, columnName string, isUnique bool, customName ...string) error
	DropTableColumnarIndex(tableName string, columnName string) error
	// Column operations for ALTER TABLE
	AddTableColumn(tableName string, column SchemaColumn) error
	DropTableColumn(tableName string, columnName string) error
	RenameTableColumn(tableName string, oldName, newName string) error
	ModifyTableColumn(tableName string, column SchemaColumn) error
	// Table operations
	RenameTable(oldName, newName string) error
	// Query operations
	Select(tableName string, columnsToFetch []string, expr Expression, originalColumns ...string) (Result, error)
	// SelectWithAliases executes a SELECT query with column aliases
	// The aliases parameter maps from alias names to original column names
	SelectWithAliases(tableName string, columnsToFetch []string, expr Expression, aliases map[string]string, originalColumns ...string) (Result, error)
}

// Engine represents the storage engine
type Engine interface {
	Open() error
	Close() error
	BeginTransaction() (Transaction, error)
	BeginTx(ctx context.Context, level sql.IsolationLevel) (Transaction, error)
	Path() string
	TableExists(tableName string) (bool, error)
	IndexExists(indexName string, tableName string) (bool, error)
	GetIndex(tableName string, indexName string) (Index, error)
	// GetTableSchema retrieves a table's schema
	GetTableSchema(tableName string) (Schema, error)
	// ListTableIndexes retrieves all indexes for a table
	ListTableIndexes(tableName string) (map[string]string, error)
	// GetAllIndexes retrieves all index objects for a table
	GetAllIndexes(tableName string) ([]Index, error)
	// GetIsolationLevel retrieves the current transaction isolation level
	GetIsolationLevel() IsolationLevel
	// SetIsolationLevel sets the transaction isolation level
	SetIsolationLevel(level IsolationLevel) error
	// GetConfig returns the current storage engine configuration
	GetConfig() Config
	// UpdateConfig updates the storage engine configuration
	UpdateConfig(config Config) error
	// CreateSnapshot manually triggers snapshot creation for all tables
	CreateSnapshot() error
}

// IsolationLevel represents the transaction isolation level
type IsolationLevel int

const (
	// ReadCommitted is the isolation level where transactions see only committed data
	ReadCommitted IsolationLevel = iota
	// SnapshotIsolation (equivalent to Repeatable Read) ensures transactions see a consistent
	// snapshot of the database as it existed at the start of the transaction
	SnapshotIsolation
)

// PersistenceConfig represents configuration options for the persistence layer
type PersistenceConfig struct {
	// Enabled indicates whether persistence is enabled
	// Default: true if Path is not empty
	Enabled bool

	// SyncMode controls the WAL sync strategy: 0=None, 1=Normal, 2=Full
	// None: Fastest but least durable - doesn't force syncs
	// Normal: Syncs on transaction commits - good balance of performance and durability
	// Full: Forces syncs on every WAL write - slowest but most durable
	// Default: 1 (Normal)
	SyncMode int

	// SnapshotInterval is the time between snapshots in seconds
	// Default: 300 (5 minutes)
	SnapshotInterval int

	// KeepSnapshots is the number of snapshots to keep
	// Default: 5
	KeepSnapshots int

	// WALFlushTrigger is the size in bytes that triggers a WAL flush
	// Default: 32768 (32KB)
	WALFlushTrigger int

	// WALBufferSize is the initial WAL buffer size in bytes
	// Default: 65536 (64KB)
	WALBufferSize int

	// WALMaxSize is the maximum size of a WAL file before rotation in bytes
	// Default: 67108864 (64MB)
	WALMaxSize int

	// CommitBatchSize is the number of commits to batch before syncing in SyncNormal mode
	// Default: 100
	CommitBatchSize int

	// SyncIntervalMs is the minimum time between syncs in milliseconds in SyncNormal mode
	// Default: 10
	SyncIntervalMs int
}

// DefaultPersistenceConfig returns a PersistenceConfig with default values
func DefaultPersistenceConfig() PersistenceConfig {
	return PersistenceConfig{
		Enabled:          true,
		SyncMode:         1,                // Normal
		SnapshotInterval: 300,              // 5 minutes
		KeepSnapshots:    5,                // Keep 5 snapshots
		WALFlushTrigger:  32 * 1024,        // 32KB
		WALBufferSize:    64 * 1024,        // 64KB
		WALMaxSize:       64 * 1024 * 1024, // 64MB
		CommitBatchSize:  100,              // Batch 100 commits before syncing in SyncNormal mode
		SyncIntervalMs:   10,               // 10ms minimum interval between syncs in SyncNormal mode
	}
}

// Config represents the configuration for the storage engine
type Config struct {
	// Path to the database directory
	// If empty, database operates in memory-only mode
	Path string

	// Persistence contains configuration options for disk persistence
	// Only used if Path is not empty
	Persistence PersistenceConfig
}
