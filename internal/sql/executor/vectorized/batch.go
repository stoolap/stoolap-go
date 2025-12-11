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
package vectorized

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Batch represents a columnar batch of data for vectorized processing
type Batch struct {
	// Size is the number of rows in the batch
	Size int

	// Column arrays organized by data type for efficient processing
	IntColumns    map[string][]int64
	FloatColumns  map[string][]float64
	StringColumns map[string][]string
	BoolColumns   map[string][]bool
	TimeColumns   map[string][]time.Time

	// Null indicators for each column (true = NULL)
	NullBitmaps map[string][]bool

	// List of column names in the batch (for ordered processing)
	ColumnNames []string
}

// batchPool is a pool of reusable batch objects
var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{
			Size:          0,
			IntColumns:    make(map[string][]int64),
			FloatColumns:  make(map[string][]float64),
			StringColumns: make(map[string][]string),
			BoolColumns:   make(map[string][]bool),
			TimeColumns:   make(map[string][]time.Time),
			NullBitmaps:   make(map[string][]bool),
			ColumnNames:   make([]string, 0, 32),
		}
	},
}

// batchBufferPool holds pre-allocated arrays for reuse
var (
	intBufferPool    = sync.Pool{New: func() interface{} { return make([]int64, 0, MaxBatchSize) }}
	floatBufferPool  = sync.Pool{New: func() interface{} { return make([]float64, 0, MaxBatchSize) }}
	stringBufferPool = sync.Pool{New: func() interface{} { return make([]string, 0, MaxBatchSize) }}
	boolBufferPool   = sync.Pool{New: func() interface{} { return make([]bool, 0, MaxBatchSize) }}
	timeBufferPool   = sync.Pool{New: func() interface{} { return make([]time.Time, 0, MaxBatchSize) }}
	nullBufferPool   = sync.Pool{New: func() interface{} { return make([]bool, 0, MaxBatchSize) }}
)

// getBuffer gets a buffer of the appropriate type from the pool
func getIntBuffer(size int) []int64 {
	buf := intBufferPool.Get().([]int64)
	if cap(buf) < size {
		// If the buffer is too small, discard it and create a new one
		buf = make([]int64, size)
	} else {
		// Resize the existing buffer
		buf = buf[:size]
	}
	return buf
}

// getFloatBuffer gets a float buffer from the pool
func getFloatBuffer(size int) []float64 {
	buf := floatBufferPool.Get().([]float64)
	if cap(buf) < size {
		buf = make([]float64, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

// getStringBuffer gets a string buffer from the pool
func getStringBuffer(size int) []string {
	buf := stringBufferPool.Get().([]string)
	if cap(buf) < size {
		buf = make([]string, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

// getBoolBuffer gets a bool buffer from the pool
func getBoolBuffer(size int) []bool {
	buf := boolBufferPool.Get().([]bool)
	if cap(buf) < size {
		buf = make([]bool, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

// getTimeBuffer gets a time buffer from the pool
func getTimeBuffer(size int) []time.Time {
	buf := timeBufferPool.Get().([]time.Time)
	if cap(buf) < size {
		buf = make([]time.Time, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

// getNullBuffer gets a null bitmap buffer from the pool
func getNullBuffer(size int) []bool {
	buf := nullBufferPool.Get().([]bool)
	if cap(buf) < size {
		buf = make([]bool, size)
	} else {
		buf = buf[:size]
	}
	return buf
}

// NewBatch creates a new batch with the specified capacity
func NewBatch(capacity int) *Batch {
	if capacity > MaxBatchSize {
		capacity = MaxBatchSize
	}

	batch := batchPool.Get().(*Batch)
	batch.Size = capacity // Set initial size

	// Reuse the maps but clear their contents
	for k := range batch.IntColumns {
		delete(batch.IntColumns, k)
	}
	for k := range batch.FloatColumns {
		delete(batch.FloatColumns, k)
	}
	for k := range batch.StringColumns {
		delete(batch.StringColumns, k)
	}
	for k := range batch.BoolColumns {
		delete(batch.BoolColumns, k)
	}
	for k := range batch.TimeColumns {
		delete(batch.TimeColumns, k)
	}
	for k := range batch.NullBitmaps {
		delete(batch.NullBitmaps, k)
	}

	batch.ColumnNames = batch.ColumnNames[:0]

	return batch
}

// ReleaseBatch returns a batch to the pool for reuse
func ReleaseBatch(batch *Batch) {
	if batch == nil {
		return
	}

	// Return all the buffers to their respective pools
	for k, buf := range batch.IntColumns {
		// Only return buffers with substantial capacity to the pool
		// to avoid keeping very small buffers around
		if cap(buf) >= MaxBatchSize/4 {
			intBufferPool.Put(buf[:0])
		}
		delete(batch.IntColumns, k)
	}

	for k, buf := range batch.FloatColumns {
		if cap(buf) >= MaxBatchSize/4 {
			floatBufferPool.Put(buf[:0])
		}
		delete(batch.FloatColumns, k)
	}

	for k, buf := range batch.StringColumns {
		if cap(buf) >= MaxBatchSize/4 {
			stringBufferPool.Put(buf[:0])
		}
		delete(batch.StringColumns, k)
	}

	for k, buf := range batch.BoolColumns {
		if cap(buf) >= MaxBatchSize/4 {
			boolBufferPool.Put(buf[:0])
		}
		delete(batch.BoolColumns, k)
	}

	for k, buf := range batch.TimeColumns {
		if cap(buf) >= MaxBatchSize/4 {
			timeBufferPool.Put(buf[:0])
		}
		delete(batch.TimeColumns, k)
	}

	for k, buf := range batch.NullBitmaps {
		if cap(buf) >= MaxBatchSize/4 {
			nullBufferPool.Put(buf[:0])
		}
		delete(batch.NullBitmaps, k)
	}

	// Reset size and column list
	batch.Size = 0
	batch.ColumnNames = batch.ColumnNames[:0]

	// Return the batch to the pool
	batchPool.Put(batch)
}

// Column addition methods

// AddIntColumn adds an integer column to the batch
func (b *Batch) AddIntColumn(name string) {
	if _, exists := b.IntColumns[name]; !exists {
		// Add to column list
		b.ColumnNames = append(b.ColumnNames, name)

		// Get column array and NULL bitmap from pool
		b.IntColumns[name] = getIntBuffer(b.Size)
		b.NullBitmaps[name] = getNullBuffer(b.Size)
	}
}

// AddFloatColumn adds a float column to the batch
func (b *Batch) AddFloatColumn(name string) {
	if _, exists := b.FloatColumns[name]; !exists {
		// Add to column list
		b.ColumnNames = append(b.ColumnNames, name)

		// Get column array and NULL bitmap from pool
		b.FloatColumns[name] = getFloatBuffer(b.Size)
		b.NullBitmaps[name] = getNullBuffer(b.Size)
	}
}

// AddStringColumn adds a string column to the batch
func (b *Batch) AddStringColumn(name string) {
	if _, exists := b.StringColumns[name]; !exists {
		// Add to column list
		b.ColumnNames = append(b.ColumnNames, name)

		// Get column array and NULL bitmap from pool
		b.StringColumns[name] = getStringBuffer(b.Size)
		b.NullBitmaps[name] = getNullBuffer(b.Size)
	}
}

// AddBoolColumn adds a boolean column to the batch
func (b *Batch) AddBoolColumn(name string) {
	if _, exists := b.BoolColumns[name]; !exists {
		// Add to column list
		b.ColumnNames = append(b.ColumnNames, name)

		// Get column array and NULL bitmap from pool
		b.BoolColumns[name] = getBoolBuffer(b.Size)
		b.NullBitmaps[name] = getNullBuffer(b.Size)
	}
}

// AddTimeColumn adds a time column to the batch
func (b *Batch) AddTimeColumn(name string) {
	if _, exists := b.TimeColumns[name]; !exists {
		// Add to column list
		b.ColumnNames = append(b.ColumnNames, name)

		// Get column array and NULL bitmap from pool
		b.TimeColumns[name] = getTimeBuffer(b.Size)
		b.NullBitmaps[name] = getNullBuffer(b.Size)
	}
}

// Column copying methods

// AddIntColumnFrom adds an integer column from another batch
func (b *Batch) AddIntColumnFrom(name string, source *Batch) {
	if _, exists := source.IntColumns[name]; exists {
		b.AddIntColumn(name)
		copy(b.IntColumns[name], source.IntColumns[name])
		copy(b.NullBitmaps[name], source.NullBitmaps[name])
	}
}

// AddFloatColumnFrom adds a float column from another batch
func (b *Batch) AddFloatColumnFrom(name string, source *Batch) {
	if _, exists := source.FloatColumns[name]; exists {
		b.AddFloatColumn(name)
		copy(b.FloatColumns[name], source.FloatColumns[name])
		copy(b.NullBitmaps[name], source.NullBitmaps[name])
	}
}

// AddStringColumnFrom adds a string column from another batch
func (b *Batch) AddStringColumnFrom(name string, source *Batch) {
	if _, exists := source.StringColumns[name]; exists {
		b.AddStringColumn(name)
		copy(b.StringColumns[name], source.StringColumns[name])
		copy(b.NullBitmaps[name], source.NullBitmaps[name])
	}
}

// AddBoolColumnFrom adds a boolean column from another batch
func (b *Batch) AddBoolColumnFrom(name string, source *Batch) {
	if _, exists := source.BoolColumns[name]; exists {
		b.AddBoolColumn(name)
		copy(b.BoolColumns[name], source.BoolColumns[name])
		copy(b.NullBitmaps[name], source.NullBitmaps[name])
	}
}

// AddTimeColumnFrom adds a time column from another batch
func (b *Batch) AddTimeColumnFrom(name string, source *Batch) {
	if _, exists := source.TimeColumns[name]; exists {
		b.AddTimeColumn(name)
		copy(b.TimeColumns[name], source.TimeColumns[name])
		copy(b.NullBitmaps[name], source.NullBitmaps[name])
	}
}

// Methods with renamed columns

// AddIntColumnFromWithName adds an integer column from another batch with a new name
func (b *Batch) AddIntColumnFromWithName(sourceName string, source *Batch, newName string) {
	if _, exists := source.IntColumns[sourceName]; exists {
		b.AddIntColumn(newName)
		copy(b.IntColumns[newName], source.IntColumns[sourceName])
		copy(b.NullBitmaps[newName], source.NullBitmaps[sourceName])
	}
}

// AddFloatColumnFromWithName adds a float column from another batch with a new name
func (b *Batch) AddFloatColumnFromWithName(sourceName string, source *Batch, newName string) {
	if _, exists := source.FloatColumns[sourceName]; exists {
		b.AddFloatColumn(newName)
		copy(b.FloatColumns[newName], source.FloatColumns[sourceName])
		copy(b.NullBitmaps[newName], source.NullBitmaps[sourceName])
	}
}

// AddStringColumnFromWithName adds a string column from another batch with a new name
func (b *Batch) AddStringColumnFromWithName(sourceName string, source *Batch, newName string) {
	if _, exists := source.StringColumns[sourceName]; exists {
		b.AddStringColumn(newName)
		copy(b.StringColumns[newName], source.StringColumns[sourceName])
		copy(b.NullBitmaps[newName], source.NullBitmaps[sourceName])
	}
}

// AddBoolColumnFromWithName adds a boolean column from another batch with a new name
func (b *Batch) AddBoolColumnFromWithName(sourceName string, source *Batch, newName string) {
	if _, exists := source.BoolColumns[sourceName]; exists {
		b.AddBoolColumn(newName)
		copy(b.BoolColumns[newName], source.BoolColumns[sourceName])
		copy(b.NullBitmaps[newName], source.NullBitmaps[sourceName])
	}
}

// AddTimeColumnFromWithName adds a time column from another batch with a new name
func (b *Batch) AddTimeColumnFromWithName(sourceName string, source *Batch, newName string) {
	if _, exists := source.TimeColumns[sourceName]; exists {
		b.AddTimeColumn(newName)
		copy(b.TimeColumns[newName], source.TimeColumns[sourceName])
		copy(b.NullBitmaps[newName], source.NullBitmaps[sourceName])
	}
}

// Column existence check methods

// HasIntColumn checks if the batch has an integer column with the given name
func (b *Batch) HasIntColumn(name string) bool {
	_, exists := b.IntColumns[name]
	return exists
}

// HasFloatColumn checks if the batch has a float column with the given name
func (b *Batch) HasFloatColumn(name string) bool {
	_, exists := b.FloatColumns[name]
	return exists
}

// HasStringColumn checks if the batch has a string column with the given name
func (b *Batch) HasStringColumn(name string) bool {
	_, exists := b.StringColumns[name]
	return exists
}

// HasBoolColumn checks if the batch has a boolean column with the given name
func (b *Batch) HasBoolColumn(name string) bool {
	_, exists := b.BoolColumns[name]
	return exists
}

// HasTimeColumn checks if the batch has a time column with the given name
func (b *Batch) HasTimeColumn(name string) bool {
	_, exists := b.TimeColumns[name]
	return exists
}

// HasColumn checks if the batch has any column with the given name
func (b *Batch) HasColumn(name string) bool {
	return b.HasIntColumn(name) || b.HasFloatColumn(name) ||
		b.HasStringColumn(name) || b.HasBoolColumn(name) ||
		b.HasTimeColumn(name)
}

// RestoreColumnStructures restores all column structures from another batch
// This is useful for fixing batches that have missing column structures
func (b *Batch) RestoreColumnStructures(source *Batch, preserveData bool) {
	// Copy column list from source
	b.ColumnNames = make([]string, len(source.ColumnNames))
	copy(b.ColumnNames, source.ColumnNames)

	// Create empty maps if not initialized
	if b.IntColumns == nil {
		b.IntColumns = make(map[string][]int64)
	}
	if b.FloatColumns == nil {
		b.FloatColumns = make(map[string][]float64)
	}
	if b.StringColumns == nil {
		b.StringColumns = make(map[string][]string)
	}
	if b.BoolColumns == nil {
		b.BoolColumns = make(map[string][]bool)
	}
	if b.TimeColumns == nil {
		b.TimeColumns = make(map[string][]time.Time)
	}
	if b.NullBitmaps == nil {
		b.NullBitmaps = make(map[string][]bool)
	}

	// Restore column structures for all source columns
	for _, colName := range source.ColumnNames {
		if source.HasIntColumn(colName) {
			// Int column
			if _, exists := b.IntColumns[colName]; !exists {
				// Create new array with batch size
				b.IntColumns[colName] = make([]int64, b.Size)
				b.NullBitmaps[colName] = make([]bool, b.Size)

				// Copy values if requested and possible
				if preserveData && source.Size >= b.Size {
					copy(b.IntColumns[colName], source.IntColumns[colName][:b.Size])
					copy(b.NullBitmaps[colName], source.NullBitmaps[colName][:b.Size])
				} else {
					// Mark as NULL
					for i := 0; i < b.Size; i++ {
						b.NullBitmaps[colName][i] = true
					}
				}
			}
		} else if source.HasFloatColumn(colName) {
			// Float column
			if _, exists := b.FloatColumns[colName]; !exists {
				b.FloatColumns[colName] = make([]float64, b.Size)
				b.NullBitmaps[colName] = make([]bool, b.Size)

				if preserveData && source.Size >= b.Size {
					copy(b.FloatColumns[colName], source.FloatColumns[colName][:b.Size])
					copy(b.NullBitmaps[colName], source.NullBitmaps[colName][:b.Size])
				} else {
					for i := 0; i < b.Size; i++ {
						b.NullBitmaps[colName][i] = true
					}
				}
			}
		} else if source.HasStringColumn(colName) {
			// String column
			if _, exists := b.StringColumns[colName]; !exists {
				b.StringColumns[colName] = make([]string, b.Size)
				b.NullBitmaps[colName] = make([]bool, b.Size)

				if preserveData && source.Size >= b.Size {
					copy(b.StringColumns[colName], source.StringColumns[colName][:b.Size])
					copy(b.NullBitmaps[colName], source.NullBitmaps[colName][:b.Size])
				} else {
					for i := 0; i < b.Size; i++ {
						b.NullBitmaps[colName][i] = true
					}
				}
			}
		} else if source.HasBoolColumn(colName) {
			// Bool column
			if _, exists := b.BoolColumns[colName]; !exists {
				b.BoolColumns[colName] = make([]bool, b.Size)
				b.NullBitmaps[colName] = make([]bool, b.Size)

				if preserveData && source.Size >= b.Size {
					copy(b.BoolColumns[colName], source.BoolColumns[colName][:b.Size])
					copy(b.NullBitmaps[colName], source.NullBitmaps[colName][:b.Size])
				} else {
					for i := 0; i < b.Size; i++ {
						b.NullBitmaps[colName][i] = true
					}
				}
			}
		} else if source.HasTimeColumn(colName) {
			// Time column
			if _, exists := b.TimeColumns[colName]; !exists {
				b.TimeColumns[colName] = make([]time.Time, b.Size)
				b.NullBitmaps[colName] = make([]bool, b.Size)

				if preserveData && source.Size >= b.Size {
					copy(b.TimeColumns[colName], source.TimeColumns[colName][:b.Size])
					copy(b.NullBitmaps[colName], source.NullBitmaps[colName][:b.Size])
				} else {
					for i := 0; i < b.Size; i++ {
						b.NullBitmaps[colName][i] = true
					}
				}
			}
		}
	}
}

// DebugStructure prints detailed debug information about the batch structure
func (b *Batch) DebugStructure() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Batch size: %d\n", b.Size))
	sb.WriteString(fmt.Sprintf("Column names: %v\n", b.ColumnNames))

	sb.WriteString("Column types:\n")
	for _, colName := range b.ColumnNames {
		if b.HasIntColumn(colName) {
			sb.WriteString(fmt.Sprintf("  %s: int64 (len=%d)\n",
				colName, len(b.IntColumns[colName])))
		} else if b.HasFloatColumn(colName) {
			sb.WriteString(fmt.Sprintf("  %s: float64 (len=%d)\n",
				colName, len(b.FloatColumns[colName])))
		} else if b.HasStringColumn(colName) {
			sb.WriteString(fmt.Sprintf("  %s: string (len=%d)\n",
				colName, len(b.StringColumns[colName])))
		} else if b.HasBoolColumn(colName) {
			sb.WriteString(fmt.Sprintf("  %s: bool (len=%d)\n",
				colName, len(b.BoolColumns[colName])))
		} else if b.HasTimeColumn(colName) {
			sb.WriteString(fmt.Sprintf("  %s: time.Time (len=%d)\n",
				colName, len(b.TimeColumns[colName])))
		} else {
			sb.WriteString(fmt.Sprintf("  %s: UNKNOWN TYPE\n", colName))
		}
	}

	return sb.String()
}

// VectorizedResult implements the storage.Result interface for vectorized batch results
type VectorizedResult struct {
	// The batch containing the data
	batch *Batch

	// Current position in the batch
	currentIndex int

	// Column names for the result set
	columnNames []string

	// Context for the operation
	ctx context.Context

	// Current row values (for Scan)
	currentRow []interface{}
}

// NewVectorizedResult creates a new vectorized result
func NewVectorizedResult(ctx context.Context, batch *Batch, columnNames []string) *VectorizedResult {
	return &VectorizedResult{
		batch:        batch,
		currentIndex: -1, // Before first row
		columnNames:  columnNames,
		ctx:          ctx,
		currentRow:   make([]interface{}, len(columnNames)),
	}
}

// Columns returns the column names in the result
func (vr *VectorizedResult) Columns() []string {
	return vr.columnNames
}

// Next advances to the next row
func (vr *VectorizedResult) Next() bool {
	vr.currentIndex++

	// Check if we've reached the end
	if vr.currentIndex >= vr.batch.Size {
		return false
	}

	// Populate the current row values for Scan
	for i, colName := range vr.columnNames {
		// Check for NULL
		if isNull := vr.isColumnValueNull(colName, vr.currentIndex); isNull {
			vr.currentRow[i] = nil
			continue
		}

		// Get value based on column type
		vr.currentRow[i] = vr.getColumnValue(colName, vr.currentIndex)
	}

	return true
}

// Error constants
var (
	ErrNoRows      = fmt.Errorf("no rows in result set")
	ErrTooManyArgs = fmt.Errorf("too many scan arguments")
)

// Scan copies the current row values into the provided destinations
func (vr *VectorizedResult) Scan(dest ...interface{}) error {
	if vr.currentIndex < 0 || vr.currentIndex >= vr.batch.Size {
		return ErrNoRows
	}

	if len(dest) > len(vr.currentRow) {
		return ErrTooManyArgs
	}

	// Copy values to destinations
	for i, d := range dest {
		if vr.currentRow[i] == nil {
			// Handle NULL values
			switch v := d.(type) {
			case *int64:
				*v = 0
			case *float64:
				*v = 0
			case *string:
				*v = ""
			case *bool:
				*v = false
			case *interface{}:
				*v = nil
			}
			continue
		}

		// Copy based on destination type
		switch v := d.(type) {
		case *int64:
			switch val := vr.currentRow[i].(type) {
			case int64:
				*v = val
			case int32:
				*v = int64(val)
			case int:
				*v = int64(val)
			case float64:
				*v = int64(val)
			}
		case *float64:
			switch val := vr.currentRow[i].(type) {
			case float64:
				*v = val
			case int64:
				*v = float64(val)
			case int:
				*v = float64(val)
			}
		case *string:
			switch val := vr.currentRow[i].(type) {
			case string:
				*v = val
			default:
				*v = vr.currentRow[i].(string)
			}
		case *bool:
			switch val := vr.currentRow[i].(type) {
			case bool:
				*v = val
			}
		case *interface{}:
			*v = vr.currentRow[i]
		}
	}

	return nil
}

// Close releases the resources used by the result
func (vr *VectorizedResult) Close() error {
	// Release the batch back to the pool
	ReleaseBatch(vr.batch)
	vr.batch = nil
	return nil
}

// Context returns the context associated with this result
func (vr *VectorizedResult) Context() context.Context {
	return vr.ctx
}

// RowsAffected returns the number of rows in the result
func (vr *VectorizedResult) RowsAffected() int64 {
	return int64(vr.batch.Size)
}

// LastInsertID returns the last insert ID
func (vr *VectorizedResult) LastInsertID() int64 {
	return 0 // Not applicable for SELECT
}

// Helper methods

// isColumnValueNull checks if a column value is NULL
func (vr *VectorizedResult) isColumnValueNull(colName string, rowIndex int) bool {
	if bm, exists := vr.batch.NullBitmaps[colName]; exists {
		if rowIndex < len(bm) {
			return bm[rowIndex]
		}
	}
	return true // Default to NULL if not found
}

// getColumnValue gets a column value
func (vr *VectorizedResult) getColumnValue(colName string, rowIndex int) interface{} {
	// Check each column type
	if col, exists := vr.batch.IntColumns[colName]; exists && rowIndex < len(col) {
		return col[rowIndex]
	}
	if col, exists := vr.batch.FloatColumns[colName]; exists && rowIndex < len(col) {
		return col[rowIndex]
	}
	if col, exists := vr.batch.StringColumns[colName]; exists && rowIndex < len(col) {
		return col[rowIndex]
	}
	if col, exists := vr.batch.BoolColumns[colName]; exists && rowIndex < len(col) {
		return col[rowIndex]
	}
	if col, exists := vr.batch.TimeColumns[colName]; exists && rowIndex < len(col) {
		return col[rowIndex]
	}

	// Column not found
	return nil
}

// WithAliases returns a new result with the column names replaced by the provided aliases
func (vr *VectorizedResult) WithAliases(aliases map[string]string) storage.Result {
	// Create new column names list with aliases applied
	newNames := make([]string, len(vr.columnNames))
	for i, name := range vr.columnNames {
		if alias, exists := aliases[name]; exists {
			newNames[i] = alias
		} else {
			newNames[i] = name
		}
	}

	// Create new result with same batch but different column names
	return &VectorizedResult{
		batch:        vr.batch,
		currentIndex: -1, // Reset position
		columnNames:  newNames,
		ctx:          vr.ctx,
		currentRow:   make([]interface{}, len(newNames)),
	}
}

// Row implements the storage.Result interface
// Returns the current row as a slice of storage.ColumnValue
func (vr *VectorizedResult) Row() storage.Row {
	if vr.currentIndex < 0 || vr.currentIndex >= vr.batch.Size {
		return nil
	}

	// Create a row with column values
	row := make(storage.Row, len(vr.columnNames))

	// Populate the row with column values
	for i, colName := range vr.columnNames {
		// Check if the value is NULL
		if vr.isColumnValueNull(colName, vr.currentIndex) {
			row[i] = storage.StaticNullUnknown
			continue
		}

		// Get the value based on column type and convert to storage.ColumnValue
		if col, exists := vr.batch.IntColumns[colName]; exists && vr.currentIndex < len(col) {
			row[i] = storage.NewIntegerValue(col[vr.currentIndex])
		} else if col, exists := vr.batch.FloatColumns[colName]; exists && vr.currentIndex < len(col) {
			row[i] = storage.NewFloatValue(col[vr.currentIndex])
		} else if col, exists := vr.batch.StringColumns[colName]; exists && vr.currentIndex < len(col) {
			row[i] = storage.NewStringValue(col[vr.currentIndex])
		} else if col, exists := vr.batch.BoolColumns[colName]; exists && vr.currentIndex < len(col) {
			row[i] = storage.NewBooleanValue(col[vr.currentIndex])
		} else if col, exists := vr.batch.TimeColumns[colName]; exists && vr.currentIndex < len(col) {
			t := col[vr.currentIndex]
			row[i] = storage.NewTimestampValue(t)
		} else {
			// Not found or unsupported type
			row[i] = storage.StaticNullUnknown
		}
	}

	return row
}
