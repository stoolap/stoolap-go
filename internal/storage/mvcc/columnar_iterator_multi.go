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
package mvcc

import (
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// MultiColumnarIndexIterator provides optimized iteration over multi-column index matches
// with specialized handling for multi-column constraints and efficient prefetching
type MultiColumnarIndexIterator struct {
	versionStore  *VersionStore
	txnID         int64
	schema        storage.Schema
	columnIndices []int

	// For multi-column index context
	indexColumnNames []string
	indexColumnIDs   []int

	// For direct sorted iteration
	rowIDs    []int64
	idIndex   int
	batchSize int

	// For current row
	currentRow   storage.Row
	projectedRow storage.Row

	// For pre-fetching
	prefetchRowIDs []int64
	prefetchIndex  int
	prefetchMap    *fastmap.Int64Map[storage.Row]
}

// NewMultiColumnarIndexIterator creates an optimized iterator for multi-column indexes
func NewMultiColumnarIndexIterator(
	versionStore *VersionStore,
	rowIDs []int64,
	txnID int64,
	schema storage.Schema,
	columnIndices []int,
	indexColumnNames []string,
	indexColumnIDs []int) storage.Scanner {

	// Sort row IDs for efficient access pattern and sequential prefetching
	SIMDSortInt64s(rowIDs)

	// Choose optimal batch size based on result set size and number of columns
	batchSize := 100 // Default batch size
	if len(rowIDs) > 1000 {
		// For large result sets, use larger batches
		batchSize = 200
	} else if len(rowIDs) < 50 {
		// For tiny result sets, use smaller batches to avoid waste
		batchSize = max(10, len(rowIDs))
	}

	// For multi-column indexes with many columns, use larger batches
	// as these queries are often more selective and benefit from fewer round trips
	if len(indexColumnNames) > 2 {
		batchSize = batchSize * 2
	}

	return &MultiColumnarIndexIterator{
		versionStore:     versionStore,
		txnID:            txnID,
		schema:           schema,
		columnIndices:    columnIndices,
		indexColumnNames: indexColumnNames,
		indexColumnIDs:   indexColumnIDs,
		rowIDs:           rowIDs,
		idIndex:          -1,
		batchSize:        batchSize,
		prefetchIndex:    0,
		projectedRow:     make(storage.Row, len(columnIndices)),
		prefetchMap:      GetRowMap(),
	}
}

// Next advances to the next matching row
func (it *MultiColumnarIndexIterator) Next() bool {
	// Check if we need to prefetch the next batch
	if it.prefetchIndex >= len(it.prefetchRowIDs) {
		// Calculate next batch to prefetch
		remainingRows := len(it.rowIDs) - it.idIndex - 1
		if remainingRows <= 0 {
			return false // No more rows
		}

		// Determine batch size (don't exceed remaining rows)
		// Optimize batch size based on remaining rows and data density
		batchSize := it.batchSize
		if remainingRows < batchSize {
			batchSize = remainingRows
		} else if remainingRows > batchSize*10 {
			// For large result sets, use a larger batch size to reduce the
			// number of fetches and improve performance
			batchSize = min(remainingRows/5, 500) // Cap at 500 to avoid excessive memory usage
		}

		// Clear previous batch data
		it.prefetchMap.Clear()

		// Prefetch batch of IDs with optimized slice handling
		endIdx := it.idIndex + 1 + batchSize
		if endIdx > len(it.rowIDs) {
			endIdx = len(it.rowIDs)
		}

		// Reuse the prefetch slice if possible to reduce allocations
		if cap(it.prefetchRowIDs) >= endIdx-(it.idIndex+1) {
			it.prefetchRowIDs = it.prefetchRowIDs[:0] // Reset length but keep capacity
			it.prefetchRowIDs = append(it.prefetchRowIDs, it.rowIDs[it.idIndex+1:endIdx]...)
		} else {
			it.prefetchRowIDs = it.rowIDs[it.idIndex+1 : endIdx]
		}
		it.prefetchIndex = 0

		// Batch fetch visible versions
		versions := it.versionStore.GetVisibleVersionsByIDs(it.prefetchRowIDs, it.txnID)

		// Track if any rows were found to determine early exit
		foundAny := false

		// Extract non-deleted rows
		versions.ForEach(func(rowID int64, version *RowVersion) bool {
			if !version.IsDeleted() {
				it.prefetchMap.Put(rowID, version.Data)
				foundAny = true
			}

			return true
		})

		// IMPORTANT: Free the versions map to avoid memory leak
		ReturnVisibleVersionMap(versions)

		// If nothing found, skip to next batch immediately rather than looping
		if !foundAny {
			// Advance the idIndex past this entire batch to avoid revisiting
			it.idIndex += len(it.prefetchRowIDs)
			// Try next batch directly, skipping the loop below
			return it.Next()
		}
	}

	// Fast path: Advance until we find a visible row
	for it.prefetchIndex < len(it.prefetchRowIDs) {
		rowID := it.prefetchRowIDs[it.prefetchIndex]
		it.prefetchIndex++
		it.idIndex++

		// Check if this row is in our prefetch map
		if row, exists := it.prefetchMap.Get(rowID); exists {
			// Use direct assignment for better performance
			it.currentRow = row
			return true
		}
	}

	// Try next batch
	return it.Next()
}

// Row returns the current row
func (it *MultiColumnarIndexIterator) Row() storage.Row {
	// Fast path for full row projection
	if len(it.columnIndices) == 0 {
		return it.currentRow
	}

	// Fast path for single column projection (common case)
	if len(it.columnIndices) == 1 {
		colIdx := it.columnIndices[0]
		if colIdx < len(it.currentRow) {
			it.projectedRow[0] = it.currentRow[colIdx]
		} else {
			// Column doesn't exist, set to nil
			it.projectedRow[0] = nil
		}
		return it.projectedRow
	}

	// Special case for indexed column-only projection
	// This is a fast path for when the query only requests the indexed columns
	if len(it.columnIndices) == len(it.indexColumnIDs) {
		// Check if columnIndices matches indexColumnIDs
		match := true
		for i, colIdx := range it.columnIndices {
			if colIdx != it.indexColumnIDs[i] {
				match = false
				break
			}
		}

		if match {
			// This is an index-only scan, optimize it
			for i, colIdx := range it.indexColumnIDs {
				if colIdx < len(it.currentRow) {
					it.projectedRow[i] = it.currentRow[colIdx]
				} else {
					it.projectedRow[i] = nil
				}
			}
			return it.projectedRow
		}
	}

	// Regular column projection for multiple columns
	for i, colIdx := range it.columnIndices {
		if colIdx < len(it.currentRow) {
			it.projectedRow[i] = it.currentRow[colIdx]
		} else {
			it.projectedRow[i] = nil
		}
	}
	return it.projectedRow
}

// Err returns any error
func (it *MultiColumnarIndexIterator) Err() error {
	return nil
}

// Close releases resources
func (it *MultiColumnarIndexIterator) Close() error {
	clear(it.projectedRow)
	it.projectedRow = it.projectedRow[:0]

	it.rowIDs = nil
	it.prefetchRowIDs = nil
	it.currentRow = nil

	it.prefetchMap.Clear()
	PutRowMap(it.prefetchMap)

	return nil
}
