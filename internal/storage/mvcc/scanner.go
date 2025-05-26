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
	"sync"
	"sync/atomic"
	"time"

	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// MVCCScannerMode defines the scanner's operation mode
type MVCCScannerMode int

// MVCCScanner is a simple scanner implementation for MVCC results
type MVCCScanner struct {
	sourceRows    *fastmap.Int64Map[storage.Row] // Original row references, not copied
	rowIDs        []int64                        // Sorted row IDs for iteration
	currentIndex  int                            // Current position in rowIDs
	columnIndices []int                          // Which columns to return
	schema        storage.Schema                 // Schema information
	err           error                          // Any error that occurred
	rowMap        *fastmap.Int64Map[storage.Row] // The original map, will be returned to pool on Close
	projectedRow  storage.Row                    // Reusable buffer for column projection

	closed      atomic.Bool
	shouldClose atomic.Int64
}

// Next advances to the next row
func (s *MVCCScanner) Next() bool {
	// Check for errors
	if s.err != nil {
		s.shouldClose.Store(time.Now().UnixNano())
		return false
	}

	// Advance to next row
	s.currentIndex++

	// Check if we've reached the end
	next := s.currentIndex < len(s.rowIDs)
	if !next {
		s.shouldClose.Store(time.Now().UnixNano())
		return false
	}

	return true
}

// Row returns the current row, with projection if needed
func (s *MVCCScanner) Row() storage.Row {
	if s.currentIndex < 0 || s.currentIndex >= len(s.rowIDs) {
		s.shouldClose.Store(time.Now().UnixNano())
		return nil
	}

	// Row mode implementation (the original logic)
	// Get the current row
	rowID := s.rowIDs[s.currentIndex]
	row, ok := s.sourceRows.Get(rowID)
	if !ok {
		return nil // Row not found
	}

	// If no column projection is required, return the row directly
	if len(s.columnIndices) == 0 {
		return row
	}

	// Fill the projection buffer with values from the source row
	for i, colIdx := range s.columnIndices {
		if colIdx < len(row) {
			s.projectedRow[i] = row[colIdx]
		} else {
			s.projectedRow[i] = nil // Handle out-of-bounds gracefully
		}
	}

	return s.projectedRow
}

// Err returns any error that occurred during scanning
func (s *MVCCScanner) Err() error {
	return s.err
}

// Close releases any resources held by the scanner
func (s *MVCCScanner) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	clear(s.projectedRow)
	s.projectedRow = s.projectedRow[:0]

	// Return the row map to the pool if it came from there
	if s.rowMap != nil {
		PutRowMap(s.rowMap)
	}

	// Return the scanner itself to the pool
	ReturnMVCCScanner(s)

	return nil
}

// Pool for rowIDs slices to reduce allocations
var rowIDsPool = sync.Pool{
	New: func() interface{} {
		// Default initial capacity for rowIDs
		sp := make([]int64, 0, 128)
		return &sp
	},
}

// GetRowIDSlice gets a slice from the pool with appropriate capacity
func GetRowIDSlice(capacity int) []int64 {
	sliceInterface := rowIDsPool.Get()
	sp, ok := sliceInterface.(*[]int64)
	if !ok || sp == nil {
		return make([]int64, 0, capacity)
	}
	slice := *sp
	// Clear the slice but preserve capacity
	slice = slice[:0]

	// If existing capacity is too small, create a new slice
	if cap(slice) < capacity {
		return make([]int64, 0, capacity)
	}

	return slice
}

// PutRowIDSlice returns a slice to the pool
func PutRowIDSlice(slice []int64) {
	if slice == nil || cap(slice) == 0 {
		return
	}

	// Clear the slice and return to pool
	slice = slice[:0]
	rowIDsPool.Put(&slice)
}

// RangeScanner is a specialized scanner for efficient ID range queries
type RangeScanner struct {
	txnID           int64                          // Transaction ID for visibility checks
	versionStore    *VersionStore                  // Access to versions
	currentID       int64                          // Current ID in range
	endID           int64                          // End ID in range (inclusive)
	columnIndices   []int                          // Columns to include
	schema          storage.Schema                 // Schema information
	err             error                          // Any scanning errors
	projectedRow    storage.Row                    // Reused buffer for projection
	currentRow      storage.Row                    // Current row being processed
	batchSize       int                            // Size of batches for prefetching
	currentBatch    *fastmap.Int64Map[*RowVersion] // Current batch of prefetched rows
	inclusive       bool                           // Whether endID is inclusive
	currentBatchEnd int64                          // End of the current batch range
}

// NewRangeScanner creates a scanner optimized for ID range scans
// This implementation works efficiently with consecutive ID ranges
func NewRangeScanner(
	versionStore *VersionStore,
	startID, endID int64,
	inclusive bool,
	txnID int64,
	schema storage.Schema,
	columnIndices []int,
) *RangeScanner {
	return &RangeScanner{
		txnID:           txnID,
		versionStore:    versionStore,
		currentID:       startID,
		endID:           endID,
		inclusive:       inclusive,
		columnIndices:   columnIndices,
		schema:          schema,
		batchSize:       1000, // Batch size for prefetching
		currentBatch:    nil,  // Initialize to nil
		projectedRow:    make(storage.Row, len(columnIndices)),
		currentBatchEnd: 0, // Will be set in Next()
	}
}

// Next advances to the next row in the range
func (s *RangeScanner) Next() bool {
	// Check for errors
	if s.err != nil {
		return false
	}

	// Calculate actual end condition based on inclusive flag
	actualEnd := s.endID
	if !s.inclusive {
		actualEnd = s.endID - 1
	}

	// Check if we've reached the end of the range
	if s.currentID > actualEnd {
		return false
	}

	// Fetch the next visible row
	for s.currentID <= actualEnd {
		// Check if we need to fetch a new batch:
		// 1. If currentBatch is nil
		// 2. If currentBatch is empty
		// 3. If we've moved beyond the current batch's range
		if s.currentBatch == nil || s.currentBatch.Len() == 0 || s.currentID > s.currentBatchEnd {
			// Return the previous batch to the pool if it exists
			if s.currentBatch != nil {
				ReturnVisibleVersionMap(s.currentBatch)
				s.currentBatch = nil
			}

			// Calculate new batch boundaries
			batchStart := s.currentID
			s.currentBatchEnd = batchStart + int64(s.batchSize) - 1
			if s.currentBatchEnd > actualEnd {
				s.currentBatchEnd = actualEnd
			}

			// Prepare ID slice for batch
			ids := make([]int64, 0, s.currentBatchEnd-batchStart+1)
			for id := batchStart; id <= s.currentBatchEnd; id++ {
				ids = append(ids, id)
			}

			// Fetch batch of versions
			s.currentBatch = s.versionStore.GetVisibleVersionsByIDs(ids, s.txnID)

			// If batch is empty, we can skip to the next batch range
			if s.currentBatch.Len() == 0 {
				s.currentID = s.currentBatchEnd + 1
				continue
			}
		}

		// Check if the current ID exists and is visible
		if version, exists := s.currentBatch.Get(s.currentID); exists && !version.IsDeleted {
			s.currentRow = version.Data
			s.currentID++ // Advance ID for next iteration
			return true
		}

		// ID doesn't exist or is deleted, try next ID
		s.currentBatch.Del(s.currentID)
		s.currentID++
	}

	return false
}

// Row returns the current row, with projection if needed
func (s *RangeScanner) Row() storage.Row {
	if s.currentRow == nil {
		return nil
	}

	// If no column projection is required, return the row directly
	if len(s.columnIndices) == 0 {
		return s.currentRow
	}

	// Fill the projection buffer with values from the source row
	for i, colIdx := range s.columnIndices {
		if colIdx < len(s.currentRow) {
			s.projectedRow[i] = s.currentRow[colIdx]
		} else {
			s.projectedRow[i] = nil // Handle out-of-bounds gracefully
		}
	}

	return s.projectedRow
}

// Err returns any error that occurred during scanning
func (s *RangeScanner) Err() error {
	return s.err
}

// Close releases resources
func (s *RangeScanner) Close() error {
	clear(s.projectedRow)
	s.projectedRow = s.projectedRow[:0]

	// Return the version map to the pool
	if s.currentBatch != nil {
		ReturnVisibleVersionMap(s.currentBatch)
		s.currentBatch = nil
	}

	// Clear references
	s.currentRow = nil
	s.versionStore = nil

	return nil
}

// Pool for MVCCScanner instances to reduce allocations
var mvccScannerPool = sync.Pool{
	New: func() interface{} {
		return &MVCCScanner{
			currentIndex: -1,
		}
	},
}

// GetMVCCScanner gets a scanner from the pool
func GetMVCCScanner() *MVCCScanner {
	return mvccScannerPool.Get().(*MVCCScanner)
}

// ReturnMVCCScanner returns a scanner to the pool
func ReturnMVCCScanner(scanner *MVCCScanner) {
	if scanner == nil {
		return
	}

	// Return the rowIDs slice to the pool
	if scanner.rowIDs != nil {
		PutRowIDSlice(scanner.rowIDs)
	}

	// Return the scanner to the pool after cleaning up references
	scanner.sourceRows = nil
	scanner.rowIDs = nil
	scanner.columnIndices = nil
	// We can't set schema to nil as it's a value type, not a pointer
	scanner.projectedRow = nil
	scanner.rowMap = nil
	scanner.err = nil
	scanner.currentIndex = -1

	scanner.shouldClose.Store(0) // Reset shouldClose to 0
	scanner.closed.Store(false)  // Reset closed state

	mvccScannerPool.Put(scanner)
}

// NewMVCCScanner creates a new scanner for MVCC-tracked rows
// Memory-optimized version that doesn't copy rows unnecessarily
func NewMVCCScanner(rows *fastmap.Int64Map[storage.Row], schema storage.Schema, columnIndices []int, where storage.Expression) *MVCCScanner {
	// Get a scanner from the pool instead of creating a new one
	scanner := GetMVCCScanner()

	// Initialize reused scanner fields
	scanner.sourceRows = rows                  // Use the rows map directly to avoid copying
	scanner.rowIDs = GetRowIDSlice(rows.Len()) // Get a reused slice from the pool
	scanner.currentIndex = -1
	scanner.columnIndices = columnIndices
	scanner.schema = schema
	scanner.rowMap = rows // Store the original map for returning to the pool

	scanner.projectedRow = make(storage.Row, len(columnIndices))

	// Filtering phase - apply where expression
	rows.ForEach(func(rowID int64, row storage.Row) bool {
		// Skip rows that don't match the filter
		if where != nil {
			matches, err := where.Evaluate(row)
			if err != nil {
				scanner.err = err
				return false
			}
			if !matches {
				return true
			}
		}

		// Add the matching row ID to the list
		scanner.rowIDs = append(scanner.rowIDs, rowID)

		return true
	})

	// For most database operations, we want rows sorted by ID
	// Use our SIMD-optimized sorting algorithm for int64 slices
	if len(scanner.rowIDs) > 0 {
		SIMDSortInt64s(scanner.rowIDs)
	}

	go scanner.resourceWatch()

	return scanner
}

func (s *MVCCScanner) resourceWatch() {
	maxDuration := time.Second

	for {
		if s.closed.Load() {
			return // Exit if scanner is closed
		}

		shouldClose := s.shouldClose.Load()
		if shouldClose > 0 {
			if elapsed := time.Now().UnixNano() - shouldClose; elapsed > maxDuration.Nanoseconds() {
				// log.Printf("Warning: scanner has been idle for %s, closing...\n", time.Duration(elapsed))
				s.Close()
				return
			}
		}

		time.Sleep(100 * time.Millisecond) // Check every 100ms
	}
}
