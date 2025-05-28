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

	"github.com/stoolap/stoolap/internal/storage"
)

// Pool for MVCCDirectScanner instances to reduce allocations
var directScannerPool = sync.Pool{
	New: func() interface{} {
		return &MVCCDirectScanner{
			currentIndex: -1,
		}
	},
}

// GetDirectScanner gets a scanner from the pool
func GetDirectScanner() *MVCCDirectScanner {
	return directScannerPool.Get().(*MVCCDirectScanner)
}

// ReturnDirectScanner returns a scanner to the pool
func ReturnDirectScanner(scanner *MVCCDirectScanner) {
	if scanner == nil {
		return
	}

	// Clean up references
	scanner.versionStore = nil
	scanner.registry = nil
	scanner.rowIDs = nil
	scanner.columnIndices = nil
	scanner.whereExpr = nil
	scanner.err = nil
	scanner.currentIndex = -1
	scanner.projectedRow = nil

	directScannerPool.Put(scanner)
}

// MVCCDirectScanner is a scanner that directly uses the version store without creating intermediate maps
type MVCCDirectScanner struct {
	versionStore  *VersionStore        // Source version store
	txnID         int64                // Transaction ID for visibility checks
	registry      *TransactionRegistry // Registry for visibility checks
	schema        storage.Schema       // Schema information
	columnIndices []int                // Which columns to return
	whereExpr     storage.Expression   // Where expression for filtering
	rowIDs        []int64              // Sorted visible row IDs
	currentIndex  int                  // Current position in rowIDs
	projectedRow  storage.Row          // Reusable buffer for column projection
	err           error                // Any error that occurred
}

// NewMVCCDirectScanner creates a scanner that works directly with the version store
// This avoids creating intermediate maps, reducing memory allocations
func NewMVCCDirectScanner(versionStore *VersionStore, txnID int64, registry *TransactionRegistry,
	schema storage.Schema, columnIndices []int, where storage.Expression) *MVCCDirectScanner {

	// Get a scanner from the pool instead of allocating a new one
	scanner := GetDirectScanner()

	// Initialize fields
	scanner.versionStore = versionStore
	scanner.txnID = txnID
	scanner.registry = registry
	scanner.schema = schema
	scanner.whereExpr = where
	scanner.currentIndex = -1

	// Handle column indices to avoid unnecessary allocations
	if columnIndices != nil {
		scanner.columnIndices = columnIndices
	} else {
		scanner.columnIndices = nil
	}

	scanner.projectedRow = make(storage.Row, len(columnIndices))

	// Collect and filter visible row IDs upfront
	// This is more efficient than checking visibility on each Next() call
	scanner.collectVisibleRowIDs()

	return scanner
}

// collectVisibleRowIDs gathers all visible row IDs that match the filter
func (s *MVCCDirectScanner) collectVisibleRowIDs() {
	// Fast path for bulk operations
	if s.versionStore.versions.Len() > 100 {
		// For bulk operations, optimize the case of no filter
		if s.whereExpr == nil {
			// Preallocate with exact capacity to avoid resizing
			s.rowIDs = make([]int64, 0, int(s.versionStore.versions.Len()))

			// Use ForEach to iterate through all entries
			s.versionStore.versions.ForEach(func(rowID int64, version *RowVersion) bool {
				if !version.IsDeleted() && s.registry.IsVisible(version.TxnID, s.txnID) {
					s.rowIDs = append(s.rowIDs, rowID)
				}

				return true
			})

			// Sort row IDs for consistent order using SIMD-optimized sorting
			if len(s.rowIDs) > 0 {
				SIMDSortInt64s(s.rowIDs)
			}
			return
		}
	}

	// For smaller datasets or with filters, use a single-pass approach
	// Preallocate to avoid reallocations
	initialCapacity := int(s.versionStore.versions.Len())
	if initialCapacity > 1000 {
		initialCapacity = initialCapacity / 2 // Optimize for common case
	}
	s.rowIDs = make([]int64, 0, initialCapacity)

	// Single pass approach - apply visibility and filter in one go
	s.versionStore.versions.ForEach(func(rowID int64, version *RowVersion) bool {
		// Skip deleted or invisible versions
		if version.IsDeleted() || !s.registry.IsVisible(version.TxnID, s.txnID) {
			return true
		}

		// Apply filter if provided
		if s.whereExpr != nil {
			matches, err := s.whereExpr.Evaluate(version.Data)
			if err != nil {
				s.err = err
				return false
			}
			if !matches {
				return true
			}
		}

		// Add matching row ID
		s.rowIDs = append(s.rowIDs, rowID)

		return true
	})

	// Sort row IDs for consistent order using SIMD-optimized sorting
	if len(s.rowIDs) > 0 {
		SIMDSortInt64s(s.rowIDs)
	}
}

// Next advances to the next row
func (s *MVCCDirectScanner) Next() bool {
	if s.err != nil {
		return false
	}

	s.currentIndex++
	return s.currentIndex < len(s.rowIDs)
}

// Row returns the current row, with projection if needed
func (s *MVCCDirectScanner) Row() storage.Row {
	if s.currentIndex < 0 || s.currentIndex >= len(s.rowIDs) {
		return nil
	}

	// Get the current row ID
	rowID := s.rowIDs[s.currentIndex]

	// Use haxmap's Get method which is concurrency-safe
	versionPtr, exists := s.versionStore.versions.Get(rowID)

	if !exists || versionPtr.IsDeleted() {
		return nil
	}

	row := versionPtr.Data

	// If no column projection is required, return the row directly
	if len(s.columnIndices) == 0 {
		return row
	}

	// Fill the projection buffer
	for i, colIdx := range s.columnIndices {
		if colIdx < len(row) {
			s.projectedRow[i] = row[colIdx]
		} else {
			s.projectedRow[i] = nil
		}
	}

	return s.projectedRow
}

// Err returns any error that occurred during scanning
func (s *MVCCDirectScanner) Err() error {
	return s.err
}

// Close releases resources
func (s *MVCCDirectScanner) Close() error {
	clear(s.projectedRow)
	s.projectedRow = s.projectedRow[:0]

	// Return the scanner to the pool
	ReturnDirectScanner(s)

	return nil
}
