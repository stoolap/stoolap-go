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
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/stoolap/stoolap/internal/btree"
	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/fastmap"
	"github.com/stoolap/stoolap/internal/storage"
)

// DiskReader handles reading row versions from disk
type DiskReader struct {
	mu       sync.Mutex
	file     *os.File
	filePath string

	// File components
	fileSize int64
	header   FileHeader
	footer   Footer
	schema   *storage.Schema

	// Row access using optimized B-Tree index
	index *btree.Int64BTree[int64] // B-Tree index for fast lookups

	// Loaded rowids tracking
	LoadedRowIDs *fastmap.Int64Map[struct{}] // Maps loaded rowIDs

	// Read buffers to reduce allocations in hot paths
	lenBuffer []byte // Buffer for reading length prefix
}

// NewDiskReader creates a new disk reader
func NewDiskReader(filePath string) (*DiskReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	reader := &DiskReader{
		file:         file,
		filePath:     filePath,
		fileSize:     stat.Size(),
		lenBuffer:    make([]byte, 4),                     // Pre-allocate buffer for length prefix
		LoadedRowIDs: fastmap.NewInt64Map[struct{}](1000), // Initialize loaded rowids map
	}

	// Get buffers from the pool for header and footer
	headerBuffer := common.GetBufferPool()
	defer common.PutBufferPool(headerBuffer)

	// Ensure buffer is sized correctly for header
	if cap(headerBuffer.B) < FileHeaderSize {
		headerBuffer.B = make([]byte, FileHeaderSize)
	} else {
		headerBuffer.B = headerBuffer.B[:FileHeaderSize]
	}

	// Read header
	if _, err := file.ReadAt(headerBuffer.B, 0); err != nil {
		file.Close()
		return nil, err
	}

	reader.header.Magic = binary.LittleEndian.Uint64(headerBuffer.B[0:8])
	reader.header.Version = binary.LittleEndian.Uint32(headerBuffer.B[8:12])
	reader.header.Flags = binary.LittleEndian.Uint32(headerBuffer.B[12:16])

	// Validate magic bytes
	if reader.header.Magic != MagicBytes {
		file.Close()
		return nil, fmt.Errorf("invalid file format: magic mismatch")
	}

	// Get another buffer for footer
	footerBuffer := common.GetBufferPool()
	defer common.PutBufferPool(footerBuffer)

	// Ensure buffer is sized correctly for footer
	if cap(footerBuffer.B) < FooterSize {
		footerBuffer.B = make([]byte, FooterSize)
	} else {
		footerBuffer.B = footerBuffer.B[:FooterSize]
	}

	// Read footer
	if _, err := file.ReadAt(footerBuffer.B, stat.Size()-FooterSize); err != nil {
		file.Close()
		return nil, err
	}

	reader.footer.IndexOffset = binary.LittleEndian.Uint64(footerBuffer.B[0:8])
	reader.footer.IndexSize = binary.LittleEndian.Uint64(footerBuffer.B[8:16])
	reader.footer.RowCount = binary.LittleEndian.Uint64(footerBuffer.B[16:24])
	reader.footer.TxnIDsOffset = binary.LittleEndian.Uint64(footerBuffer.B[24:32])
	reader.footer.TxnIDsCount = binary.LittleEndian.Uint64(footerBuffer.B[32:40])
	reader.footer.Magic = binary.LittleEndian.Uint64(footerBuffer.B[40:48])

	// Validate footer magic
	if reader.footer.Magic != MagicBytes {
		file.Close()
		return nil, fmt.Errorf("invalid file format: footer magic mismatch")
	}

	// Load the index for fast access
	if err := reader.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	// Read schema
	if err := reader.readSchema(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// Close closes the reader
func (r *DiskReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// loadIndex loads the index from the file
func (r *DiskReader) loadIndex() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Calculate the number of index entries
	numEntries := r.footer.IndexSize / IndexEntrySize

	// Get a buffer from the pool
	indexBuffer := common.GetBufferPool()
	defer common.PutBufferPool(indexBuffer)

	// Ensure buffer has enough capacity
	if cap(indexBuffer.B) < int(r.footer.IndexSize) {
		indexBuffer.B = make([]byte, r.footer.IndexSize)
	} else {
		indexBuffer.B = indexBuffer.B[:r.footer.IndexSize]
	}

	// Read the index
	if _, err := r.file.ReadAt(indexBuffer.B, int64(r.footer.IndexOffset)); err != nil {
		return err
	}

	// Create optimized B-Tree index
	r.index = btree.NewInt64BTree[int64]()

	// Parse index entries and prepare for batch insertion
	keys := make([]int64, numEntries)
	values := make([]int64, numEntries)

	for i := uint64(0); i < numEntries; i++ {
		offset := i * IndexEntrySize
		keys[i] = int64(binary.LittleEndian.Uint64(indexBuffer.B[offset : offset+8]))
		values[i] = int64(binary.LittleEndian.Uint64(indexBuffer.B[offset+8 : offset+16]))
	}

	// Use optimized batch insertion
	r.index.BatchInsert(keys, values)

	return nil
}

// readSchema reads the schema from the file
func (r *DiskReader) readSchema() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Skip header
	offset := int64(FileHeaderSize)

	// Get a buffer from the pool for the length
	lenBuffer := common.GetBufferPool()
	defer common.PutBufferPool(lenBuffer)

	// Ensure buffer has enough capacity for length (4 bytes)
	if cap(lenBuffer.B) < 4 {
		lenBuffer.B = make([]byte, 4)
	} else {
		lenBuffer.B = lenBuffer.B[:4]
	}

	// Read schema length
	if _, err := r.file.ReadAt(lenBuffer.B, offset); err != nil {
		return err
	}
	schemaLen := binary.LittleEndian.Uint32(lenBuffer.B)
	offset += 4

	// Get another buffer from the pool for schema data
	schemaBuffer := common.GetBufferPool()
	defer common.PutBufferPool(schemaBuffer)

	// Ensure buffer has enough capacity
	if cap(schemaBuffer.B) < int(schemaLen) {
		schemaBuffer.B = make([]byte, schemaLen)
	} else {
		schemaBuffer.B = schemaBuffer.B[:schemaLen]
	}

	// Read schema data
	if _, err := r.file.ReadAt(schemaBuffer.B, offset); err != nil {
		return err
	}

	// Deserialize schema
	schema, err := deserializeSchema(schemaBuffer.B)
	if err != nil {
		return err
	}

	r.schema = schema
	return nil
}

// GetRow retrieves a row version by rowID
func (r *DiskReader) GetRow(rowID int64) (RowVersion, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.LoadedRowIDs.Has(rowID) {
		return RowVersion{}, false // Already loaded
	}

	// Using B-Tree for faster lookup
	offset, found := r.index.Search(rowID)

	if !found {
		return RowVersion{}, false
	}

	// Read length prefix using pre-allocated buffer
	if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
		return RowVersion{}, false
	}
	rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

	// Get a buffer from the pool instead of allocating
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Ensure the buffer has enough capacity
	if cap(buffer.B) < int(rowLen) {
		buffer.B = make([]byte, rowLen)
	} else {
		buffer.B = buffer.B[:rowLen]
	}

	// Read row data into the reused buffer
	if _, err := r.file.ReadAt(buffer.B, offset+4); err != nil {
		return RowVersion{}, false
	}

	// Deserialize row version
	version, err := deserializeRowVersion(buffer.B)
	if err != nil {
		return RowVersion{}, false
	}

	// Mark this rowID as loaded
	r.LoadedRowIDs.Put(rowID, struct{}{})

	return version, true
}

// GetRowBatch retrieves multiple row versions by rowIDs
func (r *DiskReader) GetRowBatch(rowIDs []int64) map[int64]RowVersion {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(rowIDs) == 0 {
		return nil
	}

	result := make(map[int64]RowVersion, len(rowIDs))

	// Get a buffer from the pool that we'll reuse for each row
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	for _, rowID := range rowIDs {
		if _, ok := r.LoadedRowIDs.Get(rowID); ok {
			continue // Already loaded
		}

		// Using B-Tree for faster lookup
		offset, found := r.index.Search(rowID)
		if !found {
			continue
		}

		// Read length prefix
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			continue
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Ensure the buffer has enough capacity
		if cap(buffer.B) < int(rowLen) {
			buffer.B = make([]byte, rowLen)
		} else {
			buffer.B = buffer.B[:rowLen]
		}

		// Read row data
		if _, err := r.file.ReadAt(buffer.B, offset+4); err != nil {
			continue
		}

		// Deserialize row version
		version, err := deserializeRowVersion(buffer.B)
		if err != nil {
			continue
		}

		// Mark this rowID as loaded
		r.LoadedRowIDs.Put(rowID, struct{}{})

		result[rowID] = version
	}

	return result
}

// ForEach iterates through all rows in the file and calls the provided function for each row
// This is more memory-efficient than GetAllRows as it doesn't create a map of all rows
func (r *DiskReader) ForEach(callback func(rowID int64, version RowVersion) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if callback == nil || r.index == nil {
		return
	}

	// Track process counts for debugging/monitoring
	processed := 0
	errors := 0

	// Get a buffer from the pool that we'll reuse for each row
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	for rowID, offset := range r.index.GetAll() {
		// Check if we've already loaded this rowID
		if r.LoadedRowIDs.Has(rowID) {
			continue
		}

		// Read length prefix using pre-allocated buffer
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			errors++
			continue
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Ensure the buffer has enough capacity
		if cap(buffer.B) < int(rowLen) {
			buffer.B = make([]byte, rowLen)
		} else {
			buffer.B = buffer.B[:rowLen]
		}

		// Read row data
		if _, err := r.file.ReadAt(buffer.B, offset+4); err != nil {
			errors++
			continue
		}

		// Deserialize row version
		version, err := deserializeRowVersion(buffer.B)
		if err != nil {
			errors++
			continue
		}

		processed++

		// Mark this rowID as processed
		r.LoadedRowIDs.Put(rowID, struct{}{})

		// Call the callback with the rowID and version
		callback(rowID, version)
	}

	// Optional debug info if needed
	if errors > 0 {
		log.Printf("Error: DiskReader processed %d rows, encountered %d errors\n",
			processed, errors)
	}
}

// GetAllRows retrieves all rows in the file
// This is used for full table scans from disk
func (r *DiskReader) GetAllRows() map[int64]RowVersion {
	// Estimate initial capacity based on row count
	result := make(map[int64]RowVersion, r.footer.RowCount)

	// Get a buffer from the pool that we'll reuse for each row
	buffer := common.GetBufferPool()
	defer common.PutBufferPool(buffer)

	// Iterate through all entries in the index
	r.index.ForEach(func(rowID int64, offset int64) bool {
		// Read length prefix using pre-allocated buffer
		if _, err := r.file.ReadAt(r.lenBuffer, offset); err != nil {
			return true // Continue on error
		}
		rowLen := binary.LittleEndian.Uint32(r.lenBuffer)

		// Ensure the buffer has enough capacity
		if cap(buffer.B) < int(rowLen) {
			buffer.B = make([]byte, rowLen)
		} else {
			buffer.B = buffer.B[:rowLen]
		}

		// Read row data
		if _, err := r.file.ReadAt(buffer.B, offset+4); err != nil {
			return true // Continue on error
		}

		// Deserialize row version
		version, err := deserializeRowVersion(buffer.B)
		if err != nil {
			return true // Continue on error
		}

		result[rowID] = version

		return true
	})

	return result
}

// GetSchema returns the schema
func (r *DiskReader) GetSchema() *storage.Schema {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.schema
}
