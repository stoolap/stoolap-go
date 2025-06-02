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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/stoolap/stoolap/internal/common"
	"github.com/stoolap/stoolap/internal/storage"
)

// DiskAppender handles writing row versions to disk
type DiskAppender struct {
	file            *os.File
	writer          *bufio.Writer
	indexBuffer     *bytes.Buffer
	dataOffset      uint64
	rowCount        uint64
	rowIDIndex      map[int64]uint64 // Maps rowIDs to offsets
	committedTxnIDs map[int64]int64  // Maps committed TxnIDs to timestamps

	fails bool // Indicates if the appender has failed
}

// NewDiskAppender creates a new disk appender
func NewDiskAppender(filePath string) (*DiskAppender, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Initialize with 16 byte header
	header := FileHeader{
		Magic:   MagicBytes,
		Version: FileFormatVersion,
		Flags:   0,
	}

	// Get a buffer from the pool
	headerBuffer := common.GetBufferPool()
	defer common.PutBufferPool(headerBuffer)

	// Ensure buffer has enough capacity
	if cap(headerBuffer.B) < FileHeaderSize {
		headerBuffer.B = make([]byte, FileHeaderSize)
	} else {
		headerBuffer.B = headerBuffer.B[:FileHeaderSize]
	}

	binary.LittleEndian.PutUint64(headerBuffer.B[0:8], header.Magic)
	binary.LittleEndian.PutUint32(headerBuffer.B[8:12], header.Version)
	binary.LittleEndian.PutUint32(headerBuffer.B[12:16], header.Flags)

	writer := bufio.NewWriterSize(file, DefaultBlockSize)
	if _, err := writer.Write(headerBuffer.B); err != nil {
		file.Close()

		os.Remove(filePath) // Clean up on error
		return nil, err
	}

	return &DiskAppender{
		file:            file,
		writer:          writer,
		indexBuffer:     bytes.NewBuffer(make([]byte, 0, 1024*1024)), // 1MB initial capacity
		dataOffset:      FileHeaderSize,                              // Start after header
		rowIDIndex:      make(map[int64]uint64),
		committedTxnIDs: make(map[int64]int64), // Initialize map of committed TxnIDs
	}, nil
}

// Fail marks the appender as failed, preventing further writes
func (a *DiskAppender) Fail() {
	a.fails = true
}

// Close closes the appender and its file
func (a *DiskAppender) Close() error {
	if a.file != nil {
		if a.writer != nil {
			a.writer.Flush()
		}

		fileName := a.file.Name() // Store the filename before closing
		closeErr := a.file.Close()

		if a.fails {
			removeErr := os.Remove(fileName)
			if closeErr != nil {
				return fmt.Errorf("failed to close file: %w (and tried to remove it)", closeErr)
			}
			return removeErr
		}

		return closeErr
	}
	return nil
}

// WriteSchema writes the table schema to the file
func (a *DiskAppender) WriteSchema(schema *storage.Schema) error {
	// Get a buffer from the pool for the schema serialization
	schemaBuffer := common.GetBufferPool()
	defer common.PutBufferPool(schemaBuffer)

	// Serialize schema into the buffer
	serializedSchema, err := serializeSchema(schema)
	if err != nil {
		return err
	}

	// Get another buffer for the length prefix
	lenBuffer := common.GetBufferPool()
	defer common.PutBufferPool(lenBuffer)

	// Set length buffer capacity
	if cap(lenBuffer.B) < 4 {
		lenBuffer.B = make([]byte, 4)
	} else {
		lenBuffer.B = lenBuffer.B[:4]
	}

	// Write length of schema data (uint32)
	binary.LittleEndian.PutUint32(lenBuffer.B, uint32(len(serializedSchema)))
	if _, err := a.writer.Write(lenBuffer.B); err != nil {
		return err
	}

	// Write schema data
	if _, err := a.writer.Write(serializedSchema); err != nil {
		return err
	}

	// Update offset
	a.dataOffset += 4 + uint64(len(serializedSchema))

	return nil
}

// AppendRow appends a single row version to the file
func (a *DiskAppender) AppendRow(version RowVersion) error {
	// Serialize row
	rowBytes, err := serializeRowVersion(version)
	if err != nil {
		return err
	}

	// Check if this rowID already exists
	if _, exists := a.rowIDIndex[version.RowID]; exists {
		return fmt.Errorf("duplicate rowID: %d", version.RowID)
	}

	// Store the offset in our index
	a.rowIDIndex[version.RowID] = a.dataOffset

	// Track committed transaction ID and timestamp
	// If this version is in the snapshot, its transaction must have been committed
	// Store the CreateTime as the approximate commit timestamp
	a.committedTxnIDs[version.TxnID] = version.CreateTime

	// Get a buffer from the pool for the length
	lenBuffer := common.GetBufferPool()
	defer common.PutBufferPool(lenBuffer)

	// Ensure buffer has enough capacity
	if cap(lenBuffer.B) < 4 {
		lenBuffer.B = make([]byte, 4)
	} else {
		lenBuffer.B = lenBuffer.B[:4]
	}

	// Write row length (uint32)
	binary.LittleEndian.PutUint32(lenBuffer.B, uint32(len(rowBytes)))
	if _, err := a.writer.Write(lenBuffer.B); err != nil {
		return err
	}

	// Write row data
	if _, err := a.writer.Write(rowBytes); err != nil {
		return err
	}

	// Update offset and counter
	a.dataOffset += 4 + uint64(len(rowBytes))
	a.rowCount++

	return nil
}

// AppendBatch appends a batch of row versions to the file
func (a *DiskAppender) AppendBatch(versions []RowVersion) error {
	for _, version := range versions {
		if err := a.AppendRow(version); err != nil {
			return err
		}
	}
	return nil
}

// Finalize completes the file by writing the index and footer
func (a *DiskAppender) Finalize() error {
	// Flush any pending data
	if err := a.writer.Flush(); err != nil {
		return err
	}

	// Build index - sort by rowID for binary search
	var sortedKeys []int64
	for rowID := range a.rowIDIndex {
		sortedKeys = append(sortedKeys, rowID)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return sortedKeys[i] < sortedKeys[j]
	})

	// Get buffers from the pool for index entry parts
	rowIDBuffer := common.GetBufferPool()
	defer common.PutBufferPool(rowIDBuffer)

	offsetBuffer := common.GetBufferPool()
	defer common.PutBufferPool(offsetBuffer)

	// Ensure buffers have enough capacity
	if cap(rowIDBuffer.B) < 8 {
		rowIDBuffer.B = make([]byte, 8)
	} else {
		rowIDBuffer.B = rowIDBuffer.B[:8]
	}

	if cap(offsetBuffer.B) < 8 {
		offsetBuffer.B = make([]byte, 8)
	} else {
		offsetBuffer.B = offsetBuffer.B[:8]
	}

	// Write each index entry
	indexOffset := a.dataOffset
	for _, rowID := range sortedKeys {
		offset := a.rowIDIndex[rowID]

		// Write rowID (int64)
		binary.LittleEndian.PutUint64(rowIDBuffer.B, uint64(rowID))
		a.indexBuffer.Write(rowIDBuffer.B)

		// Write offset (uint64)
		binary.LittleEndian.PutUint64(offsetBuffer.B, offset)
		a.indexBuffer.Write(offsetBuffer.B)
	}

	// Write index to file
	indexBytes := a.indexBuffer.Bytes()
	if _, err := a.file.WriteAt(indexBytes, int64(indexOffset)); err != nil {
		return err
	}

	// Prepare TxnIDs data - write transaction IDs and their commit timestamps
	txnIDsOffset := indexOffset + uint64(len(indexBytes))

	// Use a ByteBuffer from our pool instead of a bytes.Buffer
	txnIdsBufferObj := common.GetBufferPool()
	defer common.PutBufferPool(txnIdsBufferObj)

	// Get buffers from the pool for txnID and timestamp
	txnIDBuffer := common.GetBufferPool()
	defer common.PutBufferPool(txnIDBuffer)

	timestampBuffer := common.GetBufferPool()
	defer common.PutBufferPool(timestampBuffer)

	// Ensure buffers have enough capacity
	if cap(txnIDBuffer.B) < 8 {
		txnIDBuffer.B = make([]byte, 8)
	} else {
		txnIDBuffer.B = txnIDBuffer.B[:8]
	}

	if cap(timestampBuffer.B) < 8 {
		timestampBuffer.B = make([]byte, 8)
	} else {
		timestampBuffer.B = timestampBuffer.B[:8]
	}

	for txnID, timestamp := range a.committedTxnIDs {
		// Write TxnID (int64)
		binary.LittleEndian.PutUint64(txnIDBuffer.B, uint64(txnID))
		txnIdsBufferObj.Write(txnIDBuffer.B)

		// Write commit timestamp (int64)
		binary.LittleEndian.PutUint64(timestampBuffer.B, uint64(timestamp))
		txnIdsBufferObj.Write(timestampBuffer.B)
	}

	// Write transaction IDs to file
	if _, err := a.file.WriteAt(txnIdsBufferObj.B, int64(txnIDsOffset)); err != nil {
		return err
	}

	// Prepare footer
	footer := Footer{
		IndexOffset:  indexOffset,
		IndexSize:    uint64(len(indexBytes)),
		RowCount:     a.rowCount,
		TxnIDsOffset: txnIDsOffset,
		TxnIDsCount:  uint64(len(a.committedTxnIDs)),
		Magic:        MagicBytes,
	}

	// Get a buffer from the pool for the footer
	footerBuffer := common.GetBufferPool()
	defer common.PutBufferPool(footerBuffer)

	// Ensure buffer has enough capacity
	if cap(footerBuffer.B) < 48 {
		footerBuffer.B = make([]byte, 48)
	} else {
		footerBuffer.B = footerBuffer.B[:48]
	}

	// Write footer
	footerOffset := txnIDsOffset + uint64(len(txnIdsBufferObj.B))
	binary.LittleEndian.PutUint64(footerBuffer.B[0:8], footer.IndexOffset)
	binary.LittleEndian.PutUint64(footerBuffer.B[8:16], footer.IndexSize)
	binary.LittleEndian.PutUint64(footerBuffer.B[16:24], footer.RowCount)
	binary.LittleEndian.PutUint64(footerBuffer.B[24:32], footer.TxnIDsOffset)
	binary.LittleEndian.PutUint64(footerBuffer.B[32:40], footer.TxnIDsCount)
	binary.LittleEndian.PutUint64(footerBuffer.B[40:48], footer.Magic)

	if _, err := a.file.WriteAt(footerBuffer.B, int64(footerOffset)); err != nil {
		return err
	}

	// Force sync to ensure data is on disk
	return a.file.Sync()
}
