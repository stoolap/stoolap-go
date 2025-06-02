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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stoolap/stoolap/internal/storage"
)

// Helper functions for creating and manipulating WAL entries for testing
func createTestEntry(txnID int64, tableName string, rowID int64, op WALOperationType) WALEntry {
	return WALEntry{
		TxnID:     txnID,
		TableName: tableName,
		RowID:     rowID,
		Operation: op,
		Data:      []byte("test data"),
		Timestamp: time.Now().UnixNano(),
	}
}

// TestWALReplayUpdatesCurrentLSN verifies that the WAL.currentLSN is updated during replay
func TestWALReplayUpdatesCurrentLSN(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager with test configuration
	walManager, err := NewWALManager(testDir, SyncMode(config.SyncMode), &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Create and record test entries
	var expectedLSN uint64
	for i := 0; i < 5; i++ {
		entry := createTestEntry(int64(i), "test_table", int64(i), WALInsert)
		lsn, err := walManager.AppendEntry(entry)
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}

		// Save the last LSN for later comparison
		expectedLSN = lsn

		// Save the entry for debugging
		entry.LSN = lsn
	}

	// Ensure data is flushed to disk
	walManager.Flush()
	walManager.Close()

	// Create a new WAL manager with the same directory
	newManager, err := NewWALManager(testDir, SyncMode(config.SyncMode), &config)
	if err != nil {
		t.Fatalf("Failed to create new WAL manager: %v", err)
	}
	defer newManager.Close()

	// Force currentLSN to a lower value to test update during replay
	newManager.currentLSN.Store(0)

	// Replay WAL entries from LSN 0
	lastLSN, err := newManager.Replay(0, func(entry WALEntry) error {
		t.Logf("Replayed entry: LSN=%d, Table=%s, RowID=%d, TxnID=%d",
			entry.LSN, entry.TableName, entry.RowID, entry.TxnID)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify the replay returned the correct lastLSN
	if lastLSN != expectedLSN {
		t.Fatalf("Replay returned incorrect lastLSN: got %d, expected %d", lastLSN, expectedLSN)
	}

	// Verify currentLSN was updated to match lastLSN
	currentLSN := newManager.currentLSN.Load()
	if currentLSN != lastLSN {
		t.Fatalf("currentLSN not updated to match lastLSN: currentLSN=%d, lastLSN=%d",
			currentLSN, lastLSN)
	}

	t.Logf("Success: currentLSN correctly updated to %d during replay", currentLSN)
}

// TestSnapshotTimestampParsing verifies that parsing timestamps with correct timezone
// handling makes a difference when running in a non-UTC timezone
func TestSnapshotTimestampParsing(t *testing.T) {
	// Get the current local time
	now := time.Now()

	// Format the time as it would be in a snapshot filename
	formatted := now.Format("20060102-150405.000")

	// Parse using time.Parse (UTC timezone)
	utcTime, err := time.Parse("20060102-150405.000", formatted)
	if err != nil {
		t.Fatalf("Failed to parse timestamp with UTC: %v", err)
	}

	// Parse using time.ParseInLocation with local timezone
	localTime, err := time.ParseInLocation("20060102-150405.000", formatted, time.Local)
	if err != nil {
		t.Fatalf("Failed to parse timestamp with local timezone: %v", err)
	}

	// Convert both to nanoseconds for comparison
	utcNanos := utcTime.UnixNano()
	localNanos := localTime.UnixNano()
	nanosDiff := localNanos - utcNanos

	// Log the timezone information
	_, utcOffset := now.Zone()
	utcOffsetHours := float64(utcOffset) / 3600.0

	t.Logf("Local timezone offset: %.1f hours", utcOffsetHours)
	t.Logf("Original time: %v", now.Format(time.RFC3339))
	t.Logf("UTC parsed:    %v", utcTime.Format(time.RFC3339))
	t.Logf("Local parsed:  %v", localTime.Format(time.RFC3339))
	t.Logf("Nanosecond difference: %d", nanosDiff)

	// Check if we're in a non-UTC timezone by looking at the offset
	_, offset := now.Zone()
	if offset != 0 {
		// In a non-UTC timezone, there should be a difference
		if nanosDiff == 0 {
			t.Errorf("Expected timezone difference but got none")
		} else {
			t.Logf("Test confirms timezone difference: %d nanoseconds", nanosDiff)
			t.Logf("This is correctly handled by using time.ParseInLocation instead of time.Parse")
		}

		// The local-parsed time should match the original time's hour
		if localTime.Hour() != now.Hour() {
			t.Errorf("Local parsed time has wrong hour: got %d, expected %d",
				localTime.Hour(), now.Hour())
		}
	} else {
		t.Logf("Running in UTC timezone (offset=0), test is less meaningful but should still pass")
		// In UTC, the parsed times should be identical
		if nanosDiff != 0 {
			t.Errorf("In UTC timezone, expected no difference but got %d nanoseconds", nanosDiff)
		}
	}

	// Simulate a WAL entry timestamp
	walTimestamp := time.Now().UnixNano()

	// Convert the local-parsed time to nanoseconds for comparison with WAL timestamp
	snapshotTimestampNano := localTime.UnixNano()

	// This comparison should work correctly now with local timezone parsing
	if walTimestamp >= snapshotTimestampNano {
		t.Logf("WAL timestamp (%d) >= snapshot timestamp (%d) - entry would be processed",
			walTimestamp, snapshotTimestampNano)
	} else {
		t.Logf("WAL timestamp (%d) < snapshot timestamp (%d) - entry would be skipped",
			walTimestamp, snapshotTimestampNano)
	}
}

// TestWALSyncModes tests the different WAL sync modes to ensure they work as expected
func TestWALSyncModes(t *testing.T) {
	// Test sync modes (skipping SyncNone as it may not flush in time for tests)
	syncModes := []struct {
		name string
		mode SyncMode
	}{
		// SyncNone skipped as it's not reliable in tests
		{"Normal", SyncNormal},
		{"Full", SyncFull},
	}

	for _, sm := range syncModes {
		t.Run(sm.name, func(t *testing.T) {
			// Create a temporary directory for the test
			testDir := t.TempDir()

			// Create test configuration with the current sync mode
			config := storage.PersistenceConfig{
				Enabled:  true,
				SyncMode: int(sm.mode),
			}

			// Create WAL manager with the specified sync mode
			walManager, err := NewWALManager(testDir, sm.mode, &config)
			if err != nil {
				t.Fatalf("Failed to create WAL manager: %v", err)
			}

			// Add 10 test entries
			for i := 0; i < 10; i++ {
				entry := createTestEntry(int64(i), "test_table", int64(i), WALInsert)
				_, err := walManager.AppendEntry(entry)
				if err != nil {
					t.Fatalf("Failed to append entry: %v", err)
				}
			}

			// Force flush to disk
			walManager.Flush()
			walManager.Close()

			// Reopen the WAL and check if entries were persisted
			newManager, err := NewWALManager(testDir, sm.mode, &config)
			if err != nil {
				t.Fatalf("Failed to reopen WAL manager: %v", err)
			}
			defer newManager.Close()

			// Count entries during replay
			entryCount := 0
			lastLSN, err := newManager.Replay(0, func(entry WALEntry) error {
				t.Logf("Replayed entry: LSN=%d, Table=%s, RowID=%d, TxnID=%d",
					entry.LSN, entry.TableName, entry.RowID, entry.TxnID)
				entryCount++
				return nil
			})
			if err != nil {
				t.Fatalf("Failed to replay WAL: %v", err)
			}

			// All 10 entries should be replayed regardless of sync mode
			if entryCount != 10 {
				t.Errorf("Expected 10 replayed entries, got %d", entryCount)
			}

			// Last LSN should be 10
			if lastLSN != 10 {
				t.Errorf("Expected last LSN to be 10, got %d", lastLSN)
			}

			t.Logf("Sync mode %s: correctly persisted and replayed %d entries with last LSN %d",
				sm.name, entryCount, lastLSN)
		})
	}
}

// TestWALTransactionTypes tests the recording and replay of different WAL operation types
func TestWALTransactionTypes(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager
	walManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Create a transaction with multiple operations
	txnID := int64(1000)

	// Record an insert
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     txnID,
		TableName: "test_table",
		RowID:     1,
		Operation: WALInsert,
		Data:      []byte("insert data"),
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append insert: %v", err)
	}

	// Record an update
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     txnID,
		TableName: "test_table",
		RowID:     2,
		Operation: WALUpdate,
		Data:      []byte("update data"),
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append update: %v", err)
	}

	// Record a delete
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     txnID,
		TableName: "test_table",
		RowID:     3,
		Operation: WALDelete,
		Data:      nil, // No data for deletes
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append delete: %v", err)
	}

	// Record a commit
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     txnID,
		TableName: "",
		RowID:     0,
		Operation: WALCommit,
		Data:      nil,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append commit: %v", err)
	}

	// Record a rollback for another transaction
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     txnID + 1, // Different transaction
		TableName: "",
		RowID:     0,
		Operation: WALRollback,
		Data:      nil,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append rollback: %v", err)
	}

	// Force flush to disk
	walManager.Flush()
	walManager.Close()

	// Reopen the WAL and check if all transaction types were persisted
	newManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to reopen WAL manager: %v", err)
	}
	defer newManager.Close()

	// Track operation counts during replay
	opCounts := map[WALOperationType]int{}
	txnCounts := map[int64]int{}

	_, err = newManager.Replay(0, func(entry WALEntry) error {
		opCounts[entry.Operation]++
		txnCounts[entry.TxnID]++

		// Log the replayed entries
		t.Logf("Replayed: LSN=%d Op=%d TxnID=%d Table=%s RowID=%d",
			entry.LSN, entry.Operation, entry.TxnID, entry.TableName, entry.RowID)

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all operation types were correctly replayed
	if opCounts[WALInsert] != 1 {
		t.Errorf("Expected 1 insert, got %d", opCounts[WALInsert])
	}
	if opCounts[WALUpdate] != 1 {
		t.Errorf("Expected 1 update, got %d", opCounts[WALUpdate])
	}
	if opCounts[WALDelete] != 1 {
		t.Errorf("Expected 1 delete, got %d", opCounts[WALDelete])
	}
	if opCounts[WALCommit] != 1 {
		t.Errorf("Expected 1 commit, got %d", opCounts[WALCommit])
	}
	if opCounts[WALRollback] != 1 {
		t.Errorf("Expected 1 rollback, got %d", opCounts[WALRollback])
	}

	// Verify all transactions were correctly replayed
	if txnCounts[txnID] != 4 { // Insert, update, delete, commit
		t.Errorf("Expected 4 operations for txnID %d, got %d", txnID, txnCounts[txnID])
	}
	if txnCounts[txnID+1] != 1 { // Just the rollback
		t.Errorf("Expected 1 operation for txnID %d, got %d", txnID+1, txnCounts[txnID+1])
	}
}

// TestWALCheckpointRecovery tests that checkpoint recovery works correctly with WAL truncation
func TestWALCheckpointRecovery(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager
	walManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Add 5 entries before creating a checkpoint
	for i := 0; i < 5; i++ {
		entry := createTestEntry(int64(i), "test_table", int64(i), WALInsert)
		_, err := walManager.AppendEntry(entry)
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	// Force flush to disk
	walManager.Flush()

	// Close the WAL manager to finalize this set of entries
	walManager.Close()

	// Open a new WAL manager to continue
	walManager, err = NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Create a checkpoint based on the current LSN
	checkpointLSN := walManager.currentLSN.Load()
	t.Logf("Creating checkpoint at LSN: %d", checkpointLSN)
	walManager.createConsistentCheckpoint(checkpointLSN, true)

	// Explicitly truncate the WAL to simulate the effect of truncation after a checkpoint
	err = walManager.TruncateWAL(checkpointLSN)
	if err != nil {
		t.Fatalf("Failed to truncate WAL: %v", err)
	}

	// Add 5 more entries after the checkpoint
	for i := 5; i < 10; i++ {
		entry := createTestEntry(int64(i), "test_table", int64(i), WALInsert)
		_, err := walManager.AppendEntry(entry)
		if err != nil {
			t.Fatalf("Failed to append entry after checkpoint: %v", err)
		}
	}

	// Force flush again
	walManager.Flush()

	// Close the WAL manager
	walManager.Close()

	// Read checkpoint metadata for information
	checkpointFile := filepath.Join(testDir, "checkpoint.meta")
	meta, err := ReadCheckpointMeta(checkpointFile)
	if err != nil {
		t.Fatalf("Failed to read checkpoint metadata: %v", err)
	}

	t.Logf("Checkpoint info: LSN=%d, WAL=%s", meta.LSN, meta.WALFile)

	// List WAL files to verify truncation occurred
	// Since the WAL files might be directly in the testDir instead of a wal subdirectory,
	// we'll check both locations
	walFiles := []string{}

	// Check if the wal directory exists before trying to read it
	walDir := filepath.Join(testDir, "wal")
	if info, err := os.Stat(walDir); err == nil && info.IsDir() {
		files, err := os.ReadDir(walDir)
		if err == nil {
			for _, file := range files {
				if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
					t.Logf("Found WAL file in wal dir: %s", file.Name())
					walFiles = append(walFiles, filepath.Join(walDir, file.Name()))
				}
			}
		}
	}

	// Also check main directory
	files, err := os.ReadDir(testDir)
	if err == nil {
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
				t.Logf("Found WAL file in main dir: %s", file.Name())
				walFiles = append(walFiles, filepath.Join(testDir, file.Name()))
			}
		}
	}

	if len(walFiles) == 0 {
		t.Logf("No WAL files found in any directory - this is unexpected")
	}

	// Create a manager for final testing
	testManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create test WAL manager: %v", err)
	}
	defer testManager.Close()

	// With truncation, we expect:
	// 1. Replay from 0 to only see entries after the checkpoint (because earlier entries are truncated)
	// 2. Replay from checkpoint to also only see entries after the checkpoint
	// So both should return the same entries and LSNs

	// Test replay from LSN 0
	entriesFromZero := 0
	lastLSNFromZero, err := testManager.Replay(0, func(entry WALEntry) error {
		entriesFromZero++
		t.Logf("Replayed from zero: LSN=%d Op=%d Table=%s RowID=%d",
			entry.LSN, entry.Operation, entry.TableName, entry.RowID)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL from 0: %v", err)
	}

	// Test replay from checkpoint LSN
	entriesFromCheckpoint := 0
	lastLSNFromCheckpoint, err := testManager.Replay(checkpointLSN, func(entry WALEntry) error {
		entriesFromCheckpoint++
		t.Logf("Replayed from checkpoint: LSN=%d Op=%d Table=%s RowID=%d",
			entry.LSN, entry.Operation, entry.TableName, entry.RowID)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL from checkpoint: %v", err)
	}

	// With WAL truncation, both replays should see the same entries and return the same last LSN
	t.Logf("From zero: replayed %d entries, final LSN=%d", entriesFromZero, lastLSNFromZero)
	t.Logf("From checkpoint: replayed %d entries, final LSN=%d", entriesFromCheckpoint, lastLSNFromCheckpoint)

	// The test should pass if both replays return the same results as expected with truncation
	if lastLSNFromZero != lastLSNFromCheckpoint {
		t.Errorf("Last LSN mismatch: from zero=%d, from checkpoint=%d",
			lastLSNFromZero, lastLSNFromCheckpoint)
	} else {
		t.Logf("Last LSN matched correctly: %d", lastLSNFromZero)
	}

	if entriesFromZero != entriesFromCheckpoint {
		t.Errorf("Entry count mismatch: from zero=%d, from checkpoint=%d",
			entriesFromZero, entriesFromCheckpoint)
	} else {
		t.Logf("Entry count matched correctly: %d", entriesFromZero)
	}
}

// TestDataTypePersistence tests persisting and loading various data types
// through the WAL system
func TestDataTypePersistence(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager
	walManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Create a sample WAL entry with binary data for each data type
	// The binary data contains metadata at the beginning
	// followed by values of different types in a simple format

	entryData := new(bytes.Buffer)

	// First, write metadata markers
	// These are arbitrary markers to identify the test data format
	entryData.Write([]byte{0x01, 0x02, 0x03, 0x04}) // Magic number
	entryData.Write([]byte{0x01})                   // Version

	// Write integer
	intValue := int64(42)
	binary.Write(entryData, binary.LittleEndian, int8(1)) // Type marker for int
	binary.Write(entryData, binary.LittleEndian, intValue)

	// Write float
	floatValue := 3.14159
	binary.Write(entryData, binary.LittleEndian, int8(2)) // Type marker for float
	binary.Write(entryData, binary.LittleEndian, floatValue)

	// Write string
	strValue := "hello world"
	binary.Write(entryData, binary.LittleEndian, int8(3)) // Type marker for string
	binary.Write(entryData, binary.LittleEndian, int32(len(strValue)))
	entryData.WriteString(strValue)

	// Write boolean
	boolValue := true
	binary.Write(entryData, binary.LittleEndian, int8(4)) // Type marker for bool
	if boolValue {
		entryData.Write([]byte{1})
	} else {
		entryData.Write([]byte{0})
	}

	// Write timestamp
	timeValue := time.Now().Round(time.Microsecond)       // Round to avoid precision issues
	binary.Write(entryData, binary.LittleEndian, int8(5)) // Type marker for timestamp
	binary.Write(entryData, binary.LittleEndian, timeValue.UnixNano())

	// Write date (using standard date format, stored as string)
	dateValue := "2025-04-25"
	binary.Write(entryData, binary.LittleEndian, int8(6)) // Type marker for date
	binary.Write(entryData, binary.LittleEndian, int32(len(dateValue)))
	entryData.WriteString(dateValue)

	// Write time (using standard time format, stored as string)
	timeOfDayValue := "14:30:45"
	binary.Write(entryData, binary.LittleEndian, int8(7)) // Type marker for time of day
	binary.Write(entryData, binary.LittleEndian, int32(len(timeOfDayValue)))
	entryData.WriteString(timeOfDayValue)

	// Write JSON (as a string representation)
	jsonValue := `{"name":"test", "values":[1,2,3], "nested":{"key":"value"}}`
	binary.Write(entryData, binary.LittleEndian, int8(8)) // Type marker for JSON
	binary.Write(entryData, binary.LittleEndian, int32(len(jsonValue)))
	entryData.WriteString(jsonValue)

	// Create WAL entry
	entry := WALEntry{
		TxnID:     1,
		TableName: "test_data_types",
		RowID:     1,
		Operation: WALInsert,
		Data:      entryData.Bytes(),
		Timestamp: time.Now().UnixNano(),
	}

	// Append to WAL
	_, err = walManager.AppendEntry(entry)
	if err != nil {
		t.Fatalf("Failed to append entry with data types: %v", err)
	}

	// Add commit entry
	_, err = walManager.AppendEntry(WALEntry{
		TxnID:     1,
		Operation: WALCommit,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("Failed to append commit: %v", err)
	}

	// Force flush to disk
	walManager.Flush()
	walManager.Close()

	// Create a new WAL manager to test replay
	newManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create new WAL manager: %v", err)
	}
	defer newManager.Close()

	// Replay WAL to verify data
	_, err = newManager.Replay(0, func(replayedEntry WALEntry) error {
		t.Logf("Replayed entry: LSN=%d Op=%d Table=%s RowID=%d",
			replayedEntry.LSN, replayedEntry.Operation, replayedEntry.TableName, replayedEntry.RowID)

		// For insert operations, verify the data
		if replayedEntry.Operation == WALInsert && replayedEntry.RowID == 1 {
			data := replayedEntry.Data

			// Verify magic number
			if len(data) < 5 || !bytes.Equal(data[0:4], []byte{0x01, 0x02, 0x03, 0x04}) {
				t.Errorf("Invalid magic number in replayed data")
				return nil
			}

			// Verify version
			if data[4] != 0x01 {
				t.Errorf("Invalid version in replayed data")
				return nil
			}

			// Read and verify the data
			buf := bytes.NewReader(data[5:])

			// Read integer
			var typeMarker int8
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 1 {
				t.Errorf("Invalid type marker for integer: %d", typeMarker)
			} else {
				var readInt int64
				binary.Read(buf, binary.LittleEndian, &readInt)
				if readInt != intValue {
					t.Errorf("Integer value mismatch: got %d, expected %d", readInt, intValue)
				} else {
					t.Logf("Integer value verified: %d", readInt)
				}
			}

			// Read float
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 2 {
				t.Errorf("Invalid type marker for float: %d", typeMarker)
			} else {
				var readFloat float64
				binary.Read(buf, binary.LittleEndian, &readFloat)
				if math.Abs(readFloat-floatValue) > 0.000001 {
					t.Errorf("Float value mismatch: got %f, expected %f", readFloat, floatValue)
				} else {
					t.Logf("Float value verified: %f", readFloat)
				}
			}

			// Read string
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 3 {
				t.Errorf("Invalid type marker for string: %d", typeMarker)
			} else {
				var strLen int32
				binary.Read(buf, binary.LittleEndian, &strLen)
				strBytes := make([]byte, strLen)
				buf.Read(strBytes)
				readStr := string(strBytes)
				if readStr != strValue {
					t.Errorf("String value mismatch: got %s, expected %s", readStr, strValue)
				} else {
					t.Logf("String value verified: %s", readStr)
				}
			}

			// Read boolean
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 4 {
				t.Errorf("Invalid type marker for boolean: %d", typeMarker)
			} else {
				boolByte := make([]byte, 1)
				buf.Read(boolByte)
				readBool := boolByte[0] != 0
				if readBool != boolValue {
					t.Errorf("Boolean value mismatch: got %v, expected %v", readBool, boolValue)
				} else {
					t.Logf("Boolean value verified: %v", readBool)
				}
			}

			// Read timestamp
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 5 {
				t.Errorf("Invalid type marker for timestamp: %d", typeMarker)
			} else {
				var nanos int64
				binary.Read(buf, binary.LittleEndian, &nanos)
				readTime := time.Unix(0, nanos)
				if readTime.UnixNano() != timeValue.UnixNano() {
					t.Errorf("Timestamp value mismatch: got %v, expected %v",
						readTime, timeValue)
				} else {
					t.Logf("Timestamp value verified: %v", readTime)
				}
			}

			// Read date
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 6 {
				t.Errorf("Invalid type marker for date: %d", typeMarker)
			} else {
				var strLen int32
				binary.Read(buf, binary.LittleEndian, &strLen)
				strBytes := make([]byte, strLen)
				buf.Read(strBytes)
				readDate := string(strBytes)
				if readDate != dateValue {
					t.Errorf("Date value mismatch: got %s, expected %s", readDate, dateValue)
				} else {
					t.Logf("Date value verified: %s", readDate)
				}
			}

			// Read time of day
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 7 {
				t.Errorf("Invalid type marker for time of day: %d", typeMarker)
			} else {
				var strLen int32
				binary.Read(buf, binary.LittleEndian, &strLen)
				strBytes := make([]byte, strLen)
				buf.Read(strBytes)
				readTimeOfDay := string(strBytes)
				if readTimeOfDay != timeOfDayValue {
					t.Errorf("Time of day value mismatch: got %s, expected %s", readTimeOfDay, timeOfDayValue)
				} else {
					t.Logf("Time of day value verified: %s", readTimeOfDay)
				}
			}

			// Read JSON
			binary.Read(buf, binary.LittleEndian, &typeMarker)
			if typeMarker != 8 {
				t.Errorf("Invalid type marker for JSON: %d", typeMarker)
			} else {
				var strLen int32
				binary.Read(buf, binary.LittleEndian, &strLen)
				strBytes := make([]byte, strLen)
				buf.Read(strBytes)
				readJSON := string(strBytes)
				if readJSON != jsonValue {
					t.Errorf("JSON value mismatch: got %s, expected %s", readJSON, jsonValue)
				} else {
					t.Logf("JSON value verified: %s", readJSON)
				}
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}
}

// TestWALDDLOperations tests that DDL operations are correctly recorded and replayed in the WAL
func TestWALDDLOperations(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager
	walManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Create a sample schema for testing
	sampleSchema := createTestSchema("test_table", 3)
	schemaData, err := serializeSchemaForTest(sampleSchema)
	if err != nil {
		t.Fatalf("Failed to serialize schema: %v", err)
	}

	// Record a CREATE TABLE operation
	createTableEntry := WALEntry{
		TxnID:     -999, // System transaction ID for DDL
		TableName: "test_table",
		RowID:     0, // Not applicable for DDL
		Operation: WALCreateTable,
		Data:      schemaData,
		Timestamp: time.Now().UnixNano(),
	}
	createLSN, err := walManager.AppendEntry(createTableEntry)
	if err != nil {
		t.Fatalf("Failed to append CREATE TABLE entry: %v", err)
	}
	t.Logf("Recorded CREATE TABLE with LSN %d", createLSN)

	// Record a few INSERT operations for the table
	for i := 1; i <= 3; i++ {
		insertEntry := WALEntry{
			TxnID:     1,
			TableName: "test_table",
			RowID:     int64(i),
			Operation: WALInsert,
			Data:      []byte(fmt.Sprintf("row %d data", i)),
			Timestamp: time.Now().UnixNano(),
		}
		lsn, err := walManager.AppendEntry(insertEntry)
		if err != nil {
			t.Fatalf("Failed to append INSERT entry: %v", err)
		}
		t.Logf("Recorded INSERT with LSN %d", lsn)
	}

	// Record an ALTER TABLE operation (adding a column)
	alteredSchema := createTestSchema("test_table", 4) // Schema with one extra column
	alteredSchemaData, err := serializeSchemaForTest(alteredSchema)
	if err != nil {
		t.Fatalf("Failed to serialize altered schema: %v", err)
	}

	alterTableEntry := WALEntry{
		TxnID:     -999, // System transaction ID for DDL
		TableName: "test_table",
		RowID:     0, // Not applicable for DDL
		Operation: WALAlterTable,
		Data:      alteredSchemaData,
		Timestamp: time.Now().UnixNano(),
	}
	alterLSN, err := walManager.AppendEntry(alterTableEntry)
	if err != nil {
		t.Fatalf("Failed to append ALTER TABLE entry: %v", err)
	}
	t.Logf("Recorded ALTER TABLE with LSN %d", alterLSN)

	// Record more INSERT operations after the ALTER
	for i := 4; i <= 6; i++ {
		insertEntry := WALEntry{
			TxnID:     2,
			TableName: "test_table",
			RowID:     int64(i),
			Operation: WALInsert,
			Data:      []byte(fmt.Sprintf("row %d data", i)),
			Timestamp: time.Now().UnixNano(),
		}
		lsn, err := walManager.AppendEntry(insertEntry)
		if err != nil {
			t.Fatalf("Failed to append INSERT entry after ALTER: %v", err)
		}
		t.Logf("Recorded post-ALTER INSERT with LSN %d", lsn)
	}

	// Record a DROP TABLE operation
	dropTableEntry := WALEntry{
		TxnID:     -999, // System transaction ID for DDL
		TableName: "test_table",
		RowID:     0, // Not applicable for DDL
		Operation: WALDropTable,
		Data:      nil, // No data needed for drop
		Timestamp: time.Now().UnixNano(),
	}
	dropLSN, err := walManager.AppendEntry(dropTableEntry)
	if err != nil {
		t.Fatalf("Failed to append DROP TABLE entry: %v", err)
	}
	t.Logf("Recorded DROP TABLE with LSN %d", dropLSN)

	// Force flush to disk
	walManager.Flush()
	walManager.Close()

	// Open a new WAL manager to test replay
	newManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create new WAL manager: %v", err)
	}
	defer newManager.Close()

	// Track operations during replay
	ddlOps := map[WALOperationType]int{}
	dmlOps := map[WALOperationType]int{}
	createFound := false
	alterFound := false
	dropFound := false

	// Replay WAL entries to verify operations
	_, err = newManager.Replay(0, func(entry WALEntry) error {
		// Log the entry
		t.Logf("Replayed: LSN=%d Op=%d TxnID=%d Table=%s",
			entry.LSN, entry.Operation, entry.TxnID, entry.TableName)

		// Categorize operation
		if entry.Operation >= WALCreateTable && entry.Operation <= WALAlterTable {
			ddlOps[entry.Operation]++

			// Check specific DDL operations
			if entry.Operation == WALCreateTable {
				createFound = true
				// Try to deserialize the schema
				schema, err := deserializeSchemaForTest(entry.Data)
				if err != nil {
					t.Errorf("Failed to deserialize CREATE TABLE schema: %v", err)
				} else {
					// Check schema attributes
					if schema.TableName != "test_table" {
						t.Errorf("Wrong table name in CREATE schema: got %s, expected test_table",
							schema.TableName)
					}
					if len(schema.Columns) != 3 {
						t.Errorf("Wrong column count in CREATE schema: got %d, expected 3",
							len(schema.Columns))
					}
					t.Logf("CREATE TABLE schema verified with %d columns", len(schema.Columns))
				}
			} else if entry.Operation == WALAlterTable {
				alterFound = true
				// Try to deserialize the schema
				schema, err := deserializeSchemaForTest(entry.Data)
				if err != nil {
					t.Errorf("Failed to deserialize ALTER TABLE schema: %v", err)
				} else {
					// Check schema attributes (should have 4 columns now)
					if len(schema.Columns) != 4 {
						t.Errorf("Wrong column count in ALTER schema: got %d, expected 4",
							len(schema.Columns))
					}
					t.Logf("ALTER TABLE schema verified with %d columns", len(schema.Columns))
				}
			} else if entry.Operation == WALDropTable {
				dropFound = true
				if entry.TableName != "test_table" {
					t.Errorf("Wrong table name in DROP operation: got %s, expected test_table",
						entry.TableName)
				}
				t.Logf("DROP TABLE operation verified for table %s", entry.TableName)
			}
		} else {
			dmlOps[entry.Operation]++
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all operations were replayed correctly
	t.Logf("DDL operations: %v", ddlOps)
	t.Logf("DML operations: %v", dmlOps)

	// Check we found all the DDL operations
	if !createFound {
		t.Errorf("CREATE TABLE operation not found in replay")
	}
	if !alterFound {
		t.Errorf("ALTER TABLE operation not found in replay")
	}
	if !dropFound {
		t.Errorf("DROP TABLE operation not found in replay")
	}

	// Check DDL operation counts
	if ddlOps[WALCreateTable] != 1 {
		t.Errorf("Expected 1 CREATE TABLE operation, got %d", ddlOps[WALCreateTable])
	}
	if ddlOps[WALAlterTable] != 1 {
		t.Errorf("Expected 1 ALTER TABLE operation, got %d", ddlOps[WALAlterTable])
	}
	if ddlOps[WALDropTable] != 1 {
		t.Errorf("Expected 1 DROP TABLE operation, got %d", ddlOps[WALDropTable])
	}

	// Check DML operation counts (6 inserts in total)
	if dmlOps[WALInsert] != 6 {
		t.Errorf("Expected 6 INSERT operations, got %d", dmlOps[WALInsert])
	}
}

// Helper function to create a test schema with the specified number of columns
func createTestSchema(tableName string, columnCount int) *storage.Schema {
	schema := &storage.Schema{
		TableName: tableName,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Create columns
	schema.Columns = make([]storage.SchemaColumn, columnCount)
	for i := 0; i < columnCount; i++ {
		schema.Columns[i] = storage.SchemaColumn{
			ID:         i,
			Name:       fmt.Sprintf("col%d", i+1),
			Type:       storage.TypeInteger, // Default to integer for simplicity
			Nullable:   i > 0,               // First column non-nullable, rest nullable
			PrimaryKey: i == 0,              // First column is primary key
		}
	}

	return schema
}

// Simple schema serialization for testing (mimics the snapshotter.serializeSchema)
func serializeSchemaForTest(schema *storage.Schema) ([]byte, error) {
	// Create a buffer
	buffer := make([]byte, 0, 1024)

	// Write schema format version (uint32)
	pos := len(buffer)
	buffer = append(buffer, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(buffer[pos:], 1) // Schema version

	// Write table name length
	nameLen := len(schema.TableName)
	buffer = append(buffer, byte(nameLen>>8), byte(nameLen))

	// Write table name
	buffer = append(buffer, schema.TableName...)

	// Write number of columns (uint16)
	buffer = append(buffer, byte(len(schema.Columns)>>8), byte(len(schema.Columns)))

	// Write each column
	for _, col := range schema.Columns {
		// Write column ID (uint16)
		buffer = append(buffer, byte(col.ID>>8), byte(col.ID))

		// Write column name length
		nameLen = len(col.Name)

		// Write column name length (uint16)
		buffer = append(buffer, byte(nameLen>>8), byte(nameLen))

		// Write column name
		buffer = append(buffer, col.Name...)

		// Write column type
		buffer = append(buffer, byte(col.Type))

		// Write flags (nullable, primary key)
		var flags byte
		if col.Nullable {
			flags |= 1
		}
		if col.PrimaryKey {
			flags |= 2
		}
		buffer = append(buffer, flags)
	}

	// Write timestamps
	// CreatedAt
	pos = len(buffer)
	buffer = append(buffer, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(schema.CreatedAt.UnixNano()))

	// UpdatedAt
	pos = len(buffer)
	buffer = append(buffer, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(buffer[pos:], uint64(schema.UpdatedAt.UnixNano()))

	return buffer, nil
}

// Simple schema deserialization for testing (mimics the snapshotter.deserializeSchema)
func deserializeSchemaForTest(data []byte) (*storage.Schema, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid schema data: too short")
	}

	// Create a new schema object
	schema := &storage.Schema{}
	pos := 0

	// Read schema format version
	version := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if version != 1 {
		return nil, fmt.Errorf("unsupported schema format version: %d", version)
	}

	// Read table name length
	if pos+2 > len(data) {
		return nil, fmt.Errorf("unexpected end of data while reading table name length")
	}
	nameLen := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
	pos += 2

	// Read table name
	if pos+nameLen > len(data) {
		return nil, fmt.Errorf("unexpected end of data while reading table name")
	}
	schema.TableName = string(data[pos : pos+nameLen])
	pos += nameLen

	// Read number of columns
	if pos+2 > len(data) {
		return nil, fmt.Errorf("unexpected end of data while reading column count")
	}
	colCount := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
	pos += 2

	// Allocate columns slice
	schema.Columns = make([]storage.SchemaColumn, colCount)

	// Read each column
	for i := 0; i < colCount; i++ {
		// Read column ID
		if pos+2 > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column ID")
		}
		colID := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
		pos += 2

		// Read column name length
		if pos+2 > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column name length")
		}
		nameLen := int(uint16(data[pos])<<8 | uint16(data[pos+1]))
		pos += 2

		// Read column name
		if pos+nameLen > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column name")
		}
		colName := string(data[pos : pos+nameLen])
		pos += nameLen

		// Read column type
		if pos+1 > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column type")
		}
		colType := storage.DataType(data[pos])
		pos++

		// Read flags
		if pos+1 > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column flags")
		}
		flags := data[pos]
		pos++

		// Parse flags
		nullable := (flags & 1) != 0
		primaryKey := (flags & 2) != 0

		// Create column
		schema.Columns[i] = storage.SchemaColumn{
			ID:         colID,
			Name:       colName,
			Type:       colType,
			Nullable:   nullable,
			PrimaryKey: primaryKey,
		}
	}

	// Read timestamps if there's enough data
	if pos+16 <= len(data) {
		// Read CreatedAt timestamp
		createdAt := binary.LittleEndian.Uint64(data[pos:])
		schema.CreatedAt = time.Unix(0, int64(createdAt))
		pos += 8

		// Read UpdatedAt timestamp
		updatedAt := binary.LittleEndian.Uint64(data[pos:])
		schema.UpdatedAt = time.Unix(0, int64(updatedAt))
		// pos += 8 // Not needed as pos is not used after this
	} else {
		// Use current time if timestamps aren't available
		now := time.Now()
		schema.CreatedAt = now
		schema.UpdatedAt = now
	}

	return schema, nil
}

// TestWALEmptyFileHandling tests the handling of empty WAL files
func TestWALEmptyFileHandling(t *testing.T) {
	// Create a temporary directory for the test
	testDir := t.TempDir()

	// Create test configuration
	config := storage.PersistenceConfig{
		Enabled:  true,
		SyncMode: int(SyncNormal),
	}

	// Create WAL manager
	walManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Initial WAL file path
	initialWALPath := testDir + "/" + walManager.currentWALFile

	// Close without writing any entries (should create empty WAL file)
	walManager.Close()

	// Verify empty WAL file is deleted on close
	_, err = os.Stat(initialWALPath)
	if err == nil {
		t.Errorf("Empty WAL file was not deleted on close: %s", initialWALPath)
	} else if !os.IsNotExist(err) {
		t.Errorf("Unexpected error checking WAL file: %v", err)
	} else {
		t.Logf("Empty WAL file was correctly deleted on close: %s", initialWALPath)
	}

	// Create a new WAL manager
	walManager, err = NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Add a single entry
	_, err = walManager.AppendEntry(createTestEntry(1, "test_table", 1, WALInsert))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// Force flush to disk
	walManager.Flush()

	// New WAL file path
	newWALPath := testDir + "/" + walManager.currentWALFile

	// Close the WAL manager
	walManager.Close()

	// Verify non-empty WAL file is not deleted
	_, err = os.Stat(newWALPath)
	if err != nil {
		t.Errorf("Non-empty WAL file was deleted or not found: %s, error: %v", newWALPath, err)
	} else {
		t.Logf("Non-empty WAL file was correctly preserved: %s", newWALPath)
	}
}
