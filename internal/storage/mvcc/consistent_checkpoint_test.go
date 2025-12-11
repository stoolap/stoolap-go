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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/common"
	"github.com/stoolap/stoolap-go/internal/storage"
)

func TestConsistentCheckpoint(t *testing.T) {
	// Create a temporary directory for testing
	testDir := common.TempDir(t)

	// Create a WAL manager with basic configuration
	config := storage.PersistenceConfig{
		Enabled:          true,
		SyncMode:         int(SyncNormal),
		WALFlushTrigger:  1024,
		WALBufferSize:    4096,
		WALMaxSize:       1024 * 1024,
		SnapshotInterval: 300,
		KeepSnapshots:    3,
	}

	manager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// 1. TEST CHECKPOINT WITH NO ACTIVE TRANSACTIONS
	// Record some operations
	entry1 := WALEntry{
		TxnID:     1001,
		TableName: "test_table",
		RowID:     1,
		Operation: WALInsert,
		Data:      []byte("test data 1"),
		Timestamp: time.Now().UnixNano(),
	}
	entry2 := WALEntry{
		TxnID:     1002,
		TableName: "test_table",
		RowID:     2,
		Operation: WALInsert,
		Data:      []byte("test data 2"),
		Timestamp: time.Now().UnixNano(),
	}

	_, err = manager.AppendEntry(entry1)
	if err != nil {
		t.Fatalf("Failed to append entry 1: %v", err)
	}
	lastLSN, err := manager.AppendEntry(entry2)
	if err != nil {
		t.Fatalf("Failed to append entry 2: %v", err)
	}

	// Create a consistent checkpoint with no active transactions
	manager.createConsistentCheckpoint(lastLSN, true)

	// Verify checkpoint file exists
	checkpointPath := filepath.Join(testDir, "checkpoint.meta")
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("Checkpoint file not created")
	}

	// Read and verify checkpoint metadata
	checkpoint, err := ReadCheckpointMeta(checkpointPath)
	if err != nil {
		t.Fatalf("Failed to read checkpoint metadata: %v", err)
	}

	// Check that fields are properly set
	if checkpoint.LSN != lastLSN {
		t.Errorf("Checkpoint LSN mismatch: got %d, expected %d", checkpoint.LSN, lastLSN)
	}
	if !checkpoint.IsConsistent {
		t.Errorf("Expected checkpoint to be marked as consistent")
	}
	if len(checkpoint.ActiveTransactions) != 0 {
		t.Errorf("Expected no active transactions, got %d", len(checkpoint.ActiveTransactions))
	}

	// 2. TEST CHECKPOINT WITH ACTIVE TRANSACTIONS
	// Simulate active transactions
	manager.UpdateActiveTransactions(1003, true)
	manager.UpdateActiveTransactions(1004, true)

	// Record more operations
	entry3 := WALEntry{
		TxnID:     1003,
		TableName: "test_table",
		RowID:     3,
		Operation: WALInsert,
		Data:      []byte("test data 3"),
		Timestamp: time.Now().UnixNano(),
	}
	lastLSN, err = manager.AppendEntry(entry3)
	if err != nil {
		t.Fatalf("Failed to append entry 3: %v", err)
	}

	// Create a checkpoint with active transactions
	manager.createConsistentCheckpoint(lastLSN, true)

	// Read and verify checkpoint metadata
	checkpoint, err = ReadCheckpointMeta(checkpointPath)
	if err != nil {
		t.Fatalf("Failed to read checkpoint metadata: %v", err)
	}

	// With active transactions, the checkpoint should NOT be marked as consistent
	if checkpoint.IsConsistent {
		t.Errorf("Expected checkpoint to be marked as inconsistent due to active transactions")
	}

	// Verify active transactions are recorded
	if len(checkpoint.ActiveTransactions) != 2 {
		t.Errorf("Expected 2 active transactions, got %d", len(checkpoint.ActiveTransactions))
	}

	// 3. TEST CHECKPOINT AFTER TRANSACTIONS COMPLETE
	// Complete transactions
	manager.UpdateActiveTransactions(1003, false)
	manager.UpdateActiveTransactions(1004, false)

	// Create a new checkpoint after transactions are completed
	manager.createConsistentCheckpoint(lastLSN, true)

	// Read and verify checkpoint metadata
	checkpoint, err = ReadCheckpointMeta(checkpointPath)
	if err != nil {
		t.Fatalf("Failed to read checkpoint metadata: %v", err)
	}

	// Now the checkpoint should be consistent
	if !checkpoint.IsConsistent {
		t.Errorf("Expected checkpoint to be consistent after transactions completed")
	}
	if len(checkpoint.ActiveTransactions) != 0 {
		t.Errorf("Expected no active transactions, got %d", len(checkpoint.ActiveTransactions))
	}

	// 4. TEST CHECKPOINT LOADING DURING RECOVERY
	// Close the current manager and create a new one
	manager.Close()

	// Create a new WAL manager that will use the existing checkpoint
	newManager, err := NewWALManager(testDir, SyncNormal, &config)
	if err != nil {
		t.Fatalf("Failed to create new WAL manager: %v", err)
	}
	defer newManager.Close()

	// Verify the new manager loaded the checkpoint correctly
	if newManager.lastCheckpoint != checkpoint.LSN {
		t.Errorf("New manager did not load checkpoint LSN correctly: got %d, expected %d",
			newManager.lastCheckpoint, checkpoint.LSN)
	}

	// Clean up
	err = newManager.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL manager: %v", err)
	}
}
