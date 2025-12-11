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
	"testing"
	"time"

	"github.com/stoolap/stoolap-go/internal/common"
)

func TestWALEncodeDecode(t *testing.T) {
	// Test cases with different scenarios
	testCases := []struct {
		name  string
		entry WALEntry
	}{
		{
			name: "Basic insert entry",
			entry: WALEntry{
				LSN:       12345,
				TxnID:     67890,
				TableName: "test_table",
				RowID:     111213,
				Operation: WALInsert,
				Data:      []byte("test data"),
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			name: "Commit entry with no data",
			entry: WALEntry{
				LSN:       99999,
				TxnID:     12345,
				TableName: "",
				RowID:     0,
				Operation: WALCommit,
				Data:      nil,
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			name: "Large data entry",
			entry: WALEntry{
				LSN:       54321,
				TxnID:     98765,
				TableName: "large_table",
				RowID:     999999,
				Operation: WALUpdate,
				Data:      make([]byte, 10*1024), // 10KB
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			name: "Unicode table name",
			entry: WALEntry{
				LSN:       11111,
				TxnID:     22222,
				TableName: "测试表_テスト",
				RowID:     33333,
				Operation: WALDelete,
				Data:      []byte("unicode test"),
				Timestamp: time.Now().UnixNano(),
			},
		},
	}

	wm := &WALManager{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode using the new method
			buf := common.GetBufferPool()
			defer common.PutBufferPool(buf)

			err := encodeEntry(buf, tc.entry)
			if err != nil {
				t.Fatalf("Failed to encode entry: %v", err)
			}

			// Get encoded bytes
			encoded := buf.Bytes()

			// Verify we have the header
			if len(encoded) < 16 {
				t.Fatalf("Encoded data too short: %d bytes", len(encoded))
			}

			// Decode using existing method
			decoded, err := wm.deserializeEntry(tc.entry.LSN, encoded[16:])
			if err != nil {
				t.Fatalf("Failed to decode entry: %v", err)
			}

			// Verify fields match
			if decoded.LSN != tc.entry.LSN {
				t.Errorf("LSN mismatch: got %d, want %d", decoded.LSN, tc.entry.LSN)
			}
			if decoded.TxnID != tc.entry.TxnID {
				t.Errorf("TxnID mismatch: got %d, want %d", decoded.TxnID, tc.entry.TxnID)
			}
			if decoded.TableName != tc.entry.TableName {
				t.Errorf("TableName mismatch: got %q, want %q", decoded.TableName, tc.entry.TableName)
			}
			if decoded.RowID != tc.entry.RowID {
				t.Errorf("RowID mismatch: got %d, want %d", decoded.RowID, tc.entry.RowID)
			}
			if decoded.Operation != tc.entry.Operation {
				t.Errorf("Operation mismatch: got %d, want %d", decoded.Operation, tc.entry.Operation)
			}
			if decoded.Timestamp != tc.entry.Timestamp {
				t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, tc.entry.Timestamp)
			}
			if len(decoded.Data) != len(tc.entry.Data) {
				t.Errorf("Data length mismatch: got %d, want %d", len(decoded.Data), len(tc.entry.Data))
			}
		})
	}
}
