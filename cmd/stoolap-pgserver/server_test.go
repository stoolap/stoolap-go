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
package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stoolap/stoolap"
)

func TestServerStartup(t *testing.T) {
	// Test server startup and shutdown
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}

	// Create a test listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Test connection handling in a goroutine
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		srv.handleConnection(conn)
	}()

	// Connect as a client
	addr := ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send startup message
	frontend := pgproto3.NewFrontend(conn, conn)

	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "test",
			"database": "test",
		},
	}

	frontend.Send(startupMsg)
	err = frontend.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Small delay to let server process
	time.Sleep(100 * time.Millisecond)

	// We successfully sent the startup message, that's enough for this test
}

func TestHandleQuery(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}
	connState := newConnectionState(db)

	// Mock backend
	var buf bytes.Buffer
	backend := pgproto3.NewBackend(&buf, &buf)

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "SELECT 1",
			query:   "SELECT 1",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE",
			query:   "CREATE TABLE test (id INT, name TEXT)",
			wantErr: false,
		},
		{
			name:    "INSERT",
			query:   "INSERT INTO test VALUES (1, 'test')",
			wantErr: false,
		},
		{
			name:    "SELECT FROM TABLE",
			query:   "SELECT * FROM test",
			wantErr: false,
		},
		{
			name:    "BEGIN",
			query:   "BEGIN",
			wantErr: false,
		},
		{
			name:    "COMMIT",
			query:   "COMMIT",
			wantErr: false,
		},
		{
			name:    "ROLLBACK",
			query:   "ROLLBACK",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := srv.handleQuery(connState, backend, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTransactionHandling(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}
	connState := newConnectionState(db)

	var buf bytes.Buffer
	backend := pgproto3.NewBackend(&buf, &buf)

	// Test BEGIN with different isolation levels
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "BEGIN READ COMMITTED",
			query: "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
		},
		{
			name:  "BEGIN REPEATABLE READ",
			query: "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		},
		{
			name:  "BEGIN SERIALIZABLE",
			query: "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		},
		{
			name:  "BEGIN SNAPSHOT",
			query: "BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT",
		},
		{
			name:  "BEGIN READ ONLY",
			query: "BEGIN TRANSACTION READ ONLY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := srv.handleBegin(connState, backend, tt.query)
			if err != nil {
				t.Errorf("handleBegin() error = %v", err)
			}

			if !connState.isInTransaction() {
				t.Error("Expected to be in transaction")
			}

			// Clean up
			connState.rollbackTransaction()
		})
	}
}

func TestConnectionState(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cs := newConnectionState(db)

	// Test initial state
	if cs.isInTransaction() {
		t.Error("Should not be in transaction initially")
	}

	if cs.getTxStatus() != 'I' {
		t.Errorf("Expected status 'I', got '%c'", cs.getTxStatus())
	}

	// Test begin transaction
	err = cs.beginTransaction(&sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !cs.isInTransaction() {
		t.Error("Should be in transaction after begin")
	}

	if cs.getTxStatus() != 'T' {
		t.Errorf("Expected status 'T', got '%c'", cs.getTxStatus())
	}

	// Test commit
	err = cs.commitTransaction()
	if err != nil {
		t.Fatal(err)
	}

	if cs.isInTransaction() {
		t.Error("Should not be in transaction after commit")
	}

	// Test rollback
	err = cs.beginTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}

	err = cs.rollbackTransaction()
	if err != nil {
		t.Fatal(err)
	}

	if cs.isInTransaction() {
		t.Error("Should not be in transaction after rollback")
	}
}

func TestHandleMessage(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}
	connState := newConnectionState(db)

	var buf bytes.Buffer
	backend := pgproto3.NewBackend(&buf, &buf)

	// Test Query message
	queryMsg := &pgproto3.Query{String: "SELECT 1"}
	err = srv.handleMessage(connState, backend, queryMsg)
	if err != nil {
		t.Errorf("handleMessage(Query) error = %v", err)
	}

	// Test Parse message
	parseMsg := &pgproto3.Parse{}
	err = srv.handleMessage(connState, backend, parseMsg)
	if err != nil {
		t.Errorf("handleMessage(Parse) error = %v", err)
	}

	// Test Bind message
	bindMsg := &pgproto3.Bind{}
	err = srv.handleMessage(connState, backend, bindMsg)
	if err != nil {
		t.Errorf("handleMessage(Bind) error = %v", err)
	}

	// Test Describe message
	describeMsg := &pgproto3.Describe{}
	err = srv.handleMessage(connState, backend, describeMsg)
	if err != nil {
		t.Errorf("handleMessage(Describe) error = %v", err)
	}

	// Test Execute message
	executeMsg := &pgproto3.Execute{}
	err = srv.handleMessage(connState, backend, executeMsg)
	if err != nil {
		t.Errorf("handleMessage(Execute) error = %v", err)
	}

	// Test Sync message
	syncMsg := &pgproto3.Sync{}
	err = srv.handleMessage(connState, backend, syncMsg)
	if err != nil {
		t.Errorf("handleMessage(Sync) error = %v", err)
	}

	// Test Terminate message
	terminateMsg := &pgproto3.Terminate{}
	err = srv.handleMessage(connState, backend, terminateMsg)
	if err == nil || err.Error() != "terminate" {
		t.Errorf("handleMessage(Terminate) should return 'terminate' error")
	}
}

func TestSpecialQueries(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}

	var buf bytes.Buffer
	backend := pgproto3.NewBackend(&buf, &buf)

	// Test handleSelect1
	err = srv.handleSelect1(backend)
	if err != nil {
		t.Errorf("handleSelect1() error = %v", err)
	}

	// Test handleVersion
	buf.Reset()
	err = srv.handleVersion(backend)
	if err != nil {
		t.Errorf("handleVersion() error = %v", err)
	}

	// Test handleCurrentDatabase
	buf.Reset()
	err = srv.handleCurrentDatabase(backend)
	if err != nil {
		t.Errorf("handleCurrentDatabase() error = %v", err)
	}
}

func TestSSLHandling(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}

	// Create test connection pair
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Handle connection in goroutine
	done := make(chan error, 1)
	go func() {
		srv.handleConnection(server)
		done <- nil
	}()

	// Send SSL request
	frontend := pgproto3.NewFrontend(client, client)
	frontend.Send(&pgproto3.SSLRequest{})
	frontend.Flush()

	// Read SSL response (should be 'N')
	response := make([]byte, 1)
	_, err = client.Read(response)
	if err != nil {
		t.Fatal(err)
	}

	if response[0] != 'N' {
		t.Errorf("Expected 'N' for SSL response, got '%c'", response[0])
	}

	// Close connection
	client.Close()

	// Wait for handler to finish
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Error("Handler didn't finish in time")
	}
}

func TestVerboseLogging(t *testing.T) {
	// Save original verbose flag
	oldVerbose := verbose
	verbose = true
	defer func() { verbose = oldVerbose }()

	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}
	connState := newConnectionState(db)

	var buf bytes.Buffer
	backend := pgproto3.NewBackend(&buf, &buf)

	// Test query with verbose logging
	err = srv.handleQuery(connState, backend, "SELECT 1")
	if err != nil {
		t.Errorf("handleQuery() with verbose error = %v", err)
	}

	// Test transaction with verbose logging
	err = srv.handleBegin(connState, backend, "BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT")
	if err != nil {
		t.Errorf("handleBegin() with verbose error = %v", err)
	}

	err = srv.handleCommit(connState, backend)
	if err != nil {
		t.Errorf("handleCommit() with verbose error = %v", err)
	}
}

func TestGetVersion(t *testing.T) {
	version := getVersion()
	if version == "" {
		t.Error("getVersion() returned empty string")
	}

	// Should have format like "0.0.9-dev"
	if len(version) < 5 {
		t.Errorf("Version string too short: %s", version)
	}
}

func TestParseIsolationLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected sql.IsolationLevel
	}{
		{"READ UNCOMMITTED", sql.LevelReadUncommitted},
		{"READ_UNCOMMITTED", sql.LevelReadUncommitted},
		{"READ COMMITTED", sql.LevelReadCommitted},
		{"READ_COMMITTED", sql.LevelReadCommitted},
		{"REPEATABLE READ", sql.LevelRepeatableRead},
		{"REPEATABLE_READ", sql.LevelRepeatableRead},
		{"SERIALIZABLE", sql.LevelSerializable},
		{"SNAPSHOT", sql.LevelSnapshot},
		{"INVALID", sql.LevelDefault},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseIsolationLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseIsolationLevel(%s) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestConcurrentConnections tests multiple concurrent connections
func TestConcurrentConnections(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	srv := &server{db: db}

	// Create a test listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Accept connections in background
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConnection(conn)
		}
	}()

	// Create multiple concurrent connections
	numConnections := 5
	done := make(chan bool, numConnections)

	for i := 0; i < numConnections; i++ {
		go func(id int) {
			defer func() { done <- true }()

			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Errorf("Connection %d: dial error: %v", id, err)
				return
			}
			defer conn.Close()

			// Send startup
			frontend := pgproto3.NewFrontend(conn, conn)
			startupMsg := &pgproto3.StartupMessage{
				ProtocolVersion: pgproto3.ProtocolVersionNumber,
				Parameters: map[string]string{
					"user":     fmt.Sprintf("user%d", id),
					"database": "test",
				},
			}
			frontend.Send(startupMsg)
			frontend.Flush()

			// Small delay to simulate real client
			time.Sleep(10 * time.Millisecond)
		}(i)
	}

	// Wait for all connections to complete
	for i := 0; i < numConnections; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for connections")
		}
	}
}
