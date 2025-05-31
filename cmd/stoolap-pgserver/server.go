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
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stoolap/stoolap"
	"github.com/stoolap/stoolap/internal/common"
)

type server struct {
	db *stoolap.DB
}

func runServer() error {
	// Open database
	db, err := stoolap.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	srv := &server{db: db}

	// Create listener
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
	}
	defer ln.Close()

	log.Printf("Stoolap PostgreSQL wire protocol server listening on %s", bindAddr)
	log.Printf("Database: %s", dbPath)
	log.Printf("Stoolap version: %s", getVersion())

	// Handle shutdown gracefully
	shutdownChan := make(chan struct{})
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down...")
		close(shutdownChan)
		ln.Close()
	}()

	// Accept connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-shutdownChan:
				return nil
			default:
				if strings.Contains(err.Error(), "use of closed network connection") {
					return nil
				}
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
		}

		go srv.handleConnection(conn)
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()

	if verbose {
		log.Printf("New connection from %s", conn.RemoteAddr())
	}

	// Create connection state for transaction management
	connState := newConnectionState(s.db)

	backend := pgproto3.NewBackend(conn, conn)

	// Handle startup
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Printf("Failed to receive startup message: %v", err)
		return
	}

	switch startupMsg.(type) {
	case *pgproto3.StartupMessage:
		// Send auth OK
		backend.Send(&pgproto3.AuthenticationOk{})

		// Send parameter status messages
		params := []struct{ key, value string }{
			{"server_version", "14.0 (Stoolap)"},
			{"server_encoding", "UTF8"},
			{"client_encoding", "UTF8"},
			{"DateStyle", "ISO, MDY"},
			{"TimeZone", "UTC"},
			{"integer_datetimes", "on"},
			{"standard_conforming_strings", "on"},
		}

		for _, p := range params {
			backend.Send(&pgproto3.ParameterStatus{
				Name:  p.key,
				Value: p.value,
			})
		}

		// Send ready for query
		backend.Send(&pgproto3.BackendKeyData{
			ProcessID: uint32(os.Getpid()),
			SecretKey: 0,
		})

		backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})

	case *pgproto3.SSLRequest:
		// We don't support SSL
		if _, err := conn.Write([]byte("N")); err != nil {
			log.Printf("Failed to send SSL response: %v", err)
			return
		}
		// Continue to receive the actual startup message
		s.handleConnection(conn)
		return

	default:
		log.Printf("Unknown startup message type: %T", startupMsg)
		return
	}

	// Main message loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			if err.Error() != "EOF" {
				if verbose {
					log.Printf("Connection closed: %v", err)
				}
			}
			return
		}

		if verbose {
			log.Printf("Received message type: %T", msg)
		}

		if err := s.handleMessage(connState, backend, msg); err != nil {
			if err.Error() == "terminate" {
				if verbose {
					log.Printf("Client requested termination")
				}
				return
			}
			log.Printf("Error handling message: %v", err)
			// Send error response
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "XX000",
				Message:  err.Error(),
			})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
		}
	}
}

func (s *server) handleMessage(connState *connectionState, backend *pgproto3.Backend, msg pgproto3.FrontendMessage) error {
	switch m := msg.(type) {
	case *pgproto3.Query:
		return s.handleQuery(connState, backend, m.String)

	case *pgproto3.Parse:
		// Simple implementation - just acknowledge for now
		backend.Send(&pgproto3.ParseComplete{})
		return nil

	case *pgproto3.Bind:
		// Simple implementation - just acknowledge for now
		backend.Send(&pgproto3.BindComplete{})
		return nil

	case *pgproto3.Describe:
		// Send empty row description for now
		backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}})
		return nil

	case *pgproto3.Execute:
		// Send command complete
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
		return nil

	case *pgproto3.Sync:
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
		return nil

	case *pgproto3.Terminate:
		return fmt.Errorf("terminate")

	default:
		if verbose {
			log.Printf("Unhandled message type: %T", m)
		}
		return nil
	}
}

func (s *server) handleQuery(connState *connectionState, backend *pgproto3.Backend, query string) error {
	ctx := context.Background()
	query = strings.TrimSpace(query)

	if verbose {
		log.Printf("[QUERY] %s", query)
	}

	// Handle simple commands
	upperQuery := strings.ToUpper(query)

	// Handle transaction commands first
	switch {
	case strings.HasPrefix(upperQuery, "BEGIN TRANSACTION"):
		if verbose {
			log.Printf("[TRANSACTION] BEGIN TRANSACTION detected")
		}
		return s.handleBegin(connState, backend, query)
	case strings.HasPrefix(upperQuery, "BEGIN"):
		return s.handleBegin(connState, backend, query)
	case strings.HasPrefix(upperQuery, "START TRANSACTION"):
		return s.handleBegin(connState, backend, query)
	case upperQuery == "COMMIT" || upperQuery == "COMMIT;":
		return s.handleCommit(connState, backend)
	case upperQuery == "ROLLBACK" || upperQuery == "ROLLBACK;":
		return s.handleRollback(connState, backend)
	case strings.HasPrefix(upperQuery, "SET TRANSACTION"):
		return s.handleSetTransaction(connState, backend, query)
	}

	// Handle special PostgreSQL compatibility queries
	switch {
	case upperQuery == "SELECT 1" || upperQuery == "SELECT 1;":
		return s.handleSelect1(backend)
	case strings.HasPrefix(upperQuery, "SELECT VERSION()"):
		return s.handleVersion(backend)
	case strings.HasPrefix(upperQuery, "SELECT CURRENT_DATABASE()"):
		return s.handleCurrentDatabase(backend)
	}

	// Check if it's a SELECT or other query
	if strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "PRAGMA") {
		// Execute query using transaction if in one
		var rows stoolap.Rows
		var err error

		if connState.isInTransaction() {
			if verbose {
				log.Printf("[EXECUTE] Running query in transaction")
			}
			rows, err = connState.currentTx.QueryContext(ctx, query)
		} else {
			if verbose {
				log.Printf("[EXECUTE] Running query without transaction")
			}
			rows, err = connState.db.Query(ctx, query)
		}
		if err != nil {
			return err
		}
		defer rows.Close()

		// Get column info
		columns := rows.Columns()

		// Send row description
		fields := make([]pgproto3.FieldDescription, len(columns))
		for i, col := range columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          uint32(pgtype.TextOID), // Default to text for now
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}

		backend.Send(&pgproto3.RowDescription{Fields: fields})

		// Send rows
		rowCount := 0
		for rows.Next() {
			values := make([]any, len(columns))
			valuePtrs := make([]any, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				return err
			}

			rowValues := make([][]byte, len(columns))
			for i, v := range values {
				if v == nil {
					rowValues[i] = nil
				} else {
					rowValues[i] = []byte(fmt.Sprintf("%v", v))
				}
			}

			backend.Send(&pgproto3.DataRow{Values: rowValues})
			rowCount++
		}

		// Send command complete
		tag := fmt.Sprintf("SELECT %d", rowCount)
		if verbose {
			log.Printf("[RESULT] Query returned %d rows", rowCount)
		}
		backend.Send(&pgproto3.CommandComplete{
			CommandTag: []byte(tag),
		})

	} else {
		// Execute non-query command using transaction if in one
		var result sql.Result
		var err error

		if connState.isInTransaction() {
			result, err = connState.currentTx.ExecContext(ctx, query)
		} else {
			result, err = connState.db.Exec(ctx, query)
		}
		if err != nil {
			return err
		}

		// Determine command tag
		var tag string
		switch {
		case strings.HasPrefix(upperQuery, "INSERT"):
			rowsAffected, _ := result.RowsAffected()
			tag = fmt.Sprintf("INSERT 0 %d", rowsAffected)
		case strings.HasPrefix(upperQuery, "UPDATE"):
			rowsAffected, _ := result.RowsAffected()
			tag = fmt.Sprintf("UPDATE %d", rowsAffected)
		case strings.HasPrefix(upperQuery, "DELETE"):
			rowsAffected, _ := result.RowsAffected()
			tag = fmt.Sprintf("DELETE %d", rowsAffected)
		case strings.HasPrefix(upperQuery, "CREATE"):
			tag = "CREATE TABLE"
		case strings.HasPrefix(upperQuery, "DROP"):
			tag = "DROP TABLE"
		case strings.HasPrefix(upperQuery, "BEGIN"):
			tag = "BEGIN"
		case strings.HasPrefix(upperQuery, "COMMIT"):
			tag = "COMMIT"
		case strings.HasPrefix(upperQuery, "ROLLBACK"):
			tag = "ROLLBACK"
		default:
			tag = strings.ToUpper(strings.Split(query, " ")[0])
		}

		backend.Send(&pgproto3.CommandComplete{
			CommandTag: []byte(tag),
		})
	}

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
	return nil
}

func (s *server) handleSelect1(backend *pgproto3.Backend) error {
	// Send row description
	backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("?column?"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          uint32(pgtype.Int4OID),
				DataTypeSize:         4,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	})

	// Send data row
	backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("1")},
	})

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return nil
}

func (s *server) handleVersion(backend *pgproto3.Backend) error {
	// Send row description
	backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("version"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          uint32(pgtype.TextOID),
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	})

	// Send data row
	backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("PostgreSQL 14.0 (Stoolap)")},
	})

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return nil
}

func (s *server) handleCurrentDatabase(backend *pgproto3.Backend) error {
	// Send row description
	backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("current_database"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          uint32(pgtype.TextOID),
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	})

	// Send data row
	backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("stoolap")},
	})

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return nil
}

// Transaction handling functions

func (s *server) handleBegin(connState *connectionState, backend *pgproto3.Backend, query string) error {
	// Parse transaction options
	upperQuery := strings.ToUpper(query)
	opts := &sql.TxOptions{}

	if verbose {
		log.Printf("[BEGIN] Processing: %s", query)
	}

	// Check for isolation level
	if strings.Contains(upperQuery, "ISOLATION LEVEL") {
		if strings.Contains(upperQuery, "READ UNCOMMITTED") {
			opts.Isolation = sql.LevelReadUncommitted
		} else if strings.Contains(upperQuery, "READ COMMITTED") {
			opts.Isolation = sql.LevelReadCommitted
		} else if strings.Contains(upperQuery, "REPEATABLE READ") {
			opts.Isolation = sql.LevelRepeatableRead
		} else if strings.Contains(upperQuery, "SERIALIZABLE") {
			// Map SERIALIZABLE to SNAPSHOT since Stoolap doesn't support true SERIALIZABLE
			opts.Isolation = sql.LevelSnapshot
		} else if strings.Contains(upperQuery, "SNAPSHOT") {
			opts.Isolation = sql.LevelSnapshot
		}
	}

	// Check for read only
	if strings.Contains(upperQuery, "READ ONLY") {
		opts.ReadOnly = true
	}

	// Begin transaction
	if err := connState.beginTransaction(opts); err != nil {
		return err
	}

	if verbose {
		isolationName := "DEFAULT"
		switch opts.Isolation {
		case sql.LevelReadCommitted:
			isolationName = "READ COMMITTED"
		case sql.LevelSnapshot:
			isolationName = "SNAPSHOT"
		case sql.LevelRepeatableRead:
			isolationName = "REPEATABLE READ"
		}
		log.Printf("[BEGIN] Transaction started with isolation: %s", isolationName)
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("BEGIN"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
	return nil
}

func (s *server) handleCommit(connState *connectionState, backend *pgproto3.Backend) error {
	if verbose {
		log.Printf("[COMMIT] Committing transaction")
	}

	if err := connState.commitTransaction(); err != nil {
		return err
	}

	if verbose {
		log.Printf("[COMMIT] Transaction committed successfully")
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("COMMIT"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
	return nil
}

func (s *server) handleRollback(connState *connectionState, backend *pgproto3.Backend) error {
	if verbose {
		log.Printf("[ROLLBACK] Rolling back transaction")
	}

	if err := connState.rollbackTransaction(); err != nil {
		return err
	}

	if verbose {
		log.Printf("[ROLLBACK] Transaction rolled back successfully")
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("ROLLBACK"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
	return nil
}

func (s *server) handleSetTransaction(connState *connectionState, backend *pgproto3.Backend, query string) error {
	// This is a simplified implementation
	// In a full implementation, we'd parse and apply the settings

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SET"),
	})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: connState.getTxStatus()})
	return nil
}

// getVersion returns the Stoolap version string
func getVersion() string {
	return fmt.Sprintf("v%s.%s.%s-%s",
		common.VersionMajor, common.VersionMinor, common.VersionPatch, common.VersionSuffix)
}
