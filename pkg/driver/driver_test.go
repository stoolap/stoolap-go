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
package driver

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestDatabaseSQLOpen(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLExecQuery(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	_, err = db.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.ExecContext(ctx, "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", affected)
	}

	rows, err := db.QueryContext(ctx, "SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	type user struct {
		id   int64
		name string
		age  int64
	}
	var users []user

	for rows.Next() {
		var u user
		if err := rows.Scan(&u.id, &u.name, &u.age); err != nil {
			t.Fatal(err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].name != "Alice" || users[1].name != "Bob" {
		t.Fatalf("unexpected users: %v", users)
	}
}

func TestDatabaseSQLQueryRow(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO test VALUES (1, 'Alice')"); err != nil {
		t.Fatal(err)
	}

	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}

	err = db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = 999").Scan(&name)
	if err != sql.ErrNoRows {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
}

func TestDatabaseSQLParams(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, "INSERT INTO test VALUES ($1, $2)", int64(1), "Alice")
	if err != nil {
		t.Fatal(err)
	}

	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM test WHERE id = $1", int64(1)).Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}
}

func TestDatabaseSQLTransaction(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (1, 'Alice')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (2, 'Bob')"); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDatabaseSQLTransactionRollback(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO test VALUES (1, 'Alice')"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO test VALUES (2, 'Bob')"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 after rollback, got %d", count)
	}
}

func TestDatabaseSQLPrepared(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, "INSERT INTO test VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	for i := int64(1); i <= 5; i++ {
		_, err := stmt.ExecContext(ctx, i, "User")
		if err != nil {
			t.Fatal(err)
		}
	}

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestDatabaseSQLNullTypes(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE null_test (id INTEGER, s TEXT, i INTEGER, f FLOAT, b BOOLEAN)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO null_test VALUES (1, NULL, NULL, NULL, NULL)"); err != nil {
		t.Fatal(err)
	}

	var (
		id int64
		s  sql.NullString
		i  sql.NullInt64
		f  sql.NullFloat64
		b  sql.NullBool
	)
	err = db.QueryRowContext(ctx, "SELECT * FROM null_test").Scan(&id, &s, &i, &f, &b)
	if err != nil {
		t.Fatal(err)
	}
	if s.Valid || i.Valid || f.Valid || b.Valid {
		t.Fatal("expected all NULLs to be invalid")
	}
}

func TestDatabaseSQLAllTypes(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE types (i INTEGER, f FLOAT, t TEXT, b BOOLEAN, ts TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	_, err = db.ExecContext(ctx, "INSERT INTO types VALUES ($1, $2, $3, $4, $5)",
		int64(42), 3.14, "hello", true, now)
	if err != nil {
		t.Fatal(err)
	}

	var (
		i  int64
		f  float64
		s  string
		b  bool
		ts time.Time
	)
	err = db.QueryRowContext(ctx, "SELECT * FROM types").Scan(&i, &f, &s, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}

	if i != 42 || f != 3.14 || s != "hello" || !b {
		t.Fatalf("unexpected values: %d, %f, %s, %v", i, f, s, b)
	}
	if !ts.Equal(now) {
		t.Fatalf("timestamp: expected %v, got %v", now, ts)
	}
}

func TestDatabaseSQLSnapshotIsolation(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO iso_test VALUES (1, 'original')"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}

	var val string
	err = tx.QueryRowContext(ctx, "SELECT val FROM iso_test WHERE id = 1").Scan(&val)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if val != "original" {
		_ = tx.Rollback()
		t.Fatalf("expected original, got %s", val)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLPreparedQuery(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE pq_test (id INTEGER, name TEXT, active BOOLEAN, score FLOAT, ts TIMESTAMP, data JSON, bl VECTOR)"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.ExecContext(ctx, "INSERT INTO pq_test VALUES ($1, $2, $3, $4, $5, $6, $7)",
		int64(1), "Alice", true, 99.5, now, `{"k":"v"}`, "[1.0, 2.0]"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, "SELECT id, name, active, score, ts, data, bl FROM pq_test WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	var (
		id     int64
		name   string
		active bool
		score  float64
		ts     time.Time
		data   string
		bl     []byte
	)
	err = stmt.QueryRowContext(ctx, int64(1)).Scan(&id, &name, &active, &score, &ts, &data, &bl)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "Alice" || !active || score != 99.5 {
		t.Fatalf("unexpected values: %d %s %v %f", id, name, active, score)
	}
	if !ts.Equal(now) {
		t.Fatalf("timestamp: expected %v, got %v", now, ts)
	}
	if data != `{"k":"v"}` {
		t.Fatalf("JSON: expected {\"k\":\"v\"}, got %s", data)
	}
	if bl == nil {
		t.Fatal("blob: expected non-nil vector data")
	}
}

func TestDatabaseSQLTransactionWithParams(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE tx_params (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO tx_params VALUES ($1, $2)", int64(1), "Alice")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	var name string
	err = tx.QueryRowContext(ctx, "SELECT name FROM tx_params WHERE id = $1", int64(1)).Scan(&name)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if name != "Alice" {
		_ = tx.Rollback()
		t.Fatalf("expected Alice, got %s", name)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLTransactionPrepared(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE tx_prep (id INTEGER, val TEXT)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO tx_prep VALUES ($1, $2)")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	for i := int64(1); i <= 3; i++ {
		_, err := stmt.ExecContext(ctx, i, "val")
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	_ = stmt.Close()

	// Query within transaction
	qstmt, err := tx.PrepareContext(ctx, "SELECT val FROM tx_prep WHERE id = $1")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var val string
	err = qstmt.QueryRowContext(ctx, int64(2)).Scan(&val)
	_ = qstmt.Close()
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if val != "val" {
		_ = tx.Rollback()
		t.Fatalf("expected 'val', got %q", val)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tx_prep").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

func TestDatabaseSQLRepeatableRead(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE rr_test (id INTEGER)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLUnsupportedIsolation(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation level")
	}
}

func TestDatabaseSQLPreparedNoArgs(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE noargs (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO noargs VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.PrepareContext(ctx, "SELECT id FROM noargs ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	_ = rows.Close()
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestDatabaseSQLExecNoArgs(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE noargs_exec (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.ExecContext(ctx, "INSERT INTO noargs_exec VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("expected 1, got %d", affected)
	}

	// LastInsertId
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	_ = id // stoolap returns 0
}

func TestDatabaseSQLQueryNoArgs(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE qa (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO qa VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT id FROM qa")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected row")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("expected 1, got %d", id)
	}
}

func TestDatabaseSQLTransactionExecNoArgs(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE txna (id INTEGER)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO txna VALUES (1)")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	var count int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM txna").Scan(&count); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if count != 1 {
		_ = tx.Rollback()
		t.Fatalf("expected 1, got %d", count)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLMultipleConnections(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	db.SetMaxOpenConns(2)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE mc (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO mc VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	var id int64
	if err := db.QueryRowContext(ctx, "SELECT id FROM mc").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("expected 1, got %d", id)
	}
}

func TestDatabaseSQLClosedRows(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE cr (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO cr VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, "SELECT id FROM cr")
	if err != nil {
		t.Fatal(err)
	}

	// Close before iterating
	_ = rows.Close()

	// Close again should be safe
	_ = rows.Close()
}

func TestDatabaseSQLPing(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Multiple pings
	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			t.Fatal(err)
		}
	}
}
