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
package cs

import (
	"testing"
	"time"
)

func mustExec(t *testing.T, db *DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

// ---- Database lifecycle ----

func TestVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Fatal("version should not be empty")
	}
}

func TestOpenInMemory(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
}

func TestOpenInvalid(t *testing.T) {
	_, err := Open("invalid://")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestClone(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	clone, err := db.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := clone.Close(); err != nil {
			t.Error(err)
		}
	}()
}

func TestCloneNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.Clone()
	if err == nil {
		t.Fatal("expected error for nil DB clone")
	}
}

func TestCloseNilDB(t *testing.T) {
	db := &DB{}
	err := db.Close()
	if err != nil {
		t.Fatal("closing nil DB should return nil")
	}
}

func TestDoubleClose(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal("double close should be safe")
	}
}

// ---- Exec / Query ----

func TestExec(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	affected, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 affected, got %d", affected)
	}
}

func TestExecNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected error for nil DB exec")
	}
}

func TestExecError(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("INVALID SQL HERE !!!")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestExecParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	affected, err := db.ExecParams("INSERT INTO t VALUES ($1, $2)", []Value{
		IntegerValue(1),
		TextValue("Alice"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected, got %d", affected)
	}
}

func TestExecParamsNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.ExecParams("INSERT INTO t VALUES ($1)", []Value{IntegerValue(1)})
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestExecParamsEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecParams("INSERT INTO t VALUES (1)", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQuery(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if rows.ColumnCount() != 2 {
		t.Fatalf("expected 2 columns, got %d", rows.ColumnCount())
	}
	if rows.ColumnName(0) != "id" {
		t.Fatalf("expected column name 'id', got '%s'", rows.ColumnName(0))
	}
	if rows.ColumnName(1) != "name" {
		t.Fatalf("expected column name 'name', got '%s'", rows.ColumnName(1))
	}

	hasRow, err := rows.Next()
	if err != nil || !hasRow {
		t.Fatal("expected first row")
	}

	if rows.ColumnType(0) != TypeInteger {
		t.Fatalf("expected TypeInteger, got %d", rows.ColumnType(0))
	}
	if rows.ColumnInt64(0) != 1 {
		t.Fatalf("expected 1, got %d", rows.ColumnInt64(0))
	}
	if rows.ColumnText(1) != "Alice" {
		t.Fatalf("expected Alice, got %s", rows.ColumnText(1))
	}

	hasRow, err = rows.Next()
	if err != nil || !hasRow {
		t.Fatal("expected second row")
	}

	hasRow, _ = rows.Next()
	if hasRow {
		t.Fatal("expected no more rows")
	}
}

func TestQueryNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.Query("SELECT 1")
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestQueryParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")

	rows, err := db.QueryParams("SELECT name FROM t WHERE id = $1", []Value{IntegerValue(1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	if rows.ColumnText(0) != "Alice" {
		t.Fatalf("expected Alice, got %s", rows.ColumnText(0))
	}
}

func TestQueryParamsNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.QueryParams("SELECT $1", []Value{IntegerValue(1)})
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestQueryParamsEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	rows, err := db.QueryParams("SELECT id FROM t", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
}

// ---- All column types ----

func TestAllColumnTypes(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE types (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, j JSON)")

	now := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	_, err = db.ExecParams("INSERT INTO types VALUES ($1, $2, $3, $4, $5, $6)", []Value{
		IntegerValue(42),
		FloatValue(3.14),
		TextValue("hello"),
		BoolValue(true),
		TimestampValue(now),
		JSONValue(`{"key":"val"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT i, f, s, b, ts, j FROM types")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}

	// Integer
	if rows.ColumnType(0) != TypeInteger {
		t.Fatalf("col 0: expected TypeInteger, got %d", rows.ColumnType(0))
	}
	if rows.ColumnInt64(0) != 42 {
		t.Fatalf("col 0: expected 42, got %d", rows.ColumnInt64(0))
	}
	if rows.ColumnIsNull(0) {
		t.Fatal("col 0 should not be null")
	}

	// Float
	if rows.ColumnType(1) != TypeFloat {
		t.Fatalf("col 1: expected TypeFloat, got %d", rows.ColumnType(1))
	}
	if rows.ColumnDouble(1) != 3.14 {
		t.Fatalf("col 1: expected 3.14, got %f", rows.ColumnDouble(1))
	}

	// Text
	if rows.ColumnType(2) != TypeText {
		t.Fatalf("col 2: expected TypeText, got %d", rows.ColumnType(2))
	}
	if rows.ColumnText(2) != "hello" {
		t.Fatalf("col 2: expected hello, got %s", rows.ColumnText(2))
	}

	// Boolean
	if rows.ColumnType(3) != TypeBoolean {
		t.Fatalf("col 3: expected TypeBoolean, got %d", rows.ColumnType(3))
	}
	if !rows.ColumnBool(3) {
		t.Fatal("col 3: expected true")
	}

	// Timestamp
	if rows.ColumnType(4) != TypeTimestamp {
		t.Fatalf("col 4: expected TypeTimestamp, got %d", rows.ColumnType(4))
	}
	ts := rows.ColumnTimestamp(4)
	if !ts.Equal(now) {
		t.Fatalf("col 4: expected %v, got %v", now, ts)
	}

	// JSON
	if rows.ColumnType(5) != TypeJSON {
		t.Fatalf("col 5: expected TypeJSON, got %d", rows.ColumnType(5))
	}
	if rows.ColumnText(5) != `{"key":"val"}` {
		t.Fatalf("col 5: expected JSON, got %s", rows.ColumnText(5))
	}
}

func TestVectorBlob(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE vec (id INTEGER, embedding VECTOR)")
	mustExec(t, db, "INSERT INTO vec VALUES (1, '[1.0, 2.0, 3.0]')")

	rows, err := db.Query("SELECT embedding FROM vec")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}

	if rows.ColumnType(0) != TypeBlob {
		t.Fatalf("expected TypeBlob, got %d", rows.ColumnType(0))
	}
	blob := rows.ColumnBlob(0)
	if len(blob) != 12 { // 3 f32 values = 12 bytes
		t.Fatalf("expected 12 bytes, got %d", len(blob))
	}
}

func TestNullColumns(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE null_test (id INTEGER, val TEXT)")
	mustExec(t, db, "INSERT INTO null_test VALUES (1, NULL)")

	rows, err := db.Query("SELECT val FROM null_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}

	if rows.ColumnType(0) != TypeNull {
		t.Fatalf("expected TypeNull, got %d", rows.ColumnType(0))
	}
	if !rows.ColumnIsNull(0) {
		t.Fatal("expected null")
	}
}

func TestEmptyText(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecParams("INSERT INTO t VALUES ($1)", []Value{TextValue("")}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	got := rows.ColumnText(0)
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

// ---- Rows nil pointer safety ----

func TestRowsNilPointer(t *testing.T) {
	r := &Rows{}

	_, err := r.Next()
	if err == nil {
		t.Fatal("expected error for nil Rows.Next")
	}

	if r.ColumnCount() != 0 {
		t.Fatal("expected 0 for nil ColumnCount")
	}
	if r.ColumnName(0) != "" {
		t.Fatal("expected empty for nil ColumnName")
	}
	if r.ColumnType(0) != TypeNull {
		t.Fatal("expected TypeNull for nil ColumnType")
	}
	if !r.ColumnIsNull(0) {
		t.Fatal("expected true for nil ColumnIsNull")
	}
	if r.Affected() != 0 {
		t.Fatal("expected 0 for nil Affected")
	}
	if err := r.Close(); err != nil {
		t.Fatal("nil Rows.Close should return nil")
	}
}

func TestRowsFetchAllNil(t *testing.T) {
	r := &Rows{}
	_, err := r.FetchAll()
	if err == nil {
		t.Fatal("expected error for nil Rows.FetchAll")
	}
}

// ---- Prepared statements ----

func TestPrepare(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}

	if stmt.SQL() != "INSERT INTO t VALUES ($1, $2)" {
		t.Fatalf("unexpected SQL: %s", stmt.SQL())
	}

	affected, err := stmt.Exec([]Value{IntegerValue(1), TextValue("Alice")})
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected, got %d", affected)
	}

	affected, err = stmt.Exec([]Value{IntegerValue(2), TextValue("Bob")})
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected, got %d", affected)
	}

	stmt.Finalize()
}

func TestPrepareNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.Prepare("SELECT 1")
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestStmtQuery(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")

	stmt, err := db.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	rows, err := stmt.Query([]Value{IntegerValue(2)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	if rows.ColumnText(0) != "Bob" {
		t.Fatalf("expected Bob, got %s", rows.ColumnText(0))
	}
}

func TestStmtExecNilStmt(t *testing.T) {
	s := &Stmt{}
	_, err := s.Exec(nil)
	if err == nil {
		t.Fatal("expected error for nil stmt exec")
	}
}

func TestStmtQueryNilStmt(t *testing.T) {
	s := &Stmt{}
	_, err := s.Query(nil)
	if err == nil {
		t.Fatal("expected error for nil stmt query")
	}
}

func TestStmtSQLNil(t *testing.T) {
	s := &Stmt{}
	if s.SQL() != "" {
		t.Fatal("expected empty SQL for nil stmt")
	}
}

func TestStmtFinalizeNil(t *testing.T) {
	s := &Stmt{}
	s.Finalize() // should not panic
}

func TestStmtExecEmptyParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER DEFAULT 1)")

	stmt, err := db.Prepare("INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStmtQueryEmptyParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	stmt, err := db.Prepare("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
}

// ---- Transactions ----

func TestTransaction(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec("INSERT INTO t VALUES (1, 'Alice')")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if _, err := rows.Next(); err != nil {
		t.Fatal(err)
	}
	if rows.ColumnInt64(0) != 1 {
		t.Fatalf("expected 1 row, got %d", rows.ColumnInt64(0))
	}
}

func TestTransactionRollback(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec("INSERT INTO t VALUES (2)"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = tx.Rollback()

	rows, err := db.Query("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if _, err := rows.Next(); err != nil {
		t.Fatal(err)
	}
	if rows.ColumnInt64(0) != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", rows.ColumnInt64(0))
	}
}

func TestTransactionNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.Begin()
	if err == nil {
		t.Fatal("expected error for nil DB begin")
	}
}

func TestBeginWithIsolation(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")

	tx, err := db.BeginWithIsolation(IsolationSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
}

func TestBeginWithIsolationNilDB(t *testing.T) {
	db := &DB{}
	_, err := db.BeginWithIsolation(IsolationSnapshot)
	if err == nil {
		t.Fatal("expected error for nil DB")
	}
}

func TestTxExecParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecParams("INSERT INTO t VALUES ($1, $2)", []Value{
		IntegerValue(1), TextValue("Alice"),
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	_ = tx.Commit()
}

func TestTxExecParamsEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecParams("INSERT INTO t VALUES (1)", nil)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = tx.Commit()
}

func TestTxExecNilTx(t *testing.T) {
	tx := &Tx{}
	_, err := tx.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected error for nil Tx exec")
	}
}

func TestTxExecParamsNilTx(t *testing.T) {
	tx := &Tx{}
	_, err := tx.ExecParams("SELECT $1", []Value{IntegerValue(1)})
	if err == nil {
		t.Fatal("expected error for nil Tx")
	}
}

func TestTxQuery(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	rows, err := tx.Query("SELECT id FROM t")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	hasRow, _ := rows.Next()
	if !hasRow {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected row in tx")
	}
	_ = rows.Close()
	_ = tx.Commit()
}

func TestTxQueryNilTx(t *testing.T) {
	tx := &Tx{}
	_, err := tx.Query("SELECT 1")
	if err == nil {
		t.Fatal("expected error for nil Tx query")
	}
}

func TestTxQueryParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'Alice')")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.QueryParams("SELECT name FROM t WHERE id = $1", []Value{IntegerValue(1)})
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	hasRow, _ := rows.Next()
	if !hasRow || rows.ColumnText(0) != "Alice" {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected Alice")
	}
	_ = rows.Close()
	_ = tx.Commit()
}

func TestTxQueryParamsNilTx(t *testing.T) {
	tx := &Tx{}
	_, err := tx.QueryParams("SELECT $1", []Value{IntegerValue(1)})
	if err == nil {
		t.Fatal("expected error for nil Tx")
	}
}

func TestTxQueryParamsEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.QueryParams("SELECT id FROM t", nil)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = rows.Close()
	_ = tx.Commit()
}

func TestTxStmtExec(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.StmtExec(stmt, []Value{IntegerValue(1), TextValue("Alice")})
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = tx.Commit()
}

func TestTxStmtExecNilTx(t *testing.T) {
	tx := &Tx{}
	s := &Stmt{}
	_, err := tx.StmtExec(s, nil)
	if err == nil {
		t.Fatal("expected error for nil Tx")
	}
}

func TestTxStmtExecNilStmt(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	s := &Stmt{}
	_, err = tx.StmtExec(s, nil)
	if err == nil {
		t.Fatal("expected error for nil Stmt")
	}
}

func TestTxStmtExecEmptyParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER DEFAULT 1)")

	stmt, err := db.Prepare("INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.StmtExec(stmt, nil)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = tx.Commit()
}

func TestTxStmtQuery(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'Alice')")

	stmt, err := db.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.StmtQuery(stmt, []Value{IntegerValue(1)})
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	hasRow, _ := rows.Next()
	if !hasRow || rows.ColumnText(0) != "Alice" {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected Alice")
	}
	_ = rows.Close()
	_ = tx.Commit()
}

func TestTxStmtQueryNilTx(t *testing.T) {
	tx := &Tx{}
	s := &Stmt{}
	_, err := tx.StmtQuery(s, nil)
	if err == nil {
		t.Fatal("expected error for nil Tx")
	}
}

func TestTxStmtQueryNilStmt(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	s := &Stmt{}
	_, err = tx.StmtQuery(s, nil)
	if err == nil {
		t.Fatal("expected error for nil Stmt")
	}
}

func TestTxStmtQueryEmptyParams(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	stmt, err := db.Prepare("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.StmtQuery(stmt, nil)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_ = rows.Close()
	_ = tx.Commit()
}

func TestCommitNilTx(t *testing.T) {
	tx := &Tx{}
	err := tx.Commit()
	if err == nil {
		t.Fatal("expected error for nil Tx commit")
	}
}

func TestRollbackNilTx(t *testing.T) {
	tx := &Tx{}
	err := tx.Rollback()
	if err != nil {
		t.Fatal("rollback nil Tx should return nil")
	}
}

// ---- FetchAll ----

func TestFetchAll(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 10; i++ {
		if _, err := db.ExecParams("INSERT INTO t VALUES ($1, $2)", []Value{IntegerValue(i), TextValue("row")}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	buf, err := rows.FetchAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) == 0 {
		t.Fatal("expected non-empty buffer")
	}
}

func TestRowsAffected(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1), (2), (3)")

	rows, err := db.Query("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	// Affected may be 0 for SELECT, just verify it doesn't crash
	_ = rows.Affected()
}

// ---- Value constructors and GoValueToFFI ----

func TestValueConstructors(t *testing.T) {
	v := NullValue()
	if v.Type != TypeNull {
		t.Fatalf("expected TypeNull, got %d", v.Type)
	}

	v = IntegerValue(42)
	if v.Type != TypeInteger || v.Integer != 42 {
		t.Fatalf("unexpected IntegerValue: %+v", v)
	}

	v = FloatValue(3.14)
	if v.Type != TypeFloat || v.Float != 3.14 {
		t.Fatalf("unexpected FloatValue: %+v", v)
	}

	v = TextValue("hello")
	if v.Type != TypeText || v.Text != "hello" {
		t.Fatalf("unexpected TextValue: %+v", v)
	}

	v = BoolValue(true)
	if v.Type != TypeBoolean || !v.Bool {
		t.Fatalf("unexpected BoolValue: %+v", v)
	}

	now := time.Now()
	v = TimestampValue(now)
	if v.Type != TypeTimestamp || !v.Timestamp.Equal(now) {
		t.Fatalf("unexpected TimestampValue: %+v", v)
	}

	v = JSONValue(`{"a":1}`)
	if v.Type != TypeJSON || v.Text != `{"a":1}` {
		t.Fatalf("unexpected JSONValue: %+v", v)
	}

	v = BlobValue([]byte{1, 2, 3})
	if v.Type != TypeBlob || len(v.Blob) != 3 {
		t.Fatalf("unexpected BlobValue: %+v", v)
	}
}

func TestGoValueToFFI(t *testing.T) {
	tests := []struct {
		input   any
		want    int32
		wantErr bool
	}{
		{nil, TypeNull, false},
		{int64(42), TypeInteger, false},
		{float64(3.14), TypeFloat, false},
		{true, TypeBoolean, false},
		{"hello", TypeText, false},
		{[]byte{1, 2}, TypeBlob, false},
		{time.Now(), TypeTimestamp, false},
		{int(42), TypeInteger, false},
		{int32(42), TypeInteger, false},
		{float32(3.14), TypeFloat, false},
		{struct{}{}, TypeNull, true}, // unsupported type → error
	}

	for _, tt := range tests {
		v, err := GoValueToFFI(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("GoValueToFFI(%T) expected error, got nil", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("GoValueToFFI(%T) unexpected error: %v", tt.input, err)
			continue
		}
		if v.Type != tt.want {
			t.Errorf("GoValueToFFI(%T) = type %d, want %d", tt.input, v.Type, tt.want)
		}
	}
}

// ---- ValueBuf pool ----

func TestValueBufPool(t *testing.T) {
	vb := AcquireValueBuf(3)
	if len(vb.Values) != 3 {
		t.Fatalf("expected length 3, got %d", len(vb.Values))
	}

	vb.Values[0] = IntegerValue(1)
	vb.Values[1] = TextValue("hello")
	vb.Values[2] = BlobValue([]byte{1})
	vb.Release()

	// Acquire again, should get pooled buffer
	vb2 := AcquireValueBuf(2)
	if len(vb2.Values) != 2 {
		t.Fatalf("expected length 2, got %d", len(vb2.Values))
	}
	vb2.Release()

	// Acquire larger than pool capacity
	vb3 := AcquireValueBuf(100)
	if len(vb3.Values) != 100 {
		t.Fatalf("expected length 100, got %d", len(vb3.Values))
	}
	vb3.Release()
}

func TestValueBufPoolZeroLength(t *testing.T) {
	vb := AcquireValueBuf(0)
	if len(vb.Values) != 0 {
		t.Fatalf("expected length 0, got %d", len(vb.Values))
	}
	vb.Release()
}

// ---- Timestamp edge cases ----

func TestTimestampRoundtrip(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE ts_rt (ts TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 3, 15, 12, 0, 0, 0, time.UTC)
	if _, err := db.ExecParams("INSERT INTO ts_rt VALUES ($1)", []Value{TimestampValue(now)}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT ts FROM ts_rt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	ts := rows.ColumnTimestamp(0)
	if !ts.Equal(now) {
		t.Fatalf("expected %v, got %v", now, ts)
	}
}

// ---- Boolean false param ----

func TestBooleanFalseParam(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE t (val BOOLEAN)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecParams("INSERT INTO t VALUES ($1)", []Value{BoolValue(false)}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	if rows.ColumnBool(0) {
		t.Fatal("expected false")
	}
}

// ---- Blob empty ----

func TestBlobEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE TABLE blob_empty (val VECTOR)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecParams("INSERT INTO blob_empty VALUES ($1)", []Value{BlobValue(nil)}); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT val FROM blob_empty")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	hasRow, _ := rows.Next()
	if !hasRow {
		t.Fatal("expected row")
	}
	blob := rows.ColumnBlob(0)
	if blob != nil {
		t.Fatalf("expected nil blob, got %v", blob)
	}
}
