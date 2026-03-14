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
package wasm

import (
	"context"
	"database/sql"
	"math"
	"os"
	"testing"
	"time"
)

var testEngine *Engine

func TestMain(m *testing.M) {
	wasmPath := os.Getenv("STOOLAP_WASM")
	if wasmPath == "" {
		wasmPath = "stoolap.wasm"
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		panic("cannot read WASM file: " + err.Error() + " (set STOOLAP_WASM env)")
	}

	ctx := context.Background()
	testEngine, err = NewEngine(ctx, wasmBytes)
	if err != nil {
		panic("cannot create engine: " + err.Error())
	}

	globalEngine = testEngine
	os.Exit(m.Run())
}

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := testEngine.OpenMemory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// ─── Direct API ─────────────────────────────────────────────────────────────

func TestVersion(t *testing.T) {
	v := testEngine.Version(context.Background())
	if v == "" {
		t.Fatal("version should not be empty")
	}
	t.Logf("stoolap version: %s", v)
}

func TestOpenClose(t *testing.T) {
	ctx := context.Background()
	db, err := testEngine.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db.Close() // double close safe
}

func TestOpenDSN(t *testing.T) {
	ctx := context.Background()
	db, err := testEngine.Open(ctx, "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestClone(t *testing.T) {
	ctx := context.Background()
	db, _ := testEngine.Open(ctx, "memory://mydb")
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO t VALUES (1)")

	clone, err := db.Clone(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()

	rows, _ := clone.Query(ctx, "SELECT COUNT(*) FROM t")
	defer rows.Close()
	rows.Next()
	var count int64
	rows.Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

func TestDirectCRUD(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	res, _ := db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	affected, _ := res.RowsAffected()
	if affected != 2 {
		t.Fatalf("expected 2 affected, got %d", affected)
	}
	lastID, _ := res.LastInsertId()
	_ = lastID

	db.Exec(ctx, "UPDATE t SET name = 'ALICE' WHERE id = 1")
	db.Exec(ctx, "DELETE FROM t WHERE id = 2")

	rows, _ := db.Query(ctx, "SELECT id, name FROM t")
	defer rows.Close()
	rows.Next()
	var id int64
	var name string
	rows.Scan(&id, &name)
	if name != "ALICE" {
		t.Fatalf("expected ALICE, got %s", name)
	}
}

func TestDirectExecParams(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)")
	db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4)", []any{int64(1), "alice", 99.5, true})
	db.ExecParams(ctx, "INSERT INTO t VALUES (2, 'bob', 88.0, false)", nil) // no-args path

	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	rows.Next()
	var count int64
	rows.Scan(&count)
	rows.Close()
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDirectQueryParams(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	rows, _ := db.QueryParams(ctx, "SELECT name FROM t WHERE id = $1", []any{int64(2)})
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}

	rows2, _ := db.QueryParams(ctx, "SELECT COUNT(*) FROM t", nil) // no-args path
	defer rows2.Close()
	rows2.Next()
	var count int64
	rows2.Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDirectTransaction(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO t VALUES (1)")
	tx.Exec(ctx, "INSERT INTO t VALUES (2)")
	tx.Commit()

	// Rollback
	tx2, _ := db.Begin(ctx)
	tx2.Exec(ctx, "INSERT INTO t VALUES (3)")
	tx2.Rollback()
	tx2.Rollback() // double rollback safe

	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	rows.Next()
	var count int64
	rows.Scan(&count)
	rows.Close()
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDirectTransactionExecParams(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val FLOAT)")

	tx, _ := db.Begin(ctx)
	tx.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), 3.14})
	tx.ExecParams(ctx, "INSERT INTO t VALUES (2, 2.71)", nil) // no-args path
	tx.Commit()

	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	rows.Next()
	var count int64
	rows.Scan(&count)
	rows.Close()
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDirectTransactionQuery(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	db.Exec(ctx, "INSERT INTO t VALUES (1), (2), (3)")

	tx, _ := db.Begin(ctx)
	rows, _ := tx.Query(ctx, "SELECT COUNT(*) FROM t")
	rows.Next()
	var count int64
	rows.Scan(&count)
	rows.Close()
	tx.Commit()
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

func TestDirectTransactionQueryParams(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	tx, _ := db.Begin(ctx)
	rows, err := tx.QueryParams(ctx, "SELECT name FROM t WHERE id = $1", []any{int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	tx.Commit()

	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}

	// Also test no-args path
	tx2, _ := db.Begin(ctx)
	rows2, _ := tx2.QueryParams(ctx, "SELECT COUNT(*) FROM t", nil)
	defer rows2.Close()
	rows2.Next()
	var count int64
	rows2.Scan(&count)
	tx2.Commit()
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
}

func TestDirectBeginTx(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	tx, _ := db.BeginTx(ctx, nil) // default
	tx.Rollback()

	tx2, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	tx2.Rollback()

	_, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation")
	}
}

func TestDirectPreparedExec(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	stmt, _ := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2)")
	for i := int64(1); i <= 5; i++ {
		stmt.ExecContext(ctx, []any{i, "user"})
	}
	stmt.Close()
	stmt.Close() // double close safe

	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	rows.Next()
	var count int64
	rows.Scan(&count)
	rows.Close()
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestDirectPreparedQuery(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	stmt, _ := db.Prepare(ctx, "SELECT name FROM t WHERE id = $1")
	defer stmt.Close()
	rows, _ := stmt.QueryContext(ctx, []any{int64(2)})
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}
}

func TestDirectAllTypes(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, `CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, j JSON)`)
	db.Exec(ctx, "INSERT INTO t VALUES (42, 3.14, 'hello', true, '2024-01-15 10:30:00', '{\"a\":1}')")

	rows, _ := db.Query(ctx, "SELECT i, f, s, b, ts, j FROM t")
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 6 {
		t.Fatalf("expected 6 cols, got %d", len(cols))
	}

	rows.Next()
	var i int64
	var f float64
	var s string
	var b bool
	var ts time.Time
	var j string
	rows.Scan(&i, &f, &s, &b, &ts, &j)

	if i != 42 {
		t.Fatalf("int: expected 42, got %d", i)
	}
	if math.Abs(f-3.14) > 0.001 {
		t.Fatalf("float: expected 3.14, got %f", f)
	}
	if s != "hello" {
		t.Fatalf("text: expected hello, got %s", s)
	}
	if !b {
		t.Fatal("bool: expected true")
	}
	if ts.IsZero() {
		t.Fatal("timestamp: expected non-zero")
	}
	if j != `{"a":1}` {
		t.Fatalf("json: expected {\"a\":1}, got %s", j)
	}
}

func TestDirectScanNullTypes(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.Exec(ctx, "INSERT INTO t VALUES (42, 3.14, 'hi', true, '2024-01-15 10:00:00')")
	db.Exec(ctx, "INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL)")

	// Non-null row
	rows, _ := db.Query(ctx, "SELECT i, f, s, b, ts FROM t WHERE i IS NOT NULL")
	defer rows.Close()
	rows.Next()
	var ni sql.NullInt64
	var nf sql.NullFloat64
	var ns sql.NullString
	var nb sql.NullBool
	var nt sql.NullTime
	rows.Scan(&ni, &nf, &ns, &nb, &nt)
	if !ni.Valid || ni.Int64 != 42 {
		t.Fatalf("NullInt64: %v", ni)
	}
	if !nf.Valid {
		t.Fatalf("NullFloat64: %v", nf)
	}
	if !ns.Valid || ns.String != "hi" {
		t.Fatalf("NullString: %v", ns)
	}
	if !nb.Valid || !nb.Bool {
		t.Fatalf("NullBool: %v", nb)
	}
	if !nt.Valid {
		t.Fatalf("NullTime: %v", nt)
	}

	// Null row
	rows2, _ := db.Query(ctx, "SELECT i, f, s, b, ts FROM t WHERE i IS NULL")
	defer rows2.Close()
	rows2.Next()
	var ni2 sql.NullInt64
	var nf2 sql.NullFloat64
	var ns2 sql.NullString
	var nb2 sql.NullBool
	var nt2 sql.NullTime
	rows2.Scan(&ni2, &nf2, &ns2, &nb2, &nt2)
	if ni2.Valid || nf2.Valid || ns2.Valid || nb2.Valid || nt2.Valid {
		t.Fatal("expected all invalid for null row")
	}
}

func TestDirectScanNullIntoTyped(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.Exec(ctx, "INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL)")

	rows, _ := db.Query(ctx, "SELECT i, f, s, b, ts FROM t")
	defer rows.Close()
	rows.Next()
	var i int64
	var f float64
	var s string
	var b bool
	var ts time.Time
	rows.Scan(&i, &f, &s, &b, &ts)
	if i != 0 || f != 0 || s != "" || b || !ts.IsZero() {
		t.Fatal("expected zero values for null")
	}
}

func TestDirectScanAny(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.Exec(ctx, "INSERT INTO t VALUES (42, 3.14, 'hello', true, '2024-01-15 10:30:00')")

	rows, _ := db.Query(ctx, "SELECT i, f, s, b, ts FROM t")
	defer rows.Close()
	rows.Next()
	var vi, vf, vs, vb, vts any
	rows.Scan(&vi, &vf, &vs, &vb, &vts)

	if vi.(int64) != 42 {
		t.Fatalf("expected 42, got %v", vi)
	}
	if _, ok := vf.(float64); !ok {
		t.Fatalf("expected float64, got %T", vf)
	}
	if vs.(string) != "hello" {
		t.Fatalf("expected hello, got %v", vs)
	}
	if vb.(bool) != true {
		t.Fatalf("expected true, got %v", vb)
	}
	if _, ok := vts.(time.Time); !ok {
		t.Fatalf("expected time.Time, got %T", vts)
	}
}

func TestDirectScanAnyNull(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, s TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (NULL, NULL)")

	rows, _ := db.Query(ctx, "SELECT i, s FROM t")
	defer rows.Close()
	rows.Next()
	var vi, vs any
	rows.Scan(&vi, &vs)
	if vi != nil || vs != nil {
		t.Fatalf("expected nil, got %v %v", vi, vs)
	}
}

func TestDirectRowsClosed(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER)")
	db.Exec(ctx, "INSERT INTO t VALUES (1)")

	rows, _ := db.Query(ctx, "SELECT id FROM t")
	rows.Close()
	if rows.Next() {
		t.Fatal("Next after Close should be false")
	}
	if err := rows.Scan(); err == nil {
		t.Fatal("Scan after Close should error")
	}
	rows.Close() // double close safe
}

func TestDirectParamTimestamp(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER, ts TIMESTAMP)")
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), ts})

	rows, _ := db.Query(ctx, "SELECT ts FROM t WHERE id = 1")
	defer rows.Close()
	rows.Next()
	var got time.Time
	rows.Scan(&got)
	if got.Year() != 2024 || got.Month() != 6 {
		t.Fatalf("expected 2024-06, got %v", got)
	}
}

func TestDirectParamNull(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER, name TEXT)")
	db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), nil})

	rows, _ := db.Query(ctx, "SELECT name FROM t WHERE id = 1")
	defer rows.Close()
	rows.Next()
	var ns sql.NullString
	rows.Scan(&ns)
	if ns.Valid {
		t.Fatal("expected null")
	}
}

// ─── database/sql Driver ────────────────────────────────────────────────────

func TestDatabaseSQLBasic(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.ExecContext(ctx, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')")

	rows, _ := db.QueryContext(ctx, "SELECT id, name FROM t ORDER BY id")
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		var name string
		rows.Scan(&id, &name)
		ids = append(ids, id)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ids))
	}
}

func TestDatabaseSQLPing(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLTransaction(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	tx, _ := db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, "INSERT INTO t VALUES (1)")
	tx.ExecContext(ctx, "INSERT INTO t VALUES (2)")
	tx.Commit()

	var count int64
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}

	tx2, _ := db.BeginTx(ctx, nil)
	tx2.ExecContext(ctx, "INSERT INTO t VALUES (3)")
	tx2.Rollback()

	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2 after rollback, got %d", count)
	}
}

func TestDatabaseSQLPrepared(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	stmt, _ := db.PrepareContext(ctx, "INSERT INTO t VALUES ($1, $2)")
	stmt.ExecContext(ctx, 1, "alice")
	stmt.ExecContext(ctx, 2, "bob")
	stmt.Close()

	var count int64
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDatabaseSQLExecResult(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE exec_result (id INTEGER PRIMARY KEY, name TEXT)")
	res, err := db.ExecContext(ctx, "INSERT INTO exec_result VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := res.RowsAffected()
	if affected != 3 {
		t.Fatalf("expected 3, got %d", affected)
	}
}

func TestDatabaseSQLQueryRow(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE qrow (id INTEGER PRIMARY KEY, name TEXT)")
	db.ExecContext(ctx, "INSERT INTO qrow VALUES (1, 'alice')")

	var name string
	err := db.QueryRowContext(ctx, "SELECT name FROM qrow WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}
}

// ─── File Persistence ───────────────────────────────────────────────────────

func TestFilePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	wasmPath := os.Getenv("STOOLAP_WASM")
	if wasmPath == "" {
		wasmPath = "stoolap.wasm"
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Skip("WASM file not found")
	}

	engine, err := NewEngineWithFS(ctx, wasmBytes, tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	db, err := engine.Open(ctx, "file:///data/testdb")
	if err != nil {
		t.Skip("file persistence not supported by this WASM build:", err)
	}

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
	db.Close()

	db2, err := engine.Open(ctx, "file:///data/testdb")
	if err != nil {
		t.Fatal("reopen:", err)
	}
	defer db2.Close()
	rows, _ := db2.Query(ctx, "SELECT COUNT(*) FROM t")
	defer rows.Close()
	rows.Next()
	var count int64
	rows.Scan(&count)
	if count != 2 {
		t.Fatalf("persistence failed: got %d rows", count)
	}
	t.Log("File persistence works via WASI")
}

// ─── Error Paths ────────────────────────────────────────────────────────────

func TestErrorExec(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(ctx, "INVALID SQL")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestErrorQuery(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Query(ctx, "SELECT * FROM nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestErrorPrepare(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Prepare(ctx, "INVALID SQL")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDatabaseSQLAllColumnTypes(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE types_test (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.ExecContext(ctx, "INSERT INTO types_test VALUES (42, 3.14, 'hello', true, '2024-06-15 12:00:00')")
	db.ExecContext(ctx, "INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL)")

	// Non-null row
	var i sql.NullInt64
	var f sql.NullFloat64
	var s sql.NullString
	var b sql.NullBool
	var ts sql.NullString // timestamp as string through driver
	err := db.QueryRowContext(ctx, "SELECT i, f, s, b, ts FROM types_test WHERE i IS NOT NULL").Scan(&i, &f, &s, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}
	if !i.Valid || i.Int64 != 42 {
		t.Fatalf("int: %v", i)
	}
	if !f.Valid {
		t.Fatalf("float: %v", f)
	}
	if !s.Valid || s.String != "hello" {
		t.Fatalf("text: %v", s)
	}

	// Null row
	err = db.QueryRowContext(ctx, "SELECT i, f, s, b, ts FROM types_test WHERE i IS NULL").Scan(&i, &f, &s, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}
	if i.Valid || f.Valid || s.Valid {
		t.Fatal("expected null")
	}
}

func TestDatabaseSQLTransactionWithParams(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE tx_params (id INTEGER PRIMARY KEY, name TEXT)")

	tx, _ := db.BeginTx(ctx, nil)
	tx.ExecContext(ctx, "INSERT INTO tx_params VALUES (1, 'in_tx')")
	tx.Commit()

	var name string
	db.QueryRowContext(ctx, "SELECT name FROM tx_params WHERE id = 1").Scan(&name)
	if name != "in_tx" {
		t.Fatalf("expected in_tx, got %s", name)
	}
}

func TestDatabaseSQLPreparedQuery(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE pq (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
	db.ExecContext(ctx, "INSERT INTO pq VALUES (1, 'alice', 95.5), (2, 'bob', 88.0)")

	stmt, _ := db.PrepareContext(ctx, "SELECT name, score FROM pq WHERE id = $1")
	defer stmt.Close()

	var name string
	var score float64
	stmt.QueryRowContext(ctx, 1).Scan(&name, &score)
	if name != "alice" || score < 95 {
		t.Fatalf("unexpected: %s %f", name, score)
	}

	stmt.QueryRowContext(ctx, 2).Scan(&name, &score)
	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}
}

func TestDatabaseSQLUnsupportedIsolation(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	_, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation")
	}
}

func TestDatabaseSQLSnapshotIsolation(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE snap (id INTEGER PRIMARY KEY)")
	db.ExecContext(ctx, "INSERT INTO snap VALUES (1)")

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var count int64
	tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM snap").Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

func TestDatabaseSQLClosedRows(t *testing.T) {
	db, _ := sql.Open("stoolap-wasm", "memory://")
	defer db.Close()
	ctx := context.Background()

	db.ExecContext(ctx, "CREATE TABLE cr (id INTEGER)")
	db.ExecContext(ctx, "INSERT INTO cr VALUES (1)")

	rows, _ := db.QueryContext(ctx, "SELECT id FROM cr")
	rows.Close()
	if rows.Next() {
		t.Fatal("Next after Close should be false")
	}
}

func TestEngineClose(t *testing.T) {
	wasmPath := os.Getenv("STOOLAP_WASM")
	if wasmPath == "" {
		wasmPath = "stoolap.wasm"
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Skip("WASM not found")
	}

	ctx := context.Background()
	engine, err := NewEngine(ctx, wasmBytes)
	if err != nil {
		t.Fatal(err)
	}

	db, _ := engine.OpenMemory(ctx)
	db.Exec(ctx, "SELECT 1")
	db.Close()

	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Double close safe
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSetWASM(t *testing.T) {
	wasmPath := os.Getenv("STOOLAP_WASM")
	if wasmPath == "" {
		wasmPath = "stoolap.wasm"
	}
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Skip("WASM not found")
	}
	if err := SetWASM(context.Background(), wasmBytes); err != nil {
		t.Fatal(err)
	}
}
