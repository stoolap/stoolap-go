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
package stoolap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var testDBCounter atomic.Int64

// openTestDB opens a uniquely-named in-memory database for test isolation.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	n := testDBCounter.Add(1)
	dsn := fmt.Sprintf("memory://puregotest%d", n)
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open(%s): %v", dsn, err)
	}
	return db
}

func TestVersion(t *testing.T) {
	v, err := Version()
	if err != nil {
		t.Fatal(err)
	}
	if v == "" {
		t.Fatal("empty version")
	}
	t.Logf("stoolap version: %s", v)
}

func TestOpenClose(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db.Close() // double close safe
}

func TestCRUD(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val FLOAT)"); err != nil {
		t.Fatal("CREATE:", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, 'carol', 3.5)"); err != nil {
		t.Fatal("INSERT:", err)
	}

	rows, err := db.Query(ctx, "SELECT id, name, val FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 3 || cols[0] != "id" {
		t.Fatalf("unexpected columns: %v", cols)
	}

	var names []string
	for rows.Next() {
		var id int64
		var name string
		var val float64
		rows.Scan(&id, &name, &val)
		names = append(names, name)
	}
	if len(names) != 3 || names[0] != "alice" || names[2] != "carol" {
		t.Fatalf("unexpected: %v", names)
	}

	res, _ := db.Exec(ctx, "UPDATE t SET name = 'ALICE' WHERE id = 1")
	affected, _ := res.RowsAffected()
	if affected != 1 {
		t.Fatalf("expected 1 affected, got %d", affected)
	}

	db.Exec(ctx, "DELETE FROM t WHERE id = 3")

	rows2, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	defer rows2.Close()
	rows2.Next()
	var count int64
	rows2.Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestExecParams(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")

	_, err := db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3)", []any{int64(1), "alice", 95.5})
	if err != nil {
		t.Fatal("INSERT:", err)
	}
	_, err = db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3)", []any{int64(2), "bob", 88.0})
	if err != nil {
		t.Fatal("INSERT:", err)
	}

	rows, err := db.QueryParams(ctx, "SELECT name, score FROM t WHERE id = $1", []any{int64(1)})
	if err != nil {
		t.Fatal("SELECT:", err)
	}
	defer rows.Close()
	rows.Next()
	var name string
	var score float64
	rows.Scan(&name, &score)
	if name != "alice" || score != 95.5 {
		t.Fatalf("expected alice/95.5, got %s/%f", name, score)
	}
}

func TestTransaction(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	tx, _ := db.Begin(ctx)
	tx.Exec(ctx, "INSERT INTO t VALUES (1)")
	tx.Exec(ctx, "INSERT INTO t VALUES (2)")
	tx.Commit()

	tx2, _ := db.Begin(ctx)
	tx2.Exec(ctx, "INSERT INTO t VALUES (3)")
	tx2.Rollback()
	tx2.Rollback() // double rollback safe

	rows, _ := db.Query(ctx, "SELECT COUNT(*) FROM t")
	defer rows.Close()
	rows.Next()
	var count int64
	rows.Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestTransactionParams(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	tx, _ := db.Begin(ctx)
	_, err := tx.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), "alice"})
	if err != nil {
		t.Fatal("tx INSERT:", err)
	}
	tx.Commit()

	rows, err := db.Query(ctx, "SELECT name FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}
}

func TestTransactionQueryParams(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	tx, _ := db.Begin(ctx)
	rows, err := tx.QueryParams(ctx, "SELECT name FROM t WHERE id = $1", []any{int64(2)})
	if err != nil {
		t.Fatal("tx QueryParams:", err)
	}
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	tx.Rollback()

	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}
}

func TestClone(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER)")
	db.Exec(ctx, "INSERT INTO t VALUES (1)")

	clone, err := db.Clone()
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

func TestScanAny(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN)")
	db.Exec(ctx, "INSERT INTO t VALUES (42, 3.14, 'hello', true)")
	db.Exec(ctx, "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)")

	rows, _ := db.Query(ctx, "SELECT i, f, s, b FROM t WHERE i IS NOT NULL")
	defer rows.Close()
	rows.Next()
	var vi, vf, vs, vb any
	rows.Scan(&vi, &vf, &vs, &vb)
	if vi.(int64) != 42 {
		t.Fatalf("expected 42, got %v", vi)
	}
	if vs.(string) != "hello" {
		t.Fatalf("expected hello, got %v", vs)
	}

	rows2, _ := db.Query(ctx, "SELECT i, s FROM t WHERE i IS NULL")
	defer rows2.Close()
	rows2.Next()
	var ni, ns any
	rows2.Scan(&ni, &ns)
	if ni != nil || ns != nil {
		t.Fatalf("expected nil, got %v %v", ni, ns)
	}
}

func TestScanNullTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.Exec(ctx, "INSERT INTO t VALUES (42, 3.14, 'hello', true, '2025-01-15T10:30:00Z')")
	db.Exec(ctx, "INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL)")

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
	if !ns.Valid || ns.String != "hello" {
		t.Fatalf("NullString: %v", ns)
	}
	if !nb.Valid || !nb.Bool {
		t.Fatalf("NullBool: %v", nb)
	}

	rows2, _ := db.Query(ctx, "SELECT i, f, s FROM t WHERE i IS NULL")
	defer rows2.Close()
	rows2.Next()
	var ni2 sql.NullInt64
	var nf2 sql.NullFloat64
	var ns2 sql.NullString
	rows2.Scan(&ni2, &nf2, &ns2)
	if ni2.Valid || nf2.Valid || ns2.Valid {
		t.Fatalf("expected all invalid, got %v %v %v", ni2, nf2, ns2)
	}
}

func TestErrorPaths(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "INVALID SQL")
	if err == nil {
		t.Fatal("expected error")
	}

	_, err = db.Query(ctx, "SELECT * FROM nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPreparedStatement(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	// Prepared exec
	stmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx, []any{int64(3), "carol"})
	if err != nil {
		t.Fatal("stmt exec:", err)
	}
	stmt.Close()

	// Prepared query
	stmt2, err := db.Prepare(ctx, "SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := stmt2.QueryContext(ctx, []any{int64(3)})
	if err != nil {
		t.Fatal("stmt query:", err)
	}
	defer rows.Close()
	rows.Next()
	var name string
	rows.Scan(&name)
	if name != "carol" {
		t.Fatalf("expected carol, got %s", name)
	}
	stmt2.Close()
	stmt2.Close() // double close safe
}

func TestFetchAll(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
	db.Exec(ctx, "INSERT INTO t VALUES (1, 'alice', 95.5), (2, 'bob', 88.0), (3, 'carol', 92.3)")

	rows, err := db.Query(ctx, "SELECT id, name, score FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	allRows, err := rows.FetchAll()
	if err != nil {
		t.Fatal("FetchAll:", err)
	}
	if len(allRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(allRows))
	}
	if allRows[0][1].(string) != "alice" {
		t.Fatalf("expected alice, got %v", allRows[0][1])
	}
	if allRows[2][2].(float64) != 92.3 {
		t.Fatalf("expected 92.3, got %v", allRows[2][2])
	}
}

func TestBeginTx(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, nil)
	tx.Rollback()

	_, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation")
	}
}

func TestParamTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")

	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	_, err := db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4, $5)",
		[]any{int64(42), 3.14, "hello", true, ts})
	if err != nil {
		t.Fatal("INSERT:", err)
	}

	// Null params
	_, err = db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4, $5)",
		[]any{nil, nil, nil, nil, nil})
	if err != nil {
		t.Fatal("INSERT NULL:", err)
	}

	rows, _ := db.Query(ctx, "SELECT i, f, s, b FROM t WHERE i IS NOT NULL")
	defer rows.Close()
	rows.Next()
	var vi int64
	var vf float64
	var vs string
	var vb bool
	rows.Scan(&vi, &vf, &vs, &vb)
	if vi != 42 || vs != "hello" || !vb {
		t.Fatalf("unexpected: %d %f %s %v", vi, vf, vs, vb)
	}
}

// ─── database/sql driver tests ──────────────────────────────────────────────

func TestDatabaseSQLBasic(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqltest%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal("ping:", err)
	}

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal("CREATE:", err)
	}
	_, err = db.Exec("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatal("INSERT:", err)
	}

	var name string
	err = db.QueryRow("SELECT name FROM t WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal("QueryRow:", err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}

	rows, err := db.Query("SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatal("Query:", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var id int64
		var n string
		rows.Scan(&id, &n)
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestDatabaseSQLParams(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlparams%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
	_, err = db.Exec("INSERT INTO t VALUES ($1, $2, $3)", int64(1), "alice", 95.5)
	if err != nil {
		t.Fatal("INSERT:", err)
	}

	var name string
	var score float64
	err = db.QueryRow("SELECT name, score FROM t WHERE id = $1", int64(1)).Scan(&name, &score)
	if err != nil {
		t.Fatal("QueryRow:", err)
	}
	if name != "alice" || score != 95.5 {
		t.Fatalf("expected alice/95.5, got %s/%f", name, score)
	}
}

func TestDatabaseSQLTransaction(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqltx%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tx.Exec("INSERT INTO t VALUES (1)")
	tx.Exec("INSERT INTO t VALUES (2)")
	tx.Commit()

	tx2, _ := db.Begin()
	tx2.Exec("INSERT INTO t VALUES (3)")
	tx2.Rollback()

	var count int64
	db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestDatabaseSQLPrepared(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlprep%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

	stmt, err := db.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	var name string
	err = stmt.QueryRow(int64(2)).Scan(&name)
	if err != nil {
		t.Fatal("stmt QueryRow:", err)
	}
	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}
}

func TestDatabaseSQLExecResult(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlres%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

	res, err := db.Exec("UPDATE t SET name = 'updated' WHERE id > 1")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := res.RowsAffected()
	if affected != 2 {
		t.Fatalf("expected 2 affected, got %d", affected)
	}
}

// ─── Additional Direct API tests (parity with wasm driver) ─────────────────

func TestAllTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestScanNullIntoTyped(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestScanAnyAllTypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestRowsClosed(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestTransactionQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestTransactionExecParamsNoArgs(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val FLOAT)")

	tx, _ := db.Begin(ctx)
	tx.ExecParams(ctx, "INSERT INTO t VALUES (1, 3.14)", nil)
	tx.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(2), 2.71})
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

func TestPreparedExecLoop(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestPreparedStatementParamReuse(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	insertStmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer insertStmt.Close()

	longName := "this-is-a-much-longer-name-than-the-next-one"
	if _, err := insertStmt.ExecContext(ctx, []any{int64(1), longName}); err != nil {
		t.Fatal(err)
	}
	if _, err := insertStmt.ExecContext(ctx, []any{int64(2), "x"}); err != nil {
		t.Fatal(err)
	}

	queryStmt, err := db.Prepare(ctx, "SELECT id FROM t WHERE name = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer queryStmt.Close()

	rows, err := queryStmt.QueryContext(ctx, []any{longName})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected row for long name")
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}

	rows, err = queryStmt.QueryContext(ctx, []any{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected row for short name")
	}
	if err := rows.Scan(&id); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if id != 2 {
		t.Fatalf("expected id 2, got %d", id)
	}
}

func TestBeginTxSnapshot(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
}

func TestParamTimestamp(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestParamNull(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

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

func TestErrorPrepare(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Prepare(ctx, "INVALID SQL")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ─── Additional database/sql tests (parity with wasm driver) ───────────────

func TestDatabaseSQLQueryRow(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlqrow%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'alice')")

	var name string
	err = db.QueryRow("SELECT name FROM t WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %s", name)
	}
}

func TestDatabaseSQLAllColumnTypes(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqltypes%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)")
	db.Exec("INSERT INTO t VALUES (42, 3.14, 'hello', true, '2024-06-15 12:00:00')")
	db.Exec("INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL)")

	var i sql.NullInt64
	var f sql.NullFloat64
	var s sql.NullString
	var b sql.NullBool
	var ts sql.NullString
	err = db.QueryRow("SELECT i, f, s, b, ts FROM t WHERE i IS NOT NULL").Scan(&i, &f, &s, &b, &ts)
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

	err = db.QueryRow("SELECT i, f, s, b, ts FROM t WHERE i IS NULL").Scan(&i, &f, &s, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}
	if i.Valid || f.Valid || s.Valid {
		t.Fatal("expected null")
	}
}

func TestDatabaseSQLTransactionWithParams(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqltxp%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	tx, _ := db.Begin()
	tx.Exec("INSERT INTO t VALUES (1, 'in_tx')")
	tx.Commit()

	var name string
	db.QueryRow("SELECT name FROM t WHERE id = 1").Scan(&name)
	if name != "in_tx" {
		t.Fatalf("expected in_tx, got %s", name)
	}
}

func TestDatabaseSQLPreparedQuery(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlpq%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)")
	db.Exec("INSERT INTO t VALUES (1, 'alice', 95.5), (2, 'bob', 88.0)")

	stmt, _ := db.Prepare("SELECT name, score FROM t WHERE id = $1")
	defer stmt.Close()

	var name string
	var score float64
	stmt.QueryRow(int64(1)).Scan(&name, &score)
	if name != "alice" || score < 95 {
		t.Fatalf("unexpected: %s %f", name, score)
	}

	stmt.QueryRow(int64(2)).Scan(&name, &score)
	if name != "bob" {
		t.Fatalf("expected bob, got %s", name)
	}
}

func TestDatabaseSQLUnsupportedIsolation(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqliso%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Fatal("expected error for unsupported isolation")
	}
}

func TestDatabaseSQLSnapshotIsolation(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlsnap%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	db.Exec("INSERT INTO t VALUES (1)")

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var count int64
	tx.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

func TestDatabaseSQLClosedRows(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlcr%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")

	rows, _ := db.Query("SELECT id FROM t")
	rows.Close()
	if rows.Next() {
		t.Fatal("Next after Close should be false")
	}
}

func TestDatabaseSQLPreparedExec(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqlpe%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	stmt, _ := db.Prepare("INSERT INTO t VALUES ($1, $2)")
	stmt.Exec(int64(1), "alice")
	stmt.Exec(int64(2), "bob")
	stmt.Close()

	var count int64
	db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count)
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}
}

func TestErrorCodes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), "alice@example.com"})

	tests := []struct {
		name    string
		query   string
		args    []any
		code    ErrorCode
		isConst bool
	}{
		{
			name:    "primary key duplicate",
			query:   "INSERT INTO t VALUES ($1, $2)",
			args:    []any{int64(1), "bob@example.com"},
			code:    ErrPrimaryKeyConstraint,
			isConst: true,
		},
		{
			name:    "unique constraint",
			query:   "INSERT INTO t VALUES ($1, $2)",
			args:    []any{int64(2), "alice@example.com"},
			code:    ErrUniqueConstraint,
			isConst: true,
		},
		{
			name:    "table not found",
			query:   "SELECT * FROM nonexistent",
			args:    nil,
			code:    ErrTableNotFound,
			isConst: false,
		},
		{
			name:    "table already exists",
			query:   "CREATE TABLE t (x INTEGER)",
			args:    nil,
			code:    ErrTableExists,
			isConst: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if len(tc.args) > 0 {
				_, err = db.ExecParams(ctx, tc.query, tc.args)
			} else if tc.code == ErrTableNotFound {
				_, err = db.Query(ctx, tc.query)
			} else {
				_, err = db.Exec(ctx, tc.query)
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			var stErr *Error
			if !errors.As(err, &stErr) {
				t.Fatalf("expected *stoolap.Error, got %T: %v", err, err)
			}
			if stErr.Code() != tc.code {
				t.Errorf("expected code %d, got %d; msg: %s", tc.code, stErr.Code(), stErr.Error())
			}
			if tc.isConst && !stErr.IsConstraintViolation() {
				t.Error("expected IsConstraintViolation() == true")
			}
		})
	}
}

func TestClosedDBReturnsError(t *testing.T) {
	db := openTestDB(t)
	db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "SELECT 1"); err != errDBClosed {
		t.Errorf("Exec on closed DB: got %v, want errDBClosed", err)
	}
	if _, err := db.Query(ctx, "SELECT 1"); err != errDBClosed {
		t.Errorf("Query on closed DB: got %v, want errDBClosed", err)
	}
	if _, err := db.Prepare(ctx, "SELECT 1"); err != errDBClosed {
		t.Errorf("Prepare on closed DB: got %v, want errDBClosed", err)
	}
	if _, err := db.Begin(ctx); err != errDBClosed {
		t.Errorf("Begin on closed DB: got %v, want errDBClosed", err)
	}
	if _, err := db.Clone(); err != errDBClosed {
		t.Errorf("Clone on closed DB: got %v, want errDBClosed", err)
	}
}

func TestCommittedTxReturnsError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(ctx, "INSERT INTO t VALUES (1)"); err != errTxDone {
		t.Errorf("Exec on committed tx: got %v, want errTxDone", err)
	}
	if _, err := tx.Query(ctx, "SELECT 1"); err != errTxDone {
		t.Errorf("Query on committed tx: got %v, want errTxDone", err)
	}
}

func TestPreparedExecBatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2, $3)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Empty batch is a no-op
	res, err := stmt.ExecBatch(ctx, nil)
	if err != nil {
		t.Fatal("empty batch:", err)
	}
	if n, _ := res.RowsAffected(); n != 0 {
		t.Fatalf("expected 0 affected, got %d", n)
	}

	rows := [][]any{
		{int64(1), "alice", 95.5},
		{int64(2), "bob", 88.0},
		{int64(3), "", nil},
	}
	res, err = stmt.ExecBatch(ctx, rows)
	if err != nil {
		t.Fatal("batch:", err)
	}
	if n, _ := res.RowsAffected(); n != 3 {
		t.Fatalf("expected 3 affected, got %d", n)
	}

	countRows := func() int64 {
		r, err := db.Query(ctx, "SELECT COUNT(*) FROM t")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		r.Next()
		var n int64
		r.Scan(&n)
		return n
	}
	if n := countRows(); n != 3 {
		t.Fatalf("expected 3 rows, got %d", n)
	}
	r, err := db.Query(ctx, "SELECT name, score FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for r.Next() {
		var name string
		var score any
		if err := r.Scan(&name, &score); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s|%v", name, score))
	}
	r.Close()
	want := []string{"alice|95.5", "bob|88", "|<nil>"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// Ragged rows are rejected before touching the engine
	_, err = stmt.ExecBatch(ctx, [][]any{{int64(4), "x", 1.0}, {int64(5)}})
	if !errors.Is(err, errBatchRowLength) {
		t.Fatalf("expected errBatchRowLength, got %v", err)
	}

	// A failing row rolls back the whole batch
	_, err = stmt.ExecBatch(ctx, [][]any{
		{int64(10), "ten", 1.0},
		{int64(1), "dup", 1.0},
	})
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	if n := countRows(); n != 3 {
		t.Fatalf("expected rollback to keep 3 rows, got %d", n)
	}

	// Scratch buffers are reused across batches of different sizes
	big := make([][]any, 200)
	for i := range big {
		big[i] = []any{int64(100 + i), "user" + string(rune('a'+i%26)), float64(i)}
	}
	res, err = stmt.ExecBatch(ctx, big)
	if err != nil {
		t.Fatal("big batch:", err)
	}
	if n, _ := res.RowsAffected(); n != 200 {
		t.Fatalf("expected 200 affected, got %d", n)
	}

	// Statements without parameters run once per row
	noParam, err := db.Prepare(ctx, "DELETE FROM t WHERE id = (SELECT MAX(id) FROM t)")
	if err != nil {
		t.Fatal(err)
	}
	defer noParam.Close()
	res, err = noParam.ExecBatch(ctx, [][]any{{}, {}})
	if err != nil {
		t.Fatal("no-param batch:", err)
	}
	if n, _ := res.RowsAffected(); n != 2 {
		t.Fatalf("expected 2 affected, got %d", n)
	}

	stmt.Close()
	if _, err := stmt.ExecBatch(ctx, rows); !errors.Is(err, errStmtClosed) {
		t.Fatalf("expected errStmtClosed, got %v", err)
	}
}

func TestNamedParams(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)"); err != nil {
		t.Fatal(err)
	}

	res, err := db.ExecNamed(ctx, "INSERT INTO t VALUES (:id, :name, :score, :active)", []sql.NamedArg{
		sql.Named("id", int64(1)),
		sql.Named("name", "alice"),
		sql.Named("score", 95.5),
		sql.Named("active", true),
	})
	if err != nil {
		t.Fatal("exec named:", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 affected, got %d", n)
	}

	// Same name used twice, plus a NULL
	_, err = db.ExecNamed(ctx, "INSERT INTO t VALUES (:id, :name, :id, :flag)", []sql.NamedArg{
		sql.Named("id", int64(2)),
		sql.Named("name", "bob"),
		sql.Named("flag", nil),
	})
	if err != nil {
		t.Fatal("exec named reuse:", err)
	}

	rows, err := db.QueryNamed(ctx, "SELECT name, score FROM t WHERE score > :min ORDER BY id", []sql.NamedArg{sql.Named("min", 50.0)})
	if err != nil {
		t.Fatal("query named:", err)
	}
	var got []string
	for rows.Next() {
		var name string
		var score float64
		if err := rows.Scan(&name, &score); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s|%v", name, score))
	}
	rows.Close()
	if fmt.Sprint(got) != "[alice|95.5]" {
		t.Fatalf("got %v", got)
	}

	// Transaction path
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecNamed(ctx, "UPDATE t SET name = :n WHERE id = :id", []sql.NamedArg{sql.Named("n", "carol"), sql.Named("id", int64(2))}); err != nil {
		t.Fatal("tx exec named:", err)
	}
	r2, err := tx.QueryNamed(ctx, "SELECT name FROM t WHERE id = :id", []sql.NamedArg{sql.Named("id", int64(2))})
	if err != nil {
		t.Fatal("tx query named:", err)
	}
	var name string
	r2.Next()
	r2.Scan(&name)
	r2.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if name != "carol" {
		t.Fatalf("expected carol, got %q", name)
	}

	// Empty name rejected before the engine sees it
	if _, err := db.ExecNamed(ctx, "SELECT :x", []sql.NamedArg{{Value: 1}}); !errors.Is(err, errEmptyParamName) {
		t.Fatalf("expected errEmptyParamName, got %v", err)
	}
	// Empty args fall back to plain exec
	if _, err := db.ExecNamed(ctx, "DELETE FROM t WHERE id = 99", nil); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLNamedArgs(t *testing.T) {
	db, err := sql.Open("stoolap", fmt.Sprintf("memory://sqlnamed%d", testDBCounter.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (:id, :name)", sql.Named("id", 1), sql.Named("name", "alice")); err != nil {
		t.Fatal("exec:", err)
	}

	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM t WHERE id = :id", sql.Named("id", 1)).Scan(&name); err != nil {
		t.Fatal("query:", err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %q", name)
	}

	// Inside a database/sql transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO t VALUES (:id, :name)", sql.Named("id", 2), sql.Named("name", "bob")); err != nil {
		t.Fatal("tx exec:", err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT name FROM t WHERE id = :id", sql.Named("id", 2)).Scan(&name); err != nil {
		t.Fatal("tx query:", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if name != "bob" {
		t.Fatalf("expected bob, got %q", name)
	}

	// Mixing named and positional is rejected
	_, err = db.ExecContext(ctx, "INSERT INTO t VALUES (:id, ?)", sql.Named("id", 3), "x")
	if !errors.Is(err, errMixedArgs) {
		t.Fatalf("expected errMixedArgs, got %v", err)
	}

	// Prepared statements take named args too, for queries and executes.
	stmt, err := db.PrepareContext(ctx, "SELECT name FROM t WHERE id = :id")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.QueryRowContext(ctx, sql.Named("id", 2)).Scan(&name); err != nil {
		t.Fatal("prepared named query:", err)
	}
	if name != "bob" {
		t.Fatalf("expected bob, got %q", name)
	}
	upd, err := db.PrepareContext(ctx, "UPDATE t SET name = :name WHERE id = :id")
	if err != nil {
		t.Fatal(err)
	}
	defer upd.Close()
	res, err := upd.ExecContext(ctx, sql.Named("name", "robert"), sql.Named("id", 2))
	if err != nil {
		t.Fatal("prepared named exec:", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 affected, got %d", n)
	}
	if err := stmt.QueryRowContext(ctx, sql.Named("id", 2)).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "robert" {
		t.Fatalf("expected robert, got %q", name)
	}
}

func TestDatabaseSQLTxPrepared(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqltxp%d", testDBCounter.Add(1))
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(int64(1), "alice"); err != nil {
		t.Fatal("exec in tx:", err)
	}

	// The transaction sees its own write through a prepared query.
	q, err := tx.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	var name string
	if err := q.QueryRow(int64(1)).Scan(&name); err != nil {
		t.Fatal("query in tx:", err)
	}
	if name != "alice" {
		t.Fatalf("expected alice, got %q", name)
	}
	q.Close()
	stmt.Close()

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("prepared statement ran outside the transaction: %d rows survived rollback", count)
	}
}

// seedRows creates t(id, name, score, active) with n rows.
func seedRows(t *testing.T, db *DB, n int) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)"); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	batch := make([][]any, n)
	for i := range n {
		batch[i] = []any{int64(i + 1), fmt.Sprintf("user_%d", i+1), float64(i) / 2, i%3 == 0}
	}
	if _, err := stmt.ExecBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
}

func TestRowsChunkedScan(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	const n = 1000 // crosses the 32, 64, 128 and 256 row chunk sizes
	seedRows(t, db, n)

	rows, err := db.Query(ctx, "SELECT id, name, score, active FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if cols := rows.Columns(); len(cols) != 4 || cols[1] != "name" || cols[3] != "active" {
		t.Fatalf("columns before Next: %v", cols)
	}

	var id int64
	var name string
	var score float64
	var active bool
	count := 0
	for rows.Next() {
		if err := rows.Scan(&id, &name, &score, &active); err != nil {
			t.Fatal(err)
		}
		count++
		if id != int64(count) || name != fmt.Sprintf("user_%d", count) || score != float64(count-1)/2 || active != ((count-1)%3 == 0) {
			t.Fatalf("row %d: %d %q %v %v", count, id, name, score, active)
		}
		if rows.Int64(0) != id || rows.Float64(2) != score || rows.Bool(3) != active || rows.IsNull(1) {
			t.Fatalf("row %d: accessors disagree with Scan", count)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("expected %d rows, got %d", n, count)
	}
	if rows.Next() {
		t.Fatal("Next returned true after the end")
	}
	if cols := rows.Columns(); len(cols) != 4 {
		t.Fatalf("columns after the end: %v", cols)
	}
}

func TestRowsScanBeforeNext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	seedRows(t, db, 3)

	rows, err := db.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var id int64
	if err := rows.Scan(&id); !errors.Is(err, errNoRow) {
		t.Fatalf("expected errNoRow, got %v", err)
	}
	if rows.Int64(0) != 0 || !rows.IsNull(0) {
		t.Fatal("accessors without a row must read NULL")
	}
}

func TestFetchAllAfterNext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	const n = 100 // more than the first chunk of 32
	seedRows(t, db, n)

	rows, err := db.Query(context.Background(), "SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i := 0; i < 5; i++ {
		if !rows.Next() {
			t.Fatal("Next")
		}
	}
	rest, err := rows.FetchAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != n-5 {
		t.Fatalf("expected %d remaining rows, got %d", n-5, len(rest))
	}
	for i, row := range rest {
		if row[0].(int64) != int64(i+6) || row[1].(string) != fmt.Sprintf("user_%d", i+6) {
			t.Fatalf("remaining row %d: %v", i, row)
		}
	}
	if rows.Next() {
		t.Fatal("Next returned true after FetchAll")
	}
	if more, err := rows.FetchAll(); err != nil || len(more) != 0 {
		t.Fatalf("second FetchAll: %v %v", more, err)
	}
}

func TestRowsRuntimeError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	seedRows(t, db, 40)

	// The pattern turns invalid on row 35, after the first chunk of 32 was served.
	if _, err := db.Exec(ctx, "UPDATE t SET name = '[' WHERE id = 35"); err != nil {
		t.Fatal(err)
	}
	q := "SELECT id, 'x' REGEXP name FROM t"
	rows, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if rows.Err() == nil {
		t.Fatalf("expected an error from Next after %d rows", count)
	}
	if count != 32 {
		t.Fatalf("expected the first chunk of 32 rows, got %d", count)
	}
	rows.Close()

	if _, err := db.QueryAll(ctx, q, nil); err == nil {
		t.Fatal("QueryAll must report the runtime error")
	}
	rows, err = db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rows.FetchAll(); err == nil {
		t.Fatal("FetchAll must report the runtime error")
	}
	rows.Close()
}

func TestQueryAll(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	seedRows(t, db, 10)

	rs, err := db.QueryAll(ctx, "SELECT id, name, score, active FROM t WHERE id > $1 ORDER BY id", []any{int64(7)})
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Columns) != 4 || rs.Columns[0] != "id" || rs.Columns[3] != "active" {
		t.Fatalf("columns: %v", rs.Columns)
	}
	if len(rs.Rows) != 3 || rs.Rows[0][0].(int64) != 8 || rs.Rows[2][1].(string) != "user_10" || rs.Rows[0][2].(float64) != 3.5 || rs.Rows[2][3].(bool) != true {
		t.Fatalf("rows: %v", rs.Rows)
	}

	// Without parameters, and an empty result still names its columns.
	rs, err = db.QueryAll(ctx, "SELECT name FROM t WHERE id > 100", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 0 || len(rs.Columns) != 1 || rs.Columns[0] != "name" {
		t.Fatalf("empty result: %v %v", rs.Columns, rs.Rows)
	}

	// Errors come back as engine errors.
	if _, err := db.QueryAll(ctx, "SELECT * FROM missing", nil); err == nil {
		t.Fatal("expected an error")
	}

	// A transaction sees its own writes.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4)", []any{int64(11), "eleven", 1.5, false}); err != nil {
		t.Fatal(err)
	}
	rs, err = tx.QueryAll(ctx, "SELECT COUNT(*) FROM t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0].(int64) != 11 {
		t.Fatalf("inside tx: %v", rs.Rows)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.QueryAll(ctx, "SELECT 1", nil); !errors.Is(err, errTxDone) {
		t.Fatalf("expected errTxDone, got %v", err)
	}
	rs, err = db.QueryAll(ctx, "SELECT COUNT(*) FROM t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0].(int64) != 10 {
		t.Fatalf("after rollback: %v", rs.Rows)
	}

	// Prepared, positional and named.
	stmt, err := db.Prepare(ctx, "SELECT name FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := range 3 {
		rs, err := stmt.QueryAll(ctx, []any{int64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		if len(rs.Rows) != 1 || rs.Rows[0][0].(string) != fmt.Sprintf("user_%d", i+1) {
			t.Fatalf("stmt QueryAll %d: %v", i, rs.Rows)
		}
	}
	named, err := db.Prepare(ctx, "SELECT name FROM t WHERE id = :id")
	if err != nil {
		t.Fatal(err)
	}
	defer named.Close()
	rs, err = named.QueryNamedAll(ctx, []sql.NamedArg{sql.Named("id", int64(4))})
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0][0].(string) != "user_4" {
		t.Fatalf("stmt QueryNamedAll: %v", rs.Rows)
	}

	// Ad hoc named, outside and inside a transaction.
	rs, err = db.QueryNamedAll(ctx, "SELECT id FROM t WHERE name = :name OR id = :id ORDER BY id", []sql.NamedArg{sql.Named("name", "user_9"), sql.Named("id", int64(2))})
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 2 || rs.Rows[0][0].(int64) != 2 || rs.Rows[1][0].(int64) != 9 {
		t.Fatalf("QueryNamedAll: %v", rs.Rows)
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecNamed(ctx, "DELETE FROM t WHERE id = :id", []sql.NamedArg{sql.Named("id", int64(2))}); err != nil {
		t.Fatal(err)
	}
	rs, err = tx.QueryNamedAll(ctx, "SELECT COUNT(*) FROM t WHERE id <= :max", []sql.NamedArg{sql.Named("max", int64(3))})
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0].(int64) != 2 {
		t.Fatalf("tx QueryNamedAll: %v", rs.Rows)
	}
}

func TestPreparedNamed(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	seedRows(t, db, 5)

	upd, err := db.Prepare(ctx, "UPDATE t SET name = :name WHERE id = :id")
	if err != nil {
		t.Fatal(err)
	}
	defer upd.Close()
	res, err := upd.ExecNamed(ctx, []sql.NamedArg{sql.Named("name", "renamed"), sql.Named("id", int64(2))})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("expected 1 affected, got %d", n)
	}

	sel, err := db.Prepare(ctx, "SELECT id, name FROM t WHERE name = :name")
	if err != nil {
		t.Fatal(err)
	}
	defer sel.Close()
	rows, err := sel.QueryNamed(ctx, []sql.NamedArg{sql.Named("name", "renamed")})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 2 || name != "renamed" {
		t.Fatalf("got %d %q", id, name)
	}
	if _, err := upd.ExecNamed(ctx, []sql.NamedArg{sql.Named("", "x")}); !errors.Is(err, errEmptyParamName) {
		t.Fatalf("expected errEmptyParamName, got %v", err)
	}
}

func TestScanStringConversions(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT 42, 2.5, true, 'text', NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var a, b, c, d string
	var e sql.NullString
	if err := rows.Scan(&a, &b, &c, &d, &e); err != nil {
		t.Fatal(err)
	}
	if a != "42" || b != "2.5" || c != "true" || d != "text" || e.Valid {
		t.Fatalf("got %q %q %q %q %v", a, b, c, d, e)
	}
	// Numeric text converts the way the engine's accessors do.
	rows2, err := db.Query(ctx, "SELECT '17', '1.5', 'yes', 3.9")
	if err != nil {
		t.Fatal(err)
	}
	defer rows2.Close()
	rows2.Next()
	if rows2.Int64(0) != 17 || rows2.Float64(1) != 1.5 || !rows2.Bool(2) || rows2.Int64(3) != 3 {
		t.Fatalf("conversions: %d %v %v %d", rows2.Int64(0), rows2.Float64(1), rows2.Bool(2), rows2.Int64(3))
	}
}

func TestDatabaseSQLLargeResult(t *testing.T) {
	dsn := fmt.Sprintf("memory://sqllarge%d", testDBCounter.Add(1))
	sdb, err := sql.Open("stoolap", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	direct, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	seedRows(t, direct, 5000)

	rows, err := sdb.Query("SELECT id, name, score, active FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) != 4 || cols[2] != "score" {
		t.Fatalf("columns: %v", cols)
	}
	count := 0
	for rows.Next() {
		var id int64
		var name string
		var score float64
		var active bool
		if err := rows.Scan(&id, &name, &score, &active); err != nil {
			t.Fatal(err)
		}
		count++
		if id != int64(count) {
			t.Fatalf("row %d has id %d", count, id)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 5000 {
		t.Fatalf("expected 5000 rows, got %d", count)
	}
}

func TestEmptyParamsAreNotNull(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	check := func(label string, rs *ResultSet, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(rs.Rows) != 1 || rs.Rows[0][0] != false {
			t.Fatalf("%s: empty parameter became NULL: %v", label, rs.Rows)
		}
	}
	rs, err := db.QueryAll(ctx, "SELECT $1 IS NULL", []any{""})
	check("text", rs, err)
	rs, err = db.QueryAll(ctx, "SELECT $1 IS NULL", []any{[]byte{}})
	check("blob", rs, err)
	rs, err = db.QueryNamedAll(ctx, "SELECT :s IS NULL", []sql.NamedArg{sql.Named("s", "")})
	check("named", rs, err)

	stmt, err := db.Prepare(ctx, "SELECT $1 IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rs, err = stmt.QueryAll(ctx, []any{""})
	check("prepared", rs, err)

	if _, err := db.Exec(ctx, "CREATE TABLE e (id INTEGER PRIMARY KEY, s TEXT)"); err != nil {
		t.Fatal(err)
	}
	ins, err := db.Prepare(ctx, "INSERT INTO e VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Close()
	if _, err := ins.ExecBatch(ctx, [][]any{{1, ""}, {2, "x"}}); err != nil {
		t.Fatal(err)
	}
	rs, err = db.QueryAll(ctx, "SELECT COUNT(*) FROM e WHERE s = ''", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0] != int64(1) {
		t.Fatalf("batch: empty string not stored as '': %v", rs.Rows)
	}

	sdb, err := sql.Open("stoolap", fmt.Sprintf("memory://empty%d", testDBCounter.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()
	var isNull bool
	if err := sdb.QueryRow("SELECT ? IS NULL", "").Scan(&isNull); err != nil {
		t.Fatal(err)
	}
	if isNull {
		t.Fatal("database/sql: empty string became NULL")
	}
}

func TestTimestampFromText(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT '2024-03-04T05:06:07Z', '2024-03-04 05:06:07.5', '2024-03-04', '2024-03-04T05:06:07+02:00', 'nope'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	want := []time.Time{
		time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC),
		time.Date(2024, 3, 4, 5, 6, 7, 500_000_000, time.UTC),
		time.Date(2024, 3, 4, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 4, 3, 6, 7, 0, time.UTC),
		{},
	}
	for i, w := range want {
		if got := rows.Timestamp(i); !got.Equal(w) {
			t.Errorf("Timestamp(%d) = %v, want %v", i, got, w)
		}
	}
	var a time.Time
	var b sql.NullTime
	if err := rows.Scan(&a, &b, new(string), new(string), new(string)); err != nil {
		t.Fatal(err)
	}
	if !a.Equal(want[0]) || !b.Valid || !b.Time.Equal(want[1]) {
		t.Fatalf("Scan: %v %v", a, b)
	}
}

func TestScanStringMatchesEngineText(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE f (id INTEGER PRIMARY KEY, v1 VECTOR(3), v2 VECTOR(3), j JSON)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO f VALUES (1, '[1.5, 2, -3]', '[0.1, 100000, 1e-7]', '{"a": 1}')`); err != nil {
		t.Fatal(err)
	}
	exprs := []string{
		"1e21", "2.5", "3.0", "-0.0", "0.00001", "123456789.125", "1.0e-4", "1.5e300",
		"TIMESTAMP '2024-03-04 05:06:07'", "TIMESTAMP '2024-03-04 05:06:07.123'",
		"TIMESTAMP '2024-03-04 05:06:07.123456'", "TIMESTAMP '2024-03-04 05:06:07.123456789'",
		"v1", "v2", "j", "true", "-7",
	}
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i, e := range exprs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(e + ", CAST(" + e + " AS TEXT)")
	}
	sb.WriteString(" FROM f")
	rows, err := db.Query(ctx, sb.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	dest := make([]any, 2*len(exprs))
	got := make([]string, 2*len(exprs))
	for i := range dest {
		dest[i] = &got[i]
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatal(err)
	}
	for i, e := range exprs {
		if got[2*i] != got[2*i+1] {
			t.Errorf("%s: Scan gave %q, engine text is %q", e, got[2*i], got[2*i+1])
		}
	}
}

func TestRowsDoubleClose(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	seedRows(t, db, 3)

	r1, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	r1.Close()
	r2, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	r1.Close() // a second Close must not touch r2
	n := 0
	for r2.Next() {
		n++
	}
	if err := r2.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("second Close of an earlier Rows broke a live result: got %d rows, want 3", n)
	}
}

func TestNilBytesParamIsNull(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	rs, err := db.QueryAll(ctx, "SELECT $1 IS NULL", []any{[]byte(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0] != true {
		t.Fatalf("nil []byte did not bind as NULL: %v", rs.Rows)
	}
	rs, err = db.QueryNamedAll(ctx, "SELECT :b IS NULL", []sql.NamedArg{sql.Named("b", []byte(nil))})
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0] != true {
		t.Fatalf("named nil []byte did not bind as NULL: %v", rs.Rows)
	}
}

func TestPointQueryAllocs(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()
	seedRows(t, db, 10)
	stmt, err := db.Prepare(ctx, "SELECT id, score FROM t WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	allocs := testing.AllocsPerRun(100, func() {
		rows, err := stmt.QueryContext(ctx, []any{5})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() || rows.Int64(0) != 5 {
			t.Fatal("expected id 5")
		}
		rows.Close()
	})
	if allocs > 1 {
		t.Fatalf("point query allocates %.0f times, want at most 1 (the Rows struct)", allocs)
	}
}

func TestScanTimeRejectsNonTimestamps(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	for _, expr := range []string{"'nope'", "1.5", "true"} {
		rows, err := db.Query(ctx, "SELECT "+expr)
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		var tt time.Time
		var nt sql.NullTime
		if err := rows.Scan(&tt); !errors.Is(err, errNotTimestamp) {
			t.Errorf("%s into time.Time: err = %v, want errNotTimestamp", expr, err)
		}
		if err := rows.Scan(&nt); !errors.Is(err, errNotTimestamp) {
			t.Errorf("%s into NullTime: err = %v, want errNotTimestamp", expr, err)
		}
		rows.Close()
	}
}

func TestTimestampTextFormsMatchEngine(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	inputs := []string{
		"2024-03-04T05:06:07Z", "2024-03-04T05:06:07.25Z", "2024-03-04T05:06:07+02:00",
		"2024-03-04T05:06:07+0200", "2024-03-04T05:06:07.5-0130", "2024-03-04T05:06:07 +02:00",
		"2024-03-04 05:06:07", "2024-03-04 05:06:07.123456", "2024-03-04", "2024/03/04 05:06:07",
		"2024/03/04", "03/04/2024", "04/03/2024", "15/05/2023", "05:06:07", "05:06:07.5", "05:06", "nope", "",
	}
	for _, in := range inputs {
		rows, err := db.QueryParams(ctx, "SELECT CAST($1 AS TIMESTAMP), $1", []any{in})
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		engineNull := rows.IsNull(0)
		engine, got := rows.Timestamp(0), rows.Timestamp(1)
		rows.Close()
		if engineNull != got.IsZero() || (!engineNull && !engine.Equal(got)) {
			t.Errorf("%q: engine null=%v %v, driver %v", in, engineNull, engine, got)
		}
	}
}
