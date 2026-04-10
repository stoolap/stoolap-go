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
