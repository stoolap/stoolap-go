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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var testDBCounter atomic.Int64

// openTestSQL opens a fresh named in-memory database through database/sql.
func openTestSQL(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("stoolap-wasm", fmt.Sprintf("memory://packed%d", testDBCounter.Add(1)))
	if err != nil {
		t.Fatal(err)
	}
	return db
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
		t.Fatalf("expected %d rows after 5 Next calls, got %d", n-5, len(rest))
	}
	if rest[0][0].(int64) != 6 || rest[0][1].(string) != "user_6" || rest[n-6][0].(int64) != int64(n) {
		t.Fatalf("unexpected rows: first %v, last %v", rest[0], rest[n-6])
	}
	if rows.Next() {
		t.Fatal("Next after FetchAll must report the end")
	}
	again, err := rows.FetchAll()
	if err != nil || len(again) != 0 {
		t.Fatalf("second FetchAll: %v %v", again, err)
	}
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
		rs, err := db.QueryAll(ctx, "SELECT COUNT(*) FROM t", nil)
		if err != nil {
			t.Fatal(err)
		}
		return rs.Rows[0][0].(int64)
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

func TestLargeParamsGrowArena(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"); err != nil {
		t.Fatal(err)
	}
	// Four times the initial arena, in one parameter.
	big := strings.Repeat("x", 4*arenaInitSize)
	if _, err := db.ExecParams(ctx, "INSERT INTO t VALUES ($1, $2)", []any{int64(1), big}); err != nil {
		t.Fatal("large param:", err)
	}
	var n int64
	var back string
	rows, err := db.QueryParams(ctx, "SELECT LENGTH(body), body FROM t WHERE id = $1", []any{int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	if err := rows.Scan(&n, &back); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if n != int64(len(big)) || back != big {
		t.Fatalf("large text came back wrong: length %d", n)
	}

	// Many rows in one batch, more than the arena held at first.
	stmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	batch := make([][]any, 20000)
	for i := range batch {
		batch[i] = []any{int64(i + 2), strings.Repeat("y", 64)}
	}
	if _, err := stmt.ExecBatch(ctx, batch); err != nil {
		t.Fatal("large batch:", err)
	}
	rs, err := db.QueryAll(ctx, "SELECT COUNT(*) FROM t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0][0].(int64) != 20001 {
		t.Fatalf("expected 20001 rows, got %v", rs.Rows)
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
		"2024/03/04", "03/04/2024", "04/03/2024", "nope", "",
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

func TestDatabaseSQLNamedArgs(t *testing.T) {
	db := openTestSQL(t)
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
	db := openTestSQL(t)
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

func TestDatabaseSQLColumnsAndTypes(t *testing.T) {
	db := openTestSQL(t)
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, v VECTOR(2))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 2.5, 'x', true, '2024-01-02 03:04:05', '[1, 2]')"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT i, f, s, b, ts, v, NULL FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil || len(cols) != 7 || cols[4] != "ts" {
		t.Fatalf("columns: %v %v", cols, err)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var i int64
	var f float64
	var s string
	var b bool
	var ts time.Time
	var v []byte
	var n any
	if err := rows.Scan(&i, &f, &s, &b, &ts, &v, &n); err != nil {
		t.Fatal(err)
	}
	if i != 1 || f != 2.5 || s != "x" || !b || ts.Year() != 2024 || len(v) != 8 || n != nil {
		t.Fatalf("got %d %v %q %v %v %v %v", i, f, s, b, ts, v, n)
	}
}
