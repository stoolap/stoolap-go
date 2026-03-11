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
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

// mustExec is a test helper that executes a query and fails the test on error.
func mustExec(t *testing.T, db *DB, ctx context.Context, query string, args ...driver.NamedValue) {
	t.Helper()
	if len(args) == 0 {
		if _, err := db.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Fatal("version should not be empty")
	}
	t.Logf("stoolap version: %s", v)
}

func TestOpen(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
}

func TestOpenInvalidDSN(t *testing.T) {
	_, err := Open("invalid://")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestClone(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE clone_test (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO clone_test VALUES (1, 'hello')"); err != nil {
		t.Fatal(err)
	}

	clone, err := db.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clone.Close() }()

	// Clone should see the same data
	var val string
	err = clone.QueryRow(ctx, "SELECT val FROM clone_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatal(err)
	}
	if val != "hello" {
		t.Fatalf("expected hello, got %s", val)
	}
}

func TestExec(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", affected)
	}
}

func TestExecError(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.Exec(ctx, "SELECT FROM nonexistent_table")
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestQuery(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(ctx, "SELECT id, name, age FROM test ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	cols := rows.Columns()
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" || cols[2] != "age" {
		t.Fatalf("unexpected columns: %v", cols)
	}

	expected := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	for i, exp := range expected {
		if !rows.Next() {
			t.Fatalf("expected row %d", i)
		}
		var id, age int64
		var name string
		if err := rows.Scan(&id, &name, &age); err != nil {
			t.Fatal(err)
		}
		if id != exp.id || name != exp.name || age != exp.age {
			t.Fatalf("row %d: expected %v, got %d %s %d", i, exp, id, name, age)
		}
	}

	if rows.Next() {
		t.Fatal("expected no more rows")
	}
}

func TestQueryRow(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, ctx, "INSERT INTO test VALUES (1, 'Alice')")

	var name string
	err = db.QueryRow(ctx, "SELECT name FROM test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}

	// Test ErrNoRows
	err = db.QueryRow(ctx, "SELECT name FROM test WHERE id = 999").Scan(&name)
	if err != sql.ErrNoRows {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
}

func TestExecContext(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	result, err := db.ExecContext(ctx, "INSERT INTO test VALUES ($1, $2)",
		driver.NamedValue{Ordinal: 1, Value: int64(1)},
		driver.NamedValue{Ordinal: 2, Value: "Alice"},
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("expected 1 row affected, got %d", affected)
	}

	var name string
	err = db.QueryRow(ctx, "SELECT name FROM test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}
}

func TestQueryContext(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

	rows, err := db.QueryContext(ctx, "SELECT name FROM test WHERE id = $1",
		driver.NamedValue{Ordinal: 1, Value: int64(1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}
}

func TestTransaction(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO test VALUES (2, 'Bob')")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestTransactionRollback(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, ctx, "INSERT INTO test VALUES (1, 'Alice')")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, _ = tx.ExecContext(ctx, "INSERT INTO test VALUES (2, 'Bob')")
	_ = tx.Rollback()

	var count int64
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", count)
	}
}

func TestTransactionQuery(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, _ = tx.ExecContext(ctx, "INSERT INTO test VALUES (1, 'inside_tx')")

	// Query within the transaction should see uncommitted data
	rows, err := tx.QueryContext(ctx, "SELECT val FROM test WHERE id = 1")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected row within transaction")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	if val != "inside_tx" {
		_ = tx.Rollback()
		t.Fatalf("expected inside_tx, got %s", val)
	}

	_ = tx.Commit()
}

func TestTransactionWithParams(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO test VALUES ($1, $2)",
		driver.NamedValue{Ordinal: 1, Value: int64(1)},
		driver.NamedValue{Ordinal: 2, Value: "Alice"},
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	rows, err := tx.QueryContext(ctx, "SELECT name FROM test WHERE id = $1",
		driver.NamedValue{Ordinal: 1, Value: int64(1)},
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	if name != "Alice" {
		_ = tx.Rollback()
		t.Fatalf("expected Alice, got %s", name)
	}

	_ = tx.Commit()
}

func TestPrepare(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO test VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	for i := int64(1); i <= 5; i++ {
		_, err := stmt.ExecContext(ctx,
			driver.NamedValue{Ordinal: 1, Value: i},
			driver.NamedValue{Ordinal: 2, Value: "User"},
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	var count int64
	err = db.QueryRow(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("expected 5 rows, got %d", count)
	}
}

func TestPrepareQuery(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

	stmt, err := db.Prepare("SELECT name FROM test WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	for _, tc := range []struct {
		id   int64
		want string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	} {
		rows, err := stmt.QueryContext(ctx, driver.NamedValue{Ordinal: 1, Value: tc.id})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatalf("expected row for id %d", tc.id)
		}
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if name != tc.want {
			t.Fatalf("id %d: expected %s, got %s", tc.id, tc.want, name)
		}
	}
}

func TestPrepareInTransaction(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO test VALUES ($1, $2)")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	for i := int64(1); i <= 100; i++ {
		_, err := stmt.ExecContext(ctx,
			driver.NamedValue{Ordinal: 1, Value: i},
			driver.NamedValue{Ordinal: 2, Value: i * 10},
		)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	_ = stmt.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100 rows, got %d", count)
	}

	var sum int64
	if err := db.QueryRow(ctx, "SELECT SUM(val) FROM test").Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 50500 { // sum(10+20+...+1000) = 10*sum(1..100) = 10*5050
		t.Fatalf("expected sum 50500, got %d", sum)
	}
}

func TestAllTypes(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE types_test (i INTEGER, f FLOAT, t TEXT, b BOOLEAN, ts TIMESTAMP)")

	now := time.Now().UTC().Truncate(time.Microsecond)
	mustExec(t, db, ctx, "INSERT INTO types_test VALUES ($1, $2, $3, $4, $5)",
		driver.NamedValue{Ordinal: 1, Value: int64(42)},
		driver.NamedValue{Ordinal: 2, Value: 3.14},
		driver.NamedValue{Ordinal: 3, Value: "hello world"},
		driver.NamedValue{Ordinal: 4, Value: true},
		driver.NamedValue{Ordinal: 5, Value: now},
	)

	var i int64
	var f float64
	var s string
	var b bool
	var ts time.Time
	err = db.QueryRow(ctx, "SELECT i, f, t, b, ts FROM types_test").Scan(&i, &f, &s, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}
	if i != 42 {
		t.Fatalf("integer: expected 42, got %d", i)
	}
	if f != 3.14 {
		t.Fatalf("float: expected 3.14, got %f", f)
	}
	if s != "hello world" {
		t.Fatalf("text: expected 'hello world', got %s", s)
	}
	if !b {
		t.Fatal("boolean: expected true")
	}
	if !ts.Equal(now) {
		t.Fatalf("timestamp: expected %v, got %v", now, ts)
	}
}

func TestIntegerEdgeCases(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE int_test (val INTEGER)")

	cases := []int64{0, 1, -1, math.MaxInt64, math.MinInt64}
	for _, v := range cases {
		mustExec(t, db, ctx, "INSERT INTO int_test VALUES ($1)",
			driver.NamedValue{Ordinal: 1, Value: v})
	}

	rows, err := db.Query(ctx, "SELECT val FROM int_test ORDER BY val")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	var results []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		results = append(results, v)
	}
	if len(results) != len(cases) {
		t.Fatalf("expected %d rows, got %d", len(cases), len(results))
	}
}

func TestFloatEdgeCases(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE float_test (val FLOAT)")

	cases := []float64{0.0, math.Copysign(0, -1), 1.0, -1.0, math.SmallestNonzeroFloat64, math.MaxFloat64}
	for _, v := range cases {
		mustExec(t, db, ctx, "INSERT INTO float_test VALUES ($1)",
			driver.NamedValue{Ordinal: 1, Value: v})
	}

	var count int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM float_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != int64(len(cases)) {
		t.Fatalf("expected %d rows, got %d", len(cases), count)
	}
}

func TestNullValues(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE null_test (id INTEGER, s TEXT, i INTEGER, f FLOAT, b BOOLEAN, ts TIMESTAMP)")
	mustExec(t, db, ctx, "INSERT INTO null_test VALUES (1, NULL, NULL, NULL, NULL, NULL)")

	var (
		id int64
		s  sql.NullString
		i  sql.NullInt64
		f  sql.NullFloat64
		b  sql.NullBool
		ts sql.NullTime
	)
	err = db.QueryRow(ctx, "SELECT * FROM null_test").Scan(&id, &s, &i, &f, &b, &ts)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}
	if s.Valid {
		t.Fatalf("expected NULL string, got %s", s.String)
	}
	if i.Valid {
		t.Fatalf("expected NULL int64, got %d", i.Int64)
	}
	if f.Valid {
		t.Fatalf("expected NULL float64, got %f", f.Float64)
	}
	if b.Valid {
		t.Fatalf("expected NULL bool, got %v", b.Bool)
	}
	if ts.Valid {
		t.Fatalf("expected NULL time, got %v", ts.Time)
	}
}

func TestScanToAny(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE any_test (id INTEGER, name TEXT, active BOOLEAN, score FLOAT)")
	mustExec(t, db, ctx, "INSERT INTO any_test VALUES (1, 'Alice', true, 99.5)")

	rows, err := db.Query(ctx, "SELECT id, name, active, score FROM any_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var id, name, active, score any
	if err := rows.Scan(&id, &name, &active, &score); err != nil {
		t.Fatal(err)
	}
	if id.(int64) != 1 {
		t.Fatalf("expected 1, got %v", id)
	}
	if name.(string) != "Alice" {
		t.Fatalf("expected Alice, got %v", name)
	}
	if active.(bool) != true {
		t.Fatalf("expected true, got %v", active)
	}
	if score.(float64) != 99.5 {
		t.Fatalf("expected 99.5, got %v", score)
	}
}

func TestScanNullToAny(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE any_null_test (id INTEGER, val TEXT)")
	mustExec(t, db, ctx, "INSERT INTO any_null_test VALUES (1, NULL)")

	var id, val any
	err = db.QueryRow(ctx, "SELECT id, val FROM any_null_test").Scan(&id, &val)
	if err != nil {
		t.Fatal(err)
	}
	if id.(int64) != 1 {
		t.Fatalf("expected 1, got %v", id)
	}
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

func TestScanIntToString(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE str_test (val INTEGER)")
	mustExec(t, db, ctx, "INSERT INTO str_test VALUES (12345)")

	var s string
	err = db.QueryRow(ctx, "SELECT val FROM str_test").Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	if s != "12345" {
		t.Fatalf("expected '12345', got '%s'", s)
	}
}

func TestScanFloatToInt(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE conv_test (val FLOAT)")
	mustExec(t, db, ctx, "INSERT INTO conv_test VALUES (42.7)")

	var i int64
	err = db.QueryRow(ctx, "SELECT val FROM conv_test").Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	if i != 42 {
		t.Fatalf("expected 42, got %d", i)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE iso_test (id INTEGER PRIMARY KEY, val TEXT)")
	mustExec(t, db, ctx, "INSERT INTO iso_test VALUES (1, 'original')")

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSnapshot})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tx.QueryContext(ctx, "SELECT val FROM iso_test WHERE id = 1")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatal("expected row")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	if val != "original" {
		_ = tx.Rollback()
		t.Fatalf("expected original, got %s", val)
	}

	_ = tx.Commit()
}

func TestUnsupportedIsolationLevel(t *testing.T) {
	db, err := Open("memory://")
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

func TestEmptyResultSet(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE empty_test (id INTEGER)")

	rows, err := db.Query(ctx, "SELECT id FROM empty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		t.Fatal("expected no rows")
	}
}

func TestLargeTextValues(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE large_test (id INTEGER PRIMARY KEY, val TEXT)")

	// Create a large string (100KB)
	large := make([]byte, 100*1024)
	for i := range large {
		large[i] = byte('A' + (i % 26))
	}
	largeStr := string(large)

	mustExec(t, db, ctx, "INSERT INTO large_test VALUES ($1, $2)",
		driver.NamedValue{Ordinal: 1, Value: int64(1)},
		driver.NamedValue{Ordinal: 2, Value: largeStr},
	)

	var result string
	err = db.QueryRow(ctx, "SELECT val FROM large_test WHERE id = 1").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	if result != largeStr {
		t.Fatalf("large text roundtrip failed: got length %d, expected %d", len(result), len(largeStr))
	}
}

func TestMultipleRowsClose(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE close_test (id INTEGER)")
	mustExec(t, db, ctx, "INSERT INTO close_test VALUES (1)")

	rows, err := db.Query(ctx, "SELECT id FROM close_test")
	if err != nil {
		t.Fatal(err)
	}

	// Close multiple times should be safe
	_ = rows.Close()
	_ = rows.Close()
}

func TestJSON(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE json_test (id INTEGER PRIMARY KEY, data JSON)")
	mustExec(t, db, ctx, `INSERT INTO json_test VALUES (1, '{"name":"Alice","age":30}')`)

	var data string
	err = db.QueryRow(ctx, "SELECT data FROM json_test WHERE id = 1").Scan(&data)
	if err != nil {
		t.Fatal(err)
	}
	if data == "" {
		t.Fatal("expected JSON data, got empty string")
	}
	t.Logf("JSON data: %s", data)
}

func TestVectorBlob(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE vec_test (id INTEGER PRIMARY KEY, embedding VECTOR)")
	mustExec(t, db, ctx, "INSERT INTO vec_test VALUES (1, '[1.0, 2.0, 3.0]')")

	var blob []byte
	err = db.QueryRow(ctx, "SELECT embedding FROM vec_test WHERE id = 1").Scan(&blob)
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) == 0 {
		t.Fatal("expected vector blob data, got empty")
	}
	// Vector is packed f32, so 3 floats = 12 bytes
	if len(blob) != 12 {
		t.Fatalf("expected 12 bytes for 3 f32 values, got %d", len(blob))
	}
	t.Logf("Vector blob: %d bytes", len(blob))
}

func TestBooleanFalse(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE bool_test (id INTEGER, val BOOLEAN)")
	mustExec(t, db, ctx, "INSERT INTO bool_test VALUES (1, false), (2, true)")

	var val bool
	err = db.QueryRow(ctx, "SELECT val FROM bool_test WHERE id = 1").Scan(&val)
	if err != nil {
		t.Fatal(err)
	}
	if val {
		t.Fatal("expected false")
	}

	err = db.QueryRow(ctx, "SELECT val FROM bool_test WHERE id = 2").Scan(&val)
	if err != nil {
		t.Fatal(err)
	}
	if !val {
		t.Fatal("expected true")
	}
}

func TestTimestampPrecision(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE ts_test (ts TIMESTAMP)")

	// Test nanosecond precision roundtrip
	ts := time.Date(2025, 6, 15, 14, 30, 45, 123456000, time.UTC)
	mustExec(t, db, ctx, "INSERT INTO ts_test VALUES ($1)",
		driver.NamedValue{Ordinal: 1, Value: ts})

	var result time.Time
	err = db.QueryRow(ctx, "SELECT ts FROM ts_test").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Equal(ts) {
		t.Fatalf("timestamp precision lost: expected %v, got %v", ts, result)
	}
}

func TestMultipleStatements(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE multi_test (id INTEGER PRIMARY KEY, val TEXT)")

	// Multiple concurrent prepared statements
	insert, _ := db.Prepare("INSERT INTO multi_test VALUES ($1, $2)")
	query, _ := db.Prepare("SELECT val FROM multi_test WHERE id = $1")
	defer func() { _ = insert.Close() }()
	defer func() { _ = query.Close() }()

	for i := int64(1); i <= 10; i++ {
		if _, err := insert.ExecContext(ctx,
			driver.NamedValue{Ordinal: 1, Value: i},
			driver.NamedValue{Ordinal: 2, Value: "val"},
		); err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(1); i <= 10; i++ {
		rows, err := query.QueryContext(ctx, driver.NamedValue{Ordinal: 1, Value: i})
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			_ = rows.Close()
			t.Fatalf("expected row for id %d", i)
		}
		var val string
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if val != "val" {
			t.Fatalf("expected 'val', got '%s'", val)
		}
	}
}

func TestStmtSQL(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE stmt_sql_test (id INTEGER PRIMARY KEY, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO stmt_sql_test VALUES ($1, $2)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	got := stmt.SQL()
	if got != "INSERT INTO stmt_sql_test VALUES ($1, $2)" {
		t.Fatalf("expected original SQL, got: %s", got)
	}
}

func TestFetchAll(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE fetch_test (id INTEGER PRIMARY KEY, name TEXT, val FLOAT)")
	for i := int64(1); i <= 100; i++ {
		mustExec(t, db, ctx, "INSERT INTO fetch_test VALUES ($1, $2, $3)",
			driver.NamedValue{Ordinal: 1, Value: i},
			driver.NamedValue{Ordinal: 2, Value: "row"},
			driver.NamedValue{Ordinal: 3, Value: float64(i) * 1.5},
		)
	}

	rows, err := db.Query(ctx, "SELECT id, name, val FROM fetch_test ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	buf, err := rows.FetchAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) == 0 {
		t.Fatal("expected non-empty buffer from FetchAll")
	}
	t.Logf("FetchAll buffer: %d bytes for 100 rows", len(buf))
}

func TestDoubleClose(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Second close should be safe
	err = db.Close()
	if err != nil {
		t.Fatal("double close should be safe, got:", err)
	}
}

func TestHandle(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	h := db.Handle()
	if h == nil {
		t.Fatal("expected non-nil handle")
	}
}

func TestLastInsertId(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	result, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	// stoolap returns 0 for LastInsertId
	if id != 0 {
		t.Fatalf("expected 0, got %d", id)
	}
}

func TestTransactionID(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// ID returns 0 currently
	_ = tx.ID()
}

func TestQueryRowError(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Query on invalid SQL should propagate error through QueryRow.Scan
	var val int64
	err = db.QueryRow(context.Background(), "SELECT FROM nonexistent_table_xyz").Scan(&val)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestScanOnClosedRows(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	rows, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	// Next on closed rows
	if rows.Next() {
		t.Fatal("expected false for Next on closed rows")
	}

	// Scan on closed rows
	var id int64
	err = rows.Scan(&id)
	if err == nil {
		t.Fatal("expected error for Scan on closed rows")
	}
}

func TestFetchAllOnClosedRows(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")

	rows, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	_, err = rows.FetchAll()
	if err == nil {
		t.Fatal("expected error for FetchAll on closed rows")
	}
}

func TestScanColumnMismatch(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (a INTEGER, b TEXT)")
	mustExec(t, db, ctx, "INSERT INTO t VALUES (1, 'x')")

	rows, err := db.Query(ctx, "SELECT a, b FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	// Only pass 1 destination for 2 columns
	var a int64
	err = rows.Scan(&a)
	if err == nil {
		t.Fatal("expected column count mismatch error")
	}
}

func TestScanNullToPrimitives(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE null_prim (s TEXT, i64 INTEGER, f64 FLOAT, b BOOLEAN, ts TIMESTAMP, i32 INTEGER, i INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, ctx, "INSERT INTO null_prim VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

	rows, err := db.Query(ctx, "SELECT s, i64, f64, b, ts, i32, i FROM null_prim")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var (
		s   string
		i64 int64
		f64 float64
		b   bool
		ts  time.Time
		i32 int32
		i   int
	)
	err = rows.Scan(&s, &i64, &f64, &b, &ts, &i32, &i)
	if err != nil {
		t.Fatal(err)
	}

	if s != "" {
		t.Fatalf("expected empty string, got %q", s)
	}
	if i64 != 0 {
		t.Fatalf("expected 0 int64, got %d", i64)
	}
	if f64 != 0 {
		t.Fatalf("expected 0 float64, got %f", f64)
	}
	if b {
		t.Fatal("expected false")
	}
	if !ts.IsZero() {
		t.Fatalf("expected zero time, got %v", ts)
	}
	if i32 != 0 {
		t.Fatalf("expected 0 int32, got %d", i32)
	}
	if i != 0 {
		t.Fatalf("expected 0 int, got %d", i)
	}

	// Also test null scan to []byte and *any
	mustExec(t, db, ctx, "CREATE TABLE null_prim2 (v TEXT, a INTEGER)")
	mustExec(t, db, ctx, "INSERT INTO null_prim2 VALUES (NULL, NULL)")
	var blob []byte
	var anyVal any
	err = db.QueryRow(ctx, "SELECT v, a FROM null_prim2").Scan(&blob, &anyVal)
	if err != nil {
		t.Fatal(err)
	}
	if blob != nil {
		t.Fatalf("expected nil blob, got %v", blob)
	}
	if anyVal != nil {
		t.Fatalf("expected nil any, got %v", anyVal)
	}
}

func TestScanTypeConversions(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE conv (i INTEGER, f FLOAT, b BOOLEAN, s TEXT, ts TIMESTAMP, j JSON, bl VECTOR)")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = db.ExecContext(ctx, "INSERT INTO conv VALUES ($1, $2, $3, $4, $5, $6, $7)",
		driver.NamedValue{Ordinal: 1, Value: int64(42)},
		driver.NamedValue{Ordinal: 2, Value: 3.14},
		driver.NamedValue{Ordinal: 3, Value: true},
		driver.NamedValue{Ordinal: 4, Value: "hello"},
		driver.NamedValue{Ordinal: 5, Value: now},
		driver.NamedValue{Ordinal: 6, Value: "{}"},
		driver.NamedValue{Ordinal: 7, Value: "[1.0, 2.0]"},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("float_to_string", func(t *testing.T) {
		var s string
		err := db.QueryRow(ctx, "SELECT f FROM conv").Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		if s != "3.14" {
			t.Fatalf("expected '3.14', got %q", s)
		}
	})

	t.Run("bool_to_string", func(t *testing.T) {
		var s string
		err := db.QueryRow(ctx, "SELECT b FROM conv").Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		if s != "true" {
			t.Fatalf("expected 'true', got %q", s)
		}
	})

	t.Run("timestamp_to_string", func(t *testing.T) {
		var s string
		err := db.QueryRow(ctx, "SELECT ts FROM conv").Scan(&s)
		if err != nil {
			t.Fatal(err)
		}
		if s == "" {
			t.Fatal("expected non-empty timestamp string")
		}
	})

	t.Run("bool_to_int64", func(t *testing.T) {
		var v int64
		err := db.QueryRow(ctx, "SELECT b FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 1 {
			t.Fatalf("expected 1, got %d", v)
		}
	})

	t.Run("bool_false_to_int64", func(t *testing.T) {
		mustExec(t, db, ctx, "CREATE TABLE bf (b BOOLEAN)")
		mustExec(t, db, ctx, "INSERT INTO bf VALUES (false)")
		var v int64
		err := db.QueryRow(ctx, "SELECT b FROM bf").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 0 {
			t.Fatalf("expected 0, got %d", v)
		}
	})

	t.Run("float_to_int", func(t *testing.T) {
		var v int
		err := db.QueryRow(ctx, "SELECT f FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 3 {
			t.Fatalf("expected 3, got %d", v)
		}
	})

	t.Run("int_to_int", func(t *testing.T) {
		var v int
		err := db.QueryRow(ctx, "SELECT i FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	})

	t.Run("bool_to_int", func(t *testing.T) {
		var v int
		err := db.QueryRow(ctx, "SELECT b FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 1 {
			t.Fatalf("expected 1, got %d", v)
		}
	})

	t.Run("bool_false_to_int", func(t *testing.T) {
		var v int
		err := db.QueryRow(ctx, "SELECT b FROM bf").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 0 {
			t.Fatalf("expected 0, got %d", v)
		}
	})

	t.Run("int_to_int32", func(t *testing.T) {
		var v int32
		err := db.QueryRow(ctx, "SELECT i FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	})

	t.Run("int_to_float64", func(t *testing.T) {
		var v float64
		err := db.QueryRow(ctx, "SELECT i FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v != 42.0 {
			t.Fatalf("expected 42.0, got %f", v)
		}
	})

	t.Run("float_to_float32", func(t *testing.T) {
		var v float32
		err := db.QueryRow(ctx, "SELECT f FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v < 3.13 || v > 3.15 {
			t.Fatalf("expected ~3.14, got %f", v)
		}
	})

	t.Run("int_to_bool", func(t *testing.T) {
		var v bool
		err := db.QueryRow(ctx, "SELECT i FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v {
			t.Fatal("expected true for non-zero int")
		}
	})

	t.Run("text_to_bytes", func(t *testing.T) {
		var v []byte
		err := db.QueryRow(ctx, "SELECT s FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if string(v) != "hello" {
			t.Fatalf("expected 'hello', got %q", string(v))
		}
	})

	t.Run("blob_to_bytes", func(t *testing.T) {
		var v []byte
		err := db.QueryRow(ctx, "SELECT bl FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if v == nil {
			t.Fatal("expected non-nil blob")
		}
	})

	t.Run("null_string_valid", func(t *testing.T) {
		var v sql.NullString
		err := db.QueryRow(ctx, "SELECT s FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Valid || v.String != "hello" {
			t.Fatalf("expected valid 'hello', got %+v", v)
		}
	})

	t.Run("null_int64_valid", func(t *testing.T) {
		var v sql.NullInt64
		err := db.QueryRow(ctx, "SELECT i FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Valid || v.Int64 != 42 {
			t.Fatalf("expected valid 42, got %+v", v)
		}
	})

	t.Run("null_float64_valid", func(t *testing.T) {
		var v sql.NullFloat64
		err := db.QueryRow(ctx, "SELECT f FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Valid || v.Float64 != 3.14 {
			t.Fatalf("expected valid 3.14, got %+v", v)
		}
	})

	t.Run("null_bool_valid", func(t *testing.T) {
		var v sql.NullBool
		err := db.QueryRow(ctx, "SELECT b FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Valid || !v.Bool {
			t.Fatalf("expected valid true, got %+v", v)
		}
	})

	t.Run("null_time_valid", func(t *testing.T) {
		var v sql.NullTime
		err := db.QueryRow(ctx, "SELECT ts FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if !v.Valid || !v.Time.Equal(now) {
			t.Fatalf("expected valid %v, got %+v", now, v)
		}
	})

	t.Run("columnToAny_timestamp", func(t *testing.T) {
		var v any
		err := db.QueryRow(ctx, "SELECT ts FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := v.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", v)
		}
	})

	t.Run("columnToAny_json", func(t *testing.T) {
		var v any
		err := db.QueryRow(ctx, "SELECT j FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := v.(string); !ok {
			t.Fatalf("expected string, got %T", v)
		}
	})

	t.Run("columnToAny_blob", func(t *testing.T) {
		var v any
		err := db.QueryRow(ctx, "SELECT bl FROM conv").Scan(&v)
		if err != nil {
			t.Fatal(err)
		}
		// VECTOR columns may return []byte or string depending on engine
		if v == nil {
			t.Fatal("expected non-nil value")
		}
	})
}

func TestPrepareContextDirect(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")

	stmt, err := db.PrepareContext(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	// SQL should return the query
	if stmt.SQL() == "" {
		t.Fatal("expected non-empty SQL")
	}
}

func TestPreparedStmtNoArgs(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")
	mustExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// Exec with no args
	stmt, err := db.Prepare("INSERT INTO t VALUES (2)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = stmt.ExecContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stmt.Close()

	// Query with no args
	stmt2, err := db.Prepare("SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt2.Close() }()

	rows, err := stmt2.QueryContext(ctx)
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

func TestPreparedStmtSQLNil(t *testing.T) {
	s := &preparedStmt{}
	if s.SQL() != "" {
		t.Fatal("expected empty SQL for nil stmt")
	}
}

func TestPreparedStmtCloseNil(t *testing.T) {
	s := &preparedStmt{}
	err := s.Close()
	if err != nil {
		t.Fatal("expected nil error for Close on nil stmt")
	}
}

func TestPrepareInTxQuery(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER, name TEXT)")
	mustExec(t, db, ctx, "INSERT INTO t VALUES (1, 'Alice')")

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := tx.Prepare("SELECT name FROM t WHERE id = $1")
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}

	rows, err := stmt.QueryContext(ctx, driver.NamedValue{Ordinal: 1, Value: int64(1)})
	if err != nil {
		_ = stmt.Close()
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		_ = stmt.Close()
		_ = tx.Rollback()
		t.Fatal("expected row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	_ = stmt.Close()

	if name != "Alice" {
		_ = tx.Rollback()
		t.Fatalf("expected Alice, got %s", name)
	}
	_ = tx.Commit()
}

func TestRepeatableReadIsolation(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	mustExec(t, db, ctx, "CREATE TABLE t (id INTEGER)")

	// RepeatableRead maps to Snapshot
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
}

func TestDefaultIsolation(t *testing.T) {
	db, err := Open("memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()

	// nil opts
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	// Default level
	tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()

	// ReadCommitted
	tx, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
}
