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
	"fmt"
	"sync/atomic"
	"testing"
)

var benchDBCounter atomic.Int64

// benchSQL opens a fresh named in-memory database through database/sql. The
// bare memory:// DSN is shared across opens, so a rerun would find its table.
func benchSQL(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("stoolap-wasm", fmt.Sprintf("memory://bench%d", benchDBCounter.Add(1)))
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func benchDB(b *testing.B, n int) *DB {
	b.Helper()
	ctx := context.Background()
	db, err := testEngine.OpenMemory(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare(ctx, "INSERT INTO t VALUES ($1, $2, $3, $4)")
	if err != nil {
		b.Fatal(err)
	}
	for i := range n {
		if _, err := stmt.ExecContext(ctx, []any{int64(i + 1), fmt.Sprintf("user_%d", i+1), float64(i) / 2, i%3 == 0}); err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()
	return db
}

func BenchmarkDirectScan1000(b *testing.B) {
	db := benchDB(b, 1000)
	defer db.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Query(ctx, "SELECT id, name, score, active FROM t")
		if err != nil {
			b.Fatal(err)
		}
		var id int64
		var name string
		var score float64
		var active bool
		for rows.Next() {
			if err := rows.Scan(&id, &name, &score, &active); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkDirectPointQuery(b *testing.B) {
	db := benchDB(b, 100)
	defer db.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.QueryParams(ctx, "SELECT name FROM t WHERE id = $1", []any{int64(42)})
		if err != nil {
			b.Fatal(err)
		}
		var name string
		for rows.Next() {
			if err := rows.Scan(&name); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkPreparedPointQuery(b *testing.B) {
	db := benchDB(b, 100)
	defer db.Close()
	ctx := context.Background()
	stmt, err := db.Prepare(ctx, "SELECT name FROM t WHERE id = $1")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := stmt.QueryContext(ctx, []any{int64(42)})
		if err != nil {
			b.Fatal(err)
		}
		var name string
		for rows.Next() {
			if err := rows.Scan(&name); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkSQLScan1000(b *testing.B) {
	db := benchSQL(b)
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score FLOAT, active BOOLEAN)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO t VALUES ($1, $2, $3, $4)")
	if err != nil {
		b.Fatal(err)
	}
	for i := range 1000 {
		if _, err := stmt.Exec(int64(i+1), fmt.Sprintf("user_%d", i+1), float64(i)/2, i%3 == 0); err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Query("SELECT id, name, score, active FROM t")
		if err != nil {
			b.Fatal(err)
		}
		var id int64
		var name string
		var score float64
		var active bool
		for rows.Next() {
			if err := rows.Scan(&id, &name, &score, &active); err != nil {
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkSQLPointQuery(b *testing.B) {
	db := benchSQL(b)
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatal(err)
	}
	for i := range 100 {
		if _, err := db.Exec("INSERT INTO t VALUES ($1, $2)", int64(i+1), fmt.Sprintf("user_%d", i+1)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var name string
		if err := db.QueryRow("SELECT name FROM t WHERE id = $1", int64(42)).Scan(&name); err != nil {
			b.Fatal(err)
		}
	}
}
