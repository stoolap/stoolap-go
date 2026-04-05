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
	"strconv"
	"testing"
	"time"
)

const benchRowCount = 10_000

func seedRandom(i int) int {
	return (i*1103515245 + 12345) & 0x7FFFFFFF
}

// setupBenchDB creates and populates a benchmark database (direct API).
func setupBenchDB(b *testing.B) *DB {
	b.Helper()
	dsn := "memory://bench" + strconv.FormatInt(testDBCounter.Add(1), 10)
	db, err := Open(dsn)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		balance FLOAT NOT NULL,
		active BOOLEAN NOT NULL,
		created_at TEXT NOT NULL
	)`)
	db.Exec(ctx, "CREATE INDEX idx_users_age ON users(age)")
	db.Exec(ctx, "CREATE INDEX idx_users_active ON users(active)")

	tx, _ := db.Begin(ctx)
	args := make([]any, 7)
	for i := range benchRowCount {
		id := int64(i + 1)
		age := int64((seedRandom(int(id)) % 62) + 18)
		balance := float64(seedRandom(int(id)*7)%100000) + float64(seedRandom(int(id)*13)%100)/100.0
		active := seedRandom(int(id)*3)%10 < 7
		s := strconv.FormatInt(id, 10)
		args[0] = id
		args[1] = "User_" + s
		args[2] = "user" + s + "@example.com"
		args[3] = age
		args[4] = balance
		args[5] = active
		args[6] = "2024-01-01 00:00:00"
		tx.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)", args)
	}
	tx.Commit()
	return db
}

// setupBenchSQL creates and populates a benchmark database (database/sql).
func setupBenchSQL(b *testing.B) *sql.DB {
	b.Helper()
	dsn := "memory://benchsql" + strconv.FormatInt(testDBCounter.Add(1), 10)
	db, err := sql.Open("stoolap", dsn)
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	db.ExecContext(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		balance FLOAT NOT NULL,
		active BOOLEAN NOT NULL,
		created_at TEXT NOT NULL
	)`)
	db.ExecContext(ctx, "CREATE INDEX idx_users_age ON users(age)")
	db.ExecContext(ctx, "CREATE INDEX idx_users_active ON users(active)")

	tx, _ := db.BeginTx(ctx, nil)
	for i := range benchRowCount {
		id := int64(i + 1)
		age := int64((seedRandom(int(id)) % 62) + 18)
		balance := float64(seedRandom(int(id)*7)%100000) + float64(seedRandom(int(id)*13)%100)/100.0
		active := seedRandom(int(id)*3)%10 < 7
		s := strconv.FormatInt(id, 10)
		tx.ExecContext(ctx, "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
			id, "User_"+s, "user"+s+"@example.com", age, balance, active, "2024-01-01 00:00:00")
	}
	tx.Commit()
	return db
}

// ─── Direct API Benchmarks ──────────────────────────────────────────────────

func BenchmarkDirectSelectByID(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	args := []any{int64(0)}
	i := 0

	for b.Loop() {
		args[0] = int64((i % benchRowCount) + 1)
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE id = $1", args)
		rows.Next()
		var a1, a2, a3, a4, a5, a6, a7 any
		rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		rows.Close()
		i++
	}
}

func BenchmarkDirectSelectByIDTyped(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	args := []any{int64(0)}
	var id, age int64
	var name, email, createdAt string
	var balance float64
	var active bool
	i := 0

	for b.Loop() {
		args[0] = int64((i % benchRowCount) + 1)
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE id = $1", args)
		rows.Next()
		rows.Scan(&id, &name, &email, &age, &balance, &active, &createdAt)
		rows.Close()
		i++
	}
}

func BenchmarkDirectSelectByIndex(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	args := []any{int64(0)}
	i := 0

	for b.Loop() {
		args[0] = int64((i%62) + 18)
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE age = $1", args)
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
		i++
	}
}

func BenchmarkDirectSelectRange(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for b.Loop() {
		rows, _ := db.Query(ctx, "SELECT * FROM users WHERE age >= 30 AND age <= 40")
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
	}
}

func BenchmarkDirectSelectComplex(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for b.Loop() {
		rows, _ := db.Query(ctx, "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100")
		for rows.Next() {
			var a1, a2, a3 any
			rows.Scan(&a1, &a2, &a3)
		}
		rows.Close()
	}
}

func BenchmarkDirectSelectFullScan(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for b.Loop() {
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
	}
}

func BenchmarkDirectFetchAllFullScan(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for b.Loop() {
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		rows.FetchAll()
		rows.Close()
	}
}

func BenchmarkDirectInsert(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	base := int64(benchRowCount + 100000)
	args := []any{int64(0), "NewUser", "new@example.com", int64(30), 100.0, true, "2024-01-01 00:00:00"}
	i := int64(0)

	for b.Loop() {
		args[0] = base + i
		db.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)", args)
		i++
	}
}

func BenchmarkDirectInsertParams(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	base := int64(benchRowCount + 150000)
	args := []any{int64(0), "NewUser", "new@example.com", int64(30), 100.0, true, "2024-01-01 00:00:00"}
	i := int64(0)

	for b.Loop() {
		args[0] = base + i
		db.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)", args)
		i++
	}
}

func BenchmarkDirectPreparedInsert(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	base := int64(benchRowCount + 175000)

	stmt, err := db.Prepare(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	args := []any{int64(0), "NewUser", "new@example.com", int64(30), 100.0, true, "2024-01-01 00:00:00"}
	i := int64(0)

	for b.Loop() {
		args[0] = base + i
		stmt.ExecContext(ctx, args)
		i++
	}
}

func BenchmarkDirectUpdateByID(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()
	args := []any{0.0, int64(0)}
	i := 0

	for b.Loop() {
		args[0] = float64(i) + 0.5
		args[1] = int64((i % benchRowCount) + 1)
		db.ExecParams(ctx, "UPDATE users SET balance = $1 WHERE id = $2", args)
		i++
	}
}

func BenchmarkDirectAggregation(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()
	ctx := context.Background()

	for b.Loop() {
		rows, _ := db.Query(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")
		for rows.Next() {
			var a1, a2, a3 any
			rows.Scan(&a1, &a2, &a3)
		}
		rows.Close()
	}
}

// ─── database/sql Benchmarks ────────────────────────────────────────────────

func BenchmarkSQLSelectByID(b *testing.B) {
	db := setupBenchSQL(b)
	defer db.Close()
	ctx := context.Background()
	stmt, _ := db.PrepareContext(ctx, "SELECT * FROM users WHERE id = ?")
	defer stmt.Close()

	var dest [7]any
	ptrs := [7]any{&dest[0], &dest[1], &dest[2], &dest[3], &dest[4], &dest[5], &dest[6]}
	i := 0

	for b.Loop() {
		id := int64((i % benchRowCount) + 1)
		rows, _ := stmt.QueryContext(ctx, id)
		if rows.Next() {
			rows.Scan(ptrs[:]...)
		}
		rows.Close()
		i++
	}
}

func BenchmarkSQLSelectFullScan(b *testing.B) {
	db := setupBenchSQL(b)
	defer db.Close()
	ctx := context.Background()
	stmt, _ := db.PrepareContext(ctx, "SELECT * FROM users")
	defer stmt.Close()

	var dest [7]any
	ptrs := [7]any{&dest[0], &dest[1], &dest[2], &dest[3], &dest[4], &dest[5], &dest[6]}

	for b.Loop() {
		rows, _ := stmt.QueryContext(ctx)
		for rows.Next() {
			rows.Scan(ptrs[:]...)
		}
		rows.Close()
	}
}

func BenchmarkSQLInsert(b *testing.B) {
	db := setupBenchSQL(b)
	defer db.Close()
	ctx := context.Background()
	base := int64(benchRowCount + 200000)
	stmt, _ := db.PrepareContext(ctx, "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)")
	defer stmt.Close()
	i := int64(0)

	for b.Loop() {
		id := base + i
		stmt.ExecContext(ctx, id, "NewUser", "new@example.com", int64(30), 100.0, true, "2024-01-01 00:00:00")
		i++
	}
}

// ─── Manual timing benchmark (prints table) ─────────────────────────────────

func TestBenchmarkSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark summary in short mode")
	}

	ctx := context.Background()
	iters := 200

	dsn := "memory://benchsum" + strconv.FormatInt(testDBCounter.Add(1), 10)
	db, _ := Open(dsn)
	defer db.Close()

	db.Exec(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL,
		age INTEGER NOT NULL, balance FLOAT NOT NULL, active BOOLEAN NOT NULL,
		created_at TEXT NOT NULL)`)
	db.Exec(ctx, "CREATE INDEX idx_users_age ON users(age)")
	db.Exec(ctx, "CREATE INDEX idx_users_active ON users(active)")

	tx, _ := db.Begin(ctx)
	args := make([]any, 7)
	for i := range benchRowCount {
		id := int64(i + 1)
		age := int64((seedRandom(int(id)) % 62) + 18)
		balance := float64(seedRandom(int(id)*7)%100000) + float64(seedRandom(int(id)*13)%100)/100.0
		active := seedRandom(int(id)*3)%10 < 7
		s := strconv.FormatInt(id, 10)
		args[0] = id
		args[1] = "User_" + s
		args[2] = "user" + s + "@example.com"
		args[3] = age
		args[4] = balance
		args[5] = active
		args[6] = "2024-01-01 00:00:00"
		tx.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)", args)
	}
	tx.Commit()

	benchFn := func(name string, fn func()) {
		for range 5 {
			fn()
		}
		start := time.Now()
		for range iters {
			fn()
		}
		us := float64(time.Since(start).Nanoseconds()) / float64(iters) / 1000.0
		t.Logf("%-32s %12.1f μs/op", name, us)
	}

	t.Log("")
	t.Logf("Purego Driver Benchmark (%d rows, %d iters)", benchRowCount, iters)
	t.Logf("%-32s %12s", "Operation", "Time")
	t.Logf("%-32s %12s", "─────────", "────")

	benchFn("SELECT by ID", func() {
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE id = $1", []any{int64(42)})
		rows.Next()
		var a1, a2, a3, a4, a5, a6, a7 any
		rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		rows.Close()
	})

	benchFn("SELECT by index (exact)", func() {
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE age = $1", []any{int64(30)})
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
	})

	benchFn("SELECT by index (range)", func() {
		rows, _ := db.QueryParams(ctx, "SELECT * FROM users WHERE age >= $1 AND age <= $2", []any{int64(30), int64(40)})
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
	})

	benchFn("SELECT complex", func() {
		rows, _ := db.Query(ctx, "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100")
		for rows.Next() {
			var a1, a2, a3 any
			rows.Scan(&a1, &a2, &a3)
		}
		rows.Close()
	})

	benchFn("SELECT * (full scan)", func() {
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		for rows.Next() {
			var a1, a2, a3, a4, a5, a6, a7 any
			rows.Scan(&a1, &a2, &a3, &a4, &a5, &a6, &a7)
		}
		rows.Close()
	})

	benchFn("SELECT * (FetchAll)", func() {
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		rows.FetchAll()
		rows.Close()
	})

	benchFn("Aggregation (GROUP BY)", func() {
		rows, _ := db.Query(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")
		for rows.Next() {
			var a1, a2, a3 any
			rows.Scan(&a1, &a2, &a3)
		}
		rows.Close()
	})

	counter := int64(benchRowCount + 500000)
	benchFn("INSERT single", func() {
		counter++
		db.ExecParams(ctx, "INSERT INTO users VALUES ($1, $2, $3, $4, $5, $6, $7)",
			[]any{counter, "NewUser", "new@example.com", int64(30), 100.0, true, "2024-01-01"})
	})

	benchFn("UPDATE by ID", func() {
		db.ExecParams(ctx, "UPDATE users SET balance = $1 WHERE id = $2", []any{999.99, int64(42)})
	})
}
