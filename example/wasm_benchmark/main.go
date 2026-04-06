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

// Stoolap wasm2go vs Stoolap wazero vs SQLite wazero — Pure Go Benchmark
//
// Three engines compared:
//   - Stoolap wasm2go: transpiled to native Go via wasm2go (zero runtime overhead)
//   - Stoolap wazero:  interpreted WASM via wazero runtime
//   - SQLite wazero:   ncruces/go-sqlite3 via wazero
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/stoolap/stoolap-go/wasm"
	"github.com/stoolap/stoolap-go/wasm2go"
)

const (
	rowCount         = 10_000
	iterations       = 500
	iterationsMedium = 250
	iterationsHeavy  = 50
	warmup           = 10
	maxColumns       = 8
)

var (
	ctx      = context.Background()
	scanDest [maxColumns]any
	scanPtrs [maxColumns]any
)

func init() {
	for i := range scanPtrs {
		scanPtrs[i] = &scanDest[i]
	}
}

func fmtUs(us float64) string { return fmt.Sprintf("%12.1f", us) }

func fmtRatio(a, b float64) string {
	if a <= 0 || b <= 0 {
		return "     -"
	}
	r := b / a
	if r >= 1 {
		return fmt.Sprintf("%7.1fx", r)
	}
	return fmt.Sprintf("%6.1fx*", 1/r)
}

func printHeader(section string) {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════════════════════════")
	fmt.Println(section)
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════════════════════════")
	fmt.Printf("%-28s │ %12s │ %12s │ %12s │ %7s │ %7s\n",
		"Operation", "wasm2go(μs)", "wazero(μs)", "SQLite(μs)", "w2g/wzr", "w2g/sql")
	fmt.Println("─────────────────────────────┼──────────────┼──────────────┼──────────────┼─────────┼────────")
}

func printRow(name string, w2gUs, wzrUs, sqlUs float64) {
	fmt.Printf("%-28s │ %s │ %s │ %s │ %s │ %s\n",
		name, fmtUs(w2gUs), fmtUs(wzrUs), fmtUs(sqlUs),
		fmtRatio(w2gUs, wzrUs), fmtRatio(w2gUs, sqlUs))
}

func seedRandom(i int) int { return (i*1103515245 + 12345) & 0x7FFFFFFF }

func benchUs(fn func(), iters int) float64 {
	start := time.Now()
	for range iters {
		fn()
	}
	return float64(time.Since(start).Nanoseconds()) / float64(iters) / 1000.0
}

func queryAll(stmt *sql.Stmt, args ...any) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		log.Fatalf("queryAll: %v", err)
	}
	cols, _ := rows.Columns()
	n := len(cols)
	for rows.Next() {
		rows.Scan(scanPtrs[:n]...)
	}
	rows.Close()
}

func queryOne(stmt *sql.Stmt, args ...any) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		log.Fatalf("queryOne: %v", err)
	}
	cols, _ := rows.Columns()
	n := len(cols)
	if rows.Next() {
		rows.Scan(scanPtrs[:n]...)
	}
	rows.Close()
}

func main() {
	wasmPath := "../../wasm/stoolap.wasm"
	if len(os.Args) > 1 {
		wasmPath = os.Args[1]
	}

	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot read WASM: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Stoolap wasm2go vs Stoolap wazero vs SQLite wazero")
	fmt.Printf("Configuration: %d rows, %d iterations per test\n", rowCount, iterations)
	fmt.Println("w2g/wzr = wasm2go speedup over wazero  |  w2g/sql = wasm2go speedup over SQLite")
	fmt.Println("Ratio > 1x = wasm2go faster  |  * = other engine faster")

	// ─── wasm2go setup ──────────────────────────────────────────
	fmt.Print("\nInitializing Stoolap wasm2go... ")
	t0 := time.Now()
	if err := wasm2go.Init(); err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", time.Since(t0))

	gdb, err := sql.Open("stoolap", "memory://")
	if err != nil {
		panic(err)
	}
	defer gdb.Close()
	gdb.SetMaxOpenConns(1)

	gdb.ExecContext(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		balance FLOAT NOT NULL,
		active BOOLEAN NOT NULL,
		created_at TEXT NOT NULL
	)`)
	gdb.ExecContext(ctx, "CREATE INDEX idx_users_age ON users(age)")
	gdb.ExecContext(ctx, "CREATE INDEX idx_users_active ON users(active)")

	// ─── wazero setup ───────────────────────────────────────────
	fmt.Print("Initializing Stoolap wazero... ")
	t0 = time.Now()
	if err := wasm.SetWASM(ctx, wasmBytes); err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", time.Since(t0))

	sdb, err := sql.Open("stoolap-wasm", "memory://")
	if err != nil {
		panic(err)
	}
	defer sdb.Close()
	sdb.SetMaxOpenConns(1)

	sdb.ExecContext(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		balance FLOAT NOT NULL,
		active BOOLEAN NOT NULL,
		created_at TEXT NOT NULL
	)`)
	sdb.ExecContext(ctx, "CREATE INDEX idx_users_age ON users(age)")
	sdb.ExecContext(ctx, "CREATE INDEX idx_users_active ON users(active)")

	// ─── SQLite setup ───────────────────────────────────────────
	fmt.Print("Initializing SQLite wazero... ")
	t0 = time.Now()
	ldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	defer ldb.Close()
	ldb.SetMaxOpenConns(1)
	ldb.ExecContext(ctx, "PRAGMA journal_mode=WAL")
	ldb.ExecContext(ctx, `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		balance REAL NOT NULL,
		active INTEGER NOT NULL,
		created_at TEXT NOT NULL
	)`)
	ldb.ExecContext(ctx, "CREATE INDEX idx_users_age ON users(age)")
	ldb.ExecContext(ctx, "CREATE INDEX idx_users_active ON users(active)")
	fmt.Printf("%v\n", time.Since(t0))

	// ─── Populate ───────────────────────────────────────────────
	type userRow struct {
		id        int64
		name      string
		email     string
		age       int64
		balance   float64
		active    int64
		createdAt string
	}

	users := make([]userRow, rowCount)
	for i := range rowCount {
		id := int64(i + 1)
		age := int64((seedRandom(int(id)) % 62) + 18)
		balance := float64(seedRandom(int(id)*7)%100000) + float64(seedRandom(int(id)*13)%100)/100.0
		active := int64(0)
		if seedRandom(int(id)*3)%10 < 7 {
			active = 1
		}
		users[i] = userRow{id, fmt.Sprintf("User_%d", id), fmt.Sprintf("user%d@example.com", id), age, balance, active, "2024-01-01 00:00:00"}
	}

	seed := func(label string, db *sql.DB, paramFmt string) {
		fmt.Printf("Seeding %s... ", label)
		t := time.Now()
		tx, _ := db.BeginTx(ctx, nil)
		ins, _ := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
			paramFmt, paramFmt, paramFmt, paramFmt, paramFmt, paramFmt, paramFmt))
		for _, u := range users {
			ins.ExecContext(ctx, u.id, u.name, u.email, u.age, u.balance, u.active, u.createdAt)
		}
		ins.Close()
		tx.Commit()
		fmt.Printf("%v\n", time.Since(t))
	}

	seed("wasm2go", gdb, "$1")
	seed("wazero", sdb, "$1")
	seed("SQLite", ldb, "?")

	// ─── Benchmark helper ───────────────────────────────────────
	bench3 := func(name string, gSQL, sSQL, lSQL string, iters int, args ...any) {
		gs, _ := gdb.PrepareContext(ctx, gSQL)
		ss, _ := sdb.PrepareContext(ctx, sSQL)
		ls, _ := ldb.PrepareContext(ctx, lSQL)
		for range warmup {
			queryAll(gs, args...)
			queryAll(ss, args...)
			queryAll(ls, args...)
		}
		gu := benchUs(func() { queryAll(gs, args...) }, iters)
		su := benchUs(func() { queryAll(ss, args...) }, iters)
		lu := benchUs(func() { queryAll(ls, args...) }, iters)
		printRow(name, gu, su, lu)
		gs.Close()
		ss.Close()
		ls.Close()
	}

	benchSame := func(name, query string, iters int) {
		bench3(name, query, query, query, iters)
	}

	benchStoolap := func(name, stoolapSQL, sqliteSQL string, iters int, args ...any) {
		bench3(name, stoolapSQL, stoolapSQL, sqliteSQL, iters, args...)
	}

	// ═══════════════════════════════════════════════════════════
	printHeader("CORE OPERATIONS")

	// SELECT by ID
	{
		gs, _ := gdb.PrepareContext(ctx, "SELECT * FROM users WHERE id = $1")
		ss, _ := sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id = $1")
		ls, _ := ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id = ?")
		ids := make([]int64, iterations)
		for i := range ids {
			ids[i] = int64((i % rowCount) + 1)
		}
		for i := range warmup {
			queryOne(gs, ids[i])
			queryOne(ss, ids[i])
			queryOne(ls, ids[i])
		}
		gu := benchUs(func() {
			for _, id := range ids {
				queryOne(gs, id)
			}
		}, 1) / float64(iterations)
		su := benchUs(func() {
			for _, id := range ids {
				queryOne(ss, id)
			}
		}, 1) / float64(iterations)
		lu := benchUs(func() {
			for _, id := range ids {
				queryOne(ls, id)
			}
		}, 1) / float64(iterations)
		printRow("SELECT by ID", gu, su, lu)
		gs.Close()
		ss.Close()
		ls.Close()
	}

	// SELECT by index
	{
		gs, _ := gdb.PrepareContext(ctx, "SELECT * FROM users WHERE age = $1")
		ss, _ := sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age = $1")
		ls, _ := ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age = ?")
		ages := make([]int64, iterations)
		for i := range ages {
			ages[i] = int64((i % 62) + 18)
		}
		for i := range warmup {
			queryAll(gs, ages[i])
			queryAll(ss, ages[i])
			queryAll(ls, ages[i])
		}
		gu := benchUs(func() {
			for _, a := range ages {
				queryAll(gs, a)
			}
		}, 1) / float64(iterations)
		su := benchUs(func() {
			for _, a := range ages {
				queryAll(ss, a)
			}
		}, 1) / float64(iterations)
		lu := benchUs(func() {
			for _, a := range ages {
				queryAll(ls, a)
			}
		}, 1) / float64(iterations)
		printRow("SELECT by index (exact)", gu, su, lu)
		gs.Close()
		ss.Close()
		ls.Close()
	}

	benchStoolap("SELECT by index (range)",
		"SELECT * FROM users WHERE age >= $1 AND age <= $2",
		"SELECT * FROM users WHERE age >= ? AND age <= ?", iterations, int64(30), int64(40))

	benchStoolap("SELECT complex",
		"SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100",
		"SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100", iterations)

	benchSame("SELECT * (full scan)", "SELECT * FROM users", iterationsHeavy)

	// UPDATE by ID
	{
		gs, _ := gdb.PrepareContext(ctx, "UPDATE users SET balance = $1 WHERE id = $2")
		ss, _ := sdb.PrepareContext(ctx, "UPDATE users SET balance = $1 WHERE id = $2")
		ls, _ := ldb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE id = ?")
		type p struct {
			b float64
			i int64
		}
		ps := make([]p, iterations)
		for i := range ps {
			ps[i] = p{float64(seedRandom(i*17)%100000) + 0.5, int64((i % rowCount) + 1)}
		}
		for i := range warmup {
			gs.ExecContext(ctx, ps[i].b, ps[i].i)
			ss.ExecContext(ctx, ps[i].b, ps[i].i)
			ls.ExecContext(ctx, ps[i].b, ps[i].i)
		}
		gu := benchUs(func() {
			for _, p := range ps {
				gs.ExecContext(ctx, p.b, p.i)
			}
		}, 1) / float64(iterations)
		su := benchUs(func() {
			for _, p := range ps {
				ss.ExecContext(ctx, p.b, p.i)
			}
		}, 1) / float64(iterations)
		lu := benchUs(func() {
			for _, p := range ps {
				ls.ExecContext(ctx, p.b, p.i)
			}
		}, 1) / float64(iterations)
		printRow("UPDATE by ID", gu, su, lu)
		gs.Close()
		ss.Close()
		ls.Close()
	}

	// INSERT single
	{
		gs, _ := gdb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
		ss, _ := sdb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
		ls, _ := ldb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
		base := int64(rowCount + 10000)

		measure := func(st *sql.Stmt, off int64) float64 {
			t := time.Now()
			for i := range iterations {
				id := base + off + int64(i)
				st.ExecContext(ctx, id, fmt.Sprintf("N_%d", id), fmt.Sprintf("n%d@e.com", id), int64(25), 100.0, int64(1), "2024-01-01")
			}
			return float64(time.Since(t).Nanoseconds()) / float64(iterations) / 1000.0
		}
		gu := measure(gs, 0)
		su := measure(ss, int64(iterations))
		lu := measure(ls, int64(iterations*2))
		printRow("INSERT single", gu, su, lu)
		gs.Close()
		ss.Close()
		ls.Close()
	}

	// Aggregation
	benchSame("Aggregation (GROUP BY)", "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age", iterationsMedium)

	// ═══════════════════════════════════════════════════════════
	// Orders table setup
	for _, db := range []*sql.DB{gdb, sdb} {
		db.ExecContext(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount FLOAT NOT NULL, status TEXT NOT NULL, order_date TEXT NOT NULL)`)
		db.ExecContext(ctx, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
		db.ExecContext(ctx, "CREATE INDEX idx_orders_status ON orders(status)")
	}
	ldb.ExecContext(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL NOT NULL, status TEXT NOT NULL, order_date TEXT NOT NULL)`)
	ldb.ExecContext(ctx, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
	ldb.ExecContext(ctx, "CREATE INDEX idx_orders_status ON orders(status)")

	statuses := []string{"pending", "completed", "shipped", "cancelled"}
	type orderRow struct {
		id        int64
		userID    int64
		amount    float64
		status    string
		orderDate string
	}
	orderRows := make([]orderRow, rowCount*3)
	for i := range rowCount * 3 {
		id := int64(i + 1)
		userID := int64((seedRandom(int(id)*11) % rowCount) + 1)
		amount := float64(seedRandom(int(id)*19)%990+10) + float64(seedRandom(int(id)*23)%100)/100.0
		orderRows[i] = orderRow{id, userID, amount, statuses[seedRandom(int(id)*31)%4], "2024-01-15"}
	}

	seedOrders := func(label string, db *sql.DB, pf string) {
		fmt.Printf("Seeding orders %s... ", label)
		t := time.Now()
		tx, _ := db.BeginTx(ctx, nil)
		ins, _ := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (%s, %s, %s, %s, %s)", pf, pf, pf, pf, pf))
		for _, o := range orderRows {
			ins.ExecContext(ctx, o.id, o.userID, o.amount, o.status, o.orderDate)
		}
		ins.Close()
		tx.Commit()
		fmt.Printf("%v\n", time.Since(t))
	}
	seedOrders("wasm2go", gdb, "$1")
	seedOrders("wazero", sdb, "$1")
	seedOrders("SQLite", ldb, "?")

	printHeader("ADVANCED OPERATIONS")

	benchStoolap("INNER JOIN",
		"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100",
		"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100", 100)

	benchStoolap("LEFT JOIN + GROUP BY",
		"SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100",
		"SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100", 100)

	benchSame("Scalar subquery",
		"SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100", iterations)

	benchSame("IN subquery",
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100", 10)

	benchSame("EXISTS subquery",
		"SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100", 100)

	benchSame("CTE + JOIN",
		"WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100", 20)

	benchSame("Window ROW_NUMBER",
		"SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100", iterations)

	benchSame("Window PARTITION BY",
		"SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100", iterations)

	benchSame("UNION ALL",
		"SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100", iterations)

	benchSame("Multi aggregates (6)",
		"SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users", iterations)

	benchStoolap("Large result (no LIMIT)",
		"SELECT id, name, balance FROM users WHERE active = true",
		"SELECT id, name, balance FROM users WHERE active = 1", 20)

	// ═══════════════════════════════════════════════════════════
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════════════════════════")
	fmt.Println("NOTES:")
	fmt.Println("- wasm2go: Stoolap WASM transpiled to native Go (zero runtime overhead)")
	fmt.Println("- wazero:  Stoolap WASM interpreted via wazero runtime")
	fmt.Println("- SQLite:  ncruces/go-sqlite3 via wazero, WAL mode, in-memory")
	fmt.Println("- All engines: pure Go, zero CGO, database/sql interface")
	fmt.Println("═══════════════════════════════════════════════════════════════════════════════════════════════════")
}
