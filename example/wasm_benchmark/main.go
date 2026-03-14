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

// Stoolap WASM vs SQLite WASM — Pure Go Benchmark (zero CGO)
//
// Both engines run as WebAssembly modules via wazero.
// Both use database/sql with prepared statements.
// This is an apples-to-apples comparison of two WASM-based embedded databases.
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
	stoolapWins int
	sqliteWins  int
	ctx         = context.Background()
	scanDest    [maxColumns]any
	scanPtrs    [maxColumns]any
)

func init() {
	for i := range scanPtrs {
		scanPtrs[i] = &scanDest[i]
	}
}

// ============================================================
// Helpers
// ============================================================

func fmtUs(us float64) string {
	return fmt.Sprintf("%15.3f", us)
}

func fmtRatio(stoolapUs, sqliteUs float64) string {
	if stoolapUs <= 0 || sqliteUs <= 0 {
		return "      -"
	}
	ratio := sqliteUs / stoolapUs
	if ratio >= 1 {
		return fmt.Sprintf("%9.2fx", ratio)
	}
	return fmt.Sprintf("%8.2fx*", 1/ratio)
}

func printRow(name string, stoolapUs, sqliteUs float64) {
	ratio := fmtRatio(stoolapUs, sqliteUs)
	if stoolapUs < sqliteUs {
		stoolapWins++
	} else if sqliteUs < stoolapUs {
		sqliteWins++
	}
	fmt.Printf("%-28s | %s | %s | %s\n", name, fmtUs(stoolapUs), fmtUs(sqliteUs), ratio)
}

func printHeader(section string) {
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println(section)
	fmt.Println("================================================================================")
	fmt.Printf("%-28s | %15s | %15s | %10s\n", "Operation", "Stoolap (μs)", "SQLite (μs)", "Ratio")
	fmt.Println("--------------------------------------------------------------------------------")
}

func seedRandom(i int) int {
	return (i*1103515245 + 12345) & 0x7FFFFFFF
}

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

// ============================================================
// Main
// ============================================================

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

	fmt.Println("Stoolap WASM vs SQLite WASM (ncruces/go-sqlite3) — Pure Go Benchmark")
	fmt.Printf("Configuration: %d rows, %d iterations per test\n", rowCount, iterations)
	fmt.Println("Both engines use WASM (wazero) + database/sql — zero CGO")
	fmt.Println("Ratio > 1x = Stoolap faster  |  * = SQLite faster")

	// --- Stoolap WASM setup ---
	fmt.Print("\nInitializing Stoolap WASM... ")
	t0 := time.Now()
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

	// --- SQLite WASM setup ---
	fmt.Print("Initializing SQLite WASM... ")
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

	// --- Populate users ---
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

	// SQLite bulk insert
	fmt.Print("Seeding SQLite... ")
	t0 = time.Now()
	lTx, _ := ldb.BeginTx(ctx, nil)
	lInsert, _ := lTx.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	for _, u := range users {
		lInsert.ExecContext(ctx, u.id, u.name, u.email, u.age, u.balance, u.active, u.createdAt)
	}
	lInsert.Close()
	lTx.Commit()
	fmt.Printf("%v\n", time.Since(t0))

	// Stoolap bulk insert
	fmt.Print("Seeding Stoolap... ")
	t0 = time.Now()
	sTx, _ := sdb.BeginTx(ctx, nil)
	sInsert, _ := sTx.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
	for _, u := range users {
		sInsert.ExecContext(ctx, u.id, u.name, u.email, u.age, u.balance, u.active, u.createdAt)
	}
	sInsert.Close()
	sTx.Commit()
	fmt.Printf("%v\n", time.Since(t0))

	// ============================================================
	// CORE OPERATIONS
	// ============================================================
	printHeader("CORE OPERATIONS")

	// --- SELECT by ID ---
	sSt, _ := sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id = $1")
	lSt, _ := ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id = ?")
	ids := make([]int64, iterations)
	for i := range ids {
		ids[i] = int64((i % rowCount) + 1)
	}

	for i := range warmup {
		queryOne(sSt, ids[i])
		queryOne(lSt, ids[i])
	}

	sUs := benchUs(func() {
		for _, id := range ids {
			queryOne(sSt, id)
		}
	}, 1) / float64(iterations)
	lUs := benchUs(func() {
		for _, id := range ids {
			queryOne(lSt, id)
		}
	}, 1) / float64(iterations)
	printRow("SELECT by ID", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- SELECT by index (exact) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age = $1")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age = ?")
	ages := make([]int64, iterations)
	for i := range ages {
		ages[i] = int64((i % 62) + 18)
	}

	for i := range warmup {
		queryAll(sSt, ages[i])
		queryAll(lSt, ages[i])
	}

	sUs = benchUs(func() {
		for _, a := range ages {
			queryAll(sSt, a)
		}
	}, 1) / float64(iterations)
	lUs = benchUs(func() {
		for _, a := range ages {
			queryAll(lSt, a)
		}
	}, 1) / float64(iterations)
	printRow("SELECT by index (exact)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- SELECT by index (range) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age >= $1 AND age <= $2")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age >= ? AND age <= ?")

	for range warmup {
		queryAll(sSt, int64(30), int64(40))
		queryAll(lSt, int64(30), int64(40))
	}

	sUs = benchUs(func() { queryAll(sSt, int64(30), int64(40)) }, iterations)
	lUs = benchUs(func() { queryAll(lSt, int64(30), int64(40)) }, iterations)
	printRow("SELECT by index (range)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- SELECT complex ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100")

	for range warmup {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("SELECT complex", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- SELECT * (full scan) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users")

	for range warmup {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterationsHeavy)
	lUs = benchUs(func() { queryAll(lSt) }, iterationsHeavy)
	printRow("SELECT * (full scan)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- UPDATE by ID ---
	sSt, _ = sdb.PrepareContext(ctx, "UPDATE users SET balance = $1 WHERE id = $2")
	lSt, _ = ldb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE id = ?")

	type updateParam struct {
		balance float64
		id      int64
	}
	updateParams := make([]updateParam, iterations)
	for i := range updateParams {
		updateParams[i] = updateParam{float64(seedRandom(i*17)%100000) + 0.5, int64((i % rowCount) + 1)}
	}

	for i := range warmup {
		sSt.ExecContext(ctx, updateParams[i].balance, updateParams[i].id)
		lSt.ExecContext(ctx, updateParams[i].balance, updateParams[i].id)
	}

	sUs = benchUs(func() {
		for _, p := range updateParams {
			sSt.ExecContext(ctx, p.balance, p.id)
		}
	}, 1) / float64(iterations)
	lUs = benchUs(func() {
		for _, p := range updateParams {
			lSt.ExecContext(ctx, p.balance, p.id)
		}
	}, 1) / float64(iterations)
	printRow("UPDATE by ID", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- UPDATE complex ---
	sSt, _ = sdb.PrepareContext(ctx, "UPDATE users SET balance = $1 WHERE age >= $2 AND age <= $3 AND active = true")
	lSt, _ = ldb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = 1")
	balances := make([]float64, iterations)
	for i := range balances {
		balances[i] = float64(seedRandom(i*23)%100000) + 0.5
	}

	sUs = benchUs(func() {
		for _, b := range balances {
			sSt.ExecContext(ctx, b, int64(27), int64(28))
		}
	}, 1) / float64(iterations)
	lUs = benchUs(func() {
		for _, b := range balances {
			lSt.ExecContext(ctx, b, int64(27), int64(28))
		}
	}, 1) / float64(iterations)
	printRow("UPDATE complex", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- INSERT single ---
	sSt, _ = sdb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
	lSt, _ = ldb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	base := int64(rowCount + 1000)

	t0 = time.Now()
	for i := range iterations {
		id := base + int64(i)
		sSt.ExecContext(ctx, id, fmt.Sprintf("New_%d", id), fmt.Sprintf("new%d@example.com", id), int64(25), 100.0, int64(1), "2024-01-01 00:00:00")
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for i := range iterations {
		id := base + int64(iterations) + int64(i)
		lSt.ExecContext(ctx, id, fmt.Sprintf("New_%d", id), fmt.Sprintf("new%d@example.com", id), int64(25), 100.0, int64(1), "2024-01-01 00:00:00")
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("INSERT single", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- DELETE by ID ---
	sSt, _ = sdb.PrepareContext(ctx, "DELETE FROM users WHERE id = $1")
	lSt, _ = ldb.PrepareContext(ctx, "DELETE FROM users WHERE id = ?")

	t0 = time.Now()
	for i := range iterations {
		sSt.ExecContext(ctx, base+int64(i))
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for i := range iterations {
		lSt.ExecContext(ctx, base+int64(iterations)+int64(i))
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("DELETE by ID", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- DELETE complex ---
	sSt, _ = sdb.PrepareContext(ctx, "DELETE FROM users WHERE age >= $1 AND age <= $2 AND active = true")
	lSt, _ = ldb.PrepareContext(ctx, "DELETE FROM users WHERE age >= ? AND age <= ? AND active = 1")

	t0 = time.Now()
	for range iterations {
		sSt.ExecContext(ctx, int64(25), int64(26))
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for range iterations {
		lSt.ExecContext(ctx, int64(25), int64(26))
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("DELETE complex", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Aggregation (GROUP BY) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")

	for range warmup {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterationsMedium)
	lUs = benchUs(func() { queryAll(lSt) }, iterationsMedium)
	printRow("Aggregation (GROUP BY)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// ============================================================
	// ADVANCED OPERATIONS
	// ============================================================

	// Create orders table
	sdb.ExecContext(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER NOT NULL,
		amount FLOAT NOT NULL,
		status TEXT NOT NULL,
		order_date TEXT NOT NULL
	)`)
	sdb.ExecContext(ctx, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
	sdb.ExecContext(ctx, "CREATE INDEX idx_orders_status ON orders(status)")

	ldb.ExecContext(ctx, `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER NOT NULL,
		amount REAL NOT NULL,
		status TEXT NOT NULL,
		order_date TEXT NOT NULL
	)`)
	ldb.ExecContext(ctx, "CREATE INDEX idx_orders_user_id ON orders(user_id)")
	ldb.ExecContext(ctx, "CREATE INDEX idx_orders_status ON orders(status)")

	// Populate orders
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
		status := statuses[seedRandom(int(id)*31)%4]
		orderRows[i] = orderRow{id, userID, amount, status, "2024-01-15"}
	}

	fmt.Print("\nSeeding orders SQLite... ")
	t0 = time.Now()
	lTx, _ = ldb.BeginTx(ctx, nil)
	lOrdInsert, _ := lTx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
	for _, o := range orderRows {
		lOrdInsert.ExecContext(ctx, o.id, o.userID, o.amount, o.status, o.orderDate)
	}
	lOrdInsert.Close()
	lTx.Commit()
	fmt.Printf("%v\n", time.Since(t0))

	fmt.Print("Seeding orders Stoolap... ")
	t0 = time.Now()
	sTx, _ = sdb.BeginTx(ctx, nil)
	sOrdInsert, _ := sTx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)")
	for _, o := range orderRows {
		sOrdInsert.ExecContext(ctx, o.id, o.userID, o.amount, o.status, o.orderDate)
	}
	sOrdInsert.Close()
	sTx.Commit()
	fmt.Printf("%v\n", time.Since(t0))

	printHeader("ADVANCED OPERATIONS")

	// Helper for benchmarks with identical SQL (stoolap uses $N, sqlite uses ?)
	bench := func(name, sSQL, lSQL string, iters int, args ...any) {
		ss, _ := sdb.PrepareContext(ctx, sSQL)
		ls, _ := ldb.PrepareContext(ctx, lSQL)
		for range warmup {
			queryAll(ss, args...)
			queryAll(ls, args...)
		}
		su := benchUs(func() { queryAll(ss, args...) }, iters)
		lu := benchUs(func() { queryAll(ls, args...) }, iters)
		printRow(name, su, lu)
		ss.Close()
		ls.Close()
	}

	// Convenience for identical SQL (no params)
	benchSame := func(name, query string, iters int) {
		bench(name, query, query, iters)
	}

	bench("INNER JOIN",
		"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100",
		"SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100", 100)

	bench("LEFT JOIN + GROUP BY",
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

	benchSame("Window ROW_NUMBER (PK)",
		"SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100", iterations)

	benchSame("Window PARTITION BY",
		"SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100", iterations)

	benchSame("UNION ALL",
		"SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100", iterations)

	benchSame("CASE expression",
		"SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100", iterations)

	bench("Complex JOIN+GRP+HAVING",
		"SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50",
		"SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = 1 AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50", 20)

	// --- Batch INSERT ---
	iters := iterations
	baseID := int64(rowCount * 10)

	t0 = time.Now()
	for it := range iters {
		stx, _ := sdb.BeginTx(ctx, nil)
		bs, _ := stx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($1, $2, $3, $4, $5)")
		for j := range 100 {
			bs.ExecContext(ctx, baseID+int64(it*100+j), int64(1), 100.0, "pending", "2024-02-01")
		}
		bs.Close()
		stx.Commit()
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iters) / 1000.0

	t0 = time.Now()
	for it := range iters {
		ltx, _ := ldb.BeginTx(ctx, nil)
		bl, _ := ltx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
		for j := range 100 {
			bl.ExecContext(ctx, baseID+int64(iters*100+it*100+j), int64(1), 100.0, "pending", "2024-02-01")
		}
		bl.Close()
		ltx.Commit()
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iters) / 1000.0
	printRow("Batch INSERT (100 rows)", sUs, lUs)

	// ============================================================
	// BOTTLENECK HUNTERS
	// ============================================================
	printHeader("BOTTLENECK HUNTERS")

	benchSame("DISTINCT (no ORDER)", "SELECT DISTINCT age FROM users", iterations)
	benchSame("DISTINCT + ORDER BY", "SELECT DISTINCT age FROM users ORDER BY age", iterations)
	benchSame("COUNT DISTINCT", "SELECT COUNT(DISTINCT age) FROM users", iterations)
	benchSame("LIKE prefix (User_1%)", "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100", iterations)
	benchSame("LIKE contains (%50%)", "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100", iterations)
	benchSame("OR conditions (3 vals)", "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100", iterations)
	benchSame("IN list (7 values)", "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100", iterations)
	benchSame("NOT IN subquery", "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100", 10)
	benchSame("NOT EXISTS subquery", "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100", 100)
	benchSame("OFFSET pagination (5000)", "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000", iterations)
	benchSame("Multi-col ORDER BY (3)", "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100", iterations)
	benchSame("Self JOIN (same age)", "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100", 100)
	benchSame("Multi window funcs (3)", "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100", iterations)
	benchSame("Nested subquery (3 lvl)", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100", 20)
	benchSame("Multi aggregates (6)", "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users", iterations)
	benchSame("COALESCE + IS NOT NULL", "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100", iterations)
	benchSame("Expr in WHERE (funcs)", "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100", iterations)
	benchSame("Math expressions", "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100", iterations)
	benchSame("String concat (||)", "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100", iterations)

	bench("Large result (no LIMIT)",
		"SELECT id, name, balance FROM users WHERE active = true",
		"SELECT id, name, balance FROM users WHERE active = 1", 20)

	benchSame("Multiple CTEs (2)",
		"WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50", 100)
	benchSame("Correlated in SELECT",
		"SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100", 100)
	benchSame("BETWEEN (non-indexed)", "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100", iterations)

	bench("GROUP BY (2 columns)",
		"SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active",
		"SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active", iterations)

	benchSame("CROSS JOIN (limited)", "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100", iterations)
	benchSame("Derived table (FROM sub)", "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group", iterations)
	benchSame("Window ROWS frame", "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100", iterations)
	benchSame("HAVING complex", "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000", iterations)
	benchSame("Compare with subquery", "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100", iterations)

	// ============================================================
	// Summary
	// ============================================================
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("SCORE: Stoolap %d wins  |  SQLite %d wins\n", stoolapWins, sqliteWins)
	fmt.Println()
	fmt.Println("NOTES:")
	fmt.Println("- Both engines use WASM (wazero) + database/sql — zero CGO, pure Go")
	fmt.Println("- Stoolap: WASM driver via wazero, columnar engine, MVCC")
	fmt.Println("- SQLite: ncruces/go-sqlite3 via wazero, WAL mode, in-memory")
	fmt.Println("- Ratio > 1x = Stoolap faster  |  * = SQLite faster")
	fmt.Println("================================================================================")
}
