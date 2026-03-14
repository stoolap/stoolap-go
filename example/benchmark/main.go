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
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/stoolap/stoolap-go/pkg/driver"
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
	// Pre-allocated scan buffers shared across benchmark iterations
	// to avoid measuring allocation overhead instead of engine speed.
	scanDest [maxColumns]any
	scanPtrs [maxColumns]any
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
	for i := 0; i < iters; i++ {
		fn()
	}
	return float64(time.Since(start).Nanoseconds()) / float64(iters) / 1000.0
}

// queryAll executes a database/sql prepared statement query and consumes all rows.
func queryAll(stmt *sql.Stmt, args ...any) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		log.Fatalf("queryAll: QueryContext failed: %v", err)
	}
	cols, _ := rows.Columns()
	n := len(cols)
	for rows.Next() {
		if err := rows.Scan(scanPtrs[:n]...); err != nil {
			rows.Close()
			log.Fatalf("queryAll: Scan failed: %v", err)
		}
	}
	rows.Close()
}

// queryOne executes a database/sql prepared statement query and consumes one row.
func queryOne(stmt *sql.Stmt, args ...any) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		log.Fatalf("queryOne: QueryContext failed: %v", err)
	}
	cols, _ := rows.Columns()
	n := len(cols)
	if rows.Next() {
		if err := rows.Scan(scanPtrs[:n]...); err != nil {
			rows.Close()
			log.Fatalf("queryOne: Scan failed: %v", err)
		}
	}
	rows.Close()
}

// ============================================================
// Main
// ============================================================

func main() {
	fmt.Println("Stoolap vs SQLite (go-sqlite3) — Go Benchmark (database/sql)")
	fmt.Printf("Configuration: %d rows, %d iterations per test\n", rowCount, iterations)
	fmt.Println("Both engines use database/sql — fair apples-to-apples comparison")
	fmt.Println("Ratio > 1x = Stoolap faster  |  * = SQLite faster")

	// --- Stoolap setup (via database/sql) ---
	sdb, err := sql.Open("stoolap", "memory://")
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

	// --- SQLite setup (WAL, in-memory) ---
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
	for i := 0; i < rowCount; i++ {
		id := int64(i + 1)
		age := int64((seedRandom(int(id)) % 62) + 18)
		balance := float64(seedRandom(int(id)*7)%100000) + float64(seedRandom(int(id)*13)%100)/100.0
		active := int64(0)
		if seedRandom(int(id)*3)%10 < 7 {
			active = 1
		}
		users[i] = userRow{
			id:        id,
			name:      fmt.Sprintf("User_%d", id),
			email:     fmt.Sprintf("user%d@example.com", id),
			age:       age,
			balance:   balance,
			active:    active,
			createdAt: "2024-01-01 00:00:00",
		}
	}

	// SQLite bulk insert
	lTx, _ := ldb.BeginTx(ctx, nil)
	lInsert, _ := lTx.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	for _, u := range users {
		lInsert.ExecContext(ctx, u.id, u.name, u.email, u.age, u.balance, u.active, u.createdAt)
	}
	lInsert.Close()
	lTx.Commit()

	// Stoolap bulk insert
	sTx, _ := sdb.BeginTx(ctx, nil)
	sInsert, _ := sTx.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	for _, u := range users {
		sInsert.ExecContext(ctx, u.id, u.name, u.email, u.age, u.balance, u.active, u.createdAt)
	}
	sInsert.Close()
	sTx.Commit()

	// ============================================================
	// CORE OPERATIONS
	// ============================================================
	printHeader("CORE OPERATIONS")

	// --- SELECT by ID ---
	// Stoolap uses $1 placeholders, SQLite uses ?
	sSt, _ := sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id = ?")
	lSt, _ := ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id = ?")
	ids := make([]int64, iterations)
	for i := range ids {
		ids[i] = int64((i % rowCount) + 1)
	}

	for i := 0; i < warmup; i++ {
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
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age = ?")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age = ?")
	ages := make([]int64, iterations)
	for i := range ages {
		ages[i] = int64((i % 62) + 18)
	}

	for i := 0; i < warmup; i++ {
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
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age >= ? AND age <= ?")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age >= ? AND age <= ?")

	for i := 0; i < warmup; i++ {
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

	for i := 0; i < warmup; i++ {
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

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterationsHeavy)
	lUs = benchUs(func() { queryAll(lSt) }, iterationsHeavy)
	printRow("SELECT * (full scan)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- UPDATE by ID ---
	sSt, _ = sdb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE id = ?")
	lSt, _ = ldb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE id = ?")

	type updateParam struct {
		balance float64
		id      int64
	}
	updateParams := make([]updateParam, iterations)
	for i := range updateParams {
		updateParams[i] = updateParam{
			balance: float64(seedRandom(i*17)%100000) + 0.5,
			id:      int64((i % rowCount) + 1),
		}
	}

	for i := 0; i < warmup; i++ {
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
	sSt, _ = sdb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = true")
	lSt, _ = ldb.PrepareContext(ctx, "UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = 1")
	balances := make([]float64, iterations)
	for i := range balances {
		balances[i] = float64(seedRandom(i*23)%100000) + 0.5
	}

	for i := 0; i < warmup; i++ {
		sSt.ExecContext(ctx, balances[i], int64(27), int64(28))
		lSt.ExecContext(ctx, balances[i], int64(27), int64(28))
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
	sSt, _ = sdb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	lSt, _ = ldb.PrepareContext(ctx, "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	base := int64(rowCount + 1000)

	t0 := time.Now()
	for i := 0; i < iterations; i++ {
		id := base + int64(i)
		sSt.ExecContext(ctx, id, fmt.Sprintf("New_%d", id), fmt.Sprintf("new%d@example.com", id), int64((seedRandom(i*29)%62)+18), 100.0, int64(1), "2024-01-01 00:00:00")
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for i := 0; i < iterations; i++ {
		id := base + int64(iterations) + int64(i)
		lSt.ExecContext(ctx, id, fmt.Sprintf("New_%d", id), fmt.Sprintf("new%d@example.com", id), int64((seedRandom(i*29)%62)+18), 100.0, int64(1), "2024-01-01 00:00:00")
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("INSERT single", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- DELETE by ID ---
	sSt, _ = sdb.PrepareContext(ctx, "DELETE FROM users WHERE id = ?")
	lSt, _ = ldb.PrepareContext(ctx, "DELETE FROM users WHERE id = ?")

	t0 = time.Now()
	for i := 0; i < iterations; i++ {
		sSt.ExecContext(ctx, base+int64(i))
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for i := 0; i < iterations; i++ {
		lSt.ExecContext(ctx, base+int64(iterations)+int64(i))
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("DELETE by ID", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- DELETE complex ---
	sSt, _ = sdb.PrepareContext(ctx, "DELETE FROM users WHERE age >= ? AND age <= ? AND active = true")
	lSt, _ = ldb.PrepareContext(ctx, "DELETE FROM users WHERE age >= ? AND age <= ? AND active = 1")

	t0 = time.Now()
	for i := 0; i < iterations; i++ {
		sSt.ExecContext(ctx, int64(25), int64(26))
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0

	t0 = time.Now()
	for i := 0; i < iterations; i++ {
		lSt.ExecContext(ctx, int64(25), int64(26))
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iterations) / 1000.0
	printRow("DELETE complex", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Aggregation (GROUP BY) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age")

	for i := 0; i < warmup; i++ {
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

	// Populate orders (3 per user on average)
	statuses := []string{"pending", "completed", "shipped", "cancelled"}

	type orderRow struct {
		id        int64
		userID    int64
		amount    float64
		status    string
		orderDate string
	}

	orderRows := make([]orderRow, rowCount*3)
	for i := 0; i < rowCount*3; i++ {
		id := int64(i + 1)
		userID := int64((seedRandom(int(id)*11) % rowCount) + 1)
		amount := float64(seedRandom(int(id)*19)%990+10) + float64(seedRandom(int(id)*23)%100)/100.0
		status := statuses[seedRandom(int(id)*31)%4]
		orderRows[i] = orderRow{id, userID, amount, status, "2024-01-15"}
	}

	lTx, _ = ldb.BeginTx(ctx, nil)
	lOrdInsert, _ := lTx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
	for _, o := range orderRows {
		lOrdInsert.ExecContext(ctx, o.id, o.userID, o.amount, o.status, o.orderDate)
	}
	lOrdInsert.Close()
	lTx.Commit()

	sTx, _ = sdb.BeginTx(ctx, nil)
	sOrdInsert, _ := sTx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
	for _, o := range orderRows {
		sOrdInsert.ExecContext(ctx, o.id, o.userID, o.amount, o.status, o.orderDate)
	}
	sOrdInsert.Close()
	sTx.Commit()

	printHeader("ADVANCED OPERATIONS")

	// --- INNER JOIN ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100")
	iters := 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("INNER JOIN", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- LEFT JOIN + GROUP BY ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100")
	iters = 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("LEFT JOIN + GROUP BY", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Scalar subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Scalar subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- IN subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100")
	iters = 10

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("IN subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- EXISTS subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100")
	iters = 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("EXISTS subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- CTE + JOIN ---
	sSt, _ = sdb.PrepareContext(ctx, "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100")
	iters = 20

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("CTE + JOIN", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Window ROW_NUMBER ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Window ROW_NUMBER", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Window ROW_NUMBER (PK) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Window ROW_NUMBER (PK)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Window PARTITION BY ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Window PARTITION BY", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- UNION ALL ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("UNION ALL", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- CASE expression ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("CASE expression", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Complex JOIN+GROUP+HAVING ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = 1 AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50")
	iters = 20

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Complex JOIN+GRP+HAVING", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Batch INSERT (100 rows in transaction) ---
	iters = iterations
	baseID := int64(rowCount * 10)

	t0 = time.Now()
	for it := 0; it < iters; it++ {
		stx, _ := sdb.BeginTx(ctx, nil)
		batchStmt, _ := stx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
		for j := 0; j < 100; j++ {
			id := baseID + int64(it*100+j)
			batchStmt.ExecContext(ctx, id, int64(1), 100.0, "pending", "2024-02-01")
		}
		batchStmt.Close()
		stx.Commit()
	}
	sUs = float64(time.Since(t0).Nanoseconds()) / float64(iters) / 1000.0

	t0 = time.Now()
	for it := 0; it < iters; it++ {
		ltx, _ := ldb.BeginTx(ctx, nil)
		batchStmt, _ := ltx.PrepareContext(ctx, "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)")
		for j := 0; j < 100; j++ {
			id := baseID + int64(iters*100+it*100+j)
			batchStmt.ExecContext(ctx, id, int64(1), 100.0, "pending", "2024-02-01")
		}
		batchStmt.Close()
		ltx.Commit()
	}
	lUs = float64(time.Since(t0).Nanoseconds()) / float64(iters) / 1000.0
	printRow("Batch INSERT (100 rows)", sUs, lUs)

	// ============================================================
	// BOTTLENECK HUNTERS
	// ============================================================
	printHeader("BOTTLENECK HUNTERS")

	// --- DISTINCT (no ORDER) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT DISTINCT age FROM users")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT DISTINCT age FROM users")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("DISTINCT (no ORDER)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- DISTINCT + ORDER BY ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT DISTINCT age FROM users ORDER BY age")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT DISTINCT age FROM users ORDER BY age")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("DISTINCT + ORDER BY", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- COUNT DISTINCT ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT COUNT(DISTINCT age) FROM users")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT COUNT(DISTINCT age) FROM users")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("COUNT DISTINCT", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- LIKE prefix ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("LIKE prefix (User_1%)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- LIKE contains ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("LIKE contains (%50%)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- OR conditions ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("OR conditions (3 vals)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- IN list ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("IN list (7 values)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- NOT IN subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100")
	iters = 10

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("NOT IN subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- NOT EXISTS subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100")
	iters = 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("NOT EXISTS subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- OFFSET pagination ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("OFFSET pagination (5000)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Multi-column ORDER BY ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Multi-col ORDER BY (3)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Self JOIN (same age) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100")
	iters = 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Self JOIN (same age)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Multi window funcs (3) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Multi window funcs (3)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Nested subquery (3 levels) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100")
	iters = 20

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Nested subquery (3 lvl)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Multi aggregates (6) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Multi aggregates (6)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- COALESCE + IS NOT NULL ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("COALESCE + IS NOT NULL", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Expr in WHERE (funcs) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Expr in WHERE (funcs)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Math expressions ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Math expressions", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- String concat (||) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("String concat (||)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Large result (no LIMIT) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT id, name, balance FROM users WHERE active = true")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT id, name, balance FROM users WHERE active = 1")
	iters = 20

	for i := 0; i < 5; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Large result (no LIMIT)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Multiple CTEs (2) ---
	sSt, _ = sdb.PrepareContext(ctx, "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50")
	lSt, _ = ldb.PrepareContext(ctx, "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50")
	iters = 100

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Multiple CTEs (2)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Correlated in SELECT ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100")
	iters = 100

	for i := 0; i < 5; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iters)
	lUs = benchUs(func() { queryAll(lSt) }, iters)
	printRow("Correlated in SELECT", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- BETWEEN (non-indexed) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("BETWEEN (non-indexed)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- GROUP BY (2 columns) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("GROUP BY (2 columns)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- CROSS JOIN (limited) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("CROSS JOIN (limited)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Derived table (FROM subquery) ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Derived table (FROM sub)", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Window ROWS frame ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Window ROWS frame", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- HAVING complex ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("HAVING complex", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// --- Compare with subquery ---
	sSt, _ = sdb.PrepareContext(ctx, "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100")
	lSt, _ = ldb.PrepareContext(ctx, "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100")

	for i := 0; i < warmup; i++ {
		queryAll(sSt)
		queryAll(lSt)
	}

	sUs = benchUs(func() { queryAll(sSt) }, iterations)
	lUs = benchUs(func() { queryAll(lSt) }, iterations)
	printRow("Compare with subquery", sUs, lUs)
	sSt.Close()
	lSt.Close()

	// ============================================================
	// Summary
	// ============================================================
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("SCORE: Stoolap %d wins  |  SQLite %d wins\n", stoolapWins, sqliteWins)
	fmt.Println()
	fmt.Println("NOTES:")
	fmt.Println("- Both engines use database/sql — identical Go overhead")
	fmt.Println("- Both drivers use cgo — fair comparison")
	fmt.Println("- Stoolap: MVCC, parallel execution, columnar indexes")
	fmt.Println("- SQLite: WAL mode, in-memory, go-sqlite3 (mattn)")
	fmt.Println("- Ratio > 1x = Stoolap faster  |  * = SQLite faster")
	fmt.Println("================================================================================")
}
