// Realistic benchmark comparing SQLite, DuckDB, and Stoolap-Go
//
// Run with: go test -bench=. -benchtime=10s ./benchmarks/

package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stoolap/stoolap-go"
	_ "github.com/stoolap/stoolap-go/pkg/driver"

	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
)

const (
	RowCount  = 10000
	BatchSize = 100
)

// generateUser generates random user data
func generateUser(id int64, rng *rand.Rand) (int64, string, string, int64, float64, bool, string) {
	name := fmt.Sprintf("User_%d", id)
	email := fmt.Sprintf("user%d@example.com", id)
	age := int64(rng.Intn(62) + 18)
	balance := rng.Float64() * 100000.0
	active := rng.Float64() < 0.7
	createdAt := fmt.Sprintf("2024-%02d-%02d %02d:%02d:%02d",
		rng.Intn(12)+1,
		rng.Intn(28)+1,
		rng.Intn(24),
		rng.Intn(60),
		rng.Intn(60))
	return id, name, email, age, balance, active, createdAt
}

// ============================================================================
// SQLite Setup
// ============================================================================

func setupSQLite() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL,
			balance REAL NOT NULL,
			active INTEGER NOT NULL,
			created_at TEXT NOT NULL
		);
		CREATE INDEX idx_users_age ON users(age);
		CREATE INDEX idx_users_active ON users(active);
		CREATE INDEX idx_users_email ON users(email);
	`)
	if err != nil {
		panic(err)
	}
	return db
}

func populateSQLite(db *sql.DB, count int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stmt, _ := db.Prepare("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	defer stmt.Close()

	for i := 1; i <= count; i++ {
		id, name, email, age, balance, active, createdAt := generateUser(int64(i), rng)
		activeInt := 0
		if active {
			activeInt = 1
		}
		stmt.Exec(id, name, email, age, balance, activeInt, createdAt)
	}
}

// ============================================================================
// DuckDB Setup
// ============================================================================

func setupDuckDB() *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			email VARCHAR NOT NULL,
			age INTEGER NOT NULL,
			balance DOUBLE NOT NULL,
			active BOOLEAN NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
		CREATE INDEX idx_users_age ON users(age);
		CREATE INDEX idx_users_active ON users(active);
		CREATE INDEX idx_users_email ON users(email);
	`)
	if err != nil {
		panic(err)
	}
	return db
}

func populateDuckDB(db *sql.DB, count int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stmt, _ := db.Prepare("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
	defer stmt.Close()

	for i := 1; i <= count; i++ {
		id, name, email, age, balance, active, createdAt := generateUser(int64(i), rng)
		stmt.Exec(id, name, email, age, balance, active, createdAt)
	}
}

// ============================================================================
// Stoolap-Go Setup
// ============================================================================

var stoolapCounter int

func setupStoolap() *stoolap.DB {
	stoolapCounter++
	db, err := stoolap.Open(fmt.Sprintf("memory://%d", stoolapCounter))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	_, err = db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL,
			balance FLOAT NOT NULL,
			active BOOLEAN NOT NULL,
			created_at TIMESTAMP NOT NULL
		)
	`)
	if err != nil {
		panic(err)
	}
	db.Exec(ctx, "CREATE INDEX idx_users_age ON users(age)")
	db.Exec(ctx, "CREATE INDEX idx_users_active ON users(active)")
	db.Exec(ctx, "CREATE INDEX idx_users_email ON users(email)")
	return db
}

func populateStoolap(db *stoolap.DB, count int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	for i := 1; i <= count; i++ {
		id, name, email, age, balance, active, createdAt := generateUser(int64(i), rng)
		query := fmt.Sprintf(
			"INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (%d, '%s', '%s', %d, %f, %t, '%s')",
			id, name, email, age, balance, active, createdAt,
		)
		db.Exec(ctx, query)
	}
}

// ============================================================================
// INSERT Benchmarks
// ============================================================================

func BenchmarkInsert_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := int64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < BatchSize; j++ {
			_, name, email, age, balance, active, createdAt := generateUser(id, rng)
			activeInt := 0
			if active {
				activeInt = 1
			}
			db.Exec("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
				id, name, email, age, balance, activeInt, createdAt)
			id++
		}
	}
}

func BenchmarkInsert_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := int64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < BatchSize; j++ {
			_, name, email, age, balance, active, createdAt := generateUser(id, rng)
			db.Exec("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
				id, name, email, age, balance, active, createdAt)
			id++
		}
	}
}

func BenchmarkInsert_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := int64(1)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < BatchSize; j++ {
			_, name, email, age, balance, active, createdAt := generateUser(id, rng)
			query := fmt.Sprintf(
				"INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (%d, '%s', '%s', %d, %f, %t, '%s')",
				id, name, email, age, balance, active, createdAt,
			)
			db.Exec(ctx, query)
			id++
		}
	}
}

// ============================================================================
// SELECT ALL Benchmarks
// ============================================================================

func BenchmarkSelectAll_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT * FROM users")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkSelectAll_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT * FROM users")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkSelectAll_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		count := 0
		for rows.Next() {
			count++
		}
	}
}

// ============================================================================
// SELECT BY ID Benchmarks
// ============================================================================

func BenchmarkSelectByID_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		rows, _ := db.Query("SELECT * FROM users WHERE id = ?", id)
		rows.Next()
		rows.Close()
	}
}

func BenchmarkSelectByID_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		rows, _ := db.Query("SELECT * FROM users WHERE id = ?", id)
		rows.Next()
		rows.Close()
	}
}

func BenchmarkSelectByID_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		query := fmt.Sprintf("SELECT * FROM users WHERE id = %d", id)
		rows, _ := db.Query(ctx, query)
		rows.Next()
	}
}

// ============================================================================
// SELECT COMPLEX Benchmarks
// ============================================================================

func BenchmarkSelectComplex_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT name, email, balance FROM users WHERE age >= 30 AND age <= 50 AND active = 1 ORDER BY balance DESC LIMIT 100")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkSelectComplex_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT name, email, balance FROM users WHERE age >= 30 AND age <= 50 AND active = true ORDER BY balance DESC LIMIT 100")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkSelectComplex_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, "SELECT name, email, balance FROM users WHERE age >= 30 AND age <= 50 AND active = true ORDER BY balance DESC LIMIT 100")
		count := 0
		for rows.Next() {
			count++
		}
	}
}

// ============================================================================
// UPDATE Benchmarks
// ============================================================================

func BenchmarkUpdate_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ?", newBalance)
	}
}

func BenchmarkUpdate_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ?", newBalance)
	}
}

func BenchmarkUpdate_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		query := fmt.Sprintf("UPDATE users SET balance = %f", newBalance)
		db.Exec(ctx, query)
	}
}

// ============================================================================
// UPDATE BY ID Benchmarks
// ============================================================================

func BenchmarkUpdateByID_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ? WHERE id = ?", newBalance, id)
	}
}

func BenchmarkUpdateByID_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ? WHERE id = ?", newBalance, id)
	}
}

func BenchmarkUpdateByID_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		newBalance := rng.Float64() * 100000.0
		query := fmt.Sprintf("UPDATE users SET balance = %f WHERE id = %d", newBalance, id)
		db.Exec(ctx, query)
	}
}

// ============================================================================
// UPDATE COMPLEX Benchmarks
// ============================================================================

func BenchmarkUpdateComplex_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ? WHERE age >= 25 AND age <= 45 AND active = 1", newBalance)
	}
}

func BenchmarkUpdateComplex_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		db.Exec("UPDATE users SET balance = ? WHERE age >= 25 AND age <= 45 AND active = true", newBalance)
	}
}

func BenchmarkUpdateComplex_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBalance := rng.Float64() * 100000.0
		query := fmt.Sprintf("UPDATE users SET balance = %f WHERE age >= 25 AND age <= 45 AND active = true", newBalance)
		db.Exec(ctx, query)
	}
}

// ============================================================================
// DELETE Benchmarks
// ============================================================================

func BenchmarkDelete_SQLite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupSQLite()
		populateSQLite(db, RowCount)
		b.StartTimer()

		db.Exec("DELETE FROM users")

		b.StopTimer()
		db.Close()
	}
}

func BenchmarkDelete_DuckDB(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupDuckDB()
		populateDuckDB(db, RowCount)
		b.StartTimer()

		db.Exec("DELETE FROM users")

		b.StopTimer()
		db.Close()
	}
}

func BenchmarkDelete_StoolapGo(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupStoolap()
		populateStoolap(db, RowCount)
		b.StartTimer()

		db.Exec(ctx, "DELETE FROM users")

		b.StopTimer()
		db.Close()
	}
}

// ============================================================================
// DELETE BY ID Benchmarks
// ============================================================================

func BenchmarkDeleteByID_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount*10)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount*10)) + 1
		db.Exec("DELETE FROM users WHERE id = ?", id)
	}
}

func BenchmarkDeleteByID_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount*10)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount*10)) + 1
		db.Exec("DELETE FROM users WHERE id = ?", id)
	}
}

func BenchmarkDeleteByID_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount*10)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := rng.Int63n(int64(RowCount*10)) + 1
		query := fmt.Sprintf("DELETE FROM users WHERE id = %d", id)
		db.Exec(ctx, query)
	}
}

// ============================================================================
// DELETE COMPLEX Benchmarks
// ============================================================================

func BenchmarkDeleteComplex_SQLite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupSQLite()
		populateSQLite(db, RowCount)
		b.StartTimer()

		db.Exec("DELETE FROM users WHERE age < 25 OR (active = 0 AND balance < 1000)")

		b.StopTimer()
		db.Close()
	}
}

func BenchmarkDeleteComplex_DuckDB(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupDuckDB()
		populateDuckDB(db, RowCount)
		b.StartTimer()

		db.Exec("DELETE FROM users WHERE age < 25 OR (active = false AND balance < 1000)")

		b.StopTimer()
		db.Close()
	}
}

func BenchmarkDeleteComplex_StoolapGo(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := setupStoolap()
		populateStoolap(db, RowCount)
		b.StartTimer()

		db.Exec(ctx, "DELETE FROM users WHERE age < 25 OR (active = false AND balance < 1000)")

		b.StopTimer()
		db.Close()
	}
}

// ============================================================================
// AGGREGATION Benchmarks
// ============================================================================

func BenchmarkAggregation_SQLite(b *testing.B) {
	db := setupSQLite()
	defer db.Close()
	populateSQLite(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT active, COUNT(*), AVG(balance), SUM(balance), MIN(age), MAX(age) FROM users GROUP BY active")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkAggregation_DuckDB(b *testing.B) {
	db := setupDuckDB()
	defer db.Close()
	populateDuckDB(db, RowCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT active, COUNT(*), AVG(balance), SUM(balance), MIN(age), MAX(age) FROM users GROUP BY active")
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
	}
}

func BenchmarkAggregation_StoolapGo(b *testing.B) {
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query(ctx, "SELECT active, COUNT(*), AVG(balance), SUM(balance), MIN(age), MAX(age) FROM users GROUP BY active")
		count := 0
		for rows.Next() {
			count++
		}
	}
}
