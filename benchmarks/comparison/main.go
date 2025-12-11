// Fair database benchmark for Stoolap-Go
//
// Run with: go run ./benchmarks/comparison
//
// This outputs results in the same format as the Rust benchmark

package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/stoolap/stoolap-go"
)

const (
	RowCount         = 10000
	Iterations       = 100
	WarmupIterations = 10
)

type BenchResult struct {
	Name      string
	AvgUs     float64
	MinUs     float64
	MaxUs     float64
	OpsPerSec float64
}

func NewBenchResult(name string, times []time.Duration) BenchResult {
	timesUs := make([]float64, len(times))
	for i, t := range times {
		timesUs[i] = float64(t.Microseconds())
	}

	var sum float64
	minUs := timesUs[0]
	maxUs := timesUs[0]
	for _, t := range timesUs {
		sum += t
		if t < minUs {
			minUs = t
		}
		if t > maxUs {
			maxUs = t
		}
	}
	avgUs := sum / float64(len(timesUs))
	opsPerSec := 1000000.0 / avgUs

	return BenchResult{
		Name:      name,
		AvgUs:     avgUs,
		MinUs:     minUs,
		MaxUs:     maxUs,
		OpsPerSec: opsPerSec,
	}
}

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

var dbCounter int

func setupStoolap() *stoolap.DB {
	dbCounter++
	db, err := stoolap.Open(fmt.Sprintf("memory://bench%d", dbCounter))
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
	return db
}

func populateStoolap(db *stoolap.DB, count int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	for i := 1; i <= count; i++ {
		id, name, email, age, balance, active, createdAt := generateUser(int64(i), rng)
		query := fmt.Sprintf(
			"INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (%d, '%s', '%s', %d, %f, %t, TIMESTAMP '%s')",
			id, name, email, age, balance, active, createdAt,
		)
		db.Exec(ctx, query)
	}
}

func benchStoolap() []BenchResult {
	var results []BenchResult
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()

	// Setup
	db := setupStoolap()
	defer db.Close()
	populateStoolap(db, RowCount)

	// Warmup
	for i := 0; i < WarmupIterations; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		query := fmt.Sprintf("SELECT * FROM users WHERE id = %d", id)
		rows, _ := db.Query(ctx, query)
		rows.Next()
	}

	// 1. SELECT by ID
	times := make([]time.Duration, 0, Iterations)
	for i := 0; i < Iterations; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		query := fmt.Sprintf("SELECT * FROM users WHERE id = %d", id)
		start := time.Now()
		rows, _ := db.Query(ctx, query)
		rows.Next()
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("SELECT by ID", times))

	// 2. SELECT complex
	times = make([]time.Duration, 0, Iterations)
	for i := 0; i < Iterations; i++ {
		start := time.Now()
		rows, _ := db.Query(ctx, "SELECT name, email, balance FROM users WHERE age >= 30 AND age <= 50 AND active = true ORDER BY balance DESC LIMIT 100")
		for rows.Next() {
		}
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("SELECT complex", times))

	// 3. SELECT all (full scan)
	times = make([]time.Duration, 0, Iterations/10)
	for i := 0; i < Iterations/10; i++ {
		start := time.Now()
		rows, _ := db.Query(ctx, "SELECT * FROM users")
		for rows.Next() {
		}
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("SELECT * (full scan)", times))

	// 4. UPDATE by ID
	times = make([]time.Duration, 0, Iterations)
	for i := 0; i < Iterations; i++ {
		id := rng.Int63n(int64(RowCount)) + 1
		newBalance := rng.Float64() * 100000.0
		query := fmt.Sprintf("UPDATE users SET balance = %f WHERE id = %d", newBalance, id)
		start := time.Now()
		db.Exec(ctx, query)
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("UPDATE by ID", times))

	// 5. UPDATE complex
	times = make([]time.Duration, 0, Iterations/10)
	for i := 0; i < Iterations/10; i++ {
		newBalance := rng.Float64() * 100000.0
		query := fmt.Sprintf("UPDATE users SET balance = %f WHERE age >= 25 AND age <= 45 AND active = true", newBalance)
		start := time.Now()
		db.Exec(ctx, query)
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("UPDATE complex", times))

	// 6. INSERT single
	times = make([]time.Duration, 0, Iterations)
	nextID := int64(RowCount + 1)
	for i := 0; i < Iterations; i++ {
		_, name, email, age, balance, active, createdAt := generateUser(nextID, rng)
		query := fmt.Sprintf(
			"INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (%d, '%s', '%s', %d, %f, %t, TIMESTAMP '%s')",
			nextID, name, email, age, balance, active, createdAt,
		)
		start := time.Now()
		db.Exec(ctx, query)
		times = append(times, time.Since(start))
		nextID++
	}
	results = append(results, NewBenchResult("INSERT single", times))

	// 7. DELETE by ID
	times = make([]time.Duration, 0, Iterations)
	for i := 0; i < Iterations; i++ {
		id := int64(RowCount+1) + int64(i)
		query := fmt.Sprintf("DELETE FROM users WHERE id = %d", id)
		start := time.Now()
		db.Exec(ctx, query)
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("DELETE by ID", times))

	// 8. Aggregation
	times = make([]time.Duration, 0, Iterations)
	for i := 0; i < Iterations; i++ {
		start := time.Now()
		rows, _ := db.Query(ctx, "SELECT active, COUNT(*), AVG(balance), SUM(balance), MIN(age), MAX(age) FROM users GROUP BY active")
		for rows.Next() {
		}
		times = append(times, time.Since(start))
	}
	results = append(results, NewBenchResult("Aggregation (GROUP BY)", times))

	return results
}

func printResults(results []BenchResult) {
	fmt.Println()
	fmt.Println(repeat("=", 60))
	fmt.Println("STOOLAP-GO BENCHMARK (10,000 rows, 100 iterations, in-memory)")
	fmt.Println(repeat("=", 60))
	fmt.Println()
	fmt.Printf("%-25s | %12s | %12s\n", "Operation", "Avg (μs)", "ops/sec")
	fmt.Println(repeat("-", 60))

	for _, r := range results {
		fmt.Printf("%-25s | %12.1f | %12.0f\n", r.Name, r.AvgUs, r.OpsPerSec)
	}
	fmt.Println(repeat("=", 60))
}

func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}

func main() {
	fmt.Println("Starting Stoolap-Go benchmark...")
	fmt.Printf("Configuration: %d rows, %d iterations per test\n", RowCount, Iterations)
	fmt.Println()

	fmt.Println("Benchmarking Stoolap-Go...")
	results := benchStoolap()

	printResults(results)
}
