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
package test

import (
	"database/sql"
	"testing"

	_ "github.com/stoolap/stoolap/pkg/driver"
)

// TestHashOptimizationSimple tests basic IN subquery functionality
func TestHashOptimizationSimple(t *testing.T) {
	db, err := sql.Open("stoolap", "memory://")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`CREATE TABLE t1 (id INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE t2 (id INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO t1 VALUES (1), (2), (3), (4), (5)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO t2 VALUES (2), (4)`)
	if err != nil {
		t.Fatal(err)
	}

	// Test IN subquery
	rows, err := db.Query(`SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var results []int
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, id)
	}

	// Should return 2 and 4
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d: %v", len(results), results)
	}
	if results[0] != 2 || results[1] != 4 {
		t.Errorf("Expected [2, 4], got %v", results)
	}
}
