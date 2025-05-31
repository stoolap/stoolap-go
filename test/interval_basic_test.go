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
	"context"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
)

func TestIntervalBasicUsage(t *testing.T) {
	ctx := context.Background()
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// The original user query
	rows, err := db.Query(ctx, "SELECT NOW() - INTERVAL '24 hours'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected one row")
	}

	var result time.Time
	if err := rows.Scan(&result); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify that we got a timestamp that's approximately 24 hours ago
	diff := time.Since(result)
	if diff < 23*time.Hour || diff > 25*time.Hour {
		t.Errorf("Expected timestamp ~24 hours ago, got %v ago", diff)
	}

	t.Logf("NOW() - INTERVAL '24 hours' = %v", result)
}
