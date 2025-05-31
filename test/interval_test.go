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
	"fmt"
	"testing"
	"time"

	"github.com/stoolap/stoolap"
)

func TestIntervalSupport(t *testing.T) {
	ctx := context.Background()
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		validate func(result interface{}) error
	}{
		{
			name:  "NOW() minus 24 hours",
			query: "SELECT NOW() - INTERVAL '24 hours'",
			validate: func(result interface{}) error {
				timestamp, ok := result.(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time, got %T", result)
				}

				// Check that the result is approximately 24 hours ago
				diff := time.Since(timestamp)
				if diff < 23*time.Hour || diff > 25*time.Hour {
					return fmt.Errorf("expected timestamp ~24 hours ago, got %v ago", diff)
				}
				return nil
			},
		},
		{
			name:  "NOW() plus 1 day",
			query: "SELECT NOW() + INTERVAL '1 day'",
			validate: func(result interface{}) error {
				timestamp, ok := result.(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time, got %T", result)
				}

				// Check that the result is approximately 1 day in the future
				diff := timestamp.Sub(time.Now())
				if diff < 23*time.Hour || diff > 25*time.Hour {
					return fmt.Errorf("expected timestamp ~24 hours in future, got %v", diff)
				}
				return nil
			},
		},
		{
			name:  "NOW() minus 30 minutes",
			query: "SELECT NOW() - INTERVAL '30 minutes'",
			validate: func(result interface{}) error {
				timestamp, ok := result.(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time, got %T", result)
				}

				// Check that the result is approximately 30 minutes ago
				diff := time.Since(timestamp)
				if diff < 29*time.Minute || diff > 31*time.Minute {
					return fmt.Errorf("expected timestamp ~30 minutes ago, got %v ago", diff)
				}
				return nil
			},
		},
		{
			name:  "Interval with seconds",
			query: "SELECT NOW() + INTERVAL '90 seconds'",
			validate: func(result interface{}) error {
				timestamp, ok := result.(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time, got %T", result)
				}

				// Check that the result is approximately 90 seconds in the future
				diff := timestamp.Sub(time.Now())
				if diff < 89*time.Second || diff > 91*time.Second {
					return fmt.Errorf("expected timestamp ~90 seconds in future, got %v", diff)
				}
				return nil
			},
		},
		{
			name:  "Interval with weeks",
			query: "SELECT NOW() - INTERVAL '2 weeks'",
			validate: func(result interface{}) error {
				timestamp, ok := result.(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time, got %T", result)
				}

				// Check that the result is approximately 14 days ago
				diff := time.Since(timestamp)
				if diff < 13*24*time.Hour || diff > 15*24*time.Hour {
					return fmt.Errorf("expected timestamp ~14 days ago, got %v ago", diff)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("Expected one row")
			}

			var result interface{}
			if err := rows.Scan(&result); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}

			if err := tt.validate(result); err != nil {
				t.Errorf("Validation failed: %v", err)
			}
		})
	}
}

func TestIntervalWithTimestampLiteral(t *testing.T) {
	ctx := context.Background()
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test with a specific timestamp
	query := "SELECT TIMESTAMP '2025-01-01 12:00:00' + INTERVAL '25 hours'"

	rows, err := db.Query(ctx, query)
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

	expected := time.Date(2025, 1, 2, 13, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
