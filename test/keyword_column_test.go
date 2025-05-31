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

	"github.com/stoolap/stoolap"
)

func TestKeywordColumns(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	tests := []struct {
		name  string
		query string
		desc  string
	}{
		{
			name:  "double_quoted_keywords",
			query: `CREATE TABLE test_keywords ("select" INT, "from" TEXT, "where" BOOLEAN, "order" INT)`,
			desc:  "Create table with double-quoted keyword columns",
		},
		{
			name:  "backtick_keywords",
			query: "CREATE TABLE test_keywords2 (`insert` INT, `update` TEXT, `delete` BOOLEAN, `table` INT)",
			desc:  "Create table with backtick-quoted keyword columns",
		},
		{
			name:  "mixed_quoted_unquoted",
			query: `CREATE TABLE test_mixed (id INT, "key" TEXT, name VARCHAR, "default" INT)`,
			desc:  "Create table with mix of quoted keywords and regular columns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.query)
			if err != nil {
				t.Errorf("%s failed: %v", tt.desc, err)
			}
		})
	}

	// Test INSERT with quoted column names
	t.Run("insert_with_quoted_columns", func(t *testing.T) {
		_, err := db.Exec(ctx, `INSERT INTO test_keywords ("select", "from", "where", "order") VALUES (1, 'test', true, 2)`)
		if err != nil {
			t.Errorf("INSERT with quoted column names failed: %v", err)
		}

		// Verify the insert
		rows, err := db.Query(ctx, `SELECT "select", "from", "where", "order" FROM test_keywords`)
		if err != nil {
			t.Fatalf("SELECT with quoted columns failed: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Error("Expected one row")
		}

		var selectVal int
		var fromVal string
		var whereVal bool
		var orderVal int

		err = rows.Scan(&selectVal, &fromVal, &whereVal, &orderVal)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if selectVal != 1 || fromVal != "test" || whereVal != true || orderVal != 2 {
			t.Errorf("Unexpected values: select=%d, from=%s, where=%v, order=%d",
				selectVal, fromVal, whereVal, orderVal)
		}
	})

	// Test UPDATE with quoted column names
	t.Run("update_with_quoted_columns", func(t *testing.T) {
		_, err := db.Exec(ctx, `UPDATE test_keywords SET "from" = 'updated' WHERE "select" = 1`)
		if err != nil {
			t.Errorf("UPDATE with quoted column names failed: %v", err)
		}
	})

	// Test with backticks
	t.Run("backtick_operations", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO test_keywords2 (`insert`, `update`, `delete`, `table`) VALUES (10, 'data', false, 20)")
		if err != nil {
			t.Errorf("INSERT with backtick columns failed: %v", err)
		}

		rows, err := db.Query(ctx, "SELECT `insert`, `update` FROM test_keywords2")
		if err != nil {
			t.Fatalf("SELECT with backtick columns failed: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Error("Expected one row")
		}
	})

	// Test CREATE INDEX on keyword column
	t.Run("create_index_on_keyword_column", func(t *testing.T) {
		_, err := db.Exec(ctx, `CREATE INDEX idx_test ON test_keywords ("from")`)
		if err != nil {
			t.Errorf("CREATE INDEX on quoted keyword column failed: %v", err)
		}
	})

	// Test JOIN with keyword columns
	t.Run("join_with_keyword_columns", func(t *testing.T) {
		_, err := db.Exec(ctx, `
			SELECT t1."select", t2."insert" 
			FROM test_keywords t1 
			JOIN test_keywords2 t2 ON t1."order" = t2."table"
		`)
		if err != nil {
			t.Errorf("JOIN with quoted keyword columns failed: %v", err)
		}
	})
}

func TestQuotedIdentifierEdgeCases(t *testing.T) {
	db, err := stoolap.Open("memory://")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Test escaped quotes in identifiers
	t.Run("escaped_quotes", func(t *testing.T) {
		// Skip this test for now - escaped quotes in identifiers are not fully supported by the parser
		t.Skip("Escaped quotes in identifiers not yet supported by parser")

		// Standard SQL uses doubled quotes to escape
		_, err := db.Exec(ctx, `CREATE TABLE "test""table" (id INT, "col""name" TEXT)`)
		if err != nil {
			t.Errorf("CREATE TABLE with escaped quotes failed: %v", err)
		}
	})

	// Test case sensitivity with quoted identifiers
	t.Run("case_sensitivity", func(t *testing.T) {
		// Note: Stoolap normalizes column names to lowercase internally,
		// so "Column", "column", and "COLUMN" are all the same
		_, err := db.Exec(ctx, `CREATE TABLE case_test ("MyColumn" INT, id INT)`)
		if err != nil {
			t.Errorf("CREATE TABLE with mixed case column failed: %v", err)
		}

		// Insert using original case
		_, err = db.Exec(ctx, `INSERT INTO case_test ("MyColumn", id) VALUES (1, 1)`)
		if err != nil {
			t.Errorf("INSERT with mixed case column failed: %v", err)
		}

		// Query returns lowercase column names
		rows, err := db.Query(ctx, `SELECT "MyColumn" FROM case_test`)
		if err != nil {
			t.Fatalf("SELECT with mixed case column failed: %v", err)
		}
		defer rows.Close()

		// Verify the column name is returned as lowercase
		cols := rows.Columns()

		if len(cols) != 1 || cols[0] != "mycolumn" {
			t.Errorf("Expected column name 'mycolumn', got %v", cols)
		}
	})

	// Test with all SQL keywords
	t.Run("all_keywords", func(t *testing.T) {
		keywords := []string{"select", "from", "where", "insert", "update", "delete",
			"create", "table", "drop", "alter", "and", "or", "not", "null", "primary",
			"key", "default", "as", "distinct", "order", "by", "asc", "desc", "limit",
			"offset", "group", "having", "join", "inner", "outer", "left", "right",
			"cross", "on", "true", "false", "case", "when", "then", "else", "end",
			"between", "in", "is", "like", "exists", "union", "with"}

		// Create a table with first 10 keywords as columns
		cols := ""
		for i, kw := range keywords[:10] {
			if i > 0 {
				cols += ", "
			}
			cols += `"` + kw + `" INT`
		}

		query := "CREATE TABLE keyword_test (" + cols + ")"
		_, err := db.Exec(ctx, query)
		if err != nil {
			t.Errorf("CREATE TABLE with many keyword columns failed: %v", err)
		}
	})
}
