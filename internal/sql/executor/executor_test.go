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
package executor

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stoolap/stoolap/internal/storage"
)

// This is a minimal test to verify that the executor can be created
// and basic methods can be called without panicking.
func TestExecutorCreation(t *testing.T) {
	// Create a mock engine
	mockEngine := &mockEngine{}

	// Create an executor
	executor := NewExecutor(mockEngine)
	if executor == nil {
		t.Fatalf("Failed to create executor")
	}

	// Check that the executor has a function registry
	if executor.functionRegistry == nil {
		t.Errorf("Executor has nil function registry")
	}
}

// mockEngine is a simple mock implementation of storage.Engine
type mockEngine struct{}

func (m *mockEngine) Path() string {
	return "/mock/path"
}

func (m *mockEngine) Open() error {
	return nil
}

func (m *mockEngine) Close() error {
	return nil
}

func (m *mockEngine) BeginTx(ctx context.Context, level sql.IsolationLevel) (storage.Transaction, error) {
	return &mockTransaction{}, nil
}

func (m *mockEngine) BeginTransaction() (storage.Transaction, error) {
	return &mockTransaction{}, nil
}

func (m *mockEngine) TableExists(name string) (bool, error) {
	return false, nil
}

func (m *mockEngine) GetTableSchema(name string) (storage.Schema, error) {
	return storage.Schema{}, nil
}

func (m *mockEngine) ListTables() ([]string, error) {
	return nil, nil
}

func (m *mockEngine) GetConfig() storage.Config {
	return storage.Config{}
}

func (m *mockEngine) UpdateConfig(config storage.Config) error {
	return nil
}

func (m *mockEngine) GetIndex(tableName string, indexName string) (storage.Index, error) {
	return nil, nil
}

func (m *mockEngine) IndexExists(indexName, tableName string) (bool, error) {
	return false, nil
}

func (m *mockEngine) ListTableIndexes(tableName string) (map[string]string, error) {
	return nil, nil
}

func (m *mockEngine) GetAllIndexes(tableName string) ([]storage.Index, error) {
	return nil, nil
}

func (m *mockEngine) GetIsolationLevel() storage.IsolationLevel {
	return storage.ReadCommitted
}

func (m *mockEngine) SetIsolationLevel(level storage.IsolationLevel) error {
	return nil
}

// mockTransaction is a simple mock implementation of storage.Transaction
type mockTransaction struct{}

func (m *mockTransaction) SetIsolationLevel(level storage.IsolationLevel) error {
	return nil
}

func (m *mockTransaction) Begin() error {
	return nil
}

func (m *mockTransaction) Commit() error {
	return nil
}

func (m *mockTransaction) Rollback() error {
	return nil
}

func (m *mockTransaction) GetTable(name string) (storage.Table, error) {
	return nil, nil
}

func (m *mockTransaction) CreateTable(name string, schema storage.Schema) (storage.Table, error) {
	return nil, nil
}

func (m *mockTransaction) DropTable(name string) error {
	return nil
}

func (m *mockTransaction) RenameTable(oldName, newName string) error {
	return nil
}

func (m *mockTransaction) CreateTableIndex(tableName, indexName string, columns []string, unique bool) error {
	return nil
}

func (m *mockTransaction) CreateTableColumnarIndex(tableName string, columnName string, isUnique bool, indexName ...string) error {
	return nil
}

func (m *mockTransaction) DropTableColumnarIndex(tableName string, columnName string) error {
	return nil
}

func (m *mockTransaction) DropTableIndex(tableName, indexName string) error {
	return nil
}

func (m *mockTransaction) DropTableColumn(tableName, columnName string) error {
	return nil
}

func (m *mockTransaction) AddTableColumn(tableName string, column storage.SchemaColumn) error {
	return nil
}

func (m *mockTransaction) RenameTableColumn(tableName, oldName, newName string) error {
	return nil
}

func (m *mockTransaction) Select(tableName string, columns []string, expr storage.Expression, originalColumns ...string) (storage.Result, error) {
	return nil, nil
}

func (m *mockTransaction) SelectWithAliases(tableName string, columns []string, where storage.Expression, aliases map[string]string, originalColumns ...string) (storage.Result, error) {
	return nil, nil
}

func (m *mockTransaction) Context() context.Context {
	return context.Background()
}

func (m *mockTransaction) ID() int64 {
	return 12345
}

func (m *mockTransaction) ListTables() ([]string, error) {
	return nil, nil
}

func (m *mockTransaction) ModifyTableColumn(tableName string, column storage.SchemaColumn) error {
	return nil
}
