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
// Package common provides shared types and utilities
package common

import (
	"sync"

	"github.com/stoolap/stoolap-go/internal/storage"
)

// Global sync.Pool for map[string]storage.ColumnValue to reduce allocations
var (
	SmallColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 8) // For small number of columns
		},
	}

	MediumColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 32) // For medium number of columns
		},
	}

	LargeColumnValueMapPool = &sync.Pool{
		New: func() interface{} {
			return make(map[string]storage.ColumnValue, 64) // For large number of columns
		},
	}
)

// GetColumnValueMapPool returns the appropriate map pool based on column count
func GetColumnValueMapPool(columnCount int) *sync.Pool {
	if columnCount <= 8 {
		return SmallColumnValueMapPool
	} else if columnCount <= 32 {
		return MediumColumnValueMapPool
	}
	return LargeColumnValueMapPool
}

// PutColumnValueMap returns a map to the appropriate pool
func PutColumnValueMap(m map[string]storage.ColumnValue, columnCount int) {
	// Clear the map before returning it to the pool
	clear(m)

	// Return to the appropriate pool based on size
	pool := GetColumnValueMapPool(columnCount)
	pool.Put(m)
}

// GetColumnValueMap gets a map from the appropriate pool based on expected size
func GetColumnValueMap(columnCount int) map[string]storage.ColumnValue {
	pool := GetColumnValueMapPool(columnCount)
	m := pool.Get().(map[string]storage.ColumnValue)

	// Map should already be cleared when returned to pool, but clear it just in case
	clear(m)

	return m
}
