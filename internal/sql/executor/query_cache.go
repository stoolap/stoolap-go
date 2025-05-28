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
	"sync"
	"time"

	"github.com/stoolap/stoolap/internal/parser"
)

// CachedQueryPlan represents a parsed and prepared statement that can be reused
type CachedQueryPlan struct {
	Statement       parser.Statement // The parsed AST
	QueryText       string           // Original query text
	LastUsed        time.Time        // Last time this plan was used
	UsageCount      int              // Number of times this plan has been used
	HasParams       bool             // Whether this query has parameter placeholders
	ParamCount      int              // Number of parameters required
	NormalizedQuery string           // Normalized query text (with placeholders)
}

// QueryCache provides a cache for previously parsed SQL queries
// It stores the parse tree to avoid the overhead of parsing the same query multiple times
type QueryCache struct {
	mutex       sync.RWMutex
	plans       map[string]*CachedQueryPlan // Plans indexed by normalized query text
	maxSize     int                         // Maximum number of cached plans
	pruneFactor float64                     // Factor to determine how many plans to prune when cache is full
}

// NewQueryCache creates a new query cache with the given maximum size
func NewQueryCache(maxSize int) *QueryCache {
	return &QueryCache{
		plans:       make(map[string]*CachedQueryPlan),
		maxSize:     maxSize,
		pruneFactor: 0.2, // Prune 20% of entries when cache is full
	}
}

// Get retrieves a cached plan for a query if available
func (c *QueryCache) Get(query string) *CachedQueryPlan {
	// Normalize the query to handle irrelevant differences
	normalizedQuery := normalizeQuery(query)

	c.mutex.RLock()
	plan, exists := c.plans[normalizedQuery]
	c.mutex.RUnlock()

	if exists {
		// Update usage statistics
		c.mutex.Lock()
		plan.LastUsed = time.Now()
		plan.UsageCount++
		c.mutex.Unlock()
		return plan
	}

	return nil
}

// Put adds a plan to the cache
func (c *QueryCache) Put(query string, stmt parser.Statement, hasParams bool, paramCount int) *CachedQueryPlan {
	normalizedQuery := normalizeQuery(query)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Check if we need to prune the cache
	if len(c.plans) >= c.maxSize {
		c.pruneCache()
	}

	// Create new plan
	plan := &CachedQueryPlan{
		Statement:       stmt,
		QueryText:       query,
		LastUsed:        time.Now(),
		UsageCount:      1,
		HasParams:       hasParams,
		ParamCount:      paramCount,
		NormalizedQuery: normalizedQuery,
	}

	// Add to cache
	c.plans[normalizedQuery] = plan

	return plan
}

// Clear empties the cache
func (c *QueryCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.plans = make(map[string]*CachedQueryPlan)
}

// Size returns the number of plans in the cache
func (c *QueryCache) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return len(c.plans)
}

// pruneCache removes the least recently used entries when the cache is full
func (c *QueryCache) pruneCache() {
	// Calculate how many entries to remove
	numToRemove := int(float64(c.maxSize) * c.pruneFactor)
	if numToRemove < 1 {
		numToRemove = 1
	}

	// Build a list of all plans sorted by last used time
	type planWithKey struct {
		key  string
		plan *CachedQueryPlan
	}

	all := make([]planWithKey, 0, len(c.plans))
	for k, p := range c.plans {
		all = append(all, planWithKey{key: k, plan: p})
	}

	// Sort by last used (oldest first) and usage count (least used first)
	// This way we prioritize keeping frequently used plans
	for i := 0; i < len(all)-1; i++ {
		for j := i + 1; j < len(all); j++ {
			// If the first plan was used more recently, swap
			if all[i].plan.LastUsed.After(all[j].plan.LastUsed) {
				all[i], all[j] = all[j], all[i]
			} else if all[i].plan.LastUsed.Equal(all[j].plan.LastUsed) &&
				all[i].plan.UsageCount > all[j].plan.UsageCount {
				// If last used time is equal, compare usage count
				all[i], all[j] = all[j], all[i]
			}
		}
	}

	// Remove the oldest/least used entries
	for i := 0; i < numToRemove && i < len(all); i++ {
		delete(c.plans, all[i].key)
	}
}

// normalizeQuery creates a normalized version of the query for caching
// This handles irrelevant whitespace and comments to improve cache hits
func normalizeQuery(query string) string {
	// For now, we're using a simple normalization
	// In a more complete implementation, we would:
	// 1. Remove comments
	// 2. Normalize whitespace
	// 3. Standardize parameter placeholders (?, $1, etc.)
	// 4. Normalize case for keywords

	// The parser already handles basic normalization by trimming whitespace
	// and removing trailing semicolons, so we'll use that for now
	return query
}
