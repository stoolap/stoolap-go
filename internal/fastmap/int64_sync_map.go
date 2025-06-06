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
package fastmap

import (
	"iter"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// SyncInt64Map is a highly optimized map for int64 keys
// This is the production version incorporating all performance optimizations
type SyncInt64Map[V any] struct {
	buckets []bucket[V]
	mask    uint64
	count   atomic.Int64
}

type bucket[V any] struct {
	head unsafe.Pointer // *fnode[V]
}

type fnode[V any] struct {
	key     int64
	value   atomic.Pointer[V]
	next    unsafe.Pointer // *fnode[V]
	deleted uint32
}

// NewSyncInt64Map creates a new optimized map for int64 keys
func NewSyncInt64Map[V any](sizePower uint) *SyncInt64Map[V] {
	if sizePower < 2 {
		sizePower = 2 // Minimum 4 buckets
	}

	size := uint64(1 << sizePower)
	mask := size - 1

	// Create buckets
	buckets := make([]bucket[V], size)

	return &SyncInt64Map[V]{
		buckets: buckets,
		mask:    mask,
	}
}

// hashFast is an optimized hash function for int64 keys
// that combines speed and excellent distribution
func hashFast(x int64) uint64 {
	key := uint64(x)

	// Fast avalanche function - spreads bits quickly
	// Optimized for modern CPU pipelines
	key = key * 0xd6e8feb86659fd93
	key = bits.RotateLeft64(key, 32) ^ key

	return key
}

// Get retrieves a value by key
func (m *SyncInt64Map[V]) Get(key int64) (V, bool) {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// Search in the linked list
	for node := (*fnode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fnode[V])(atomic.LoadPointer(&node.next)) {

		if node.key == key && atomic.LoadUint32(&node.deleted) == 0 {
			value := node.value.Load()
			if value != nil {
				return *value, true
			}
			break
		}
	}

	var zero V
	return zero, false
}

// Set adds or updates a key-value pair
func (m *SyncInt64Map[V]) Set(key int64, value V) {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

retry:
	// First check if the key already exists
	var predecessor *fnode[V]
	var current *fnode[V]

	// Load the head of the list
	head := (*fnode[V])(atomic.LoadPointer(&bucket.head))

	// Check if the key exists or find insertion point
	for current = head; current != nil; {
		if current.key == key {
			// Key exists, update its value
			if atomic.LoadUint32(&current.deleted) == 0 {
				// Node is not deleted, update value
				current.value.Store(&value)
				return
			} else {
				// Node is deleted, try to resurrect it
				if atomic.CompareAndSwapUint32(&current.deleted, 1, 0) {
					current.value.Store(&value)
					m.count.Add(1)
					return
				}
				// If CAS failed, another thread modified it, retry
				atomic.CompareAndSwapUint32(&current.deleted, 0, 0) // Memory barrier
				current = (*fnode[V])(atomic.LoadPointer(&bucket.head))
				predecessor = nil
				continue
			}
		}

		predecessor = current
		current = (*fnode[V])(atomic.LoadPointer(&current.next))
	}

	// Key doesn't exist, create a new node
	newNode := &fnode[V]{
		key: key,
	}
	newNode.value.Store(&value)

	// Insert at head if first node or predecessor is nil
	if head == nil || predecessor == nil {
		// Load the current head
		currentHead := (*fnode[V])(atomic.LoadPointer(&bucket.head))

		// Set the new node's next pointer to the current head
		atomic.StorePointer(&newNode.next, unsafe.Pointer(currentHead))

		// Try to set the new node as the new head
		if !atomic.CompareAndSwapPointer(&bucket.head, unsafe.Pointer(currentHead), unsafe.Pointer(newNode)) {
			// CAS failed, retry from beginning to check if key was inserted
			// by another thread. We must restart from the beginning to ensure
			// we don't miss any concurrent insertions.
			goto retry
		}
	} else {
		// Insert after predecessor
		// Load predecessor's next
		next := (*fnode[V])(atomic.LoadPointer(&predecessor.next))

		// Set new node's next
		atomic.StorePointer(&newNode.next, unsafe.Pointer(next))

		// Try to set predecessor's next to the new node
		if !atomic.CompareAndSwapPointer(&predecessor.next, unsafe.Pointer(next), unsafe.Pointer(newNode)) {
			// CAS failed, retry from beginning to check if key was inserted
			// by another thread. We must restart from the beginning to ensure
			// we don't miss any concurrent insertions.
			goto retry
		}
	}

	// Increment counter
	m.count.Add(1)
}

// Del removes a key from the map
func (m *SyncInt64Map[V]) Del(key int64) bool {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// Search for the key
	for node := (*fnode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fnode[V])(atomic.LoadPointer(&node.next)) {

		if node.key == key && atomic.LoadUint32(&node.deleted) == 0 {
			// Found non-deleted node with matching key
			if atomic.CompareAndSwapUint32(&node.deleted, 0, 1) {
				// Successfully marked as deleted
				m.count.Add(-1)
				return true
			}
			// If CAS failed, someone else deleted it or resurrected it
			return false
		}
	}

	// Key not found or already deleted
	return false
}

// Len returns the number of elements in the map
func (m *SyncInt64Map[V]) Len() int64 {
	return m.count.Load()
}

func (m *SyncInt64Map[V]) All() iter.Seq2[int64, V] {
	return m.ForEach
}

// ForEach iterates through all key-value pairs
func (m *SyncInt64Map[V]) ForEach(f func(int64, V) bool) {
	// For each bucket
	for i := range m.buckets {
		// Get the bucket
		bucket := &m.buckets[i]

		// Iterate through the linked list
		for node := (*fnode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fnode[V])(atomic.LoadPointer(&node.next)) {

			// Skip deleted nodes
			if atomic.LoadUint32(&node.deleted) != 0 {
				continue
			}

			// Get value
			value := node.value.Load()
			if value == nil {
				continue
			}

			// Call the function
			if !f(node.key, *value) {
				return
			}
		}
	}
}

// Has checks if a key exists in the map without retrieving its value
func (m *SyncInt64Map[V]) Has(key int64) bool {
	hash := hashFast(key)
	bucket := &m.buckets[hash&m.mask]

	// Search in the linked list
	for node := (*fnode[V])(atomic.LoadPointer(&bucket.head)); node != nil; node = (*fnode[V])(atomic.LoadPointer(&node.next)) {
		if node.key == key && atomic.LoadUint32(&node.deleted) == 0 {
			// Found non-deleted node with matching key
			return true
		}
	}

	return false
}
