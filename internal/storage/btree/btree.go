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
// Package btree implements B-tree indexes for high-cardinality columns.
//
// B-trees are tree data structures that provide an ordered key-value store
// with logarithmic-time operations. They are ideal for columns with high
// cardinality (many distinct values).
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/stoolap/stoolap-go/internal/storage"
	"github.com/stoolap/stoolap-go/internal/storage/binser"
)

const (
	// DefaultOrder is the default order (max children) for a B-tree node
	// A higher order means fewer tree levels but more scanning within nodes
	DefaultOrder = 128

	// DefaultLeafCapacity is the default capacity for leaf nodes
	DefaultLeafCapacity = 128
)

// ErrKeyNotFound is returned when a key is not found in the B-tree
var ErrKeyNotFound = errors.New("key not found in B-tree")

// BTree represents a B-tree index data structure for high-cardinality columns
type BTree struct {
	// root is the root node of the B-tree
	root *Node
	// order is the maximum number of children per internal node
	order int
	// leafCapacity is the maximum number of entries per leaf node
	leafCapacity int
	// size is the total number of keys in the B-tree
	size int64
	// mutex protects concurrent access to the B-tree
	mutex sync.RWMutex
	// keyType indicates the type of keys stored in this B-tree
	keyType storage.DataType
}

// Node represents a node in the B-tree
type Node struct {
	// isLeaf indicates if this is a leaf node
	isLeaf bool
	// keys contains the keys in this node
	keys []string
	// values contains pointers to row positions (only in leaf nodes)
	values [][]int64
	// children contains pointers to child nodes (only in internal nodes)
	children []*Node
}

// NewBTree creates a new B-tree with the specified order
func NewBTree(keyType storage.DataType, order int, leafCapacity int) *BTree {
	if order <= 0 {
		order = DefaultOrder
	}
	if leafCapacity <= 0 {
		leafCapacity = DefaultLeafCapacity
	}

	// Create a new leaf node as the root
	root := &Node{
		isLeaf:   true,
		keys:     make([]string, 0, leafCapacity),
		values:   make([][]int64, 0, leafCapacity),
		children: nil,
	}

	return &BTree{
		root:         root,
		order:        order,
		leafCapacity: leafCapacity,
		size:         0,
		keyType:      keyType,
	}
}

// Insert inserts a key-value pair into the B-tree
func (bt *BTree) Insert(key string, value int64) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// If this is the first entry for this key, initialize a new slice
	bt.insertNonLocked(key, value)
}

// insertNonLocked inserts a key-value pair without locking
func (bt *BTree) insertNonLocked(key string, value int64) {
	// Start at the root
	node := bt.root

	// Traverse to the appropriate leaf node
	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool {
			return node.keys[i] >= key
		})

		// If we found an exact match, go to the child to the right
		if i < len(node.keys) && node.keys[i] == key {
			i++
		}

		node = node.children[i]
	}

	// Find the insertion point in the leaf node
	i := sort.Search(len(node.keys), func(i int) bool {
		return node.keys[i] >= key
	})

	// If the key already exists, append the value to the existing slice
	if i < len(node.keys) && node.keys[i] == key {
		// Check if this value already exists
		for _, existingValue := range node.values[i] {
			if existingValue == value {
				return // Value already exists, no need to insert
			}
		}

		// Append the new value to the existing slice
		node.values[i] = append(node.values[i], value)
		return
	}

	// Otherwise, insert a new key-value pair
	// Prepare space for the new entry
	node.keys = append(node.keys, "")
	copy(node.keys[i+1:], node.keys[i:])
	node.keys[i] = key

	node.values = append(node.values, nil)
	copy(node.values[i+1:], node.values[i:])
	node.values[i] = []int64{value}

	bt.size++

	// Check if the node needs to be split
	bt.splitLeafIfNeeded(node)
}

// splitLeafIfNeeded splits a leaf node if it exceeds the leaf capacity
func (bt *BTree) splitLeafIfNeeded(node *Node) {
	// If the node doesn't exceed capacity, no split is needed
	if len(node.keys) <= bt.leafCapacity {
		return
	}

	// If we're splitting the root, we need to create a new root
	if node == bt.root {
		newRoot := &Node{
			isLeaf:   false,
			keys:     make([]string, 0),
			values:   nil,
			children: make([]*Node, 0),
		}
		bt.root = newRoot
		newRoot.children = append(newRoot.children, node)
	}

	// Find the parent of this node
	parent := bt.findParent(bt.root, node)
	if parent == nil {
		// This should never happen if our B-tree structure is correct
		panic("splitLeafIfNeeded: parent not found")
	}

	// Split point
	mid := len(node.keys) / 2

	// Create a new sibling node
	sibling := &Node{
		isLeaf:   true,
		keys:     make([]string, len(node.keys)-mid),
		values:   make([][]int64, len(node.keys)-mid),
		children: nil,
	}

	// Copy the second half of keys and values to the sibling
	copy(sibling.keys, node.keys[mid:])
	copy(sibling.values, node.values[mid:])

	// Truncate the original node
	node.keys = node.keys[:mid]
	node.values = node.values[:mid]

	// Insert the sibling into the parent
	parentIdx := sort.Search(len(parent.keys), func(i int) bool {
		return parent.keys[i] >= sibling.keys[0]
	})

	// Insert the first key of the sibling into the parent
	parent.keys = append(parent.keys, "")
	copy(parent.keys[parentIdx+1:], parent.keys[parentIdx:])
	parent.keys[parentIdx] = sibling.keys[0]

	// Insert the sibling as a child of the parent
	parent.children = append(parent.children, nil)
	copy(parent.children[parentIdx+2:], parent.children[parentIdx+1:])
	parent.children[parentIdx+1] = sibling

	// Check if the parent needs to be split (internal node)
	bt.splitInternalIfNeeded(parent)
}

// splitInternalIfNeeded splits an internal node if it exceeds the order
func (bt *BTree) splitInternalIfNeeded(node *Node) {
	// If the node doesn't exceed capacity, no split is needed
	if len(node.children) <= bt.order {
		return
	}

	// If we're splitting the root, we need to create a new root
	if node == bt.root {
		newRoot := &Node{
			isLeaf:   false,
			keys:     make([]string, 0),
			values:   nil,
			children: make([]*Node, 0),
		}
		bt.root = newRoot
		newRoot.children = append(newRoot.children, node)
	}

	// Find the parent of this node
	parent := bt.findParent(bt.root, node)
	if parent == nil {
		// This should never happen if our B-tree structure is correct
		panic("splitInternalIfNeeded: parent not found")
	}

	// Split point
	mid := len(node.keys) / 2

	// Create a new sibling node
	sibling := &Node{
		isLeaf:   false,
		keys:     make([]string, len(node.keys)-mid-1),
		values:   nil,
		children: make([]*Node, len(node.children)-mid-1),
	}

	// Copy the keys and children to the sibling
	// Skip the middle key - it goes up to the parent
	copy(sibling.keys, node.keys[mid+1:])
	copy(sibling.children, node.children[mid+1:])

	// Save the middle key - it will be inserted into the parent
	midKey := node.keys[mid]

	// Truncate the original node
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]

	// Insert the sibling into the parent
	parentIdx := sort.Search(len(parent.keys), func(i int) bool {
		return parent.keys[i] >= midKey
	})

	// Insert the middle key into the parent
	parent.keys = append(parent.keys, "")
	copy(parent.keys[parentIdx+1:], parent.keys[parentIdx:])
	parent.keys[parentIdx] = midKey

	// Insert the sibling as a child of the parent
	parent.children = append(parent.children, nil)
	copy(parent.children[parentIdx+2:], parent.children[parentIdx+1:])
	parent.children[parentIdx+1] = sibling

	// Check if the parent needs to be split (internal node)
	bt.splitInternalIfNeeded(parent)
}

// findParent finds the parent of a given node
func (bt *BTree) findParent(current, target *Node) *Node {
	if current.isLeaf {
		return nil
	}

	// Check if any of the children is the target
	for _, child := range current.children {
		if child == target {
			return current
		}
	}

	// Recursively search in children
	for _, child := range current.children {
		if parent := bt.findParent(child, target); parent != nil {
			return parent
		}
	}

	return nil
}

// searchNode finds the node containing the given key
func (bt *BTree) searchNode(key string) (*Node, error) {
	node := bt.root

	// Traverse to the appropriate leaf node
	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool {
			return node.keys[i] >= key
		})

		// If we found an exact match, go to the child to the right
		if i < len(node.keys) && node.keys[i] == key {
			// For searching the node, we stay at this node if it contains the key
			if node.values[i] != nil {
				return node, nil
			}
			i++
		}

		node = node.children[i]
	}

	// Find the key in the leaf node
	i := sort.Search(len(node.keys), func(i int) bool {
		return node.keys[i] >= key
	})

	// Check if the key was found
	if i < len(node.keys) && node.keys[i] == key {
		return node, nil
	}

	return nil, ErrKeyNotFound
}

// Search searches for a key in the B-tree and returns all matching values
func (bt *BTree) Search(key string) ([]int64, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Find the node containing the key
	node, err := bt.searchNode(key)
	if err != nil {
		return nil, err
	}

	// Find the key within the node
	for i, k := range node.keys {
		if k == key {
			// Return a copy of the values to avoid concurrent modification issues
			result := make([]int64, len(node.values[i]))
			copy(result, node.values[i])
			return result, nil
		}
	}

	return nil, ErrKeyNotFound
}

// RangeSearch performs a range search for keys between start (inclusive) and end (exclusive)
func (bt *BTree) RangeSearch(start, end string) ([]int64, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Special case for empty range
	if start >= end {
		return []int64{}, nil
	}

	result := make([]int64, 0)
	stack := []*Node{bt.root}

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if node.isLeaf {
			// Find all keys in the range
			for i, key := range node.keys {
				if key >= start && key < end {
					result = append(result, node.values[i]...)
				}
			}
		} else {
			// Add relevant children to the stack in reverse order
			// so that we process them in the correct order
			for i := len(node.children) - 1; i >= 0; i-- {
				// Check if this subtree could contain keys in our range
				if i > 0 && node.keys[i-1] >= end {
					continue // This subtree contains keys >= end, skip it
				}
				if i < len(node.keys) && node.keys[i] < start {
					continue // This subtree contains keys < start, skip it
				}
				stack = append(stack, node.children[i])
			}
		}
	}

	return result, nil
}

// Size returns the number of key-value pairs in the B-tree
func (bt *BTree) Size() int64 {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.size
}

// Clear removes all keys from the B-tree
func (bt *BTree) Clear() {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// Create a new leaf node as the root
	bt.root = &Node{
		isLeaf:   true,
		keys:     make([]string, 0, bt.leafCapacity),
		values:   make([][]int64, 0, bt.leafCapacity),
		children: nil,
	}
	bt.size = 0
}

// PrefixSearch finds all keys with the given prefix
func (bt *BTree) PrefixSearch(prefix string) ([]int64, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	result := make([]int64, 0)
	node := bt.root

	// First, find the first node that could contain the prefix
	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool {
			return node.keys[i] >= prefix
		})

		// If we found an exact match, go to the child to the right
		if i < len(node.keys) && node.keys[i] == prefix {
			i++
		}

		node = node.children[i]
	}

	// Now, collect all values with the prefix using a stack for traversal
	stack := []*Node{node}
	foundStartPoint := false

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if node.isLeaf {
			// Find the first key with the prefix
			startIdx := 0
			if !foundStartPoint {
				startIdx = sort.Search(len(node.keys), func(i int) bool {
					return node.keys[i] >= prefix
				})
				foundStartPoint = true
			}

			// Collect all keys with the prefix
			for i := startIdx; i < len(node.keys); i++ {
				if len(node.keys[i]) >= len(prefix) && node.keys[i][:len(prefix)] == prefix {
					result = append(result, node.values[i]...)
				} else if foundStartPoint && node.keys[i] > prefix && (len(node.keys[i]) < len(prefix) || node.keys[i][:len(prefix)] != prefix) {
					// We've gone past keys with the prefix
					break
				}
			}
		} else {
			// Need to implement if we want to handle prefix searches across node boundaries
			panic("PrefixSearch not fully implemented for internal nodes yet")
		}
	}

	return result, nil
}

// SerializeMetadata serializes the B-tree metadata to binary format
func (bt *BTree) SerializeMetadata() ([]byte, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Create a new writer
	writer := binser.NewWriter()
	defer writer.Release()

	// Write metadata fields
	writer.WriteInt(int(bt.keyType))
	writer.WriteInt(bt.order)
	writer.WriteInt(bt.leafCapacity)
	writer.WriteInt64(bt.size)
	writer.WriteInt(bt.height())

	// Write timestamps
	writer.WriteString(time.Now().Format(time.RFC3339))
	writer.WriteString(time.Now().Format(time.RFC3339))

	return append([]byte(nil), writer.Bytes()...), nil
}

// height returns the height of the B-tree
func (bt *BTree) height() int {
	height := 1
	node := bt.root

	for !node.isLeaf {
		height++
		node = node.children[0]
	}

	return height
}

// Serialize serializes the B-tree to a byte slice
func (bt *BTree) Serialize() ([]byte, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	buf := bytes.NewBuffer(nil)

	// Write header information
	binary.Write(buf, binary.LittleEndian, int32(bt.keyType))
	binary.Write(buf, binary.LittleEndian, int32(bt.order))
	binary.Write(buf, binary.LittleEndian, int32(bt.leafCapacity))
	binary.Write(buf, binary.LittleEndian, int32(bt.size))

	// Serialize in a breadth-first manner
	queue := []*Node{bt.root}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		// Write node information
		binary.Write(buf, binary.LittleEndian, node.isLeaf)
		binary.Write(buf, binary.LittleEndian, int32(len(node.keys)))

		// Write keys
		for _, key := range node.keys {
			// Write key length and key bytes
			binary.Write(buf, binary.LittleEndian, int32(len(key)))
			buf.WriteString(key)
		}

		// For leaf nodes, write values
		if node.isLeaf {
			for _, vals := range node.values {
				// Write values count
				binary.Write(buf, binary.LittleEndian, int32(len(vals)))
				// Write each value
				for _, val := range vals {
					binary.Write(buf, binary.LittleEndian, int32(val))
				}
			}
		} else {
			// Add children to the queue
			queue = append(queue, node.children...)
		}
	}

	return buf.Bytes(), nil
}

// Deserialize deserializes a B-tree from a byte slice
func Deserialize(data []byte) (*BTree, error) {
	if len(data) < 16 { // 4 int32 values
		return nil, errors.New("invalid serialized B-tree data (too short)")
	}

	buf := bytes.NewBuffer(data)

	// Read header information
	var keyType, order, leafCapacity int32
	var size int64

	binary.Read(buf, binary.LittleEndian, &keyType)
	binary.Read(buf, binary.LittleEndian, &order)
	binary.Read(buf, binary.LittleEndian, &leafCapacity)
	binary.Read(buf, binary.LittleEndian, &size)

	// Create a new B-tree
	bt := NewBTree(storage.DataType(keyType), int(order), int(leafCapacity))
	bt.size = size

	// Deserialize the nodes
	if err := deserializeNodes(bt, buf); err != nil {
		return nil, err
	}

	return bt, nil
}

// deserializeNodes deserializes nodes from the buffer
func deserializeNodes(bt *BTree, buf *bytes.Buffer) error {
	// Queue for tracking parent nodes and their child indexes
	type queueItem struct {
		node     *Node
		childIdx int
		isRoot   bool
	}

	// Start with the root node
	var isLeaf bool
	if err := binary.Read(buf, binary.LittleEndian, &isLeaf); err != nil {
		return fmt.Errorf("failed to read root node leaf flag: %w", err)
	}

	// Read number of keys in the root
	var keyCount int32
	if err := binary.Read(buf, binary.LittleEndian, &keyCount); err != nil {
		return fmt.Errorf("failed to read root node key count: %w", err)
	}

	// Create the root node
	bt.root = &Node{
		isLeaf:   isLeaf,
		keys:     make([]string, int(keyCount)),
		values:   nil,
		children: nil,
	}

	// Initialize arrays based on node type
	if isLeaf {
		bt.root.values = make([][]int64, int(keyCount))
	} else {
		bt.root.children = make([]*Node, int(keyCount)+1)
	}

	// Read the keys of the root node
	for i := 0; i < int(keyCount); i++ {
		var keyLen int32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		bt.root.keys[i] = string(keyBytes)
	}

	// For leaf nodes, read values
	if isLeaf {
		for i := 0; i < int(keyCount); i++ {
			var valCount int32
			if err := binary.Read(buf, binary.LittleEndian, &valCount); err != nil {
				return fmt.Errorf("failed to read value count: %w", err)
			}

			values := make([]int64, int(valCount))
			for j := 0; j < int(valCount); j++ {
				var val int64
				if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
					return fmt.Errorf("failed to read value: %w", err)
				}
				values[j] = val
			}

			bt.root.values[i] = values
		}

		// If the root is a leaf and we've read all its keys and values, we're done
		if buf.Len() == 0 {
			return nil
		}
	}

	// Process remaining nodes in breadth-first order
	queue := make([]queueItem, 0)

	// If root has children, add all child slots to the queue
	if !isLeaf {
		for i := 0; i <= int(keyCount); i++ {
			queue = append(queue, queueItem{
				node:     bt.root,
				childIdx: i,
				isRoot:   true,
			})
		}
	}

	// Process all nodes in the queue
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		// Read if this child is a leaf
		var childIsLeaf bool
		if err := binary.Read(buf, binary.LittleEndian, &childIsLeaf); err != nil {
			return fmt.Errorf("failed to read child leaf flag: %w", err)
		}

		// Read number of keys
		var childKeyCount int32
		if err := binary.Read(buf, binary.LittleEndian, &childKeyCount); err != nil {
			return fmt.Errorf("failed to read child key count: %w", err)
		}

		// Create the child node
		childNode := &Node{
			isLeaf:   childIsLeaf,
			keys:     make([]string, int(childKeyCount)),
			values:   nil,
			children: nil,
		}

		// Initialize arrays based on node type
		if childIsLeaf {
			childNode.values = make([][]int64, int(childKeyCount))
		} else {
			childNode.children = make([]*Node, int(childKeyCount)+1)
		}

		// Connect the child to its parent
		item.node.children[item.childIdx] = childNode

		// Read the keys
		for i := 0; i < int(childKeyCount); i++ {
			var keyLen int32
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				return fmt.Errorf("failed to read child key length: %w", err)
			}

			keyBytes := make([]byte, keyLen)
			if _, err := buf.Read(keyBytes); err != nil {
				return fmt.Errorf("failed to read child key: %w", err)
			}

			childNode.keys[i] = string(keyBytes)
		}

		// For leaf nodes, read values
		if childIsLeaf {
			for i := 0; i < int(childKeyCount); i++ {
				var valCount int32
				if err := binary.Read(buf, binary.LittleEndian, &valCount); err != nil {
					return fmt.Errorf("failed to read child value count: %w", err)
				}

				values := make([]int64, int(valCount))
				for j := 0; j < int(valCount); j++ {
					var val int64
					if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
						return fmt.Errorf("failed to read child value: %w", err)
					}
					values[j] = val
				}

				childNode.values[i] = values
			}
		} else {
			// If this node has children, add all child slots to the queue
			for i := 0; i <= int(childKeyCount); i++ {
				queue = append(queue, queueItem{
					node:     childNode,
					childIdx: i,
					isRoot:   false,
				})
			}
		}
	}

	return nil
}

// saveToDisk saves the B-tree to disk
func (bt *BTree) SaveToDisk(dirPath, name string) error {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	// Serialize the B-tree
	data, err := bt.Serialize()
	if err != nil {
		return err
	}

	// Write the serialized data to disk
	if err := os.WriteFile(filepath.Join(dirPath, name+".btree"), data, 0644); err != nil {
		return err
	}

	// Write binary metadata only
	metadataBytes, err := bt.SerializeMetadata()
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(dirPath, name+".metadata.bin"), metadataBytes, 0644); err != nil {
		return err
	}

	return nil
}

// LoadFromDisk loads a B-tree from disk
func LoadFromDisk(dirPath, name string) (*BTree, error) {
	// Read the serialized data from disk
	data, err := os.ReadFile(filepath.Join(dirPath, name+".btree"))
	if err != nil {
		return nil, err
	}

	// Deserialize the B-tree
	return Deserialize(data)
}

// IterateKeys iterates through all keys in the B-tree and calls the callback function for each key
// with its associated positions list. The callback returns true to continue iteration, or false to stop.
func (bt *BTree) IterateKeys(callback func(key string, positions []int64) bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Use a stack to traverse the tree
	type stackItem struct {
		node *Node
		idx  int
	}

	stack := make([]stackItem, 0)
	node := bt.root

	// Find the leftmost leaf
	for !node.isLeaf {
		stack = append(stack, stackItem{node, 0})
		node = node.children[0]
	}

	// Start from the leftmost leaf and iterate through all leaves
	for {
		// If this is a leaf, iterate through all its keys
		if node.isLeaf {
			for i := 0; i < len(node.keys); i++ {
				key := node.keys[i]
				positions := node.values[i]

				// Call the callback function
				if !callback(key, positions) {
					return // Stop iteration if callback returns false
				}
			}
		}

		// If the stack is empty, we're done
		if len(stack) == 0 {
			break
		}

		// Pop the parent node and index from the stack
		parent := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Move to the next child
		parent.idx++

		// If we've processed all children of this parent, continue up the stack
		if parent.idx >= len(parent.node.children) {
			node = parent.node
			continue
		}

		// Otherwise, move to the next child
		node = parent.node.children[parent.idx]

		// If this is not a leaf, find its leftmost leaf
		for !node.isLeaf {
			stack = append(stack, stackItem{node, 0})
			node = node.children[0]
		}
	}
}

// LoadBTreeFromDisk loads a B-tree from disk with a specific prefix
func LoadBTreeFromDisk(dirPath, prefix string) (*BTree, error) {
	// Check if the standard .btree file exists
	btreeFile := filepath.Join(dirPath, prefix+".btree")
	if _, err := os.Stat(btreeFile); err == nil {
		// Use the standard LoadFromDisk if the file exists
		return LoadFromDisk(dirPath, prefix)
	}

	// If the standard file doesn't exist, look for the metadata file
	metadataFile := filepath.Join(dirPath, prefix+".metadata.bin")
	if _, err := os.Stat(metadataFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("neither BTree nor metadata file found: %s", dirPath)
	}

	// Read metadata
	metadataBytes, err := os.ReadFile(metadataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Create a new empty B-tree
	bt := &BTree{
		order:        DefaultOrder,
		leafCapacity: DefaultLeafCapacity,
		keyType:      storage.TEXT, // Default to TEXT
		size:         0,
		root: &Node{
			isLeaf:   true,
			keys:     make([]string, 0),
			values:   make([][]int64, 0),
			children: nil,
		},
	}

	// Try to deserialize metadata
	reader := binser.NewReader(metadataBytes)

	// Read metadata fields
	btType, err := reader.ReadInt()
	if err == nil {
		bt.keyType = storage.DataType(btType)
	}

	order, err := reader.ReadInt()
	if err == nil {
		bt.order = order
	}

	leafCap, err := reader.ReadInt()
	if err == nil {
		bt.leafCapacity = leafCap
	}

	size, err := reader.ReadInt64()
	if err == nil {
		bt.size = size
	}

	return bt, nil
}
