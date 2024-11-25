package lsm

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/google/btree"
)

// Entry represents a key-value pair stored in the B-Tree
type Entry struct {
	Key   []byte
	Value []byte
}

// Less implements the comparison logic for B-Tree nodes
// Entries are compared by their keys
func (e *Entry) Less(than btree.Item) bool {
	return bytes.Compare(e.Key, than.(*Entry).Key) < 0
}

// Memtable represents the in-memory key-value store backed by a B-Tree
type Memtable struct {
	mu    sync.RWMutex
	tree  *btree.BTree
	order int // B-Tree order
}

// NewMemtable creates a new Memtable instance with the specified B-Tree order
func NewMemtable(order int) *Memtable {
	return &Memtable{
		tree:  btree.New(order),
		order: order,
	}
}

// Put inserts or updates a key-value pair in the Memtable
func (mt *Memtable) Put(key []byte, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	entry := &Entry{Key: key, Value: value}
	mt.tree.ReplaceOrInsert(entry)
}

// Get retrieves the value associated with a key
func (mt *Memtable) Get(key []byte) ([]byte, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	item := mt.tree.Get(&Entry{Key: key})
	if item == nil {
		return nil, false
	}
	return item.(*Entry).Value, true
}

// Delete removes a key-value pair from the Memtable
func (mt *Memtable) Delete(key []byte) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	item := mt.tree.Delete(&Entry{Key: key})
	return item != nil
}

// RangeScan retrieves all key-value pairs in the given range [start, end)
func (mt *Memtable) RangeScan(start, end []byte) []*Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	var results []*Entry
	mt.tree.AscendRange(&Entry{Key: start}, &Entry{Key: end}, func(item btree.Item) bool {
		results = append(results, item.(*Entry))
		return true
	})
	return results
}

// Iterator retrieves all key-value pairs in ascending order
func (mt *Memtable) Iterator() []*Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	var results []*Entry
	mt.tree.Ascend(func(item btree.Item) bool {
		results = append(results, item.(*Entry))
		return true
	})
	return results
}

// Size returns the number of entries in the Memtable
func (mt *Memtable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.tree.Len()
}

// Debug prints the content of the Memtable for debugging
func (mt *Memtable) Debug() {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	mt.tree.Ascend(func(item btree.Item) bool {
		entry := item.(*Entry)
		fmt.Printf("Key: %s, Value: %v\n", entry.Key, entry.Value)
		return true
	})
}

// Fold iterates through the Memtable entries and applies a user-defined function.
func (mt *Memtable) Fold(fn func(key []byte, value []byte) bool) {
	// 注意这个加锁并不是很优雅的 会存在锁嵌套问题
	// 后续优化 也可以直接不加锁
	// mt.mu.RLock()
	// defer mt.mu.RUnlock()

	mt.tree.Ascend(func(item btree.Item) bool {
		entry := item.(*Entry)
		// Apply the user-defined function
		return fn(entry.Key, entry.Value)
	})
}
