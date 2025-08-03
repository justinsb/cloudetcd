// Package bptree implements an in-memory B+ tree.
//
// This B+ tree is used as an index for key-based lookups. It does not store
// values directly, but rather a list of 64-bit revision numbers for each key.
//
// The implementation is designed for in-memory use, and as such does not have
// strictly balanced pages. It also supports multiple revision numbers for each key.
package bptree

import (
	"bytes"
	"sync"
)

const maxKeys = 32

// BPTree is a B+ tree implementation. It contains a pointer to the root node
// and a read-write mutex for concurrent access.
type BPTree struct {
	root *node
	lock sync.RWMutex
}

// New creates a new B+ tree.
func New() *BPTree {
	return &BPTree{
		root: &node{},
	}
}

// AddRevision adds a new revision to a key. If the key does not exist, it is created.
//
// The algorithm is as follows:
// 1. Traverse the tree to find the leaf node where the key should be inserted.
// 2. If the key already exists, append the new revision to the existing list of revisions.
// 3. If the key does not exist, insert the key and the new revision into the leaf node.
// 4. If the leaf node is full, split it into two nodes and promote the middle key to the parent node.
//    This splitting process may propagate up to the root of the tree.
func (t *BPTree) AddRevision(key []byte, revision int64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.root == nil {
		t.root = &node{}
	}
	if len(t.root.keys) == maxKeys {
		newRoot := &node{}
		newRoot.children = append(newRoot.children, t.root)
		t.root.split(newRoot, 0)
		t.root = newRoot
	}
	t.root.addRevision(key, revision)
}

// getLatestRevisionByKey returns the latest revision for a key that is less than or equal to the given timestamp.
//
// The algorithm is as follows:
// 1. Traverse the tree to find the leaf node containing the key.
// 2. If the key is found, iterate through its revisions and return the latest revision that is less than or equal to the given timestamp.
// 3. If the key is not found, return 0 and false.
func (t *BPTree) getLatestRevisionByKey(key []byte, atRevision int64) (int64, bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if t.root == nil {
		return 0, false
	}
	return t.root.getLatestRevisionByKey(key, atRevision)
}

// listRevisionsByKeyRange returns a list of revisions for keys in the given range.
//
// The algorithm is as follows:
// 1. Traverse the tree to find the leaf node where the startKey is located.
// 2. Iterate through the leaf nodes until the endKey is reached.
// 3. For each key in the range, find all revisions that are less than or equal to the given timestamp and add them to the result map.
func (t *BPTree) listRevisionsByKeyRange(startKey, endKey []byte, atRevision int64) map[string][]int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if t.root == nil {
		return nil
	}
	return t.root.listRevisionsByKeyRange(startKey, endKey, atRevision)
}

// node represents a node in the B+ tree. It contains a slice of keys, a slice
// of values (which are slices of 64-bit integers), and a slice of child nodes.
type node struct {
	keys     [][]byte
	values   [][]int64
	children []*node
}

// split splits a full node into two.
// When a node becomes full (i.e., it contains the maximum number of keys), it is split into two nodes.
// The middle key is promoted to the parent node, and the remaining keys are divided between the two new nodes.
// This process ensures that the tree remains balanced.
func (n *node) split(parent *node, i int) {
	newChild := &node{}
	mid := len(n.keys) / 2
	parent.keys = append(parent.keys, nil)
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = n.keys[mid]

	newChild.keys = append(newChild.keys, n.keys[mid+1:]...)
	n.keys = n.keys[:mid]

	if len(n.values) > 0 {
		newChild.values = append(newChild.values, n.values[mid+1:]...)
		n.values = n.values[:mid]
	}

	if len(n.children) > 0 {
		newChild.children = append(newChild.children, n.children[mid+1:]...)
		n.children = n.children[:mid+1]
	}

	parent.children = append(parent.children, nil)
	copy(parent.children[i+1:], parent.children[i:])
	parent.children[i+1] = newChild
}

func (n *node) addRevision(key []byte, revision int64) {
	i := 0
	for i < len(n.keys) && bytes.Compare(n.keys[i], key) < 0 {
		i++
	}

	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		n.values[i] = append(n.values[i], revision)
		return
	}

	if len(n.children) > 0 {
		if len(n.children[i].keys) == maxKeys {
			n.children[i].split(n, i)
			if bytes.Compare(key, n.keys[i]) > 0 {
				i++
			}
		}
		n.children[i].addRevision(key, revision)
		return
	}

	n.keys = append(n.keys, nil)
	copy(n.keys[i+1:], n.keys[i:])
	n.keys[i] = key

	n.values = append(n.values, nil)
	copy(n.values[i+1:], n.values[i:])
	n.values[i] = []int64{revision}
}


func (n *node) getLatestRevisionByKey(key []byte, atRevision int64) (int64, bool) {
	i := 0
	for i < len(n.keys) && bytes.Compare(n.keys[i], key) < 0 {
		i++
	}

	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		revisions := n.values[i]
		latest := int64(0)
		found := false
		for _, r := range revisions {
			if r <= atRevision {
				if r > latest {
					latest = r
				}
				found = true
			}
		}
		return latest, found
	}

	if len(n.children) > 0 {
		return n.children[i].getLatestRevisionByKey(key, atRevision)
	}

	return 0, false
}

func (n *node) listRevisionsByKeyRange(startKey, endKey []byte, atRevision int64) map[string][]int64 {
	results := make(map[string][]int64)
	i := 0
	for i < len(n.keys) && bytes.Compare(n.keys[i], startKey) < 0 {
		i++
	}

	if len(n.children) > 0 {
		for j := i; j < len(n.children); j++ {
			childResults := n.children[j].listRevisionsByKeyRange(startKey, endKey, atRevision)
			for k, v := range childResults {
				results[k] = v
			}
			if j < len(n.keys) && bytes.Compare(n.keys[j], endKey) >= 0 {
				break
			}
		}
	} else {
		for j := i; j < len(n.keys); j++ {
			key := n.keys[j]
			if bytes.Compare(key, endKey) >= 0 {
				break
			}
			revisions := n.values[j]
			matchingRevisions := []int64{}
			for _, r := range revisions {
				if r <= atRevision {
					matchingRevisions = append(matchingRevisions, r)
				}
			}
			if len(matchingRevisions) > 0 {
				results[string(key)] = matchingRevisions
			}
		}
	}

	return results
}