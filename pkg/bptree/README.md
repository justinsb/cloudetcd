# B+ Tree Implementation

This document describes the implementation of an in-memory B+ tree data structure. This data structure is used as the index for lookup by key. Note that we do not store values, we assume those are stored separately. Instead we store the revisions for each key.

## Design Goals

*   **Keys are `[]byte`**, and we want to store a list of 64-bit revisions. We consider the list of revisions as the value.
*   The primary operations are `getLatestRevisionByKey` (which accepts a timestamp), `addRevision`, and `listRevisionsByKeyRange` (which accepts a timestamp).
*   It combines ideas of a B-Tree and a prefix tree. Branch pages identify common bytes of the prefix, so that we do not need to store and compare prefix bytes repeatedly.
*   It is designed to be **in-memory**, so we accept variable page sizes and do not strictly balance the tree.
*   Somewhat unusually, we keep **multiple revision numbers for each key**.

## Data Structures

The B+ tree is composed of nodes, which can be either internal nodes or leaf nodes.

*   **`BPTree`**: The main struct for the B+ tree. It contains a pointer to the root node and a read-write mutex for concurrent access.
*   **`node`**: Represents a node in the B+ tree. It contains a slice of keys, a slice of values (which are slices of 64-bit integers), and a slice of child nodes.

```go
type BPTree struct {
	root *node
	lock sync.RWMutex
}

type node struct {
	keys     [][]byte
	values   [][]int64
	children []*node
}
```

## Operations

### `AddRevision(key []byte, revision int64)`

This operation adds a new revision to a key. If the key does not exist, it is created.

The algorithm is as follows:

1.  Traverse the tree to find the leaf node where the key should be inserted.
2.  If the key already exists, append the new revision to the existing list of revisions.
3.  If the key does not exist, insert the key and the new revision into the leaf node.
4.  If the leaf node is full, split it into two nodes and promote the middle key to the parent node. This splitting process may propagate up to the root of the tree.

### `getLatestRevisionByKey(key []byte, atRevision int64)`

This operation returns the latest revision for a key that is less than or equal to the given timestamp.

The algorithm is as follows:

1.  Traverse the tree to find the leaf node containing the key.
2.  If the key is found, iterate through its revisions and return the latest revision that is less than or equal to the given timestamp.
3.  If the key is not found, return 0 and false.

### `listRevisionsByKeyRange(startKey, endKey []byte, atRevision int64)`

This operation returns a list of revisions for keys in the given range.

The algorithm is as follows:

1.  Traverse the tree to find the leaf node where the `startKey` is located.
2.  Iterate through the leaf nodes until the `endKey` is reached.
3.  For each key in the range, find all revisions that are less than or equal to the given timestamp and add them to the result map.

## Node Splitting

When a node becomes full (i.e., it contains the maximum number of keys), it is split into two nodes. The middle key is promoted to the parent node, and the remaining keys are divided between the two new nodes. This process ensures that the tree remains balanced.
The `maxKeys` constant determines the maximum number of keys that a node can hold. This value is currently set to 32.

