# B+ Tree Implementation

This document describes the implementation of an in-memory B+ tree data structure. This data structure is used as the index for lookup by key. Note that we do not store values, we assume those are stored separately. Instead we store the revisions for each key.

## Design Goals

*   **Keys are `[]byte`**, and we want to store a list of 64-bit revisions. We consider the list of revisions as the value.
*   The primary operations are `getLatestRevisionByKey` (which accepts a timestamp), `addRevision`, and `listRevisionsByKeyRange` (which accepts a timestamp).
*   It combines ideas of a B-Tree and a prefix tree. Branch pages identify common bytes of the prefix, so that we do not need to store and compare prefix bytes repeatedly.
*   It is designed to be **in-memory**, so we accept variable page sizes and do not strictly balance the tree.
*   Somewhat unusually, we keep **multiple revision numbers for each key**.
*   In a BTree, the invariant is typically that the subtree has keys greater than or equal to the parent key (and less than the next parent key).  In our structure, the subtree has keys that strictly have as prefix the parent key; and keys in a branch do not overlap (i.e. we will not create two subtrees at the same level for `abc` and `abcd`)

## Node Splitting

When a node becomes full (i.e., it contains the maximum number of keys), it is split into two nodes. The middle key is promoted to the parent node, and the remaining keys are divided between the two new nodes. This process ensures that the tree remains balanced.
The `maxKeys` constant determines the maximum number of keys that a node can hold. This value is currently set to 32.

