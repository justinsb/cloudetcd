package bptree

import (
	"testing"
)

func TestBPTree_AddRevision(t *testing.T) {
	tree := New()
	tree.AddRevision([]byte("key1"), 1)
	tree.AddRevision([]byte("key2"), 2)
	tree.AddRevision([]byte("key1"), 3)

	if len(tree.root.keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(tree.root.keys))
	}
	if string(tree.root.keys[0]) != "key1" {
		t.Errorf("expected key1, got %s", string(tree.root.keys[0]))
	}
	if string(tree.root.keys[1]) != "key2" {
		t.Errorf("expected key2, got %s", string(tree.root.keys[1]))
	}
	if len(tree.root.values[0]) != 2 {
		t.Errorf("expected 2 values for key1, got %d", len(tree.root.values[0]))
	}
	if tree.root.values[0][0] != 1 {
		t.Errorf("expected 1, got %d", tree.root.values[0][0])
	}
	if tree.root.values[0][1] != 3 {
		t.Errorf("expected 3, got %d", tree.root.values[0][1])
	}
	if len(tree.root.values[1]) != 1 {
		t.Errorf("expected 1 value for key2, got %d", len(tree.root.values[1]))
	}
	if tree.root.values[1][0] != 2 {
		t.Errorf("expected 2, got %d", tree.root.values[1][0])
	}
}

func TestBPTree_GetLatestRevisionByKey(t *testing.T) {
	tree := New()
	tree.AddRevision([]byte("key1"), 1)
	tree.AddRevision([]byte("key2"), 2)
	tree.AddRevision([]byte("key1"), 3)
	tree.AddRevision([]byte("key1"), 5)

	rev, ok := tree.getLatestRevisionByKey([]byte("key1"), 4)
	if !ok {
		t.Errorf("expected to find a revision for key1")
	}
	if rev != 3 {
		t.Errorf("expected revision 3, got %d", rev)
	}

	rev, ok = tree.getLatestRevisionByKey([]byte("key1"), 5)
	if !ok {
		t.Errorf("expected to find a revision for key1")
	}
	if rev != 5 {
		t.Errorf("expected revision 5, got %d", rev)
	}

	rev, ok = tree.getLatestRevisionByKey([]byte("key1"), 0)
	if ok {
		t.Errorf("expected to not find a revision for key1")
	}

	rev, ok = tree.getLatestRevisionByKey([]byte("key2"), 10)
	if !ok {
		t.Errorf("expected to find a revision for key2")
	}
	if rev != 2 {
		t.Errorf("expected revision 2, got %d", rev)
	}

	rev, ok = tree.getLatestRevisionByKey([]byte("key3"), 10)
	if ok {
		t.Errorf("expected to not find a revision for key3")
	}
}

func TestBPTree_ListRevisionsByKeyRange(t *testing.T) {
	tree := New()
	tree.AddRevision([]byte("key1"), 1)
	tree.AddRevision([]byte("key2"), 2)
	tree.AddRevision([]byte("key3"), 3)
	tree.AddRevision([]byte("key1"), 4)
	tree.AddRevision([]byte("key2"), 5)

	results := tree.listRevisionsByKeyRange([]byte("key1"), []byte("key3"), 10)
	if len(results) != 2 {
		t.Errorf("expected 2 keys, got %d", len(results))
	}
	if len(results["key1"]) != 2 {
		t.Errorf("expected 2 revisions for key1, got %d", len(results["key1"]))
	}
	if results["key1"][0] != 1 || results["key1"][1] != 4 {
		t.Errorf("unexpected revisions for key1: %v", results["key1"])
	}
	if len(results["key2"]) != 2 {
		t.Errorf("expected 2 revisions for key2, got %d", len(results["key2"]))
	}
	if results["key2"][0] != 2 || results["key2"][1] != 5 {
		t.Errorf("unexpected revisions for key2: %v", results["key2"])
	}

	results = tree.listRevisionsByKeyRange([]byte("key1"), []byte("key3"), 3)
	if len(results) != 2 {
		t.Errorf("expected 2 keys, got %d", len(results))
	}
	if len(results["key1"]) != 1 {
		t.Errorf("expected 1 revision for key1, got %d", len(results["key1"]))
	}
	if results["key1"][0] != 1 {
		t.Errorf("unexpected revisions for key1: %v", results["key1"])
	}
	if len(results["key2"]) != 1 {
		t.Errorf("expected 1 revision for key2, got %d", len(results["key2"]))
	}
	if results["key2"][0] != 2 {
		t.Errorf("unexpected revisions for key2: %v", results["key2"])
	}
}

func TestBPTree_Split(t *testing.T) {
	tree := New()
	for i := 0; i < maxKeys; i++ {
		key := []byte{byte(i)}
		tree.AddRevision(key, int64(i))
	}
	if len(tree.root.keys) != maxKeys {
		t.Errorf("expected %d keys, got %d", maxKeys, len(tree.root.keys))
	}

	tree.AddRevision([]byte{byte(maxKeys)}, int64(maxKeys))

	if len(tree.root.keys) != 1 {
		t.Errorf("expected 1 key in root, got %d", len(tree.root.keys))
	}
	if len(tree.root.children) != 2 {
		t.Errorf("expected 2 children in root, got %d", len(tree.root.children))
	}
	if len(tree.root.children[0].keys) != maxKeys/2 {
		t.Errorf("expected %d keys in left child, got %d", maxKeys/2, len(tree.root.children[0].keys))
	}
	if len(tree.root.children[1].keys) != maxKeys/2 {
		t.Errorf("expected %d keys in right child, got %d", maxKeys/2, len(tree.root.children[1].keys))
	}
}



