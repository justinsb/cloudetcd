package batch

import (
	"testing"

	"justinsb.com/cloudetcd/pkg/persistence"
)

func TestTxnEffects_CanBatchWith(t *testing.T) {
	tests := []struct {
		name     string
		txn1     *TxnMeta
		txn2     *TxnMeta
		expected bool
	}{
		{
			name: "no conflicts - can batch",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key1", 5)
				txn.AddWrite("key2")
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key3", 7)
				txn.AddWrite("key4")
				return txn
			}(),
			expected: true,
		},
		{
			name: "write-write conflict - cannot batch",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddWrite("key1")
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddWrite("key1")
				return txn
			}(),
			expected: false,
		},
		{
			name: "read-write - can batch",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key1", 5)
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddWrite("key1")
				return txn
			}(),
			expected: true,
		},
		{
			name: "write-read conflict - cannot batch",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddWrite("key1")
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key1", 5)
				return txn
			}(),
			expected: false,
		},
		{
			name: "multiple keys - mixed conflicts",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key1", 5)
				txn.AddWrite("key2")
				txn.AddWrite("key3")
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key3", 7) // Conflicts with txn1's write of key3
				txn.AddRead("key4", 8)
				return txn
			}(),
			expected: false,
		},
		{
			name: "multiple keys - no conflicts",
			txn1: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key1", 5)
				txn.AddWrite("key2")
				return txn
			}(),
			txn2: func() *TxnMeta {
				txn := persistence.NewTxnMeta(1)
				txn.AddRead("key3", 7)
				txn.AddWrite("key4")
				return txn
			}(),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanBatchTogether(tt.txn1, tt.txn2)
			if result != tt.expected {
				t.Errorf("CanBatchWith() = %v, want %v", result, tt.expected)
			}
		})
	}
}
