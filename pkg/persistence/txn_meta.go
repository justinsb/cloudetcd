package persistence

// TxnMeta represents the read and write sets of a transaction
// to enable serializability checking for batch commits
type TxnMeta struct {
	SnapshotRevision Revision
	// ReadSet contains keys that were read during the transaction
	ReadSet map[string]Revision
	// WriteSet contains keys that will be written during the transaction
	WriteSet map[string]bool
}

// NewTxnMeta creates a new TxnEffects instance
func NewTxnMeta(snapshotRevision Revision) *TxnMeta {
	return &TxnMeta{
		ReadSet:          make(map[string]Revision),
		WriteSet:         make(map[string]bool),
		SnapshotRevision: snapshotRevision,
	}
}

// AddRead records a read operation on a key at a specific revision
func (t *TxnMeta) AddRead(key string, revision Revision) {
	t.ReadSet[key] = revision
}

// AddWrite records a write operation on a key
func (t *TxnMeta) AddWrite(key string) {
	t.WriteSet[key] = true
}
