package hexaboltdb

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexalog"
	"github.com/hexablock/log"
)

// KeylogIndex implements a hexalog.KeylogIndex interface backed by rocks
type KeylogIndex struct {
	mu  sync.RWMutex
	idx *hexalog.UnsafeKeylogIndex
	// Backend to flush data to
	db     *bolt.DB
	bucket []byte
	// Open index tracker used when closing the index
	kh *openIndexes
}

// Key returns the key for the index
func (idx *KeylogIndex) Key() []byte {
	return idx.idx.Key
}

// Marker returns the marker value
func (idx *KeylogIndex) Marker() []byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Marker
}

// SetMarker sets the marker for the index.  It returns true if the marker is not part of
// the index and was set.  This never returns an error as it is in-memory
func (idx *KeylogIndex) SetMarker(marker []byte) (bool, error) {
	idx.mu.Lock()
	ok := idx.idx.SetMarker(marker)
	idx.mu.Unlock()
	return ok, nil
}

// Append appends the id to the index checking the previous hash.
func (idx *KeylogIndex) Append(id, prev []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.idx.Append(id, prev)
}

// Rollback safely removes the last entry id
func (idx *KeylogIndex) Rollback() (int, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.idx.Rollback()
}

// Last safely returns the last entry id
func (idx *KeylogIndex) Last() []byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Last()
}

// Contains safely returns if the entry id is in the index
func (idx *KeylogIndex) Contains(id []byte) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Contains(id)
}

// Iter iterates over each entry id in the index
func (idx *KeylogIndex) Iter(seek []byte, cb func(id []byte) error) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Iter(seek, cb)
}

// Count returns the number of entries in the index
func (idx *KeylogIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Count()
}

// Height returns the height of the log in a thread safe way
func (idx *KeylogIndex) Height() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.idx.Height
}

// Index returns the KeylogIndex index struct.  It is meant be used as readonly
// point in time.
func (idx *KeylogIndex) Index() hexalog.UnsafeKeylogIndex {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return *idx.idx
}

// Flush writes the data out to rocks
func (idx *KeylogIndex) Flush() error {
	idx.mu.RLock()
	value, err := proto.Marshal(idx.idx)
	idx.mu.RUnlock()

	if err == nil {

		err = idx.db.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(idx.bucket)
			return bkt.Put(idx.Key(), value)
		})

	}

	if err == nil {
		log.Printf("[DEBUG] Flushed index key=%s", idx.Key())
	}

	return err
}

// Close closes the index by calling close on the open handle manager
func (idx *KeylogIndex) Close() error {
	return idx.kh.close(idx.Key())
}
