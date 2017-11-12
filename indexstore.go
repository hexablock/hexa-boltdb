package hexaboltdb

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

// Stats contains store stats
type Stats struct {
	Keys     int64
	OpenKeys int
}

// IndexStore implements an rocksdb KeylogIndex store interface
type IndexStore struct {
	db     *bolt.DB
	opt    *bolt.Options
	bucket []byte
	mode   os.FileMode
	// open indexes
	openIdxs *openIndexes
}

// NewIndexStore initializes an in-memory store for KeylogIndexes.
func NewIndexStore() *IndexStore {
	return &IndexStore{
		openIdxs: newOpenIndexes(),
		opt:      bolt.DefaultOptions,
		bucket:   []byte("index"),
		mode:     0755,
	}
}

// Open opens the index store for usage
func (store *IndexStore) Open(dir string) error {
	filename := filepath.Join(dir, "index.db")
	db, err := bolt.Open(filename, store.mode, store.opt)
	if err == nil {
		store.db = db
		err = db.Update(func(tx *bolt.Tx) error {
			_, er := tx.CreateBucketIfNotExists(store.bucket)
			return er
		})
	}
	return err
}

// Stats returns statistics about the store
func (store *IndexStore) Stats() *Stats {
	return &Stats{
		Keys:     store.Count(),
		OpenKeys: store.openIdxs.count(),
	}
}

// Name returns the name of the index store
func (store *IndexStore) Name() string {
	return dbname
}

// NewKey creates a new KeylogIndex and adds it to the store.  It returns an error if it
// already exists
func (store *IndexStore) NewKey(key []byte) (hexalog.KeylogIndex, error) {
	if store.openIdxs.isOpen(key) {
		return nil, hexatype.ErrKeyExists
	}

	err := store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)
		if d := bkt.Get(key); d != nil {
			return hexatype.ErrKeyExists
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	kli := &KeylogIndex{
		db:     store.db,
		idx:    hexalog.NewUnsafeKeylogIndex(key),
		bucket: store.bucket,
		kh:     store.openIdxs,
	}
	store.openIdxs.register(kli)

	return kli, nil
}

// MarkKey sets the marker on a key.  If the key does not exist a new one is created.
// It returns the KeylogIndex or an error.
func (store *IndexStore) MarkKey(key, marker []byte) (hexalog.KeylogIndex, error) {
	if h, ok := store.openIdxs.get(key); ok {
		_, err := h.SetMarker(marker)
		return h.KeylogIndex, err
	}

	var (
		idx hexalog.KeylogIndex
		err error
	)

	idx, err = store.openIndex(key)
	if err == hexatype.ErrKeyNotFound {
		// Create a new key
		kli := &KeylogIndex{
			db:     store.db,
			idx:    hexalog.NewUnsafeKeylogIndex(key),
			bucket: store.bucket,
			kh:     store.openIdxs,
		}
		store.openIdxs.register(kli)
		idx = kli

	} else if err != nil {
		return nil, err
	}

	_, err = idx.SetMarker(marker)

	return idx, err
}

// GetKey returns a KeylogIndex from the store or an error if not found
func (store *IndexStore) GetKey(key []byte) (hexalog.KeylogIndex, error) {
	idx, ok := store.openIdxs.get(key)
	if ok {
		return idx.KeylogIndex, nil
	}

	return store.openIndex(key)
}

// RemoveKey removes the given key's index from the store.  It does NOT remove the associated
// entry hash id's
func (store *IndexStore) RemoveKey(key []byte) error {
	if store.openIdxs.isOpen(key) {
		return errIndexOpen
	}

	err := store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)
		if d := bkt.Get(key); d == nil {
			return hexatype.ErrKeyNotFound
		}
		return bkt.Delete(key)
	})

	return err
}

// Iter iterates over each key and index
func (store *IndexStore) Iter(cb func([]byte, hexalog.KeylogIndex) error) error {

	err := store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)
		return bkt.ForEach(func(key []byte, val []byte) (err error) {

			var idx *KeylogIndex

			ih, ok := store.openIdxs.get(key)
			if !ok {

				idx, err = store.makeKeylogIndex(val)
				if err != nil {
					log.Println("[ERROR]", err)
					return nil
				}
				//store.openIdxs.register(idx)

			} else {
				idx = ih.KeylogIndex
				defer idx.Close()
			}

			err = cb(key, idx)

			return
		})
	})

	return err
}

// Count returns the total number of keys in the index
func (store *IndexStore) Count() int64 {
	var c int64
	store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)
		c = int64(bkt.Stats().KeyN)
		return nil
	})
	return c
}

// Close closes the index store by flushing all open indexes to rocka then
// closing rocks
func (store *IndexStore) Close() error {
	err := store.openIdxs.closeAll()
	store.db.Close()
	return err
}

func (store *IndexStore) openIndex(key []byte) (*KeylogIndex, error) {
	var data []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)

		if data = bkt.Get(key); data == nil {
			return hexatype.ErrKeyNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	kli, err := store.makeKeylogIndex(data)
	if err == nil {
		store.openIdxs.register(kli)
	}

	return kli, err
}

// Unmarshal data to KeylogIndex
func (store *IndexStore) makeKeylogIndex(data []byte) (*KeylogIndex, error) {

	var ukli hexalog.UnsafeKeylogIndex
	if err := proto.Unmarshal(data, &ukli); err != nil {
		return nil, err
	}

	return &KeylogIndex{
		db:     store.db,
		idx:    &ukli,
		bucket: store.bucket,
		kh:     store.openIdxs,
	}, nil
}
