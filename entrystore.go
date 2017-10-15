package hexaboltdb

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
)

// EntryStore is an entry store using rocksdb as the backend
type EntryStore struct {
	opt    *bolt.Options
	bucket []byte
	mode   os.FileMode
	db     *bolt.DB
}

// NewEntryStore inits a new rocksdb backed entry store with defaults
func NewEntryStore() *EntryStore {
	return &EntryStore{
		opt:    bolt.DefaultOptions,
		bucket: []byte("entries"),
		mode:   0755,
	}
}

// Name returns the name of the rocksdb entry store
func (store *EntryStore) Name() string {
	return "boltdb"
}

// Open opens the rocks store for writing
func (store *EntryStore) Open(datadir string) error {
	filename := filepath.Join(datadir, "entries.db")
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

// Get gets an entry by the id
func (store *EntryStore) Get(id []byte) (*hexalog.Entry, error) {
	var entry hexalog.Entry
	err := store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)

		value := bkt.Get(id)
		if value == nil {
			return hexatype.ErrEntryNotFound
		}

		return proto.Unmarshal(value, &entry)
	})

	return &entry, err
}

// Set sets the entry to the store by the id
func (store *EntryStore) Set(id []byte, entry *hexalog.Entry) error {
	value, err := proto.Marshal(entry)
	if err == nil {
		err = store.db.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(store.bucket)
			return bkt.Put(id, value)
		})
	}
	return err
}

// Delete deletes an entry by the id
func (store *EntryStore) Delete(id []byte) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(store.bucket)
		return bkt.Delete(id)
	})
}

// Count returns the approximate entry count
func (store *EntryStore) Count() int64 {
	var c int64
	store.db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket(store.bucket).Stats()
		c = int64(stats.KeyN)
		return nil
	})
	return c
}

// Close closes the rocks store after which it can no longer be used.
func (store *EntryStore) Close() error {
	return store.db.Close()
}
