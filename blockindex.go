package hexaboltdb

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

type BlockIndex struct {
	opt    *bolt.Options
	bucket []byte
	mode   os.FileMode
	db     *bolt.DB
}

// NewBlockIndex inits a new boltdb backed entry store with defaults
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{
		opt:    bolt.DefaultOptions,
		bucket: []byte("blocks"),
		mode:   0755,
	}
}

// Name returns the name of the rocksdb entry store
func (index *BlockIndex) Name() string {
	return dbname
}

// Open opens the rocks store for writing
func (index *BlockIndex) Open(datadir string) error {
	filename := filepath.Join(datadir, "index.db")
	db, err := bolt.Open(filename, index.mode, index.opt)
	if err == nil {
		index.db = db
		err = db.Update(func(tx *bolt.Tx) error {
			_, er := tx.CreateBucketIfNotExists(index.bucket)
			return er
		})
	}
	return err
}

// Close closes the bolt store after which it can no longer be used.
func (index *BlockIndex) Close() error {
	return index.db.Close()
}
