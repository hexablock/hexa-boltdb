package hexaboltdb

import (
	"log"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/hexablock/blox/block"
	"github.com/hexablock/blox/device"
)

// IN-PROGRESS

// BlockIndex is a boltdb based index of all blocks on a device
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

func (index *BlockIndex) Get(id []byte) (*device.IndexEntry, error) {
	var idx device.IndexEntry
	err := index.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(index.bucket)
		data := bkt.Get(id)
		if data == nil {
			return block.ErrBlockNotFound
		}
		return idx.UnmarshalBinary(data)
	})

	return &idx, err
}

func (index *BlockIndex) Exists(id []byte) bool {
	var exists bool
	index.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(index.bucket)
		if data := bkt.Get(id); data != nil {
			exists = true
		}
		return nil
	})
	return exists
}

// Iter iterate over all index entries in the store
func (index *BlockIndex) Iter(cb func(*device.IndexEntry) error) error {
	return index.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(index.bucket)
		return bkt.ForEach(func(k []byte, v []byte) error {
			var idx device.IndexEntry
			err := idx.UnmarshalBinary(v)
			// Skip over unmarshalable values
			if err != nil {
				log.Printf("[WARN] Failed to deserialize IndexEntry id=%x", k)
				return nil
			}
			return cb(&idx)

		})
	})
}

// Set an block index entry to the index store
func (index *BlockIndex) Set(idx *device.IndexEntry) error {
	return index.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(index.bucket)
		value, err := idx.MarshalBinary()
		if err == nil {
			err = bkt.Put(idx.ID(), value)
		}
		return err
	})
}

func (index *BlockIndex) Remove(id []byte) (*device.IndexEntry, error) {
	var idx device.IndexEntry
	err := index.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(index.bucket)
		val := bkt.Get(id)
		if val == nil {
			return block.ErrBlockNotFound
		}
		err := bkt.Delete(id)
		if err == nil {
			err = idx.UnmarshalBinary(val)
		}
		return err
	})
	return &idx, err
}

// Stats returns statistics.  Also contains raw device stats
func (index *BlockIndex) Stats() *device.Stats {
	stats := &device.Stats{}

	return stats
}

// Close closes the bolt store after which it can no longer be used.
func (index *BlockIndex) Close() error {
	return index.db.Close()
}
