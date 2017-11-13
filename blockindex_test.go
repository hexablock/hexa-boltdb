package hexaboltdb

import (
	"crypto/sha256"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/hexablock/blox"
	"github.com/hexablock/blox/device"
)

func Test_BlockIndex(t *testing.T) {
	datadir, _ := ioutil.TempDir("/tmp", "blk-idx-")
	defer os.RemoveAll(datadir)

	raw, err := device.NewFileRawDevice(filepath.Join(datadir, "blocks"), sha256.New)
	if err != nil {
		t.Fatal(err)
	}
	idx := NewBlockIndex()
	if err = idx.Open(datadir); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	dev := device.NewBlockDevice(idx, raw)
	defer dev.Close()

	blx := blox.NewBlox(dev)

	rd, err := os.Open("./blockindex_test.go")
	if err != nil {
		t.Fatal(err)
	}

	index, err := blx.WriteIndex(rd, 2)
	if err != nil {
		t.Fatal(err)
	}

	if index.BlockCount() == 0 {
		t.Fatal("should have blocks")
	}
	// wr, _ := ioutil.TempFile("/tmp", "blk-idx-")
	// defer os.Remove(wr.Name())
	// defer wr.Close()
	//
	// if err = blx.ReadIndex(index.ID(), wr); err != nil {
	// 	t.Fatal(err)
	// }
}
