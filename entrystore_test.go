package hexaboltdb

import (
	"bytes"
	"crypto/sha256"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

func TestMain(m *testing.M) {
	log.SetLevel("DEBUG")
	os.Exit(m.Run())
}

func Test_EntryStore(t *testing.T) {

	tmpdir, _ := ioutil.TempDir("/tmp", "hexalog-")
	defer os.RemoveAll(tmpdir)

	rdb := NewEntryStore()
	if rdb.Name() != "boltdb" {
		t.Fatal("wrong store name")
	}

	if err := rdb.Open(tmpdir); err != nil {
		t.Fatal(err)
	}

	defer rdb.Close()

	ent := &hexalog.Entry{
		Previous:  make([]byte, 32),
		Key:       []byte("key"),
		Timestamp: uint64(time.Now().UnixNano()),
	}

	id := ent.Hash(sha256.New())
	if err := rdb.Set(id, ent); err != nil {
		t.Fatal(err)
	}

	if rdb.Count() != 1 {
		t.Fatal("should have 1 key", rdb.Count())
	}

	ent1, err := rdb.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if string(ent1.Key) != "key" {
		t.Fatal("key mismatch")
	}

	id1 := ent1.Hash(sha256.New())
	if bytes.Compare(id, id1) != 0 {
		t.Fatal("id mismatch")
	}

	if err = rdb.Delete(id1); err != nil {
		t.Fatal(err)
	}

	_, err = rdb.Get(id)
	if err != hexatype.ErrEntryNotFound {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrEntryNotFound, err)
	}

}
