package hexaboltdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/hexablock/hexalog"
	"github.com/hexablock/hexatype"
)

func Test_IndexStore(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("/tmp", "hexarocksdb-")

	idxs := NewIndexStore()
	if err := idxs.Open(tmpdir); err != nil {
		t.Fatal(err)
	}

	if err := idxs.openIdxs.close([]byte("notfound")); err != hexatype.ErrKeyNotFound {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyNotFound, err)
	}

	ki, err := idxs.NewKey([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	id := make([]byte, 32)
	id[0] = byte('a')
	id[1] = byte('b')
	prev := make([]byte, 32)
	if err = ki.Append(id, prev); err != nil {
		t.Fatal(err)
	}

	if _, err = idxs.NewKey([]byte("key")); err != hexatype.ErrKeyExists {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyExists, err)
	}

	rki := ki.(*KeylogIndex)
	if err = rki.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = idxs.NewKey([]byte("key")); err != hexatype.ErrKeyExists {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyExists, err)
	}

	gval, err := idxs.GetKey([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if gval.Count() != 1 {
		t.Fatal("invalid count")
	}
	ih, _ := idxs.openIdxs.m["key"]
	if ih.cnt != 1 {
		t.Error("should have 1 handle", ih.cnt)
	}

	v2, err := idxs.GetKey([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	ih, _ = idxs.openIdxs.m["key"]
	if ih.cnt != 2 {
		t.Error("should have 1 handle", ih.cnt)
	}
	r2, ok := v2.(*KeylogIndex)
	if !ok {
		t.Fatalf("invalid type: %#v", v2)
	}
	if err = r2.Close(); err != nil {
		t.Fatal(err)
	}
	ih, _ = idxs.openIdxs.m["key"]
	if ih.cnt != 1 {
		t.Error("should have 1 handle", ih.cnt)
	}

	if idxs.Count() != 1 {
		t.Fatal("should have 1 key")
	}

	// CLose first one
	v1 := gval.(*KeylogIndex)
	if err = v1.Close(); err != nil {
		t.Fatal(err)
	}
	_, ok = idxs.openIdxs.m["key"]
	if ok {
		t.Error("key should not be found")
	}

	if _, err = idxs.GetKey([]byte("foo")); err != hexatype.ErrKeyNotFound {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyNotFound, err)
	}

	if err := idxs.Close(); err != nil {
		t.Fatal(err)
	}
}

func Test_IndexStore_Remove(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("/tmp", "hexarocksdb-")

	idxs := NewIndexStore()
	if err := idxs.Open(tmpdir); err != nil {
		t.Fatal(err)
	}
	if idxs.Name() != "boltdb" {
		t.Error("wrong store name")
	}
	defer idxs.Close()

	if err := idxs.RemoveKey([]byte("key")); err == nil {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyNotFound, err)
	}

	ki, err := idxs.NewKey([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if err = idxs.RemoveKey([]byte("key")); err != errIndexOpen {
		t.Fatalf("should fail with='%v' got='%v'", errIndexOpen, err)
	}

	ti := ki.(*KeylogIndex)
	if err = ti.Close(); err != nil {
		t.Fatal(err)
	}

	if err = idxs.RemoveKey([]byte("key")); err != nil {
		t.Fatal(err)
	}

	if _, err = idxs.GetKey([]byte("key")); err != hexatype.ErrKeyNotFound {
		t.Fatalf("should fail with='%v' got='%v'", hexatype.ErrKeyNotFound, err)
	}

}

func Test_IndexStore_MarkKey(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("/tmp", "hexarocksdb-")

	idxs := NewIndexStore()
	if err := idxs.Open(tmpdir); err != nil {
		t.Fatal(err)
	}
	defer idxs.Close()

	k1, err := idxs.MarkKey([]byte("key"), []byte("marker"))
	if err != nil {
		t.Fatal(err)
	}

	if string(k1.Marker()) != "marker" {
		t.Fatal("wrong marker value")
	}

	k2, err := idxs.GetKey([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if idxs.openIdxs.m["key"].cnt != 2 {
		t.Fatal("should have 2 open handles")
	}

	if string(k2.Marker()) != "marker" {
		t.Fatal("wrong marker value")
	}

	ok, err := k2.SetMarker([]byte("marker1"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should have set")
	}

	// Set marker on open handle
	k3, err := idxs.MarkKey([]byte("key"), []byte("marker2"))
	if err != nil {
		t.Fatal(err)
	}

	k1.(*KeylogIndex).Close()
	k2.(*KeylogIndex).Close()
	k3.(*KeylogIndex).Close()

	if _, ok := idxs.openIdxs.m["key"]; ok {
		t.Fatal("should have have key handle")
	}
}

func Test_IndexStore_Iter(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("/tmp", "hexarocksdb-")

	idxs := NewIndexStore()
	if err := idxs.Open(tmpdir); err != nil {
		t.Fatal(err)
	}
	defer idxs.Close()

	for i := 0; i < 20; i++ {
		idx, err := idxs.NewKey([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatal(err)
		}

		ki := idx.(*KeylogIndex)
		if (i % 2) == 0 {
			err = ki.Flush()
		} else {
			err = ki.Close()
		}
		if err != nil {
			t.Fatal(err)
		}

	}

	err := idxs.Iter(func(key []byte, idx hexalog.KeylogIndex) error {
		if bytes.Compare(idx.Key(), key) != 0 {
			return fmt.Errorf("mismatch %s != %s", idx.Key(), key)
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if idxs.Count() != 20 {
		t.Fatal(err)
	}
}
