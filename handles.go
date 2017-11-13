package hexaboltdb

import (
	"errors"
	"sync"
	"time"

	"github.com/hexablock/hexatype"
	"github.com/hexablock/log"
)

var (
	errIndexOpen = errors.New("KeylogIndex is open")
)

type indexHandle struct {
	cnt      int
	lastUsed time.Time
	*KeylogIndex
}

type openIndexes struct {
	mu sync.RWMutex
	m  map[string]*indexHandle
	// Interval for flush loop
	flushInt time.Duration
	// Time to wait after lastChanged before issuing a flush
	flushWait time.Duration

	shutdown chan struct{}
	stopped  chan struct{}
}

func newOpenIndexes() *openIndexes {
	oi := &openIndexes{
		m:         make(map[string]*indexHandle),
		flushInt:  60 * time.Second,
		flushWait: 15 * time.Second,
		shutdown:  make(chan struct{}, 1),
		stopped:   make(chan struct{}, 1),
	}
	go oi.flush()

	return oi
}

func (oi *openIndexes) flush() {
	for {
		select {
		case <-time.After(oi.flushInt):
			oi.flushOnce()
		case <-oi.shutdown:
			oi.stopped <- struct{}{}
			//log.Println("[DEBUG] Open indexes stopped!")
			return
		}
	}
}

func (oi *openIndexes) flushOnce() {
	var err error

	oi.mu.Lock()
	for k, v := range oi.m {
		// Skip recently used ones
		if time.Since(v.lastUsed) <= oi.flushWait {
			continue
		}

		// Flush
		if err = v.Flush(); err != nil {
			log.Printf("[ERROR] Flush error: %s", err)
			continue
		}
		// Close/remove index handle
		if v.cnt == 0 {
			//log.Printf("[DEBUG] Closing handle key=%s", v.Key())
			delete(oi.m, k)
		}
	}
	oi.mu.Unlock()

}

func (oi *openIndexes) closeAll() error {
	var err error

	oi.shutdown <- struct{}{}
	<-oi.stopped

	oi.mu.Lock()
	for _, v := range oi.m {
		if er := v.Flush(); er != nil {
			err = er
		}
	}
	oi.m = nil
	oi.mu.Unlock()

	return err
}

// remove is used to remove a key handle and its data directly from memory
// without flushing its contents
func (oi *openIndexes) remove(key []byte) error {
	k := string(key)

	oi.mu.Lock()
	defer oi.mu.Unlock()

	ih, ok := oi.m[k]
	if !ok {
		return hexatype.ErrKeyNotFound
	}

	if ih.cnt > 0 {
		return errIndexOpen
	}

	// mark for deletion
	delete(oi.m, k)
	return nil
}

func (oi *openIndexes) close(key []byte) error {
	k := string(key)

	oi.mu.Lock()
	defer oi.mu.Unlock()

	val, ok := oi.m[k]
	if !ok {
		return hexatype.ErrKeyNotFound
	}

	val.cnt--
	val.lastUsed = time.Now()
	return nil
}

// get an open index and up the ref count
func (oi *openIndexes) get(key []byte) (*indexHandle, bool) {
	oi.mu.RLock()
	ih, ok := oi.m[string(key)]
	if !ok {
		oi.mu.RUnlock()
		return nil, false
		//log.Printf("[DEBUG] Index handles action=open key=%s count=%d", key, ih.cnt)
	}
	oi.mu.RUnlock()

	//log.Printf("[DEBUG] Handle opened: %s", key)

	oi.mu.Lock()
	defer oi.mu.Unlock()
	ih.cnt++

	return ih, ok
}

func (oi *openIndexes) count() int {
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	return len(oi.m)
}

// isOpen returns true if the index handle exists.  It returns true even though
// the count may be zero as the data may not have been flushed.
func (oi *openIndexes) isOpen(key []byte) (int, bool) {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	ih, ok := oi.m[string(key)]
	if ok {
		return ih.cnt, true
	}
	return 0, false
}

func (oi *openIndexes) register(kli *KeylogIndex) {
	oi.mu.Lock()
	oi.m[string(kli.Key())] = &indexHandle{cnt: 1, KeylogIndex: kli, lastUsed: time.Now()}
	oi.mu.Unlock()
}
