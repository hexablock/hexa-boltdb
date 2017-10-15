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
}

func newOpenIndexes() *openIndexes {
	oi := &openIndexes{
		m:         make(map[string]*indexHandle),
		flushInt:  60 * time.Second,
		flushWait: 15 * time.Second,
	}
	go oi.flush()
	return oi
}

func (oi *openIndexes) flush() {
	for {
		time.Sleep(oi.flushInt)
		oi.flushOnce()
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
			log.Printf("[DEBUG] Closing handle key=%s", v.Key())
			delete(oi.m, k)
		}
	}
	oi.mu.Unlock()
}

func (oi *openIndexes) closeAll() error {
	var err error
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

func (oi *openIndexes) close(key []byte) error {
	k := string(key)

	oi.mu.Lock()
	defer oi.mu.Unlock()

	if val, ok := oi.m[k]; ok {
		val.cnt--
		val.lastUsed = time.Now()
		return nil
	}
	return hexatype.ErrKeyNotFound
}

// get an open index and up the ref count
func (oi *openIndexes) get(key []byte) (*indexHandle, bool) {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	ih, ok := oi.m[string(key)]
	if ok {
		ih.cnt++
		//log.Printf("[DEBUG] Index handles action=open key=%s count=%d", key, ih.cnt)
	}

	return ih, ok
}

func (oi *openIndexes) count() int {
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	return len(oi.m)
}

func (oi *openIndexes) isOpen(key []byte) bool {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	_, ok := oi.m[string(key)]
	return ok
}

func (oi *openIndexes) register(kli *KeylogIndex) {
	oi.mu.Lock()
	oi.m[string(kli.Key())] = &indexHandle{cnt: 1, KeylogIndex: kli, lastUsed: time.Now()}
	oi.mu.Unlock()
}
