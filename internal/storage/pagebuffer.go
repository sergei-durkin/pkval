package storage

import (
	"context"
	"sync"
	"time"
	"wal"
)

const (
	PageBufferSize = 1 << 20
	CountPages     = PageBufferSize / PageSize
)

type PageBuffer struct {
	cur   int
	pages [CountPages]Page
	dirty [CountPages]bool
	w     wal.WriterCloser

	bufferPool sync.Pool
	newFile    func() (wal.WriterCloser, error)

	mu sync.Mutex
}

func NewPageBuffer(ctx context.Context, syncInterval time.Duration, writerProvider func() (wal.WriterCloser, error)) (*PageBuffer, error) {
	w, err := writerProvider()
	if err != nil {
		return nil, err
	}

	pageBuffer := &PageBuffer{
		cur:   0,
		pages: [CountPages]Page{},
		w:     w,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, PageSize)
			},
		},
		newFile: writerProvider,
	}

	go func(pb *PageBuffer, interval time.Duration) {
		ticker := time.NewTicker(interval)

		// Count of unsuccessful sync attempts
		// If we fail to sync 3 times in a row, we force a sync
		// This is to prevent data loss in case of high write load
		unsuccessfulSyncs := 0

		for {
			select {
			case <-ticker.C:
				if pb.mu.TryLock() {
					pb.sync()
					pb.mu.Unlock()
					continue
				}

				unsuccessfulSyncs++
				if unsuccessfulSyncs >= 3 {
					pb.mu.Lock()
					pb.sync()
					pb.mu.Unlock()
				}

			case <-ctx.Done():
				pb.mu.Lock()
				pb.w.Close()
				pb.mu.Unlock()

				return
			}
		}

	}(pageBuffer, syncInterval)

	return pageBuffer, nil
}

// Write concurrent writes data to the disk
func (pb *PageBuffer) Write(data []byte) (err error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if len(data)+int(metaSize) < int(PageDataSize) {
		// If current page has space, write to it
		err = pb.write(data, -1)
		if nil == err {
			return nil
		} else if err != errTooLarge {
			return err
		}
	}

	// If data is larger than a page, split it into segments
	for len(data) > 0 {
		psize := min(len(data), int(PageDataSize-metaSize))
		segment := data[0:psize]
		data = data[psize:]

		err = pb.write(segment, int32(len(data)))
		if err != nil {
			return err
		}
	}

	return nil
}

// sync writes all unsynced pages to the disk
func (pb *PageBuffer) sync() {
	ln := min(pb.cur, CountPages-1)

	buff := pb.bufferPool.Get().([]byte)
	defer pb.bufferPool.Put(buff)

	for i := 0; i <= ln; i++ {
		if !pb.dirty[i] {
			continue
		}

		if pb.pages[i].Len() == 0 {
			break
		}

		pb.dirty[i] = false

		n, err := pb.w.Write(pb.pages[i].Pack())
		if err != nil {
			panic(err)
		}

		if n != len(buff) {
			panic(errShortWrite)
		}
	}

	err := pb.w.Sync()
	if err != nil {
		panic(err)
	}
}

// reset resets the page buffer to its initial state
func (pb *PageBuffer) reset() {
	pb.cur = 0
	for i := range pb.pages {
		pb.pages[i].Reset()
		pb.dirty[i] = false
	}

	err := pb.w.Close()
	if err != nil {
		panic(err)
	}

	pb.w, err = pb.newFile()
	if err != nil {
		panic(err)
	}
}

// write writes data to the current page, if the current page is full, it moves to the next page
func (pb *PageBuffer) write(data []byte, remaining int32) error {
	if !pb.dirty[pb.cur] || !pb.pages[pb.cur].HasSpace(uint32(len(data))) {
		pb.cur++
		if pb.cur >= CountPages {
			pb.sync() // If no more pages, force sync to disk and reset
			pb.reset()
		}
	}

	n, err := pb.pages[pb.cur].Write(data, remaining)
	if err != nil {
		return err
	}

	if n != len(data) {
		panic(errShortWrite)
	}

	pb.dirty[pb.cur] = true

	return nil
}
