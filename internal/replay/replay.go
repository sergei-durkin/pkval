package replay

import (
	"fmt"
	"wal"
	"wal/internal/log"
)

type Replay struct {
	readers []wal.ReaderCloser
}

func NewReplay(readers []wal.ReaderCloser) *Replay {
	return &Replay{
		readers: readers,
	}
}

func (r *Replay) Replay() ([]log.Entry, error) {
	res := []log.Entry{}

	var endless *logItem

	for len(r.readers) > 0 {
		lr, err := NewLogReader(r.readers[0])
		if err != nil {
			panic(err)
		}

		r.readers[0].Close()
		r.readers = r.readers[1:]

		if lr == nil {
			break
		}

		chs := lr.Read()
		for i := 0; i < len(chs); i++ {
			if chs[i].IsFull() {
				res = append(res, log.NewFromBytes(chs[i].data))

				continue
			}

			if chs[i].IsHeadless() {
				if endless != nil {
					endless.data = append(endless.data, chs[i].data...)
					if !chs[i].IsEndless() {
						res = append(res, log.NewFromBytes(endless.data))
						endless = nil
					}

					continue
				}

				panic("Headless chunk without endless chunk, something is wrong")
			}

			if chs[i].IsEndless() {
				if endless == nil {
					endless = &logItem{
						typ:  chs[i].typ,
						data: chs[i].data,
					}

					continue
				}
				endless.data = append(endless.data, chs[i].data...)

				continue
			}
		}
	}

	if endless != nil {
		panic(fmt.Sprintf("Endless chunk without end, something is wrong, len=%d", len(endless.data)))
	}

	cp := 0
	for i := len(res) - 1; i >= 0; i-- {
		if res[i].Type() == log.CheckpointEntry {
			cp = i
			break
		}
	}

	return res[cp:], nil
}
