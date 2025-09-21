package replay

import (
	"errors"
	"io"
	"wal"
	"wal/internal/storage"
)

type logReader struct {
	pages [storage.CountPages]storage.Page
}

func NewLogReader(reader wal.ReaderCloser) (*logReader, error) {
	pb := &logReader{}

	for i := 0; i < storage.CountPages; i++ {
		buff := make([]byte, storage.PageSize)
		n, err := reader.Read(buff)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return pb, nil
			}
			return nil, err
		}

		if n == 0 {
			return pb, nil
		}

		if n != storage.PageSize {
			return nil, err
		}

		err = pb.pages[i].FromBytes(buff)
		if err != nil {
			return nil, err
		}
	}

	return pb, nil
}

const (
	headless = 0x1
	endless  = 0x2
	full     = 0x4
)

type logItem struct {
	typ  uint8 // Type of chunk 1 = headless, 2 = endless, 3 = full
	data []byte
}

func (ch *logItem) IsHeadless() bool {
	return ch.typ&headless != 0
}

func (ch *logItem) IsEndless() bool {
	return ch.typ&endless != 0
}

func (ch *logItem) IsFull() bool {
	return ch.typ&full != 0 || (ch.typ&headless == 0 && ch.typ&endless == 0)
}

func (lr *logReader) Read() []logItem {
	var result []logItem

	for i := 0; i <= storage.CountPages; i++ {
		if lr.pages[i].Len() == 0 {
			break
		}

		pageChunk := lr.pages[i].GetSegments()
		for _, ch := range pageChunk {
			bufferChunk := logItem{}

			// Full chunk
			if !ch.IsPart() {
				bufferChunk.typ |= full
				bufferChunk.data = ch.Data
				result = append(result, bufferChunk)

				continue
			}

			// Headless chunk
			if ch.IsPart() && len(result) == 0 {
				bufferChunk.typ |= headless

				if !ch.IsEnd() {
					bufferChunk.typ |= endless
				}

				bufferChunk.data = ch.Data
				result = append(result, bufferChunk)

				continue
			}

			// Endless chunk
			if ch.IsPart() && result[len(result)-1].IsEndless() {
				result[len(result)-1].data = append(result[len(result)-1].data, ch.Data...)
				if ch.IsEnd() {
					result[len(result)-1].typ &^= endless
				}

				continue
			}

			if !ch.IsEnd() {
				bufferChunk.typ |= endless
			}
			bufferChunk.data = ch.Data
			result = append(result, bufferChunk)
		}
	}

	return result
}
