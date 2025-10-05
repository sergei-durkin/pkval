package writer

import "wal"

type stubFile struct {
	cur   int64
	pages map[int64][]byte
}

func NewInmemory() wal.WriterReaderSeekerCloser {
	return &stubFile{
		pages: make(map[int64][]byte),
	}
}

func (s *stubFile) Seek(offset int64, _ int) (ret int64, err error) {
	s.cur = offset

	return 0, nil
}

func (s *stubFile) Read(b []byte) (n int, err error) {
	res, ok := s.pages[s.cur]
	if !ok {
		return 0, nil
	}

	return copy(b, res), nil
}

func (s *stubFile) Write(p []byte) (n int, err error) {
	s.pages[s.cur] = p

	return len(p), nil
}

func (s *stubFile) Close() error {
	return nil
}

func (s *stubFile) Sync() error {
	return nil
}
