package wal

type WriterCloser interface {
	Write([]byte) (n int, err error)
	Sync() error
	Close() error
}

type ReaderCloser interface {
	Read([]byte) (n int, err error)
	Close() error
}
