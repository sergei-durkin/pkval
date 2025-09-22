package wal

type Closer interface {
	Close() error
}

type WriterCloser interface {
	Write([]byte) (n int, err error)
	Sync() error

	Closer
}

type ReaderCloser interface {
	Read([]byte) (n int, err error)

	Closer
}

type Seeker interface {
	Seek(offset int64, whence int) (ret int64, err error)
}

type WriterReaderSeekerCloser interface {
	WriterCloser
	ReaderCloser
	Seeker
}
