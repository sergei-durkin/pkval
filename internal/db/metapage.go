package db

import (
	"fmt"
	"unsafe"
	"wal"
	"wal/internal/pack"
	"wal/internal/unpack"
)

const (
	metaPageSize = int(unsafe.Sizeof(metaPage{}))
)

type metaPage struct {
	version           uint64
	rootPageOffset    uint64
	lsn               uint64
	freeMapPageOffset uint64
}

func newMetaPage(w wal.WriterReaderSeekerCloser, size uint64) metaPage {
	if size > 0 && size < pageSize {
		panic("file size is too small to contain a meta page")
	}

	if size == 0 {
		mp, err := createMetaPage(w)
		if err != nil {
			panic(err)
		}

		return mp
	}

	mp, err := readMetaPage(w)
	if err != nil {
		panic(err)
	}

	return mp
}

func createMetaPage(w wal.WriterReaderSeekerCloser) (metaPage, error) {
	mp := metaPage{
		version:           DB_VERSION,
		rootPageOffset:    0,
		lsn:               0,
		freeMapPageOffset: 0,
	}

	err := writeMetaPage(w, mp)
	if err != nil {
		return mp, err
	}

	return mp, nil
}

func readMetaPage(w wal.WriterReaderSeekerCloser) (metaPage, error) {
	var mp metaPage

	_, err := w.Seek(0, 0)
	if err != nil {
		return mp, err
	}

	buff := make([]byte, metaPageSize)
	n, err := w.Read(buff)
	if err != nil {
		return mp, err
	}

	if n != metaPageSize {
		return mp, fmt.Errorf("could not read full meta page, read %d bytes", n)
	}

	err = mp.Unpack(buff)
	return mp, err
}

func writeMetaPage(w wal.WriterReaderSeekerCloser, mp metaPage) error {
	_, err := w.Seek(0, 0)
	if err != nil {
		return err
	}

	buff := mp.Pack()

	n, err := w.Write(buff)
	if err != nil {
		return err
	}

	if n != metaPageSize {
		return errShortWrite
	}

	return nil
}

func (mp *metaPage) Pack() []byte {
	buff := make([]byte, metaPageSize)

	ptr := 0
	ptr = pack.Uint64(buff, mp.version, ptr)
	ptr = pack.Uint64(buff, mp.rootPageOffset, ptr)
	ptr = pack.Uint64(buff, mp.freeMapPageOffset, ptr)

	return buff
}

func (mp *metaPage) Unpack(b []byte) error {
	if len(b) != metaPageSize {
		return fmt.Errorf("invalid meta page size: %d", len(b))
	}

	ptr := 0
	mp.version, ptr = unpack.Uint64(b, ptr)
	mp.rootPageOffset, ptr = unpack.Uint64(b, ptr)
	mp.freeMapPageOffset, _ = unpack.Uint64(b, ptr)

	return nil
}
