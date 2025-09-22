package db

import (
	"fmt"
	"unsafe"
	"wal"
	"wal/internal/pack"
	"wal/internal/unpack"
)

const (
	DB_VERSION = 1

	zeroPageSize = int(unsafe.Sizeof(metaPage{}))

	bufferSize = 1 << 10
)

type metaPage struct {
	version           uint64
	rootPageOffset    uint64
	freeMapPageOffset uint64
}

func (mp *metaPage) Pack() []byte {
	buff := make([]byte, zeroPageSize)

	ptr := 0
	ptr = pack.Uint64(mp.version, &buff, ptr)
	ptr = pack.Uint64(mp.rootPageOffset, &buff, ptr)
	ptr = pack.Uint64(mp.freeMapPageOffset, &buff, ptr)

	return buff
}

func (mp *metaPage) Unpack(b []byte) error {
	if len(b) != zeroPageSize {
		return fmt.Errorf("invalid meta page size: %d", len(b))
	}

	ptr := 0
	mp.version, ptr = unpack.Uint64(&b, ptr)
	mp.rootPageOffset, ptr = unpack.Uint64(&b, ptr)
	mp.freeMapPageOffset, _ = unpack.Uint64(&b, ptr)

	return nil
}

type Pager struct {
	metaPage

	w          wal.WriterReaderSeekerCloser
	lastPageID uint64
}

func NewPager(w wal.WriterReaderSeekerCloser, size uint64) (*Pager, error) {
	return &Pager{
		metaPage: newMetaPage(w, size),
		w:        w,

		lastPageID: size/pageSize - 1,
	}, nil
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

	buff := make([]byte, zeroPageSize)
	n, err := w.Read(buff)
	if err != nil {
		return mp, err
	}

	if n != zeroPageSize {
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

	if n != zeroPageSize {
		return errShortWrite
	}

	return nil
}

func (pg *Pager) Alloc(typ uint16) *Page {
	pg.lastPageID++

	return NewPage(pg.lastPageID, typ)
}

func (pg *Pager) Free(p *Page) error {
	p.used = false
	return pg.Write(p)
}

func (pg *Pager) ReadRoot() (*Page, error) {
	if pg.rootPageOffset == 0 {
		return pg.Alloc(PageTypeLeaf), nil
	}

	return pg.Read(pg.rootPageOffset)
}

func (pg *Pager) WriteRoot(p *Page) error {
	err := pg.Write(p)
	if err != nil {
		return err
	}

	pg.w.Seek(0, 0)

	pg.rootPageOffset = p.id

	buff := make([]byte, zeroPageSize)

	ptr := 0
	ptr = pack.Uint64(pg.version, &buff, ptr)
	ptr = pack.Uint64(pg.rootPageOffset, &buff, ptr)
	ptr = pack.Uint64(pg.freeMapPageOffset, &buff, ptr)

	n, err := pg.w.Write(buff)
	if err != nil {
		return err
	}

	if n != zeroPageSize {
		return errShortWrite
	}

	return nil
}

func (pg *Pager) Read(id uint64) (*Page, error) {
	pg.w.Seek(int64(id*pageSize)+int64(pageSize), 0)

	buff := make([]byte, pageSize)
	n, err := pg.w.Read(buff)
	if err != nil {
		return nil, err
	}

	if n != pageSize {
		return nil, fmt.Errorf("could not read full page, read %d bytes", n)
	}

	p, err := NewPageFromBytes(buff)
	if err != nil {
		return nil, err
	}

	if !p.used {
		return nil, fmt.Errorf("page %d is not used", id)
	}

	p.id = id

	return p, nil
}

func (pg *Pager) Write(p *Page) error {
	pg.w.Seek(int64(p.id*pageSize)+int64(pageSize), 0)
	buff := make([]byte, pageSize)

	ptr := 0
	ptr = pack.Uint16(p.typ, &buff, ptr)
	var used uint16
	if p.used {
		used = 1
	}

	ptr = pack.Uint16(used, &buff, ptr)
	ptr = pack.Uint64(p.id, &buff, ptr)

	copy(buff[headerSize:], p.data[:])

	n, err := pg.w.Write(buff)
	if err != nil {
		return err
	}

	if n != pageSize {
		return errShortWrite
	}

	return nil
}

func (pg *Pager) Sync() error {
	return pg.w.Sync()
}
