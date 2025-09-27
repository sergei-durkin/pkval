package db

import (
	"fmt"
	"wal"
)

const (
	DB_VERSION = 1
)

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

func (pg *Pager) Alloc(lsn uint64, typ uint16) *Page {
	pg.lastPageID++

	return NewPage(pg.lastPageID, lsn, typ)
}

func (pg *Pager) Free(p *Page) error {
	p.used = false

	return pg.Write(p)
}

func (pg *Pager) ReadRoot() (*Page, error) {
	if pg.rootPageOffset == 0 {
		return pg.Alloc(pg.lsn, PageTypeLeaf), nil
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

	buff := pg.Pack()
	n, err := pg.w.Write(buff)
	if err != nil {
		return err
	}

	if n != metaPageSize {
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

	buff := p.Pack()
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
