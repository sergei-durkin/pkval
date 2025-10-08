package db

import (
	"fmt"
	"wal"

	"github.com/sergei-durkin/armtracer"
)

const (
	DB_VERSION = 1
)

type Pager struct {
	meta *Meta

	w          wal.WriterReaderSeekerCloser
	freePageID uint64
}

func NewPager(w wal.WriterReaderSeekerCloser, size uint64) (*Pager, error) {
	pg := &Pager{
		w: w,

		freePageID: max(1, size/pageSize),
	}

	{ // Initialize meta page
		page, err := pg.Read(0)
		if err != nil {
			page = NewPage(0, 0, PageTypeMeta)
			pg.Write(page)
		}

		pg.meta = page.Meta()
	}

	return pg, nil
}

func (pg *Pager) Alloc(lsn uint64, typ PageType) *Page {
	p := NewPage(pg.freePageID, lsn, typ)

	pg.freePageID++

	return p
}

func (pg *Pager) Free(p *Page) error {
	p.Free()

	return pg.Write(p)
}

func (pg *Pager) ReadRoot() (*Page, error) {
	pgm := pg.meta
	if pgm.root == 0 {
		return pg.Alloc(pg.meta.lsn, PageTypeLeaf), nil
	}

	return pg.Read(pgm.root)
}

func (pg *Pager) WriteRoot(p *Page) error {
	err := pg.Write(p)
	if err != nil {
		return err
	}

	pg.meta.root = p.ID()

	return pg.Write(pg.meta.Page())
}

func (pg *Pager) Read(id uint64) (*Page, error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	pg.w.Seek(int64(id*pageSize), 0)

	buff := make([]byte, pageSize)
	n, err := pg.w.Read(buff)
	if err != nil {
		return nil, fmt.Errorf("could not read page %d: %w", id, err)
	}

	if n != pageSize {
		return nil, fmt.Errorf("could not read full page, read %d bytes", n)
	}

	p, err := NewPageFromBytes(buff)
	if err != nil {
		return nil, fmt.Errorf("could not create page from bytes: %w", err)
	}

	if p.ID() != id {
		return nil, fmt.Errorf("page id mismatch: expected %d, got %d", id, p.ID())
	}

	if !p.Used() {
		return nil, fmt.Errorf("page %d is not used", id)
	}

	return p, nil
}

func (pg *Pager) Write(p *Page) error {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	pg.w.Seek(int64(p.ID()*pageSize), 0)

	n, err := pg.w.Write(p.Pack())
	if err != nil {
		return err
	}

	if n != pageSize {
		return errShortWrite
	}

	return nil
}

func (pg *Pager) Sync() error {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	return pg.w.Sync()
}
