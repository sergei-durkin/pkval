package db

import (
	"fmt"
	"unsafe"
	"wal/internal/unpack"
)

const (
	pageSize = 1 << 13

	headerSize   = int(unsafe.Sizeof(head{}))
	pageDataSize = pageSize - headerSize
)

const (
	PageTypeLeaf     uint16 = 1
	PageTypeNode     uint16 = 2
	PageTypeOverflow uint16 = 3
)

type head struct {
	typ  uint16
	used bool
}

type Page struct {
	head

	id   uint64
	data [pageDataSize]byte
}

func NewPageFromBytes(b []byte) (*Page, error) {
	if len(b) != pageSize {
		return nil, fmt.Errorf("invalid page size: %d", len(b))
	}

	p := &Page{}

	ptr := 0
	p.typ, ptr = unpack.Uint16(&b, ptr)

	var used uint16
	used, ptr = unpack.Uint16(&b, ptr)

	p.used = used != 0
	p.id, ptr = unpack.Uint64(&b, ptr)

	copy(p.data[:], b[headerSize:])

	return p, nil
}

func NewPage(id uint64, typ uint16) *Page {
	return &Page{
		head: head{
			typ:  typ,
			used: true,
		},

		id: id,
	}
}

func (p *Page) Write(data []byte) (int, error) {
	if len(data) > len(p.data) {
		return 0, fmt.Errorf("data too large for page: %d > %d", len(data), len(p.data))
	}

	n := copy(p.data[:], data)

	return n, nil
}
