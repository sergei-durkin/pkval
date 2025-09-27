package db

import (
	"fmt"
	"unsafe"
	"wal/internal/pack"
	"wal/internal/unpack"
)

const (
	pageSize = 1 << 13

	headerSize   = int(unsafe.Sizeof(header{}))
	pageDataSize = pageSize - headerSize
)

const (
	PageTypeLeaf     uint16 = 1
	PageTypeNode     uint16 = 2
	PageTypeOverflow uint16 = 3
)

type header struct {
	id   uint64
	lsn  uint64
	typ  uint16
	used bool

	_ [48]byte // padding
}

type Page struct {
	header

	data [pageDataSize]byte
}

func NewPageFromBytes(b []byte) (*Page, error) {
	if len(b) != pageSize {
		return nil, fmt.Errorf("invalid page size: %d", len(b))
	}

	p := &Page{}

	ptr := 0
	p.id, ptr = unpack.Uint64(b, ptr)
	p.lsn, ptr = unpack.Uint64(b, ptr)
	p.typ, ptr = unpack.Uint16(b, ptr)

	var used uint16
	used, ptr = unpack.Uint16(b, ptr)
	p.used = used != 0

	copy(p.data[:], b[headerSize:])

	return p, nil
}

func NewPage(id uint64, lsn uint64, typ uint16) *Page {
	return &Page{
		header: header{
			id:   id,
			lsn:  lsn,
			typ:  typ,
			used: true,
		},
	}
}

func (p *Page) Write(data []byte) (int, error) {
	if len(data) > len(p.data) {
		return 0, fmt.Errorf("data too large for page: %d > %d", len(data), len(p.data))
	}

	n := copy(p.data[:], data)

	return n, nil
}

func (p *Page) Pack() []byte {
	buff := make([]byte, pageSize)

	ptr := 0
	ptr = pack.Uint64(buff, p.id, ptr)
	ptr = pack.Uint64(buff, p.lsn, ptr)
	ptr = pack.Uint16(buff, p.typ, ptr)

	var used uint16
	if p.used {
		used = 1
	}

	ptr = pack.Uint16(buff, used, ptr)

	copy(buff[headerSize:], p.data[:])

	return buff
}
