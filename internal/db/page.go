package db

import (
	"fmt"
	"unsafe"
)

const (
	pageSize = 1 << 13

	headerSize   = unsafe.Sizeof(header{})
	pageDataSize = pageSize - headerSize
)

const (
	PageTypeMeta     uint16 = 1
	PageTypeLeaf     uint16 = 2
	PageTypeNode     uint16 = 3
	PageTypeOverflow uint16 = 4
)

const (
	magicNumber uint16 = 0xABCD
)

type header struct {
	id    uint64
	lsn   uint64
	typ   uint16
	magic uint16
	used  bool

	_ [40]byte // padding
}

type Page [pageSize]byte

func (p *Page) Header() *header {
	return (*header)(unsafe.Pointer(p))
}

func NewPageFromBytes(b []byte) (*Page, error) {
	if len(b) != pageSize {
		return nil, fmt.Errorf("invalid page size: %d", len(b))
	}

	p := (*Page)(unsafe.Pointer(&b[0]))
	if p.Header().magic != magicNumber {
		return nil, fmt.Errorf("invalid magic number: %x", p.Header().magic)
	}

	return p, nil
}

func NewPage(id uint64, lsn uint64, typ uint16) *Page {
	var p Page

	h := p.Header()
	h.id = id
	h.lsn = lsn
	h.typ = typ
	h.magic = magicNumber
	h.used = true

	return &p
}

func (p *Page) Type() uint16 {
	return p.Header().typ
}

func (p *Page) ID() uint64 {
	return p.Header().id
}

func (p *Page) Used() bool {
	return p.Header().used
}

func (p *Page) Free() {
	p.Header().used = false
}

func (p *Page) Write(data []byte) (int, error) {
	if len(data) > len(p) {
		return 0, fmt.Errorf("data too large for page: %d > %d", len(data), len(p))
	}

	n := copy(p[headerSize:], data)

	return n, nil
}

func (p *Page) Pack() []byte {
	return p[:]
}

func (p *Page) Meta() *Meta {
	h := p.Header()
	if h.typ != PageTypeMeta {
		panic(fmt.Sprintf("page is not a meta: %d", h.typ))
	}

	return (*Meta)(unsafe.Pointer(p))
}

func (p *Page) Node() *Node {
	h := p.Header()
	if h.typ != PageTypeNode {
		panic(fmt.Sprintf("page is not a node: %d", h.typ))
	}

	return (*Node)(unsafe.Pointer(p))
}

func (p *Page) Leaf() *Leaf {
	h := p.Header()
	if h.typ != PageTypeLeaf {
		panic(fmt.Sprintf("page is not a leaf: %d", h.typ))
	}

	return (*Leaf)(unsafe.Pointer(p))
}

func (p *Page) Overflow() *Overflow {
	h := p.Header()
	if h.typ != PageTypeOverflow {
		panic(fmt.Sprintf("page is not an overflow: %d", h.typ))
	}

	return (*Overflow)(unsafe.Pointer(p))
}
