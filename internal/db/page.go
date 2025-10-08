package db

import (
	"fmt"
	"unsafe"

	"github.com/sergei-durkin/armtracer"
)

const (
	pageSize  = 1 << 13
	maxDegree = 1 << 4

	headerSize   = unsafe.Sizeof(header{})
	pageDataSize = pageSize - headerSize
)

func init() {
	if maxDegree < 3 {
		panic(fmt.Errorf("maxDegree should be >= 3, actual %d", maxDegree))
	}

	p := len(Page{})
	l := len((&Leaf{}).Page())
	n := len((&Node{}).Page())
	o := len((&Overflow{}).Page())
	m := len((&Meta{}).Page())

	if p != l || p != n || p != o || p != m {
		panic(fmt.Errorf("Pages has inconsistent sizes: Page = %d, Leaf = %d, Node = %d, Overflow = %d, Meta = %d", p, l, n, o, m))
	}
}

type PageType uint16

const (
	PageTypeMeta     PageType = 1
	PageTypeLeaf     PageType = 2
	PageTypeNode     PageType = 3
	PageTypeOverflow PageType = 4
)

const (
	magicNumber uint16 = 0xABCD
)

type header struct {
	id  uint64
	lsn uint64

	head uint32
	tail uint32

	typ   PageType
	magic uint16
	used  bool

	_ [32]byte // padding
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

func NewPage(id uint64, lsn uint64, typ PageType) *Page {
	var p Page

	p.init(id, lsn, typ)

	return &p
}

func (p *Page) init(id uint64, lsn uint64, typ PageType) {
	h := p.Header()
	h.id = id
	h.lsn = lsn

	h.head = 0
	h.tail = 0

	h.typ = typ
	h.magic = magicNumber
	h.used = true

	switch typ {
	default:
		panic(fmt.Sprintf("unexpected page type: %d", typ))
	case PageTypeMeta:
		p.Meta().init()
	case PageTypeLeaf:
		p.Leaf().init()
	case PageTypeNode:
		p.Node().init()
	case PageTypeOverflow:
		// p.Overflow().init()
	}
}

func (p *Page) Type() PageType {
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

	return p.Leaf().Write(data)
}

func (p *Page) Pack() []byte {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	return p[:]
}

func (p *Page) IsMeta() bool {
	return p.Header().typ == PageTypeMeta
}

func (p *Page) Meta() *Meta {
	h := p.Header()
	if h.typ != PageTypeMeta {
		panic(fmt.Sprintf("page is not a meta: %d", h.typ))
	}

	return (*Meta)(unsafe.Pointer(p))
}

func (p *Page) IsNode() bool {
	return p.Header().typ == PageTypeNode
}

func (p *Page) Node() *Node {
	h := p.Header()
	if h.typ != PageTypeNode {
		panic(fmt.Sprintf("page is not a node: %d", h.typ))
	}

	return (*Node)(unsafe.Pointer(p))
}

func (p *Page) IsLeaf() bool {
	return p.Header().typ == PageTypeLeaf
}

func (p *Page) Leaf() *Leaf {
	h := p.Header()
	if h.typ != PageTypeLeaf {
		panic(fmt.Sprintf("page is not a leaf: %d", h.typ))
	}

	return (*Leaf)(unsafe.Pointer(p))
}

func (p *Page) IsOverflow() bool {
	return p.Header().typ == PageTypeOverflow
}

func (p *Page) Overflow() *Overflow {
	h := p.Header()
	if h.typ != PageTypeOverflow {
		panic(fmt.Sprintf("page is not an overflow: %d", h.typ))
	}

	return (*Overflow)(unsafe.Pointer(p))
}
