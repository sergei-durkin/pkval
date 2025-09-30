package db

import (
	"unsafe"
)

type Meta struct {
	header

	magic   uint64
	version uint64
	root    uint64
	freeMap uint64

	_ [pageDataSize - 4*unsafe.Sizeof(int64(0))]byte
}

func (m *Meta) Page() *Page {
	return (*Page)(unsafe.Pointer(m))
}

func (m *Meta) init() {
	m.version = DB_VERSION
	m.root = 0
	m.freeMap = 0
}
