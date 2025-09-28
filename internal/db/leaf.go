package db

import "unsafe"

type Leaf [pageSize]byte

func (l *Leaf) Page() *Page {
	return (*Page)(unsafe.Pointer(l))
}
