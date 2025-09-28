package db

import "unsafe"

type Overflow [pageSize]byte

func (o *Overflow) Page() *Page {
	return (*Page)(unsafe.Pointer(o))
}
