package db

import "unsafe"

type Node [pageSize]byte

func (n *Node) Page() *Page {
	return (*Page)(unsafe.Pointer(n))
}
