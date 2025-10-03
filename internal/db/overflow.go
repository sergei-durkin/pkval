package db

import "unsafe"

type Overflow struct {
	header

	next uint64
	len  uint32

	data [pageDataSize - unsafe.Sizeof(uint64(0)) - unsafe.Sizeof(uint32(0))]byte
}

func (o *Overflow) Page() *Page {
	return (*Page)(unsafe.Pointer(o))
}

func (o *Overflow) Write(data []byte) (n int, _ error) {
	n = copy(o.data[:], data)
	o.len = uint32(n)

	return n, nil
}

func (o *Overflow) Data() []byte {
	if o.len == 0 {
		return nil
	}

	return o.data[:o.len]
}
