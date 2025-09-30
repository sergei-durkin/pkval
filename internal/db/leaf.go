package db

import (
	"fmt"
	"unsafe"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"
)

const (
	entryLenSize = unsafe.Sizeof(uint32(0))
)

type Leaf struct {
	header

	left, right uint64
	count       uint64

	// | keys count 4b | [len | key] | .... | [value | len] |
	data [pageDataSize - 3*unsafe.Sizeof(int64(0))]byte
}

func (l *Leaf) init() {
	l.left, l.right = 0, 0
	l.count = 0
}

func (l *Leaf) Page() *Page {
	return (*Page)(unsafe.Pointer(l))
}

func (l *Leaf) Find(k key) (e entry, found bool) {
	// TODO: implement
	return entry{}, false
}

func (l *Leaf) Len() int {
	return int(l.count)
}

func (l *Leaf) Insert(k key, e entry) (err error) {
	offsets := l.getOffsets()

	var last dataOffset
	last.entry.offset = len(l.data)

	if len(offsets) > 0 {
		last = offsets[len(offsets)-1]
	}

	keyPtr := last.key.offset + last.key.len
	entryPtr := last.entry.offset

	if keyPtr+len(k) > entryPtr-len(e) {
		return errNotEnoughSpace
	}

	keyPtr = pack.Uint16(l.data[:], uint16(len(k)), keyPtr)
	copy(l.data[keyPtr:keyPtr+len(k)], k)

	entryPtr -= int(entryLenSize)
	pack.Uint32(l.data[entryPtr:], uint32(len(e)), 0)

	entryPtr -= len(e)
	copy(l.data[entryPtr:entryPtr+len(e)], e)

	l.count++
	return nil
}

func (l *Leaf) Print(id int) {
	offsets := l.getOffsets()

	fmt.Println("offsets", offsets)

	for _, o := range offsets {
		k := string(l.data[o.key.offset : o.key.offset+o.key.len])
		e := string(l.data[o.entry.offset : o.entry.offset+o.entry.len])
		fmt.Printf("ID(%d), key: %s, entry: %s\n", id, k, e)
	}
}

func (l *Leaf) Write(data []byte) (n int, err error) {
	if len(data) > len(l.data) {
		return 0, errNotEnoughSpace
	}

	return copy(l.data[:], data), nil
}

type keyOffset struct {
	len    int
	offset int
}

type entryOffset struct {
	len    int
	offset int
}

type dataOffset struct {
	key   keyOffset
	entry entryOffset
}

func (l *Leaf) getOffsets() []dataOffset {
	res := make([]dataOffset, l.count)

	keyPtr := 0
	entryPtr := len(l.data)

	for i := 0; i < int(l.count); i++ {
		var (
			lnKey   uint16
			lnEntry uint32
			o       dataOffset
		)

		lnKey, keyPtr = unpack.Uint16(l.data[:], keyPtr)
		o.key = keyOffset{len: int(lnKey), offset: keyPtr}
		keyPtr += int(lnKey)

		entryPtr -= int(entryLenSize)
		lnEntry, _ = unpack.Uint32(l.data[entryPtr:], 0)

		entryPtr -= int(lnEntry)
		o.entry = entryOffset{len: int(lnEntry), offset: entryPtr}

		res[i] = o
	}

	return res
}
