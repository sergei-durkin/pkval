package db

import (
	"sort"
	"unsafe"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"

	"github.com/sergei-durkin/armtracer"
)

const (
	keyLenSize   = unsafe.Sizeof(uint16(0))
	entryLenSize = unsafe.Sizeof(uint32(0))
	leafDataSize = pageDataSize - 3*unsafe.Sizeof(int64(0))
)

type Leaf struct {
	header

	left, right uint64
	count       uint64

	// | keys count 4b | [len | key] | .... | [value | len] |
	data [leafDataSize]byte
}

func (l *Leaf) init() {
	l.left, l.right = 0, 0
	l.count = 0
}

func (l *Leaf) Page() *Page {
	return (*Page)(unsafe.Pointer(l))
}

func (l *Leaf) Find(k Key) (e Entry, found bool) {
	// TODO: implement
	return Entry{}, false
}

func (l *Leaf) Len() int {
	return int(l.count)
}

func (l *Leaf) Insert(k Key, e Entry) (err error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	keyPtr := int(l.head)
	entryPtr := int(leafDataSize) - int(l.tail)

	keySize := len(k) + int(keyLenSize)
	entrySize := len(e) + int(entryLenSize)

	if keyPtr+keySize >= entryPtr-entrySize {
		return errNotEnoughSpace
	}

	l.head += uint32(keySize)
	l.tail += uint32(entrySize)

	keyPtr = writeKey(l.data[:], k, keyPtr)

	entryPtr -= int(entryLenSize)
	pack.Uint32(l.data[entryPtr:], uint32(len(e)), 0)

	entryPtr -= len(e)
	copy(l.data[entryPtr:entryPtr+len(e)], e)

	l.count++
	return nil
}

func (l *Leaf) Write(data []byte) (n int, err error) {
	if len(data) > len(l.data) {
		return 0, errNotEnoughSpace
	}

	return copy(l.data[:], data), nil
}

func (src *Leaf) MoveHalf(dst *Leaf) (pivot Key) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	if dst.count != 0 {
		panic("dst leaf is not empty")
	}

	if src.count == 1 {
		return nil
	}

	// anyLess->src->anyGreater => anyLess->src->dst->anyGreater
	dst.right = src.right
	src.right = dst.id

	offsets := src.sortedOffsets()
	mid := (len(offsets) + 1) / 2

	src.count = uint64(mid)
	dst.count = uint64(len(offsets)) - src.count - 1

	{ // offsets[mid:len(offsets)) dst keys
		keyPtr := 0
		entryPtr := int(leafDataSize)

		data := dst.data[:]
		for i := mid; i < len(offsets); i++ {
			o := offsets[i]

			keyPtr = writeKey(data, src.keyByOffset(o.key), keyPtr)
			entryPtr = writeEntry(data, src.entryByOffset(o.entry), entryPtr)

			if i == mid {
				pivot = data[keyPtr-o.key.len : keyPtr]
			}
		}

		dst.head = uint32(keyPtr)
		dst.tail = uint32(leafDataSize) - uint32(entryPtr)
	}

	{ // offsets[0:mid) src keys
		keyPtr := 0
		entryPtr := int(leafDataSize)

		data := make([]byte, leafDataSize)
		for i := 0; i < mid; i++ {
			o := offsets[i]

			keyPtr = writeKey(data, src.keyByOffset(o.key), keyPtr)
			entryPtr = writeEntry(data, src.entryByOffset(o.entry), entryPtr)
		}

		copy(src.data[:], data)

		src.head = uint32(keyPtr)
		src.tail = uint32(leafDataSize) - uint32(entryPtr)
	}

	return pivot
}

func (l *Leaf) offsets() []dataOffset {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

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

func (l *Leaf) sortedOffsets() []dataOffset {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	offsets := l.offsets()

	sort.Slice(offsets, func(i, j int) bool {
		return l.keyByOffset(offsets[i].key).Less(l.keyByOffset(offsets[j].key))
	})

	return offsets
}

func (l *Leaf) keyByOffset(o keyOffset) Key {
	return l.data[o.offset : o.offset+o.len]
}

func (l *Leaf) entryByOffset(o entryOffset) Entry {
	return l.data[o.offset : o.offset+o.len]
}

func writeKey(dst []byte, src []byte, ptr int) int {
	ln := len(src)

	ptr = pack.Uint16(dst, uint16(ln), ptr)
	ptr += copy(dst[ptr:ptr+ln], src)

	return ptr
}

func writeEntry(dst, src []byte, ptr int) int {
	ln := len(src)

	ptr -= int(entryLenSize)
	pack.Uint32(dst[ptr:], uint32(ln), 0)

	ptr -= ln
	copy(dst[ptr:ptr+ln], src)

	return ptr
}
