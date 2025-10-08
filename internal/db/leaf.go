package db

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"

	"github.com/sergei-durkin/armtracer"
)

const (
	leafDataSize = pageDataSize - 3*unsafe.Sizeof(int64(0))

	keyLenSize   = unsafe.Sizeof(uint16(0))
	entryLenSize = unsafe.Sizeof(uint32(0))

	maxKeySize   = 1 << 10
	maxEntrySize = (leafDataSize-2*entryLenSize)/2 - (maxKeySize + keyLenSize)
)

func init() {
	if maxEntrySize <= 0 {
		panic("page size too small")
	}
}

type Leaf struct {
	header

	left, right uint64
	count       uint64

	// | l,r,count | [len | key] | .... | [value | len] |
	data [leafDataSize]byte
}

func (l *Leaf) init() {
	l.left, l.right = 0, 0
	l.count = 0
}

func (l *Leaf) Page() *Page {
	return (*Page)(unsafe.Pointer(l))
}

func (l *Leaf) Find(k Key) (e Entry) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	o, ok := l.find(k)
	if !ok {
		return nil
	}

	return l.entryByOffset(o.entry)
}

func (l *Leaf) find(k Key) (o dataOffset, ok bool) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	offsets := l.offsets()
	for i := 0; i < len(offsets); i++ {
		o := offsets[i]

		if k.Compare(l.keyByOffset(o.key)) == 0 {
			return o, true
		}
	}

	return dataOffset{}, false
}

func (l *Leaf) Len() int {
	return int(l.count)
}

func (l *Leaf) Insert(k Key, e Entry) (err error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	if l.count >= maxDegree {
		return errNotEnoughSpace
	}

	if len(k) > int(maxKeySize) {
		panic("key too big")
	}

	if len(e) > int(maxEntrySize) {
		panic("entry too big")
	}

	keyPtr := int(l.head)
	entryPtr := int(leafDataSize) - int(l.tail)

	var h, t uint32
	{ // check overflow
		keySize := len(k) + int(keyLenSize)
		entrySize := len(e) + int(entryLenSize)

		h, t = l.head+uint32(keySize), l.tail+uint32(entrySize)

		if h+t > uint32(leafDataSize) {
			return errNotEnoughSpace
		}
	}

	l.head = h
	l.tail = t

	keyPtr = writeKey(l.data[:], k, keyPtr)
	entryPtr = writeLeafEntry(l.data[:], e, entryPtr)

	l.count++
	return nil
}

func (l *Leaf) Update(k Key, e Entry) (err error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	if len(e) > int(maxEntrySize) {
		panic("entry too big")
	}

	var (
		offset dataOffset
		ok     bool
	)

	offsets := l.offsets()
	for i := 0; i < len(offsets); i++ {
		o := offsets[i]

		if k.Compare(l.keyByOffset(o.key)) == 0 {
			offset = offsets[i]
			ok = true

			// remove key
			offsets[i], offsets[len(offsets)-1] = offsets[len(offsets)-1], offsets[i]
			offsets = offsets[:len(offsets)-1]

			break
		}
	}
	if !ok {
		return errNotFound
	}

	{ // check overflow
		tail := l.tail - uint32(offset.entry.len)
		tail = l.tail + uint32(len(e))

		if l.head+tail > uint32(leafDataSize) {
			return errNotEnoughSpace
		}
	}

	data := make([]byte, leafDataSize)

	// write as new entry
	keyPtr := writeKey(data, k, 0)
	entryPtr := writeLeafEntry(data, e, int(leafDataSize))

	for i := 0; i < len(offsets); i++ {
		o := offsets[i]

		keyPtr = writeKey(data, l.keyByOffset(o.key), keyPtr)
		entryPtr = writeLeafEntry(data, l.entryByOffset(o.entry), entryPtr)
	}

	copy(l.data[:], data)

	l.tail = uint32(leafDataSize) - uint32(entryPtr)

	return nil
}

func (l *Leaf) Write(data []byte) (n int, err error) {
	if len(data) > len(l.data) {
		return 0, errNotEnoughSpace
	}

	return copy(l.data[:], data), nil
}

func (src *Leaf) MoveAndPlace(dst *Leaf, k Key, e Entry) (pivot Key) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	if dst.count != 0 {
		panic("dst leaf is not empty")
	}

	if src.count == 1 {
		panic("inconsistent leaf page")
	}

	// anyLess<->src<->anyGreater => anyLess<->src<->dst<->anyGreater
	dst.right = src.right
	src.right = dst.id
	dst.left = src.id

	offsets := src.sortedOffsets()

	mid := (len(offsets) + 1) / 2
	midOffset := offsets[mid]
	midKey := src.keyByOffset(midOffset.key)
	midEntry := src.entryByOffset(midOffset.entry)

	cmp := k.Compare(midKey)
	lt, gt, eq := cmp == -1, cmp == 1, cmp == 0
	if eq {
		// update entry
		midEntry = e
	}

	src.count = 0
	dst.count = 1 // mid entry

	{ // offsets[mid:len(offsets)) dst keys
		keyPtr := 0
		entryPtr := int(leafDataSize)

		data := dst.data[:]

		keyPtr = writeKey(data, midKey, keyPtr)
		entryPtr = writeLeafEntry(data, midEntry, entryPtr)

		pivot = data[keyPtr-len(midKey) : keyPtr]

		for i := mid + 1; i < len(offsets); i++ {
			o := offsets[i]

			keyPtr = writeKey(data, src.keyByOffset(o.key), keyPtr)
			entryPtr = writeLeafEntry(data, src.entryByOffset(o.entry), entryPtr)
			dst.count++
		}

		// insert
		if gt {
			keyPtr = writeKey(data, k, keyPtr)
			entryPtr = writeLeafEntry(data, e, entryPtr)
			dst.count++
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
			entryPtr = writeLeafEntry(data, src.entryByOffset(o.entry), entryPtr)
			src.count++
		}

		// insert
		if lt {
			keyPtr = writeKey(data, k, keyPtr)
			entryPtr = writeLeafEntry(data, e, entryPtr)
			src.count++
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

func (l *Leaf) Print(level []byte) {
	offsets := l.sortedOffsets()

	for _, o := range offsets {
		k := string(l.data[o.key.offset : o.key.offset+o.key.len])
		e := Entry(l.data[o.entry.offset : o.entry.offset+o.entry.len])

		fmt.Fprintf(os.Stderr, "%s key: %s, entry: %s\n", level, k, e.Format())
	}

}

func writeLeafEntry(dst, src []byte, ptr int) int {
	ln := len(src)

	ptr -= int(entryLenSize)
	pack.Uint32(dst[ptr:], uint32(ln), 0)

	ptr -= ln
	copy(dst[ptr:ptr+ln], src)

	return ptr
}
