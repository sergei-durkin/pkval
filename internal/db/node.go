package db

import (
	"sort"
	"unsafe"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"

	"github.com/sergei-durkin/armtracer"
)

const (
	nodeEntrySize = unsafe.Sizeof(uint64(0))
	nodeDataSize  = pageDataSize - 2*unsafe.Sizeof(uint64(0))
)

type Node struct {
	header

	count uint64
	less  uint64 // pageID of less child

	data [nodeDataSize]byte
}

func (n *Node) Page() *Page {
	return (*Page)(unsafe.Pointer(n))
}

func (n *Node) init() {
	n.count = 0
}

func (n *Node) Len() int {
	return int(n.count)
}

func (n *Node) Entries() []uint64 {
	offsets := n.sortedOffsets()

	res := make([]uint64, 0, len(offsets)+1)
	res = append(res, n.less)
	for i := 0; i < len(offsets); i++ {
		res = append(res, n.entryByOffset(offsets[i].entry))
	}

	return res
}

func (n *Node) Find(k Key) (next uint64, ok bool) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	prev := n.less
	offsets := n.sortedOffsets()
	for i := 0; i < len(offsets); i++ {
		o := offsets[i]

		if k.Less(n.keyByOffset(o.key)) {
			return prev, prev > 0
		}

		prev = n.entryByOffset(o.entry)
	}

	return prev, prev > 0
}

func (n *Node) Insert(k Key, e uint64) (err error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	keyPtr := int(n.head)
	entryPtr := int(len(n.data)) - int(n.tail)

	keySize := len(k) + int(keyLenSize)
	entrySize := int(nodeEntrySize)

	if keyPtr+keySize >= entryPtr-entrySize {
		return errNotEnoughSpace
	}

	n.head += uint32(keySize)
	n.tail += uint32(entrySize)

	keyPtr = writeKey(n.data[:], k, keyPtr)

	entryPtr -= entrySize
	pack.Uint64(n.data[entryPtr:], e, 0)

	n.count++
	return nil
}

func (n *Node) Write(data []byte) (cnt int, err error) {
	if len(data) > len(n.data) {
		return 0, errNotEnoughSpace
	}

	return copy(n.data[:], data), nil
}

func (src *Node) MoveAndPlace(dst *Node, k Key, e uint64) (pivot Key) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	if dst.count != 0 {
		panic("dst node is not empty")
	}

	if len(k) > maxKeySize {
		panic("key too large")
	}

	if src.count <= 2 {
		panic("inconsistent node")
	}

	offsets := src.sortedOffsets()

	mid := (len(offsets) + 1) / 2
	midOffset := offsets[mid]
	midKey := src.keyByOffset(midOffset.key)
	midEntry := src.entryByOffset(midOffset.entry)

	cmp := k.Compare(midKey)
	lt, gt, eq := cmp == -1, cmp == 1, cmp == 0

	{ // if mid == key then update entry with new val
		if eq {
			midEntry = e
		}

		pivot = append([]byte{}, midKey...)
		dst.less = midEntry
	}

	src.count = 0
	dst.count = 0

	{ // offsets(mid:len(offsets)) dst keys
		keyPtr := 0
		entryPtr := int(nodeDataSize)

		data := dst.data[:]
		for i := mid + 1; i < len(offsets); i++ {
			o := offsets[i]

			keyPtr = writeKey(data, src.keyByOffset(o.key), keyPtr)
			entryPtr = writeNodeEntry(data, src.entryByOffset(o.entry), entryPtr)
			dst.count++
		}

		if gt {
			keyPtr = writeKey(data, k, keyPtr)
			entryPtr = writeNodeEntry(data, e, entryPtr)
			dst.count++
		}

		dst.head = uint32(keyPtr)
		dst.tail = uint32(nodeDataSize) - uint32(entryPtr)
	}

	{ // offsets[0:mid) src keys
		keyPtr := 0
		entryPtr := int(nodeDataSize)

		data := make([]byte, nodeDataSize)
		for i := 0; i < mid; i++ {
			o := offsets[i]

			keyPtr = writeKey(data, src.keyByOffset(o.key), keyPtr)
			entryPtr = writeNodeEntry(data, src.entryByOffset(o.entry), entryPtr)
			src.count++
		}

		if lt {
			keyPtr = writeKey(data, k, keyPtr)
			entryPtr = writeNodeEntry(data, e, entryPtr)
			src.count++
		}

		copy(src.data[:], data)

		src.head = uint32(keyPtr)
		src.tail = uint32(nodeDataSize) - uint32(entryPtr)
	}

	return pivot
}

func (l *Node) offsets() []dataOffset {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	res := make([]dataOffset, l.count)

	keyPtr := 0
	entryPtr := len(l.data)

	for i := 0; i < int(l.count); i++ {
		var (
			lnKey uint16
			o     dataOffset
		)

		lnKey, keyPtr = unpack.Uint16(l.data[:], keyPtr)
		o.key = keyOffset{len: int(lnKey), offset: keyPtr}
		keyPtr += int(lnKey)

		entryPtr -= int(nodeEntrySize)
		o.entry = entryOffset{len: int(nodeEntrySize), offset: entryPtr}

		res[i] = o
	}

	return res
}

func (l *Node) sortedOffsets() []dataOffset {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	offsets := l.offsets()

	sort.Slice(offsets, func(i, j int) bool {
		return l.keyByOffset(offsets[i].key).Less(l.keyByOffset(offsets[j].key))
	})

	return offsets
}

func (l *Node) keyByOffset(o keyOffset) Key {
	return l.data[o.offset : o.offset+o.len]
}

func (l *Node) entryByOffset(o entryOffset) uint64 {
	res, _ := unpack.Uint64(l.data[o.offset:], 0)
	return res
}

func writeNodeEntry(dst []byte, src uint64, ptr int) int {
	ptr -= int(nodeEntrySize)
	pack.Uint64(dst[ptr:], src, 0)

	return ptr
}
