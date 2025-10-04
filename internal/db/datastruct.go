package db

import (
	"fmt"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"
)

type Key []byte
type Entry []byte

type entryType uint8

const (
	entryTypeData     entryType = 1
	entryTypeOverflow entryType = 2
)

func NewOverflowEntry(next uint64) (e Entry) {
	e = make([]byte, 9)
	_ = pack.Uint64(e, next, 1)

	e[0] = byte(entryTypeOverflow)

	return e
}

func NewDataEntry(data []byte) (e Entry) {
	e = make([]byte, len(data)+1)
	copy(e[1:], data)

	e[0] = byte(entryTypeData)

	return e
}

func (e *Entry) GetData() []byte {
	t := e.Type()
	if t != entryTypeData {
		panic(fmt.Sprintf("entry is not a data: %d", t))
	}

	return (*e)[1:]
}

func (e *Entry) GetNext() uint64 {
	t := e.Type()
	if t != entryTypeOverflow {
		panic(fmt.Sprintf("entry is not a overflow: %d", t))
	}

	res, _ := unpack.Uint64((*e)[1:], 0)

	return res
}

func (e *Entry) IsData() bool {
	return e.Type() == entryTypeData
}

func (e *Entry) IsOverflow() bool {
	return e.Type() == entryTypeOverflow
}

func (e *Entry) Type() entryType {
	return entryType((*e)[0])
}

func (e *Entry) Format() string {
	if e.IsData() {
		return string(e.GetData())
	}

	return fmt.Sprintf("overflow:%d", e.GetNext())
}

func (k Key) Valid() bool {
	return len(k) <= int(maxKeySize)
}

func (k Key) Compare(other Key) int {
	if len(k) != len(other) {
		if len(k) < len(other) {
			return -1
		} else {
			return 1
		}
	}

	for i := 0; i < len(k); i++ {
		if k[i] != other[i] {
			if k[i] < other[i] {
				return -1
			} else {
				return 1
			}
		}
	}

	return 0
}

func (k Key) Less(other Key) bool {
	return k.Compare(other) < 0
}
