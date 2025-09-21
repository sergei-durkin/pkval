package log

import (
	"unsafe"
	"wal/internal/pack"
	"wal/internal/unpack"
)

type EntryType uint8

const (
	WriteEntry EntryType = iota
	DeleteEntry
	BeginEntry
	CommitEntry
	RollbackEntry
	CheckpointEntry
)

type header struct {
	typ  EntryType
	txid uint64
}

const (
	headerSize = int(unsafe.Sizeof(header{}))
	keySize    = 4 // uint32 for key length
)

type Entry struct {
	header

	Key  string
	Data []byte
}

func (e Entry) Type() EntryType {
	return e.typ
}

func (e Entry) TxID() uint64 {
	return e.txid
}

func NewBegin(txid uint64) Entry {
	return Entry{
		header: header{
			typ:  BeginEntry,
			txid: txid,
		},
	}
}

func NewCommit(txid uint64) Entry {
	return Entry{
		header: header{
			typ:  CommitEntry,
			txid: txid,
		},
	}
}

func NewRollback(txid uint64) Entry {
	return Entry{
		header: header{
			typ:  RollbackEntry,
			txid: txid,
		},
	}
}

func NewWrite(txid uint64, key string, data []byte) Entry {
	return Entry{
		header: header{
			typ:  WriteEntry,
			txid: txid,
		},
		Key:  key,
		Data: data,
	}
}

func NewDelete(txid uint64, key string) Entry {
	return Entry{
		header: header{
			typ:  DeleteEntry,
			txid: txid,
		},
		Key: key,
	}
}

func NewCheckpoint() Entry {
	return Entry{
		header: header{
			typ:  CheckpointEntry,
			txid: 0,
		},
	}
}

func (e *Entry) Serialize() []byte {
	serialized := make([]byte, headerSize+keySize+len(e.Key)+len(e.Data))

	// Header
	ptr := 0

	// Type
	ptr = pack.Uint8(uint8(e.typ), &serialized, ptr)

	// Transaction ID
	_ = pack.Uint64(e.txid, &serialized, ptr)

	// Key
	ptr = headerSize

	// Key length
	ptr = pack.Uint32(uint32(len(e.Key)), &serialized, ptr)

	// Key data
	ptr += copy(serialized[ptr:], e.Key)

	// Data
	copy(serialized[ptr:], e.Data)

	return serialized
}

func NewFromBytes(data []byte) Entry {
	var e Entry

	ptr := 0

	// Header

	// Type
	typ, ptr := unpack.Uint8(&data, ptr)
	e.typ = EntryType(typ)

	// Transaction ID
	txid, ptr := unpack.Uint64(&data, ptr)
	e.txid = txid

	ptr = headerSize

	// Key

	// Key length
	keyLen, ptr := unpack.Uint32(&data, ptr)

	// Key data
	e.Key = string(data[ptr : ptr+int(keyLen)])
	ptr += int(keyLen)

	// Data
	e.Data = make([]byte, len(data)-ptr)
	copy(e.Data, data[ptr:])

	return e
}
