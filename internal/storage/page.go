package storage

import (
	"hash/crc32"
	"unsafe"
	"wal/internal/pack"
	"wal/internal/unpack"
)

const (
	PageSize     = 1 << 13
	PageDataSize = PageSize - headerSize - tailSize

	// MetaData for each segment
	MetaDataSize = 1 + 4 + 4 // type (1 byte) + length of segment (4 bytes) + remaining length (4 bytes
)

type MetaData struct {
	typ uint8  // type of segment
	ln  uint32 // length of segment
	rem uint32 // remaining length
}

const (
	headerSize = int(unsafe.Sizeof(header{}))
	tailSize   = int(unsafe.Sizeof(tail{}))
)

type Page struct {
	isSynced bool // whether the page has been synced to disk
	cur      int

	header
	tail

	data [PageDataSize]byte
}

type header struct {
	typ     uint16 // page type
	version uint16

	_ [60]byte // padding to 64 bytes
}

type tail struct {
	checksum uint32

	_ [60]byte // padding to 64 bytes
}

type Segment struct {
	MetaData

	Data []byte
}

func (c *Segment) IsPart() bool {
	return c.typ != 1
}

func (c *Segment) IsEnd() bool {
	return c.typ == 2
}

// GetSegments returns all segments in the page.
func (p *Page) GetSegments() []Segment {
	var result []Segment

	data := p.data[:]

	for ptr := 0; ptr < PageDataSize; {
		if ptr+MetaDataSize >= PageDataSize {
			break
		}

		chunk := Segment{}

		chunk.typ, ptr = unpack.Uint8(&data, ptr)
		chunk.ln, ptr = unpack.Uint32(&data, ptr)
		chunk.rem, ptr = unpack.Uint32(&data, ptr)

		if chunk.ln == 0 || int(chunk.ln)+ptr >= PageDataSize {
			break
		}

		chunk.Data = make([]byte, chunk.ln)
		ptr += copy(chunk.Data, data[ptr:ptr+int(chunk.ln)])

		result = append(result, chunk)
	}

	return result
}

// FromBytes unpacks the page from the given buffer.
func (p *Page) FromBytes(b []byte) error {
	if len(b) != PageSize {
		return errBufferTooSmall
	}

	ptr := 0

	// Header

	// Type
	p.typ, ptr = unpack.Uint16(&b, ptr)

	// Version
	p.version, ptr = unpack.Uint16(&b, ptr)

	// Data
	copy(p.data[:], b[headerSize:PageSize-tailSize])

	// Current position is at the end of data
	p.cur = -1

	// Tail

	// Checksum
	p.checksum, _ = unpack.Uint32(&b, PageSize-tailSize)

	cks := crc32.ChecksumIEEE(p.data[:])
	if cks != p.checksum {
		return errChecksumMismatch
	}

	return nil
}

// HasSpace returns true if the page has enough space to store n bytes including metadata.
func (p *Page) HasSpace(n int) bool {
	return !p.isSynced && PageDataSize-MetaDataSize-p.cur >= n
}

func (p *Page) IsSynced() bool {
	return p.isSynced
}

func (p *Page) Len() int {
	return p.cur
}

func (p *Page) MarkAsSynced() {
	p.isSynced = true
}

// Pack packs the page into the given buffer.
func (p *Page) Pack(b []byte) error {
	if len(b) < PageSize {
		return errBufferTooSmall
	}

	// Clear buffer
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}

	ptr := 0

	// Header

	// Type
	ptr = pack.Uint16(p.typ, &b, ptr)

	// Version
	ptr = pack.Uint16(p.version, &b, ptr)

	// Data
	copy(b[headerSize:headerSize+p.cur], p.data[:])

	// Tail

	// Checksum
	_ = pack.Uint32(p.checksum, &b, PageSize-tailSize)

	return nil
}

// Reset resets the page to initial state.
func (p *Page) Reset() {
	p.isSynced = false
	p.cur = 0
	p.checksum = 0
	for i := range p.data {
		p.data[i] = 0
	}
}

// Write writes data to the page, remaining is the remaining length of the entire data size, -1 means full data.
func (p *Page) Write(data []byte, remaining int32) (n int, err error) {
	// Metadata of segment
	// | type (1 byte) means is part data | length of segment (4 bytes) | remaining length (4 bytes) | data (portion length bytes) |
	meta := make([]byte, MetaDataSize)
	ptr := 0

	// Type of segment
	switch remaining {
	case -1:
		ptr = pack.Uint8(1, &meta, ptr) // full data
		remaining = 0
	case 0:
		ptr = pack.Uint8(2, &meta, ptr) // end segment
	default:
		ptr = pack.Uint8(3, &meta, ptr) // middle segment
	}

	// Length of segment
	ln := len(data)
	ptr = pack.Uint32(uint32(ln), &meta, ptr)

	// Remaining length
	ptr = pack.Uint32(uint32(remaining), &meta, ptr)

	n = copy(p.data[p.cur:], meta)
	p.cur += n

	n = copy(p.data[p.cur:], data)
	p.cur += n

	p.checksum = crc32.ChecksumIEEE(p.data[:])

	return n, nil
}
