package storage

import (
	"hash/crc32"
	"unsafe"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"
)

const (
	PageSize     = 1 << 13
	PageDataSize = PageSize - headerSize

	// | type | len | rem |
	metaSize = 1 + 4 + 4

	headerSize   = unsafe.Sizeof(header{})
	checksumSize = unsafe.Sizeof(uint32(0))
)

type Page [PageSize]byte

func (p *Page) Header() *header {
	return (*header)(unsafe.Pointer(p))
}

type header struct {
	checksum uint32

	typ     uint16
	version uint16
	head    uint32

	_ [48]byte // padding to 64 bytes
}

type Segment struct {
	typ segmentType
	ln  uint32
	rem uint32

	data []byte
}

func (s *Segment) Data() []byte {
	return s.data
}

func (s *Segment) IsPart() bool {
	return s.typ != 1
}

func (s *Segment) IsEnd() bool {
	return s.typ == 2
}

// GetSegments returns all segments in the page.
func (p *Page) GetSegments() []Segment {
	var result []Segment

	data := p[headerSize:]
	for ptr := 0; ptr < int(PageDataSize); {
		if ptr+int(metaSize) >= int(PageDataSize) {
			break
		}

		var (
			typ uint8
			ln  uint32
			rem uint32
		)

		typ, ptr = unpack.Uint8(data, ptr)
		ln, ptr = unpack.Uint32(data, ptr)
		rem, ptr = unpack.Uint32(data, ptr)

		if ln == 0 || int(ln)+ptr >= int(PageDataSize) {
			break
		}

		s := Segment{}
		s.typ = segmentType(typ)
		s.ln = ln
		s.rem = rem
		s.data = data[ptr : ptr+int(ln)]
		ptr += int(ln)

		result = append(result, s)
	}

	return result
}

// FromBytes unpacks the page from the given buffer.
func (p *Page) FromBytes(b []byte) error {
	if len(b) != PageSize {
		return errBufferTooSmall
	}

	copy(p[:], b)

	cks := crc32.ChecksumIEEE(p[checksumSize:])
	if cks != p.Header().checksum {
		return errChecksumMismatch
	}

	return nil
}

// HasSpace returns true if the page has enough space to store n bytes including metadata.
func (p *Page) HasSpace(n uint32) bool {
	h := p.Header()
	return PageSize >= h.head+uint32(headerSize+metaSize)+n
}

func (p *Page) Len() uint32 {
	return p.Header().head
}

// Pack packs the page into the given buffer.
func (p *Page) Pack() []byte {
	return p[:]
}

// Reset resets the page to initial state.
func (p *Page) Reset() {
	for i := 0; i < PageSize; i++ {
		p[i] = 0
	}
}

type segmentType uint8

const (
	segmentTypeFull   segmentType = 1
	segmentTypeEnd    segmentType = 2
	segmentTypeMiddle segmentType = 3
)

// Write writes data to the page, remaining is the remaining length of the entire data size, -1 means full data.
func (p *Page) Write(data []byte, remaining int32) (n int, err error) {
	var typ segmentType

	switch remaining {
	case -1:
		typ = segmentTypeFull
		remaining = 0
	case 0:
		typ = segmentTypeEnd
	default:
		typ = segmentTypeMiddle
	}

	h := p.Header()

	ln := len(data)
	ptr := int(headerSize) + int(h.head)
	ptr = pack.Uint8(p[:], uint8(typ), ptr)
	ptr = pack.Uint32(p[:], uint32(ln), ptr)
	ptr = pack.Uint32(p[:], uint32(remaining), ptr)
	n = copy(p[ptr:], data)

	h.head += metaSize
	h.head += uint32(n)

	h.checksum = crc32.ChecksumIEEE(p[checksumSize:])

	return n, nil
}
