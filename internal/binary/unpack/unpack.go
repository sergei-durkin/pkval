package unpack

func Uint64(b []byte, ptr int) (uint64, int) {
	var res uint64

	res |= (uint64(b[ptr+0]) << 56)
	res |= (uint64(b[ptr+1]) << 48)
	res |= (uint64(b[ptr+2]) << 40)
	res |= (uint64(b[ptr+3]) << 32)
	res |= (uint64(b[ptr+4]) << 24)
	res |= (uint64(b[ptr+5]) << 16)
	res |= (uint64(b[ptr+6]) << 8)
	res |= (uint64(b[ptr+7]))

	return res, ptr + 8
}

func Uint32(b []byte, ptr int) (uint32, int) {
	var res uint32

	res |= (uint32(b[ptr+0]) << 24)
	res |= (uint32(b[ptr+1]) << 16)
	res |= (uint32(b[ptr+2]) << 8)
	res |= (uint32(b[ptr+3]))

	return res, ptr + 4
}

func Uint16(b []byte, ptr int) (uint16, int) {
	var res uint16

	res |= (uint16(b[ptr+0]) << 8)
	res |= (uint16(b[ptr+1]))

	return res, ptr + 2
}

func Uint8(b []byte, ptr int) (uint8, int) {
	var res uint8

	res = b[ptr]

	return res, ptr + 1
}
