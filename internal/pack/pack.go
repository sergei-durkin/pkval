package pack

func Uint64(b []byte, data uint64, ptr int) int {
	b[ptr+0] = byte(data >> 56)
	b[ptr+1] = byte(data >> 48)
	b[ptr+2] = byte(data >> 40)
	b[ptr+3] = byte(data >> 32)
	b[ptr+4] = byte(data >> 24)
	b[ptr+5] = byte(data >> 16)
	b[ptr+6] = byte(data >> 8)
	b[ptr+7] = byte(data)

	return ptr + 8
}

func Uint32(b []byte, data uint32, ptr int) int {
	b[ptr+0] = byte(data >> 24)
	b[ptr+1] = byte(data >> 16)
	b[ptr+2] = byte(data >> 8)
	b[ptr+3] = byte(data)

	return ptr + 4
}

func Uint16(b []byte, data uint16, ptr int) int {
	b[ptr+0] = byte(data >> 8)
	b[ptr+1] = byte(data)

	return ptr + 2
}

func Uint8(b []byte, data uint8, ptr int) int {
	b[ptr] = data

	return ptr + 1
}
