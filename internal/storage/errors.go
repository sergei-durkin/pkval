package storage

import "fmt"

var (
	errBufferTooSmall   = fmt.Errorf("buffer too small")
	errChecksumMismatch = fmt.Errorf("checksum mismatch")
	errTooLarge         = fmt.Errorf("too large")
	errShortWrite       = fmt.Errorf("short write")
)
