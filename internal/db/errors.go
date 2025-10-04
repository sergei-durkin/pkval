package db

import "fmt"

var (
	errShortWrite     = fmt.Errorf("short write")
	errNotEnoughSpace = fmt.Errorf("not enough space")
	errNotFound       = fmt.Errorf("not found")
)
