package writer

import (
	"fmt"
	"os"
	"wal"
	"wal/internal/cmd"
)

func NewDBFile(args []cmd.Arg) (wal.WriterReaderSeekerCloser, int64, error) {
	var path string

	for _, arg := range args {
		if arg.Name == "database" || arg.Name == "d" {
			path = arg.Value
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to open log file: %v", err))
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, stat.Size(), nil
}
