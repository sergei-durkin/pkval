package resolver

import (
	"fmt"
	"os"
	"time"
	"wal"
	"wal/internal/cmd"
)

const (
	FILENAME_FORMAT = "%s_%s.log"
	TIME_FORMAT     = "20060102-150405.000"

	LOG_FILE  = "logfile"
	MOCK_FILE = "mockfile"
	STDOUT    = "stdout"
)

func NewWriter(args []cmd.Arg) func() (wal.WriterCloser, error) {
	for _, arg := range args {
		if arg.Name == LOG_FILE && arg.Value != "" {
			return func() (wal.WriterCloser, error) {
				timestamp := time.Now().Format(TIME_FORMAT)
				f, err := os.OpenFile(fmt.Sprintf(FILENAME_FORMAT, arg.Value, timestamp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					return nil, err
				}

				return f, nil
			}
		}

		if arg.Name == MOCK_FILE {
			return func() (wal.WriterCloser, error) {
				return &stubFile{}, nil
			}
		}

		if arg.Name == STDOUT {
			break
		}
	}

	f := os.Stdout
	return func() (wal.WriterCloser, error) {
		return &stdoutFile{f: f}, nil
	}
}

type stubFile struct {
}

func (s *stubFile) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (s *stubFile) Close() error {
	return nil
}

func (s *stubFile) Sync() error {
	return nil
}

type stdoutFile struct {
	f *os.File
}

func (s *stdoutFile) Write(p []byte) (n int, err error) {
	return s.f.Write(p)
}

func (s *stdoutFile) Close() error {
	return nil
}

func (s *stdoutFile) Sync() error {
	return nil
}
