package storage_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"wal"
	"wal/internal/storage"
)

var _ wal.WriterCloser = &MockFile{}

func TestSyncPageBuffer(t *testing.T) {
	ctx := context.Background()
	syncInterval := 1 * time.Second

	expectedData := make([]byte, storage.PageSize)
	p := storage.Page{}
	p.Write([]byte("Hello, World!"), -1)
	p.Pack(expectedData)

	actualData := []byte{}
	syncCalled := int32(0)

	syncCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for range ticker.C {
			if atomic.LoadInt32(&syncCalled) == 1 {
				close(syncCh)
				return
			}
		}
	}()

	provider := func() (wal.WriterCloser, error) {
		return NewMockFile(
			func(b []byte) (n int, err error) {
				actualData = append(actualData, b...)

				return len(b), nil
			},
			func() error { return nil },
			func() error {
				atomic.StoreInt32(&syncCalled, 1)
				return nil
			},
		), nil
	}

	pb, err := storage.NewPageBuffer(ctx, syncInterval, provider)
	if err != nil {
		t.Fatal("failed to create PageBuffer:", err)
	}

	err = pb.Write([]byte("Hello, World!"))
	if err != nil {
		t.Error("failed to write to PageBuffer:", err)
	}

	<-syncCh

	if string(actualData) != string(expectedData) {
		t.Errorf("data mismatch: expected %q, got %q", expectedData, actualData)
	}
}

func TestWritePageBuffer(t *testing.T) {
	ctx := context.Background()
	syncInterval := 1 * time.Hour

	provider := func() (wal.WriterCloser, error) {
		return NewMockFile(
			func(b []byte) (n int, err error) {
				return len(b), nil
			},
			func() error { return nil },
			func() error { return nil },
		), nil
	}

	pb, err := storage.NewPageBuffer(ctx, syncInterval, provider)
	if err != nil {
		t.Fatal("failed to create PageBuffer:", err)
	}

	err = pb.Write([]byte("Hello, World!"))
	if err != nil {
		t.Error("failed to write to PageBuffer:", err)
	}
}

func TestWrite1MBPageBuffer(t *testing.T) {
	ctx := context.Background()
	syncInterval := 1 * time.Hour

	i := 0
	provider := func() (wal.WriterCloser, error) {
		return NewMockFile(
			func(b []byte) (n int, err error) {
				i++
				return len(b), nil
			},
			func() error { return nil },
			func() error { return nil },
		), nil
	}

	pb, err := storage.NewPageBuffer(ctx, syncInterval, provider)
	if err != nil {
		t.Fatal("failed to create PageBuffer:", err)
	}

	b1m := make([]byte, storage.PageBufferSize)
	err = pb.Write(b1m)
	if err != nil {
		t.Error("failed to write to PageBuffer:", err)
	}

	if i != storage.PageBufferSize/storage.PageSize {
		t.Errorf("expected %d writes, got %d", storage.PageBufferSize/storage.PageSize, i)
	}
}

func NewMockFile(
	write func(b []byte) (n int, err error),
	close func() error,
	sync func() error,
) *MockFile {
	return &MockFile{
		write: write,
		close: close,
		sync:  sync,
	}
}

type MockFile struct {
	write func(b []byte) (n int, err error)
	close func() error
	sync  func() error
}

func (m *MockFile) Write(b []byte) (n int, err error) {
	return m.write(b)
}

func (m *MockFile) Close() error {
	return m.close()
}

func (m *MockFile) Sync() error {
	return m.sync()
}
