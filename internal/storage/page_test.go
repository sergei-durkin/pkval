package storage_test

import (
	"math/rand"
	"testing"
	"wal/internal/storage"
)

func TestWrite(t *testing.T) {
	p := storage.Page{}

	data := []byte("Hello, World!")
	expectedDataSize := len(data) + storage.MetaDataSize
	n, err := p.Write(data, -1)
	if err != nil {
		t.Fatal("Write failed:", err)
	}

	if n != len(data) {
		t.Fatalf("Write returned %d, want %d", n, len(data))
	}

	if p.Len() != expectedDataSize {
		t.Fatalf("Page cur is %d, want %d", p.Len(), expectedDataSize)
	}

	segments := p.GetSegments()
	if len(segments) != 1 {
		t.Fatalf("GetSegments returned %d segments, want 1", len(segments))
	}

	if string(segments[0].Data) != string(data) {
		t.Fatalf("Segment data is %s, want %s", string(segments[0].Data), string(data))
	}
}

func TestGetSegmentsMultiple(t *testing.T) {
	p := storage.Page{}

	data := []byte("Hello, World! This is a test of multiple writes.")
	var expectedDataSize int

	temp := make([]byte, len(data))
	copy(temp, data)

	i := 0
	for len(temp) > 0 {
		rndIdx := rand.Intn(len(temp)) + 1
		expectedDataSize += rndIdx + storage.MetaDataSize

		n, err := p.Write(temp[:rndIdx], -1)
		if err != nil {
			t.Fatal("Write failed:", err)
		}

		temp = temp[rndIdx:]

		if n != rndIdx {
			t.Fatalf("Write returned %d, want %d", n, rndIdx)
		}

		i++
	}

	if p.Len() != expectedDataSize {
		t.Fatalf("Page cur is %d, want %d", p.Len(), expectedDataSize)
	}

	segments := p.GetSegments()
	if len(segments) != i {
		t.Fatalf("GetSegments returned %d segments, want %d", len(segments), i)
	}

	reconstructed := make([]byte, 0, len(data))
	for _, seg := range segments {
		reconstructed = append(reconstructed, seg.Data...)
	}

	if string(reconstructed) != string(data) {
		t.Fatal("Reconstructed data does not match original")
	}
}

func TestPackAndFromBytes(t *testing.T) {
	p := storage.Page{}
	data := []byte("Hello, World!")
	expectedDataSize := len(data) + storage.MetaDataSize
	n, err := p.Write(data, -1)
	if err != nil {
		t.Fatal("Write failed:", err)
	}

	if n != len(data) {
		t.Fatalf("Write returned %d, want %d", n, len(data))
	}

	if p.Len() != expectedDataSize {
		t.Fatalf("Page cur is %d, want %d", p.Len(), expectedDataSize)
	}

	buff := make([]byte, storage.PageSize)
	err = p.Pack(buff)
	if err != nil {
		t.Fatal("Pack failed:", err)
	}

	var p2 storage.Page
	err = p2.FromBytes(buff)
	if err != nil {
		t.Fatal("FromBytes failed:", err)
	}

	segments := p2.GetSegments()
	if len(segments) != 1 {
		t.Fatalf("GetSegments returned %d segments, want 1", len(segments))
	}

	if string(segments[0].Data) != string(data) {
		t.Fatalf("Segment data is %s, want %s", string(segments[0].Data), string(data))
	}
}
