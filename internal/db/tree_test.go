package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"wal"
)

func TestTree(t *testing.T) {
	writer, size, err := NewWriterReaderSeekerCloser()
	if err != nil {
		panic(fmt.Sprintf("failed to create writer: %v", err))
	}
	defer ClearDB()
	defer writer.Close()

	pg, err := NewPager(writer, uint64(size))
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	const entrySize = 1 << 8

	key := []byte("the_key")
	entry := make([]byte, entrySize)
	for i := range entrySize {
		entry[i] = byte(i%26) + 'a'
	}

	tree := NewTree(pg)
	tree.Insert(key, entry)

	e, ok := tree.Find(key)
	if !ok {
		t.Fatalf("key %q not found", key)
	}

	if !entryEq(e, entry) {
		t.Fatal("e and entry not equal")
	}

	for i := range 16 {
		k, e := append(key, []byte(strconv.Itoa(i))...), append(entry, []byte(strconv.Itoa(i))...)
		err = tree.Insert(k, e)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := range 16 {
		k, expected := append(key, []byte(strconv.Itoa(i))...), append(entry, []byte(strconv.Itoa(i))...)
		e, ok = tree.Find(k)
		if !ok {
			t.Fatalf("key %q not found", k)
		}

		if !entryEq(expected, e) {
			t.Fatalf("entry of %q not equal: %q != %q", k, e, expected)
		}
	}
}

func ClearDB() {
	err := os.Remove("./test.db")
	if err != nil {
		panic(err)
	}
}

func NewWriterReaderSeekerCloser() (wal.WriterReaderSeekerCloser, int64, error) {
	f, err := os.OpenFile("./test.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to open log file: %v", err))
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, stat.Size(), nil
}
