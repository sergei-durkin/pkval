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

	e, err := tree.Find(key)
	if err != nil {
		t.Fatalf("key %q not found: %s", key, err.Error())
	}

	if !entryEq(e, entry) {
		t.Fatalf("e and entry not equal: %v != %v", e, entry)
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
		e, err = tree.Find(k)
		if err != nil {
			t.Fatalf("key %q not found: %s", k, err.Error())
		}

		if !entryEq(expected, e) {
			t.Fatalf("entry of %q not equal: %q != %q", k, e, expected)
		}
	}
}

func TestTreeOverflow(t *testing.T) {
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

	const entrySize = 1 << 22 // 4Mb

	key := []byte("the_key")
	entry := make([]byte, entrySize)
	for i := range entrySize {
		entry[i] = byte(i%26) + 'a'
	}

	tree := NewTree(pg)
	tree.Insert(key, entry)

	e, err := tree.Find(key)
	if err != nil {
		t.Fatalf("key %q not found: %s", key, err.Error())
	}

	if !entryEq(e, entry) {
		t.Fatal("e and entry not equal")
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
