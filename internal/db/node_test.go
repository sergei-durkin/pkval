package db

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestNodeInsert(t *testing.T) {
	p := NewPage(5, 5, PageTypeNode)

	if !p.IsNode() {
		t.Fatal("page should be Node")
	}

	k := []byte("key")
	e := uint64(15)

	err := p.Node().Insert(k, e)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	offsets := p.Node().offsets()
	if len(offsets) != 1 {
		t.Fatal("page should have one offset")
	}

	o := offsets[0]
	if o.key.len != len(k) {
		t.Fatal("keys should be equal")
	}

	actualKey := p.Node().keyByOffset(o.key)
	if actualKey.Compare(k) != 0 {
		t.Fatal("keys should be equal")
	}

	if o.entry.len != int(unsafe.Sizeof(e)) {
		t.Fatal("entries should be equal")
	}

	actualEntry := p.Node().entryByOffset(o.entry)
	if actualEntry != e {
		t.Fatal("entries should be equal")
	}

	k2 := []byte("anotherKey")
	e2 := uint64(100)

	err = p.Node().Insert(k2, e2)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}
}

func TestNodeMoveAndPlace(t *testing.T) {
	src := NewPage(5, 5, PageTypeNode)

	k := []byte("key")
	e := uint64(15)

	err := src.Node().Insert(k, e)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k2 := []byte("anotherKey")
	e2 := uint64(100)

	err = src.Node().Insert(k2, e2)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k3 := []byte("otherKey")
	e3 := uint64(500)

	err = src.Node().Insert(k3, e3)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k4 := []byte("awesomeKey")
	e4 := uint64(1024)

	dst := NewPage(6, 6, PageTypeNode)
	pivot := src.Node().MoveAndPlace(dst.Node(), k4, e4)
	if pivot.Compare(k2) != 0 {
		t.Fatalf("pivot should be equal with k2: %q != %q", k2, pivot)
	}

	srcOffsets := src.Node().offsets()
	dstOffsets := dst.Node().offsets()
	if len(srcOffsets) != 2 {
		t.Fatalf("count src offsets should be 2: %d", len(srcOffsets))
	}

	if len(dstOffsets) != 1 {
		t.Fatalf("count dst offsets should be 1: %d", len(dstOffsets))
	}

	o1, o2 := srcOffsets[0], dstOffsets[0]
	srcFirstKey := src.Node().keyByOffset(o1.key)
	if srcFirstKey.Compare(k) != 0 {
		t.Fatalf("first key of src page should be equal with k: %q != %q", k, srcFirstKey)
	}

	srcFirstEntry := src.Node().entryByOffset(o1.entry)
	if srcFirstEntry != e {
		t.Fatalf("first entry of src page should be equal with e: %d != %d", e, srcFirstEntry)
	}

	o3 := srcOffsets[1]
	srcSecondKey := src.Node().keyByOffset(o3.key)
	if srcSecondKey.Compare(k3) != 0 {
		t.Fatalf("second key src page should be equal with k3: %q != %q", k3, srcSecondKey)
	}

	srcSecondEntry := src.Node().entryByOffset(o3.entry)
	if srcSecondEntry != e3 {
		t.Fatalf("second entry of src page should be equal with e3: %d != %d", e3, srcFirstEntry)
	}

	dstKey := dst.Node().keyByOffset(o2.key)
	if dstKey.Compare(k4) != 0 {
		t.Fatalf("first offset dst page should be equal with k4: %q != %q", k4, dstKey)
	}

	if dst.Node().less != e2 {
		t.Fatalf("less dst pointer should be equal with e2: %q != %q", dst.Node().less, e2)
	}
}
