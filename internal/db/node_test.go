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

	/*
		before MoveAndPlace
		src page
			k: key,	        e:  15
			k2: anotherKey, e2: 100
			k3: otherKey,   e3: 500
		dst page
			empty
		insert
			k4: awesomeKey, e4: 1024

		after MoveAndPlace
		src page
			k: key, e: 15
		dst page
			k2: anotherKey, e2: 100
			k4: awesomeKey, e4: 1024
		pivot
			k3: otherKey
	*/

	k4 := []byte("awesomeKey")
	e4 := uint64(1024)

	dst := NewPage(6, 6, PageTypeNode)
	pivot := src.Node().MoveAndPlace(dst.Node(), k4, e4)
	if pivot.Compare(k3) != 0 {
		t.Fatalf("pivot should be equal with k3: %q != %q", k3, pivot)
	}

	srcOffsets := src.Node().offsets()
	dstOffsets := dst.Node().offsets()
	if len(srcOffsets) != 1 {
		t.Fatalf("count src offsets should be 1: %d", len(srcOffsets))
	}

	if len(dstOffsets) != 2 {
		t.Fatalf("count dst offsets should be 2: %d", len(dstOffsets))
	}

	o1, o2 := srcOffsets[0], dstOffsets[0]
	srcKey := src.Node().keyByOffset(o1.key)
	if srcKey.Compare(k) != 0 {
		t.Fatalf("first key of src page should be equal with k: %q != %q", k, srcKey)
	}

	srcEntry := src.Node().entryByOffset(o1.entry)
	if srcEntry != e {
		t.Fatalf("first entry of src page should be equal with e: %d != %d", e, srcEntry)
	}

	dstFirstKey := dst.Node().keyByOffset(o2.key)
	if dstFirstKey.Compare(k2) != 0 {
		t.Fatalf("first offset dst page should be equal with k2: %q != %q", k2, dstFirstKey)
	}

	dstFirstEntry := dst.Node().entryByOffset(o2.entry)
	if dstFirstEntry != e2 {
		t.Fatalf("first entry of dst page should be equal with e2: %d != %d", e2, dstFirstEntry)
	}

	o3 := dstOffsets[1]
	dstSecondKey := dst.Node().keyByOffset(o3.key)
	if dstSecondKey.Compare(k4) != 0 {
		t.Fatalf("second key dst page should be equal with k4: %q != %q", k4, dstSecondKey)
	}

	dstSecondEntry := dst.Node().entryByOffset(o3.entry)
	if dstSecondEntry != e4 {
		t.Fatalf("second entry of dst page should be equal with e4: %d != %d", e4, dstSecondEntry)
	}

	if dst.Node().less != e3 {
		t.Fatalf("less dst pointer should be equal with e3: %d != %d", e3, dst.Node().less)
	}
}

func TestNodeMoveAndPlaceEq(t *testing.T) {
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

	/*
		before MoveAndPlace
		src page
			k: key,	        e:  15
			k2: anotherKey, e2: 100
			k3: otherKey,   e3: 500
		dst page
			empty
		insert
			k2: anotherKey, e2: 100

		after MoveAndPlace
		src page
			k: key, e: 15
		dst page
			k2: anotherKey, e4: 1024
		pivot
			k3: otherKey
	*/

	e4 := uint64(1024)

	dst := NewPage(6, 6, PageTypeNode)
	pivot := src.Node().MoveAndPlace(dst.Node(), k2, e4)
	if pivot.Compare(k3) != 0 {
		t.Fatalf("pivot should be equal with k3: %q != %q", k3, pivot)
	}

	srcOffsets := src.Node().offsets()
	dstOffsets := dst.Node().offsets()
	if len(srcOffsets) != 1 {
		t.Fatalf("count src offsets should be 1: %d", len(srcOffsets))
	}

	if len(dstOffsets) != 1 {
		t.Fatalf("count dst offsets should be 1: %d", len(dstOffsets))
	}

	o1, o2 := srcOffsets[0], dstOffsets[0]
	srcKey := src.Node().keyByOffset(o1.key)
	if srcKey.Compare(k) != 0 {
		t.Fatalf("first key of src page should be equal with k: %q != %q", k, srcKey)
	}

	srcEntry := src.Node().entryByOffset(o1.entry)
	if srcEntry != e {
		t.Fatalf("first entry of src page should be equal with e: %d != %d", e, srcEntry)
	}

	dstKey := dst.Node().keyByOffset(o2.key)
	if dstKey.Compare(k2) != 0 {
		t.Fatalf("first offset dst page should be equal with k2: %q != %q", k2, dstKey)
	}

	dstEntry := dst.Node().entryByOffset(o2.entry)
	if dstEntry != e4 {
		t.Fatalf("first entry of dst page should be equal with e4: %d != %d", e4, dstEntry)
	}

	if dst.Node().less != e3 {
		t.Fatalf("less dst pointer should be equal with e3: %d != %d", e3, dst.Node().less)
	}
}
