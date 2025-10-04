package db

import (
	"fmt"
	"testing"

	"github.com/sergei-durkin/armtracer"
)

func TestLeafInsert(t *testing.T) {
	armtracer.Begin()
	defer armtracer.End()

	p := NewPage(5, 5, PageTypeLeaf)

	if !p.IsLeaf() {
		t.Fatal("page should be Leaf")
	}

	k := []byte("key")
	e := []byte("entry")

	err := p.Leaf().Insert(k, e)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	offsets := p.Leaf().offsets()
	if len(offsets) != 1 {
		t.Fatal("page should have one offset")
	}

	o := offsets[0]
	if o.key.len != len(k) {
		t.Fatal("keys should be equal")
	}

	actualKey := p.Leaf().keyByOffset(o.key)
	if actualKey.Compare(k) != 0 {
		t.Fatal("keys should be equal")
	}

	if o.entry.len != len(e) {
		t.Fatal("entries should be equal")
	}

	actualEntry := p.Leaf().entryByOffset(o.entry)
	if !entryEq(actualEntry, e) {
		t.Fatal("entries should be equal")
	}

	k2 := []byte("anotherKey")
	e2 := []byte("anotherEntry")

	err = p.Leaf().Insert(k2, e2)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}
}

func TestLeafMoveAndPlace(t *testing.T) {
	armtracer.Begin()
	defer armtracer.End()

	src := NewPage(5, 5, PageTypeLeaf)
	src.Leaf().right = 100500
	src.Leaf().left = 200600

	k := []byte("key")
	e := []byte("entry")

	err := src.Leaf().Insert(k, e)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k2 := []byte("anotherKey")
	e2 := []byte("anotherEntry")

	err = src.Leaf().Insert(k2, e2)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k3 := []byte("otherKey")
	e3 := []byte("otherEntry")

	err = src.Leaf().Insert(k3, e3)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	/*
		before MoveAndPlace
		src page
			k: key,	        e:  entry
			k2: anotherKey, e2: anotherEntry
			k3: otherKey,   e3: otherEntry
		dst page
			empty
		insert
			k4: awesomeKey, e4: awesomeEntry

		after MoveAndPlace
		src page
			k: key,       e: entry
			k3: otherKey, e: otherEntry
		dst page
			k2: anotherKey, e2: anotherEntry
			k4: awesomeKey, e4: awesomeEntry
		pivot
			k2: anotherKey, e2: anotherEntry
	*/

	k4 := []byte("awesomeKey")
	e4 := []byte("awesomeEntry")

	dst := NewPage(6, 6, PageTypeLeaf)
	pivot := src.Leaf().MoveAndPlace(dst.Leaf(), k4, e4)
	if pivot.Compare(k2) != 0 {
		t.Fatalf("pivot should be equal with k2: %q != %q", k2, pivot)
	}

	srcOffsets := src.Leaf().offsets()
	dstOffsets := dst.Leaf().offsets()
	if len(srcOffsets) != 2 {
		t.Fatalf("count src offsets should be 2: %d", len(srcOffsets))
	}

	if len(dstOffsets) != 2 {
		t.Fatalf("count dst offsets should be 2: %d", len(dstOffsets))
	}

	o1, o2 := srcOffsets[0], dstOffsets[0]
	srcFirstKey := src.Leaf().keyByOffset(o1.key)
	if srcFirstKey.Compare(k) != 0 {
		t.Fatalf("first key of src page should be equal with k: %q != %q", k, srcFirstKey)
	}

	srcFirstEntry := src.Leaf().entryByOffset(o1.entry)
	if !entryEq(srcFirstEntry, e) {
		t.Fatalf("first entry of src page should be equal with e: %q != %q", e, srcFirstEntry)
	}

	o3 := srcOffsets[1]
	srcSecondKey := src.Leaf().keyByOffset(o3.key)
	if srcSecondKey.Compare(k3) != 0 {
		t.Fatalf("second key src page should be equal with k3: %q != %q", k3, srcSecondKey)
	}

	srcSecondEntry := src.Leaf().entryByOffset(o3.entry)
	if !entryEq(srcSecondEntry, e3) {
		t.Fatalf("second entry of src page should be equal with e3: %q != %q", e3, srcFirstEntry)
	}

	dstKey := dst.Leaf().keyByOffset(o2.key)
	if dstKey.Compare(k2) != 0 {
		t.Fatalf("first key dst page should be equal with k3: %q != %q", k3, srcSecondKey)
	}

	dstEntry := dst.Leaf().entryByOffset(o2.entry)
	if !entryEq(dstEntry, e2) {
		t.Fatalf("first entry of dst page should be equal with e2: %q != %q", e2, dstEntry)
	}

	if dst.Leaf().left != 5 {
		t.Fatalf("left dst neighbor should be src: %d != %d", dst.Leaf().left, 5)
	}

	if dst.Leaf().right != 100500 {
		t.Fatalf("right dst neighbor should be src.right: %d != %d", dst.Leaf().right, 100500)
	}

	if src.Leaf().left != 200600 {
		t.Fatalf("left src neighbor should be src.left: %d != %d", src.Leaf().left, 200600)
	}

	if src.Leaf().right != 6 {
		t.Fatalf("right src neighbor should be dst: %d != %d", src.Leaf().right, 6)
	}
}

func TestLeafMoveAndPlaceEq(t *testing.T) {
	armtracer.Begin()
	defer armtracer.End()

	src := NewPage(5, 5, PageTypeLeaf)
	src.Leaf().right = 100500
	src.Leaf().left = 200600

	k := []byte("key")
	e := []byte("entry")

	err := src.Leaf().Insert(k, e)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k2 := []byte("anotherKey")
	e2 := []byte("anotherEntry")

	err = src.Leaf().Insert(k2, e2)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	k3 := []byte("otherKey")
	e3 := []byte("otherEntry")

	err = src.Leaf().Insert(k3, e3)
	if err != nil {
		t.Fatal(fmt.Errorf("insert error: %w", err))
	}

	/*
		before MoveAndPlace
		src page
			k: key,	        e:  entry
			k2: anotherKey, e2: anotherEntry
			k3: otherKey,   e3: otherEntry
		dst page
			empty
		insert
			k2: awesomeKey, e4: awesomeEntry

		after MoveAndPlace
		src page
			k: key,       e: entry
			k3: otherKey, e: otherEntry
		dst page
			k2: anotherKey, e2: awesomeEntry
		pivot
			k2: anotherKey
	*/

	e4 := []byte("awesomeEntry")

	dst := NewPage(6, 6, PageTypeLeaf)
	pivot := src.Leaf().MoveAndPlace(dst.Leaf(), k2, e4)
	if pivot.Compare(k2) != 0 {
		t.Fatalf("pivot should be equal with k2: %q != %q", k2, pivot)
	}

	srcOffsets := src.Leaf().offsets()
	dstOffsets := dst.Leaf().offsets()
	if len(srcOffsets) != 2 {
		t.Fatalf("count src offsets should be 2: %d", len(srcOffsets))
	}

	if len(dstOffsets) != 1 {
		t.Fatalf("count dst offsets should be 2: %d", len(dstOffsets))
	}

	o1, o2 := srcOffsets[0], dstOffsets[0]
	srcFirstKey := src.Leaf().keyByOffset(o1.key)
	if srcFirstKey.Compare(k) != 0 {
		t.Fatalf("first key of src page should be equal with k: %q != %q", k, srcFirstKey)
	}

	srcFirstEntry := src.Leaf().entryByOffset(o1.entry)
	if !entryEq(srcFirstEntry, e) {
		t.Fatalf("first entry of src page should be equal with e: %q != %q", e, srcFirstEntry)
	}

	o3 := srcOffsets[1]
	srcSecondKey := src.Leaf().keyByOffset(o3.key)
	if srcSecondKey.Compare(k3) != 0 {
		t.Fatalf("second key src page should be equal with k3: %q != %q", k3, srcSecondKey)
	}

	srcSecondEntry := src.Leaf().entryByOffset(o3.entry)
	if !entryEq(srcSecondEntry, e3) {
		t.Fatalf("second entry of src page should be equal with e3: %q != %q", e3, srcFirstEntry)
	}

	dstKey := dst.Leaf().keyByOffset(o2.key)
	if dstKey.Compare(k2) != 0 {
		t.Fatalf("first key dst page should be equal with k3: %q != %q", k3, srcSecondKey)
	}

	dstEntry := dst.Leaf().entryByOffset(o2.entry)
	if !entryEq(dstEntry, e4) {
		t.Fatalf("first entry of dst page should be equal with e4: %q != %q", e4, dstEntry)
	}

	if dst.Leaf().left != 5 {
		t.Fatalf("left dst neighbor should be src: %d != %d", dst.Leaf().left, 5)
	}

	if dst.Leaf().right != 100500 {
		t.Fatalf("right dst neighbor should be src.right: %d != %d", dst.Leaf().right, 100500)
	}

	if src.Leaf().left != 200600 {
		t.Fatalf("left src neighbor should be src.left: %d != %d", src.Leaf().left, 200600)
	}

	if src.Leaf().right != 6 {
		t.Fatalf("right src neighbor should be dst: %d != %d", src.Leaf().right, 6)
	}
}

func entryEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
