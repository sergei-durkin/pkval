package db

import (
	"fmt"
)

const (
	maxKeySize = 1 << 10
)

type Tree struct {
	root  *Page
	pager *Pager
}

type Key []byte
type Entry []byte

func (k Key) Valid() bool {
	return len(k) <= int(maxKeySize)
}

func (k Key) Less(other Key) bool {
	if len(k) != len(other) {
		return len(k) < len(other)
	}

	for i := 0; i < len(k); i++ {
		if k[i] != other[i] {
			return k[i] < other[i]
		}
	}

	return false
}

func (t *Tree) find(k Key) (e Entry, found bool) {
	var err error

	r := t.root
	if r == nil {
		r, err = t.pager.ReadRoot()
		if err != nil {
			fmt.Println(fmt.Errorf("failed to read root: %w", err))

			return Entry{}, false
		}
	}

	if r.IsLeaf() {
		return r.Leaf().Find(k)
	}

	return Entry{}, false
}
