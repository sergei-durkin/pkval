package db

import (
	"fmt"
	"os"
	"wal/internal/binary/pack"
	"wal/internal/binary/unpack"
)

type Tree struct {
	root  *Page
	pager *Pager
}

func NewTree(pg *Pager) *Tree {
	return &Tree{
		pager: pg,
	}
}

type Key []byte
type Entry []byte

type entryType uint8

const (
	entryTypeData     entryType = 1
	entryTypeOverflow entryType = 2
)

func NewOverflowEntry(next uint64) (e Entry) {
	e = make([]byte, 9)
	_ = pack.Uint64(e, next, 1)

	e[0] = byte(entryTypeOverflow)

	return e
}

func NewDataEntry(data []byte) (e Entry) {
	e = make([]byte, len(data)+1)
	copy(e[1:], data)

	e[0] = byte(entryTypeData)

	return e
}

func (e *Entry) GetData() []byte {
	t := e.Type()
	if t != entryTypeData {
		panic(fmt.Sprintf("entry is not a data: %d", t))
	}

	return (*e)[1:]
}

func (e *Entry) GetNext() uint64 {
	t := e.Type()
	if t != entryTypeOverflow {
		panic(fmt.Sprintf("entry is not a overflow: %d", t))
	}

	res, _ := unpack.Uint64((*e)[1:], 0)

	return res
}

func (e *Entry) IsData() bool {
	return e.Type() == entryTypeData
}

func (e *Entry) IsOverflow() bool {
	return e.Type() == entryTypeOverflow
}

func (e *Entry) Type() entryType {
	return entryType((*e)[0])
}

func (e *Entry) Format() string {
	if e.IsData() {
		return string(e.GetData())
	}

	return fmt.Sprintf("overflow:%d", e.GetNext())
}

func (k Key) Valid() bool {
	return len(k) <= int(maxKeySize)
}

func (k Key) Compare(other Key) int {
	if len(k) != len(other) {
		if len(k) < len(other) {
			return -1
		} else {
			return 1
		}
	}

	for i := 0; i < len(k); i++ {
		if k[i] != other[i] {
			if k[i] < other[i] {
				return -1
			} else {
				return 1
			}
		}
	}

	return 0
}

func (k Key) Less(other Key) bool {
	return k.Compare(other) < 0
}

func (t *Tree) Insert(k Key, v []byte) error {
	var err error

	if t.root == nil {
		t.root, err = t.pager.ReadRoot()
		if err != nil {
			return fmt.Errorf("failed to read root: %w", err)
		}
	}
	p := t.root

	path := []*Page{}
	for p.IsNode() {
		path = append(path, p)

		next, ok := p.Node().Find(k)
		if !ok {
			return fmt.Errorf("failed to find leaf")
		}

		p, err = t.pager.Read(next)
		if err != nil {
			return fmt.Errorf("failed to read next")
		}
	}

	if p == nil {
		return fmt.Errorf("failed to find leaf")
	}

	var e Entry
	if len(v)+1 > int(maxEntrySize) {
		o := t.pager.Alloc(p.Header().lsn, PageTypeOverflow)
		firstID := o.ID()

		for len(v) > 0 {
			n, err := o.Overflow().Write(v)
			if err != nil {
				return fmt.Errorf("failed to write into overflow page: %w", err)
			}

			if n == len(v) {
				t.pager.Write(o)
				break
			}

			v = v[n:]

			next := t.pager.Alloc(p.Header().lsn, PageTypeOverflow)
			o.Overflow().next = next.ID()
			t.pager.Write(o)

			o = next
		}

		e = NewOverflowEntry(firstID)
	} else {
		// add entry type byte
		e = NewDataEntry(v)
	}

	err = p.Leaf().Insert(k, e)
	if nil == err {
		t.pager.Write(p)
		return nil
	}

	extra := t.pager.Alloc(p.Header().lsn, PageTypeLeaf)
	pivot := p.Leaf().MoveAndPlace(extra.Leaf(), k, e)

	t.pager.Write(p)
	t.pager.Write(extra)

	for len(path) > 0 {
		next := extra.ID()
		par := path[len(path)-1]
		path = path[:len(path)-1]

		err = par.Node().Insert(pivot, next)
		if nil == err {
			t.pager.Write(par)
			return nil
		}

		extra = t.pager.Alloc(0, PageTypeNode)
		pivot = par.Node().MoveAndPlace(extra.Node(), pivot, next)

		t.pager.Write(par)
		t.pager.Write(extra)
	}

	{ // split root
		r := t.pager.Alloc(0, PageTypeNode)
		r.Node().less = t.root.ID()
		err = r.Node().Insert(pivot, extra.ID())
		if err != nil {
			return err
		}

		t.root = r

		t.pager.WriteRoot(r)
	}

	return nil
}

func (t *Tree) Find(k Key) (e Entry, found bool) {
	var err error

	r := t.root
	if r == nil {
		r, err = t.pager.ReadRoot()
		if err != nil {
			fmt.Println(fmt.Errorf("failed to read root: %w", err))

			return Entry{}, false
		}
	}

	for r != nil {
		if r.IsLeaf() {
			e, found = r.Leaf().Find(k)
			if !found {
				return Entry{}, false
			}

			if e.IsData() {
				return e[1:], true
			}

			if e.IsOverflow() {
				overflow := make([]byte, 0, maxEntrySize)

				next := e.GetNext()
				for next > 0 {
					op, err := t.pager.Read(next)
					if err != nil {
						fmt.Println(fmt.Errorf("failed to read overflow page: %w", err))

						return Entry{}, false
					}

					next = op.Overflow().next
					overflow = append(overflow, op.Overflow().Data()...)
				}

				return overflow, true
			}

			panic("unknown entry type")
		}

		if r.IsNode() {
			next, ok := r.Node().Find(k)
			if !ok {
				return Entry{}, false
			}

			r, err = t.pager.Read(next)
			if err != nil {
				fmt.Println(fmt.Errorf("failed to read page: %w", err))

				return Entry{}, false
			}

			continue
		}

		panic(fmt.Errorf("unexpected page type: %d", r.Type()))
	}

	return Entry{}, false
}

func (t *Tree) Print() error {
	var err error

	if t.root == nil {
		t.root, err = t.pager.ReadRoot()
		if err != nil {
			return fmt.Errorf("failed to read root: %w", err)
		}
	}

	level := []byte{}
	q := []*Page{t.root}
	for len(q) > 0 {
		ln := len(q)
		for range ln {
			p := q[0]
			q = q[1:]

			if p.IsLeaf() {
				fmt.Fprintf(os.Stderr, "Leaf [%d]: \n", p.Leaf().id)
				p.Leaf().Print(level)

				fmt.Fprint(os.Stderr, "\n")
				continue
			}

			if p.IsNode() {
				fmt.Fprintf(os.Stderr, "Node [%d]: \n", p.Node().id)
				next := p.Node().Entries()

				for i := 0; i < len(next); i++ {
					fmt.Fprintf(os.Stderr, "%s %d ", level, next[i])
					r, err := t.pager.Read(next[i])
					if err != nil {
						return fmt.Errorf("failed to read page: %w", err)
					}

					q = append(q, r)
				}
				fmt.Fprint(os.Stderr, "\n\n")
			}
		}
		level = append(level, ' ')
	}

	return nil
}
