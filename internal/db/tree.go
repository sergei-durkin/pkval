package db

import (
	"fmt"
	"os"
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

func (t *Tree) Insert(k Key, e Entry) error {
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
			return r.Leaf().Find(k)
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
