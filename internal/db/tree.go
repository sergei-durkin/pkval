package db

import (
	"fmt"
	"os"

	"github.com/sergei-durkin/armtracer"
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

func (t *Tree) Root() (*Page, error) {
	var err error

	if t.root == nil {
		t.root, err = t.pager.ReadRoot()
		if err != nil {
			return nil, fmt.Errorf("failed to read root: %w", err)
		}
	}

	return t.root, nil
}

func (t *Tree) Insert(k Key, v []byte) error {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	oldRoot, err := t.Root()
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}

	newRoot, newPages, err := t.upsert(k, v, false)
	if err != nil {
		return fmt.Errorf("insertion failed: %w", err)
	}

	for i := 0; i < len(newPages); i++ {
		t.pager.Write(newPages[i])
	}

	if newRoot != nil && oldRoot != newRoot {
		t.pager.WriteRoot(newRoot)
	}

	return nil
}

func (t *Tree) Update(k Key, v []byte) error {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	oldRoot, err := t.Root()
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}

	newRoot, newPages, err := t.upsert(k, v, true)
	if err != nil {
		return fmt.Errorf("insertion failed: %w", err)
	}

	for i := 0; i < len(newPages); i++ {
		t.pager.Write(newPages[i])
	}

	if newRoot != nil && oldRoot != newRoot {
		t.pager.WriteRoot(newRoot)
	}

	return nil
}

func (t *Tree) Delete(k Key) error {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	oldRoot, err := t.Root()
	if err != nil {
		return fmt.Errorf("failed to get root: %w", err)
	}

	newRoot, newPages, err := t.delete(k)
	if err != nil {
		return fmt.Errorf("insertion failed: %w", err)
	}

	for i := 0; i < len(newPages); i++ {
		t.pager.Write(newPages[i])
	}

	if newRoot != nil && oldRoot != newRoot {
		t.pager.WriteRoot(newRoot)
	}

	return nil
}

func (t *Tree) delete(k Key) (*Page, []*Page, error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	var (
		pages []*Page
	)

	p, path, err := t.findLeaf(k)
	if p == nil {
		return nil, nil, fmt.Errorf("failed to find leaf")
	}

	existsEntry := p.Leaf().Find(k)
	if existsEntry == nil {
		return nil, nil, errNotFound
	}

	err = p.Leaf().Delete(k)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to delete key: %w", err)
	}

	if p.Leaf().Len() != 0 {
		pages = append(pages, p)
		return t.root, pages, nil
	}

	p.Free()
	pages = append(pages, p)

	next := p.ID()
	for len(path) > 0 {
		parent := path[len(path)-1]
		path = path[:len(path)-1]

		err = parent.Node().DeleteByChildID(next)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to delete child from parent: %w", err)
		}

		if parent.Node().Len() != 0 {
			return nil, append(pages, parent), nil
		}

		next = parent.ID()
		parent.Free()
		pages = append(pages, parent)
	}

	{ // remove until root
		t.root = t.pager.Alloc(0, PageTypeLeaf)
	}

	return t.root, pages, nil
}

func (t *Tree) upsert(k Key, v []byte, upsert bool) (*Page, []*Page, error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	var (
		err   error
		e     Entry
		pages []*Page
	)

	p, path, err := t.findLeaf(k)
	if p == nil {
		return nil, nil, fmt.Errorf("failed to find leaf")
	}

	if len(v)+1 > int(maxEntrySize) {
		pages, err = t.writeOverflow(p.Header().lsn, v)
		if err != nil {
			return nil, nil, err
		}

		e = NewOverflowEntry(pages[0].ID())
	} else {
		e = NewDataEntry(v)
	}

	existsEntry := p.Leaf().Find(k)
	if existsEntry != nil {
		if !upsert {
			return nil, nil, errAlreadyExists
		}

		err = p.Leaf().Update(k, e)
	} else {
		err = p.Leaf().Insert(k, e)
	}
	if nil == err {
		return t.root, append(pages, p), nil
	}

	extra := t.pager.Alloc(p.Header().lsn, PageTypeLeaf)
	pivot := p.Leaf().MoveAndPlace(extra.Leaf(), k, e)

	pages = append(pages, p)
	pages = append(pages, extra)

	for len(path) > 0 {
		next := extra.ID()
		parent := path[len(path)-1]
		path = path[:len(path)-1]

		err = parent.Node().Insert(pivot, next)
		if nil == err {
			return nil, append(pages, parent), nil
		}

		extra = t.pager.Alloc(0, PageTypeNode)
		pivot = parent.Node().MoveAndPlace(extra.Node(), pivot, next)

		pages = append(pages, parent)
		pages = append(pages, extra)
	}

	{ // split root
		r := t.pager.Alloc(0, PageTypeNode)
		r.Node().less = t.root.ID()
		err = r.Node().Insert(pivot, extra.ID())
		if err != nil {
			return nil, nil, err
		}

		t.root = r
	}

	return t.root, pages, nil
}

func (t *Tree) findLeaf(k Key) (p *Page, path []*Page, err error) {
	defer armtracer.EndTrace(armtracer.BeginTrace(""))

	p, err = t.Root()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get root: %w", err)
	}

	for p.IsNode() {
		path = append(path, p)

		next, ok := p.Node().Find(k)
		if !ok {
			return nil, nil, fmt.Errorf("failed to find leaf")
		}

		p, err = t.pager.Read(next)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read next")
		}
	}

	if p == nil {
		return nil, nil, fmt.Errorf("failed to find leaf")
	}

	return p, path, err
}

func (t *Tree) Find(k Key) (e Entry, err error) {
	p, err := t.Root()
	if err != nil {
		return nil, err
	}

	for p != nil {
		if p.IsLeaf() {
			e = p.Leaf().Find(k)
			if e == nil {
				return nil, errNotFound
			}

			if e.IsData() {
				return e[1:], nil
			}

			if e.IsOverflow() {
				next := e.GetNext()

				e, err = t.readOverflow(next)
				if err != nil {
					return nil, fmt.Errorf("read overflow page %d failed: %w", next, err)
				}

				return e, nil
			}

			panic("unknown entry type")
		}

		if p.IsNode() {
			next, ok := p.Node().Find(k)
			if !ok {
				return nil, errNotFound
			}

			p, err = t.pager.Read(next)
			if err != nil {
				return nil, fmt.Errorf("failed to read page: %w", err)
			}

			continue
		}

		panic(fmt.Errorf("unexpected page type: %d", p.Type()))
	}

	return nil, errNotFound
}

func (t *Tree) Print() error {
	root, err := t.Root()
	if err != nil {
		return err
	}

	level := []byte{}
	q := []*Page{root}
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
				continue
			}
		}

		level = append(level, ' ')
	}

	return nil
}

func (t *Tree) readOverflow(next uint64) (e Entry, err error) {
	overflow := make([]byte, 0, maxEntrySize)

	for next > 0 {
		op, err := t.pager.Read(next)
		if err != nil {
			return Entry{}, fmt.Errorf("failed to read overflow page: %w", err)
		}

		next = op.Overflow().next
		overflow = append(overflow, op.Overflow().Data()...)
	}

	return overflow, nil
}

func (t *Tree) writeOverflow(lsn uint64, v []byte) (chain []*Page, err error) {
	chain = make([]*Page, 0, len(v)/int(maxEntrySize))

	p := t.pager.Alloc(lsn, PageTypeOverflow)
	for len(v) > 0 {
		chain = append(chain, p)

		n, err := p.Overflow().Write(v)
		if err != nil {
			return nil, fmt.Errorf("failed to write entry to overflow page: %w", err)
		}

		if n == len(v) {
			break
		}

		v = v[n:]

		next := t.pager.Alloc(lsn, PageTypeOverflow)
		p.Overflow().next = next.ID()

		p = next
	}

	return chain, nil
}
