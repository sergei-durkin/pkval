package tx

import (
	"sync"
	"time"
)

type Manager struct {
	seq    *Sequence
	dld    *DeadlockDetector
	active map[TxID]struct{}
	locks  map[EntryID]lock

	mu sync.Mutex
}

type lock struct {
	rcnt  int
	wlock bool

	owners map[TxID]struct{}

	done chan struct{}
}

type TxID uint64

type TxEntry []byte

type EntryID [12]byte

type Tx struct {
	m *Manager

	id      TxID
	entries []TxEntry
	locks   []EntryID
}

func (t *Tx) Commit() bool {
	return t.m.commit(t)
}

func (t *Tx) Upgrade(id EntryID) (bool, error) {
	tick := time.NewTicker(123 * time.Millisecond)

	cnt := 0
	ok, err := t.m.upgrade(t, id)
	if err != nil {
		return false, err
	}
	for !ok && cnt < 11 {
		<-tick.C
		ok, err = t.m.upgrade(t, id)
		if err != nil {
			return false, err
		}

		cnt++
	}

	return ok, nil
}

func (t *Tx) Read(id EntryID) (bool, error) {
	ch, ok, err := t.m.read(t, id)
	if err != nil {
		return false, err
	}

	if !ok {
		<-ch
		return false, nil
	}

	return true, nil
}

func (t *Tx) Abort() {
	t.m.abort(t)
}

func (t *Tx) Write(id EntryID) (bool, error) {
	ch, ok, err := t.m.write(t, id)
	if err != nil {
		return false, err
	}
	if !ok {
		<-ch
		return false, nil
	}

	return true, nil
}

func NewManager() *Manager {
	return &Manager{
		dld:    NewDeadlockDetector(),
		seq:    NewSeq(),
		active: make(map[TxID]struct{}),
		locks:  make(map[EntryID]lock),
	}
}

func (m *Manager) abort(tx *Tx) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.active[tx.id]; !ok {
		return
	}

	for i := 0; i < len(tx.locks); i++ {
		m.release(tx, tx.locks[i])
	}

	delete(m.active, tx.id)
}

func (m *Manager) begin() *Tx {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := &Tx{
		m:  m,
		id: m.seq.Next(),
	}
	m.active[t.id] = struct{}{}

	return t
}

func (m *Manager) commit(tx *Tx) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.active[tx.id]; !ok {
		return false
	}

	for i := 0; i < len(tx.locks); i++ {
		m.release(tx, tx.locks[i])
	}

	delete(m.active, tx.id)

	return true
}

func (m *Manager) read(tx *Tx, id EntryID) (chan struct{}, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.locks[id]
	if !ok {
		m.locks[id] = lock{rcnt: 1, done: make(chan struct{}), owners: map[TxID]struct{}{tx.id: {}}}
		tx.locks = append(tx.locks, id)
		return nil, true, nil
	}

	if l.wlock {
		for o := range l.owners {
			if !m.dld.Add(tx.id, o) {
				return nil, false, errDeadlock
			}
		}

		return l.done, false, nil
	}

	l.rcnt++
	l.owners[tx.id] = struct{}{}
	m.locks[id] = l
	tx.locks = append(tx.locks, id)

	return nil, true, nil
}

func (m *Manager) upgrade(tx *Tx, id EntryID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.locks[id]
	if !ok || l.wlock {
		panic("trying to upgrade unlocked or already upgraded entry")
	}

	if l.rcnt == 1 {
		l.rcnt = 0
		l.wlock = true

		m.locks[id] = l

		return true, nil
	}

	for o := range l.owners {
		if !m.dld.Add(tx.id, o) {
			return false, errDeadlock
		}
	}

	return false, nil
}

func (m *Manager) write(tx *Tx, id EntryID) (chan struct{}, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	l, ok := m.locks[id]
	if !ok {
		m.locks[id] = lock{wlock: true, done: make(chan struct{}), owners: map[TxID]struct{}{tx.id: {}}}
		tx.locks = append(tx.locks, id)
		return nil, true, nil
	}

	for o := range l.owners {
		if !m.dld.Add(tx.id, o) {
			return nil, false, errDeadlock
		}
	}

	return l.done, false, nil
}

func (m *Manager) release(tx *Tx, id EntryID) {
	l := m.locks[id]
	delete(l.owners, tx.id)

	if l.wlock {
		delete(m.locks, id)
		close(l.done)
		return
	}

	if l.rcnt <= 0 {
		panic("inconsistent lock state")
	}

	l.rcnt--
	if l.rcnt == 0 {
		delete(m.locks, id)
		close(l.done)
		return
	}

	m.locks[id] = l
}
