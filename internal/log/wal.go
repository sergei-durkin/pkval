package log

import (
	"fmt"
	"wal/internal/storage"
)

const (
	entriesCount = 1024
)

type Log struct {
	cur     int
	entries [1024]Entry

	pb *storage.PageBuffer
}

func NewLog(pb *storage.PageBuffer) *Log {
	return &Log{
		pb: pb,
	}
}

type Transaction struct {
	committed  bool
	rolledback bool
}

func (l *Log) Append(entry Entry) error {
	err := l.pb.Write(entry.Serialize())
	if err != nil {
		return err
	}

	return l.append(entry)
}

func (l *Log) append(entry Entry) error {
	if l.cur < entriesCount {
		l.entries[l.cur] = entry
		l.cur++

		return nil
	}

	activeEntries := make([]Entry, 0, entriesCount)
	txMap := make(map[uint64]*Transaction)
	for i := l.cur - 1; i >= 0; i-- {
		e := l.entries[i]

		if _, ok := txMap[e.txid]; !ok {
			txMap[e.txid] = &Transaction{}
		}

		tx := txMap[e.txid]

		if e.typ == CommitEntry {
			tx.committed = true
			continue
		}

		if e.typ == RollbackEntry {
			tx.rolledback = true
			continue
		}

		if tx.rolledback {
			continue
		}

		if tx.committed {
			// implement apply on database
			fmt.Printf("Applying transaction %d with %v entries\n", e.txid, e)
			continue
		}

		activeEntries = append(activeEntries, e)
	}

	cp := NewCheckpoint()
	err := l.pb.Write(cp.Serialize())
	if err != nil {
		return err
	}

	if len(activeEntries) >= entriesCount {
		return ErrLogFull
	}

	activeEntries = append(activeEntries, entry)
	l.cur = len(activeEntries)

	return nil
}
