package tx

import (
	"math/rand"
	"sync/atomic"
)

type Sequence struct {
	cur TxID
}

func NewSeq() *Sequence {
	return &Sequence{cur: TxID(rand.Int63())}
}

func (s *Sequence) Next() TxID {
	return TxID(atomic.AddUint64((*uint64)(&s.cur), 1))
}
