package wal_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
	"wal/internal/cmd"
	"wal/internal/db"
	"wal/internal/db/writer"
	"wal/internal/resolver"
	"wal/internal/storage"

	"github.com/sergei-durkin/armtracer"
)

func BenchmarkWritePageBuffer(b *testing.B) {
	ctx := context.Background()
	syncInterval := 1 * time.Hour
	provider := resolver.NewWriter([]cmd.Arg{
		{Name: resolver.LOG_FILE, Value: "./tmp_test/wal"},
	})
	pb, err := storage.NewPageBuffer(ctx, syncInterval, provider)
	if err != nil {
		b.Fatal("failed to create PageBuffer:", err)
	}

	data := make([]byte, 1024) // 1KB data

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := pb.Write(data)
		if err != nil {
			b.Error("failed to write to PageBuffer:", err)
		}
	}
}

func BenchmarkTree(b *testing.B) {
	armtracer.Begin()
	defer armtracer.End()

	pg, err := db.NewPager(writer.NewInmemory(), 0)
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	const entrySize = 1 << 8

	entry := make([]byte, entrySize)
	copy(entry, []byte("test"))

	customEntry := make([]byte, entrySize)
	for i := range entrySize {
		customEntry[i] = byte(i%26) + 'a'
	}

	t := db.NewTree(pg)
	for i := 0; i < b.N; i++ {
		err = t.Insert([]byte(fmt.Sprintf("test_%d", i)), entry)
		if err != nil {
			panic(err)
		}
	}

	pg.Sync()
}

func BenchmarkLeafFind(b *testing.B) {
	armtracer.Begin()
	defer armtracer.End()

	pg, err := db.NewPager(writer.NewInmemory(), 0)
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	entry := []byte{'0'}
	page := pg.Alloc(0, db.PageTypeLeaf)
	leaf := page.Leaf()

	for i := 0; i < 256; i++ {
		_ = leaf.Insert([]byte(fmt.Sprintf("%d", i)), entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = leaf.Find([]byte("255"))
	}
}

func BenchmarkLeafInsert(b *testing.B) {
	armtracer.Begin()
	defer armtracer.End()

	pg, err := db.NewPager(writer.NewInmemory(), 0)
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	entry := []byte{'0'}
	page := pg.Alloc(0, db.PageTypeLeaf)
	leaf := page.Leaf()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = leaf.Insert([]byte(fmt.Sprintf("%d", i)), entry)
		if err != nil {
			page = pg.Alloc(0, db.PageTypeLeaf)
			leaf = page.Leaf()
		}
	}
}

const LN = 1 << 1         // len size
const N = 1 << 22         // page size
const K = 1 << 3          // key size
const V = 1 << 12         // value size
const P = LN + K + V + LN // element size
const M = N / P           // count elements

func init() {
	fmt.Fprintf(os.Stderr, "Benchmark:\n\tPage size: %d\n\tKey size: %d\n\tValue size: %d\n\tCount elements: %d\n", N, K, V, M)
}

// | len | key | len | value |
func BenchmarkReadPageSeq(b *testing.B) {
	armtracer.Begin()
	defer armtracer.End()

	k := (uint64(100500100))
	p := [N]byte{}
	cur := 0
	for i := 0; i < M-1; i++ {
		binary.BigEndian.PutUint16(p[cur:], uint16(K))
		cur += LN

		binary.BigEndian.PutUint64(p[cur:], k)
		cur += K

		binary.BigEndian.PutUint16(p[cur:], uint16(V))
		cur += LN + V
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur := 0
		for j := 0; j < M-1; j++ {
			// read len key
			lnK := binary.BigEndian.Uint16(p[cur:])
			if K != lnK {
				panic("found corrupted page")
			}
			cur += LN

			// read key
			kk := binary.BigEndian.Uint64(p[cur:])
			if k != kk {
				panic("found corrupted page")
			}
			cur += K

			// read len value
			ln := binary.BigEndian.Uint16(p[cur:])
			if V != ln {
				panic("found corrupted page")
			}
			cur += LN + V
		}
	}
}

// | len | key | len | key | ... | value | len | value | len |
func BenchmarkReadPageCompressed(b *testing.B) {
	armtracer.Begin()
	defer armtracer.End()

	k := (uint64(100500100))
	p := [N]byte{}
	cur := 0
	tail := N - V - LN
	for i := 0; i < M-1; i++ {
		binary.BigEndian.PutUint16(p[cur:], uint16(K))
		cur += LN

		binary.BigEndian.PutUint64(p[cur:], k)
		cur += K

		binary.BigEndian.PutUint16(p[tail:], uint16(V))
		tail -= V + LN
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur := 0
		for j := 0; j < M-1; j++ {
			// read len key
			lnK := binary.BigEndian.Uint16(p[cur:])
			if K != lnK {
				panic("found corrupted page")
			}
			cur += LN

			// read key
			kk := binary.BigEndian.Uint64(p[cur:])
			if k != kk {
				panic("found corrupted page")
			}
			cur += K
		}
	}
}
