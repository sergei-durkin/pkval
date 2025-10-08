package wal_test

import (
	"context"
	"fmt"
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
