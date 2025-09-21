package wal_test

import (
	"context"
	"testing"
	"time"
	"wal/internal/cmd"
	"wal/internal/resolver"
	"wal/internal/storage"
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
