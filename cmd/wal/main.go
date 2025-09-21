package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wal/internal/cmd"
	"wal/internal/log"
	"wal/internal/resolver"
	"wal/internal/storage"
)

func main() {
	args := cmd.Parse(os.Args[1:])
	for _, arg := range args {
		if arg.Name == "help" || arg.Name == "h" {
			fmt.Println("Usage: wal [--logfile <path>] [--help]")
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		cancel()

		fmt.Println("\nShutting down...")
	}()

	syncInterval := 4 * time.Second
	pb, err := storage.NewPageBuffer(ctx, syncInterval, resolver.NewWriter(args))
	w := log.NewLog(pb)

	if err != nil {
		fmt.Println("Error creating page buffer:", err)
		return
	}

	template := "HelloWorldHelloWorldHelloWorldHelloWorldHelloWorld"

	go func() {
		cc := 0
		for i := 0; i < 342; i++ {
			begin := log.NewBegin(uint64(i + 1))
			commit := log.NewCommit(uint64(i + 1))

			if i == 333 {
				w.Append(log.NewCheckpoint())
			}

			w.Append(begin)
			for j := int32(0); j < rand.Int31n(10); j++ {
				s := template[:5+j*5]
				write := log.NewWrite(uint64(i+1), "help", []byte(fmt.Sprintf("%s_%d_%d", s, i+1, 1)))
				w.Append(write)
			}
			w.Append(commit)
			cc++
		}
		fmt.Println("Wrote", cc, "entries")
	}()

	// b1mb := make([]byte, 1024*1024)
	// for i := range b1mb {
	// 	b1mb[i] = byte(i%26) + 'a'
	// }
	//
	// go func() {
	// 	cc := 0
	// 	for i := 0; i < 342; i++ {
	// 		begin := log.NewBegin(uint64(i + 1))
	// 		write := log.NewWrite(uint64(i+1), "largeEntry", b1mb)
	// 		commit := log.NewCommit(uint64(i + 1))
	//
	// 		w.Append(begin)
	// 		w.Append(write)
	// 		w.Append(commit)
	// 		cc++
	// 	}
	// 	fmt.Println("Wrote", cc, "larges entries")
	// }()

	<-ctx.Done()
}
