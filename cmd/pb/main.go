package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wal/internal/cmd"
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
	w, err := storage.NewPageBuffer(ctx, syncInterval, resolver.NewWriter(args))

	if err != nil {
		fmt.Println("Error creating page buffer:", err)
		return
	}

	// 100kb
	b100 := make([]byte, 100*1024)
	for i := range b100 {
		b100[i] = byte(i%26) + 'a'
	}

	// 1mb
	b1m := make([]byte, 1024*1024)
	for i := range b1m {
		b1m[i] = byte(i%26) + 'a'
	}

	// 100mb
	b100m := make([]byte, 100*1024*1024)
	for i := range b100m {
		b100m[i] = byte(i%26) + 'a'
	}

	go func() {
		for i := 0; i < 10; i++ {
			err := w.Write(b100)
			if err != nil {
				fmt.Println("Error appending entry:", err)
				return
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			err := w.Write(b1m)
			if err != nil {
				fmt.Println("Error appending entry:", err)
				return
			}
		}
	}()

	go func() {
		for i := 0; i < 3; i++ {
			err := w.Write(b100m)
			if err != nil {
				fmt.Println("Error appending entry:", err)
				return
			}
			fmt.Println("Wrote 100mb entry")
		}
	}()

	<-ctx.Done()
}
