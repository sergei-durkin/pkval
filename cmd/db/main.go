package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"wal"
	"wal/internal/cmd"
	"wal/internal/db"

	"github.com/sergei-durkin/armtracer"
)

func main() {
	armtracer.Begin()
	defer armtracer.End()

	args := cmd.Parse(os.Args[1:])
	for _, arg := range args {
		if arg.Name == "help" || arg.Name == "h" {
			fmt.Println("Usage: wal [--logfile <path>] [--help]")
			return
		}
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan
		cancel()

		fmt.Println("\nShutting down...")
	}()

	writer, size, err := NewWriterReaderSeekerCloser(args)
	if err != nil {
		panic(fmt.Sprintf("failed to create writer: %v", err))
	}

	pg, err := db.NewPager(writer, uint64(size))
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	var p *db.Page
	for i := 0; i < 10; i++ {
		p = pg.Alloc(505, db.PageTypeLeaf)
		fmt.Fprintf(p, "This is page %d\n", i)
		pg.Write(p)

		fmt.Printf("Allocated page %d with ID %d\n", i, p.ID())
	}

	pg.WriteRoot(p)
	root, err := pg.ReadRoot()
	if err != nil {
		panic(fmt.Sprintf("failed to read root page: %v", err))
	}

	fmt.Printf("Current root page ID: %d\n", root.ID())

	pg.Sync()
}

func NewWriterReaderSeekerCloser(args []cmd.Arg) (wal.WriterReaderSeekerCloser, int64, error) {
	var path string

	for _, arg := range args {
		if arg.Name == "database" || arg.Name == "d" {
			path = arg.Value
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("failed to open log file: %v", err))
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, stat.Size(), nil
}
