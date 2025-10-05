package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"wal"
	"wal/internal/cmd"
	"wal/internal/db"
	"wal/internal/db/writer"

	"github.com/sergei-durkin/armtracer"
)

func main() {
	armtracer.Begin()
	defer armtracer.End()

	args := cmd.Parse(os.Args[1:])
	for _, arg := range args {
		if arg.Name == "help" || arg.Name == "h" {
			fmt.Println("Usage: db [--database <path>] [--help]")
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

	var (
		w    wal.WriterReaderSeekerCloser
		size int64
		err  error
	)

	if true {
		w, size, err = writer.NewDBFile(args)
		if err != nil {
			panic(fmt.Sprintf("failed to create writer: %v", err))
		}
	} else {
		w = writer.NewInmemory()
	}

	pg, err := db.NewPager(w, uint64(size))
	if err != nil {
		panic(fmt.Sprintf("failed to create pager: %v", err))
	}

	const entrySize = 1 << 20

	entry := make([]byte, entrySize)
	copy(entry, []byte("test"))

	customEntry := make([]byte, entrySize)
	for i := range entrySize {
		customEntry[i] = byte(i%26) + 'a'
	}

	t := db.NewTree(pg)
	for i := 0; i < 1000; i++ {
		if i == 941 || i == 0 || i == 5555 || i == 9999 {
			err = t.Insert(append([]byte("test_"), []byte(strconv.Itoa(i))...), customEntry)
			if err != nil {
				panic(err)
			}
			continue
		}

		err = t.Insert(append([]byte("test_"), []byte(strconv.Itoa(i))...), entry)
		if err != nil {
			panic(err)
		}
	}

	pg.Sync()
	t.Print()

	e, ok := t.Find([]byte("test_"))
	fmt.Println(len(e), ok)

	e, ok = t.Find([]byte("test_0"))
	fmt.Println(len(e), ok)

	e, ok = t.Find([]byte("test_5555"))
	fmt.Println(len(e), ok)

	e, ok = t.Find([]byte("test_9999"))
	fmt.Println(len(e), ok)

	k := []byte("test_941")
	e, err = t.Find(k)
	if err != nil {
		panic(err)
	}
}
