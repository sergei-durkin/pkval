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

	key := []byte("the_key")
	entry := make([]byte, 1024)
	for i := 0; i < len(entry); i++ {
		entry[i] = byte(i%26) + 'a'
	}
	largeEntry := make([]byte, 7*1024)
	for i := 0; i < len(largeEntry); i++ {
		largeEntry[i] = byte(i%26) + 'a'
	}

	var p *db.Page
	p = pg.Alloc(505, db.PageTypeNode)
	p.Leaf().Insert([]byte("test"), []byte("pest"))
	pg.Write(p)

	for i := 0; i < 650; i++ {
		p, err = pg.Read(1)
		if err != nil {
			panic(fmt.Errorf("cannot read first page %w", err))
		}

		k := db.Key(strconv.Itoa(i))

		err = p.Leaf().Insert(k, []byte("test"))
		if err != nil {
			m := pg.Alloc(505, db.PageTypeLeaf)
			beforeDst := m.Leaf().Len()
			beforeSrc := p.Leaf().Len()
			pivot := p.Leaf().MoveHalf(m.Leaf())

			if k.Less(pivot) {
				p.Leaf().Insert(k, []byte("test"))
			} else {
				m.Leaf().Insert(k, []byte("test"))
			}

			pg.Write(m)

			fmt.Printf("Splitted page %d with ID %d and pivot %s. After len = [%d/%d], Before len = [%d/%d]\n", p.ID(), m.ID(), pivot, p.Leaf().Len(), m.Leaf().Len(), beforeSrc, beforeDst)
		}

		pg.Write(p)
	}

	for i := 0; i < 0; i++ {
		p = pg.Alloc(505, db.PageTypeLeaf)
		p.Leaf().Insert([]byte("test"), []byte("pest"))
		pg.Write(p)

		p, err = pg.Read(6)
		if err != nil {
			fmt.Println(err)
			continue
		}

		err = p.Leaf().Insert(append(key, byte(i%9)+'0'), []byte("test splits"))
		if err != nil {
			m := pg.Alloc(505, db.PageTypeLeaf)
			beforeDst := m.Leaf().Len()
			beforeSrc := p.Leaf().Len()
			pivot := p.Leaf().MoveHalf(m.Leaf())
			pg.Write(m)
			fmt.Printf("Splitted page %d with ID %d and pivot %s. Src len = [%d/%d], Dst len = [%d/%d]\n", p.ID(), m.ID(), pivot, p.Leaf().Len(), m.Leaf().Len(), beforeSrc, beforeDst)
		}

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
