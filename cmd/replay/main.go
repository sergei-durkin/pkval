package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"
	"wal"
	"wal/internal/cmd"
	"wal/internal/log"
	"wal/internal/replay"
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

	syncInterval := 2 * time.Second
	pb, err := storage.NewPageBuffer(ctx, syncInterval, resolver.NewWriter(args))
	w := log.NewLog(pb)

	if err != nil {
		fmt.Println("Error creating page buffer:", err)
		return
	}

	template := "HelloWorldHelloWorldHelloWorldHelloWorldHelloWorld"

	fmt.Println("Writing log entries...")

	for i := 0; i < 342; i++ {
		begin := log.NewBegin(uint64(i + 1))
		commit := log.NewCommit(uint64(i + 1))

		if i == 333 {
			fmt.Println("Writing checkpoint entry...")
			w.Append(log.NewCheckpoint())
		}

		w.Append(begin)
		for j := int32(0); j < rand.Int31n(10); j++ {
			s := template[:5+j*5]
			write := log.NewWrite(uint64(i+1), "help", []byte(fmt.Sprintf("%s_%d_%d", s, i+1, 1)))
			w.Append(write)
		}
		w.Append(commit)
	}

	time.Sleep(3 * time.Second)

	readers, err := GetReaders(args)
	if err != nil {
		fmt.Println("Error getting readers:", err)
		return
	}

	r := replay.NewReplay(readers)
	logs, err := r.Replay()
	if err != nil {
		fmt.Println("Error during replay:", err)
		return
	}

	for _, l := range logs {
		if l.Type() == log.WriteEntry {
			fmt.Printf("\tTX WRITE\t%d\n", l.TxID())
			fmt.Printf("\t\t%s\t%s\n", l.Key, string(l.Data))
			continue
		}
		if l.Type() == log.BeginEntry {
			fmt.Printf("TX BEGIN\t%d\n", l.TxID())
			continue
		}
		if l.Type() == log.CommitEntry {
			fmt.Printf("TX COMMIT\t%d\n", l.TxID())
			continue
		}
	}

	time.Sleep(3 * time.Second)
}

func GetReaders(args []cmd.Arg) ([]wal.ReaderCloser, error) {
	var dir string
	var prefix string

	for _, arg := range args {
		if arg.Name == "logdir" || arg.Name == "d" {
			dir = arg.Value
		}
		if arg.Name == "logprefix" || arg.Name == "p" {
			prefix = arg.Value
		}
	}

	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	entries, err := d.ReadDir(-1)
	if err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		if a.IsDir() || b.IsDir() {
			return -1
		}

		if a.Name() < b.Name() {
			return -1
		}

		if a.Name() > b.Name() {
			return 1
		}

		return 0
	})

	logfiles := []wal.ReaderCloser{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fmt.Println("Found log file:", entry.Name())

		if prefix != "" && len(entry.Name()) >= len(prefix) && entry.Name()[:len(prefix)] == prefix {
			f, err := os.OpenFile(dir+"/"+entry.Name(), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}

			logfiles = append(logfiles, f)
		}
	}

	return logfiles, nil
}
