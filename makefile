run-pb:
	rm ./tmp/*.log | go run ./cmd/pb/main.go --logfile ./tmp/wal

run-wal:
	rm ./tmp/*.log | go run ./cmd/wal/main.go --logfile ./tmp/wal

run-replay:
	rm ./tmp/*.log | go run ./cmd/replay/main.go --logdir ./tmp --logprefix wal --logfile ./tmp/wal
