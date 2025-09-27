run-pb:
	rm ./tmp/*.log | go run -tags=armtracer ./cmd/pb/main.go --logfile ./tmp/wal

run-wal:
	rm ./tmp/*.log | go run -tags=armtracer ./cmd/wal/main.go --logfile ./tmp/wal

run-replay:
	rm ./tmp/*.log | go run -tags=armtracer ./cmd/replay/main.go --logdir ./tmp --logprefix wal --logfile ./tmp/wal
