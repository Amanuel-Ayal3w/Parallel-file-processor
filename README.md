# gofetl

`gofetl` streams a large CSV or JSONL file, transforms each record, and writes JSONL.

A single reader goroutine hands batches to a fixed worker pool over a bounded queue (default `2 * workers`). One writer drains results as they finish, so output is unordered. A slow sink fills those queues and the reader blocks instead of buffering the file. `--sequential` keeps the single-loop baseline.

```bash
gofetl gen --lines 1000000 --output events.jsonl
gofetl run --input events.jsonl --output out.jsonl --workers 8 --batch-size 1000 --queue-size 16 --rename user=user_id
```

`go test -bench=. ./internal/bench` reports records/s for the sequential path.
