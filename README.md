# gofetl

`gofetl` streams a large CSV or JSONL file, transforms each record, and writes JSONL. This first milestone is a single-loop baseline so later parallel runs have a number to beat.

```bash
gofetl gen --lines 1000000 --output events.jsonl
gofetl run --input events.jsonl --output out.jsonl --rename user=user_id --batch-size 1000
```

`go test -bench=BenchmarkSequential ./internal/bench` reports records/s for the sequential path.
