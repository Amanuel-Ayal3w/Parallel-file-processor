# gofetl

`gofetl` streams a large CSV or JSONL file, transforms each record, and writes JSONL.

A single reader goroutine hands batches to a fixed worker pool over a bounded queue (default `2 * workers`). One writer drains results. By default it writes them as they finish. `--ordered` restores input order with a sequence-number heap, and `--reorder-cap` limits how far later batches may run ahead of a slow one. `--on-error` is `fail` (default), `skip`, or `dlq`. Ctrl+C stops reading, drains in-flight batches, and flushes the sink. `--sequential` keeps the single-loop baseline.

```bash
gofetl gen --lines 1000000 --output events.jsonl
gofetl run --input events.jsonl --output out.jsonl --workers 8 --batch-size 1000 --queue-size 16 --rename user=user_id
```

`gofetl run` reads `--transform config.yaml` for a chain of `filter`, `rename`, `cast`, and `aggregate` steps. Aggregate is last and runs in the writer. Output is a JSONL path, `sqlite://file.db`, or `postgres://...`. See [examples/transform.yaml](examples/transform.yaml) and [docker-compose.yml](docker-compose.yml).
