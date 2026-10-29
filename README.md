# gofetl

`gofetl` streams a large CSV or JSONL file, transforms records with a fixed worker pool, and writes them to a file, SQLite, or Postgres. The file is never loaded whole. Queues are bounded, so a slow sink stalls the reader instead of growing memory.

```bash
gofetl run --input events.jsonl --workers 8 --output postgres://localhost/db --transform config.yaml
```

## Architecture

```mermaid
flowchart LR
    File --> Reader["Reader, one goroutine"]
    Reader -->|"batches, sequence number"| Pool
    subgraph Pool [Worker pool]
        W1[Worker 1]
        W2[Worker 2]
        WN[Worker N]
    end
    Pool -->|results| Writer["Writer, one goroutine"]
    Writer --> Reorder["Reorder heap, ordered mode"]
    Reorder --> Sink
    Pool -->|bad records| DLQ[Dead-letter file]
```

One reader goroutine cuts the file into batches and stamps each batch with a sequence number. A fixed pool parses and transforms those batches. One writer flushes them to the sink, so the sink does not need to be thread-safe. `errgroup` cancels the run when any stage returns an error. The reader closes the batch channel. A closer waits for the workers and then closes the results channel, which is the only way that channel is closed.

Default batch size is 1000 lines. Each queue holds `2 * workers` batches unless `--queue-size` is set. `--ordered` emits batches by sequence number. The reader will not start more than `--reorder-cap` batches ahead of the last one written (default `queue-size + workers`), so a slow batch cannot grow the heap without bound.

## Usage

```bash
go build -o bin/gofetl ./cmd/gofetl

bin/gofetl gen --lines 1000000 --output events.jsonl
bin/gofetl run \
  --input events.jsonl \
  --output out.jsonl \
  --workers 8 \
  --batch-size 1000 \
  --rename user=user_id
```

| Flag | Meaning |
| --- | --- |
| `--input`, `--output` | Input file (`-` is stdin) and sink. Output is a JSONL path, `file://`, `sqlite://`, or `postgres://`. |
| `--format` | `jsonl` or `csv`. Default comes from the extension. `.gz` is decompressed while streaming. |
| `--workers` | Worker goroutines. Default is `GOMAXPROCS`. |
| `--batch-size` | Lines per batch and per sink write. Default 1000. |
| `--queue-size` | Capacity of the batch and result channels. Default `2 * workers`. |
| `--sequential` | Single-loop baseline, kept so later runs have a number to beat. |
| `--ordered`, `--reorder-cap` | Restore input order. The cap is the max batches in flight. |
| `--on-error` | `fail` (default), `skip`, or `dlq`. `--dlq-file` sets the dead-letter path. |
| `--sink-delay` | Sleep before each sink write. Use it to watch backpressure. |
| `--progress`, `--quiet` | One-second counters on stderr: records read and written, errors, queue depths, records/s. |
| `--pprof dir` | Write `cpu.pprof` and `heap.pprof`. |
| `--transform` | YAML chain. Cannot be combined with `--rename`. |

Ctrl+C stops the reader, drains batches already in flight, flushes the sink, and exits 0. A second Ctrl+C cancels immediately.

## Transforms

`examples/transform.yaml` shows the chain. Steps run in order. `filter`, `rename`, and `cast` run in the workers. `aggregate` must be last: it folds groups in the writer, which is the only goroutine that sees every record.

```yaml
transforms:
  - type: filter
    field: event
    op: eq
    value: click
  - type: rename
    from: user
    to: user_id
  - type: cast
    field: value
    to: float
  - type: aggregate
    group_by: event
    metrics:
      - type: count
        as: n
      - type: sum
        field: value
        as: total
```

Filter ops are `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, and `contains`. Cast targets are `int`, `float`, `bool`, and `time` (RFC3339).

## Sinks

JSONL is the default for a filesystem path. `sqlite://events.db` and `postgres://...` create `gofetl_records(line, payload)` if it is missing. SQLite inserts each batch in one transaction. Postgres loads each batch with `COPY`. Payload is the record as JSON text. `docker-compose.yml` starts a local Postgres; set `GOFETL_PG_URL` to run that test.

## Errors

| `--on-error` | Behavior |
| --- | --- |
| `fail` | Return the line number and cancel the run. |
| `skip` | Log the line and continue. |
| `dlq` | Append `{"line","raw","error"}` to the dead-letter file and continue. |

## Benchmarks

One million JSONL events, rename `user` to `user_id`, write to `/dev/null`, on 8 logical CPUs (11th Gen Intel i5-11320H). `scripts/bench.sh` rebuilds this chart.

![Throughput by worker count](docs/bench.png)

| Mode | Time | Records/s |
| --- | --- | --- |
| sequential | 4.84s | 207k |
| 1 worker | 3.68s | 272k |
| 2 workers | 2.63s | 380k |
| 4 workers | 2.65s | 377k |
| 8 workers | 2.70s | 371k |
| 16 workers | 2.72s | 368k |

One worker beats the sequential loop because the reader, the worker, and the writer overlap: the next batch is being read while the previous one is parsed and encoded. Two workers reach about 1.8× the sequential rate. Four, eight, and sixteen workers stay within 3% of that, slightly slower as the pool grows.

A CPU profile of the 8-worker run sampled 8.22s of CPU during 2.52s of wall time (326% CPU). Cumulative time splits roughly like this:

- JSON decode in the workers, `encoding/json` `Decode`: about 51%
- JSON encode in the single writer, `encoding/json` `Marshal`: about 31%
- GC mark and `mallocgc`: about 14%

The run is CPU-bound. Output is `/dev/null` and the input is about 100MB, which is not enough to hold an SSD for 2.5s, and the process stayed on CPU the whole time. Decode parallelizes, which is why the second worker helps. Encode does not: one writer marshals every record. Once two workers produce batches faster than that writer can encode them, more workers wait on the results channel. Extra goroutines then cost scheduling and allocation without more throughput. GC from per-record maps shows up beside the JSON work, which is the other reason the curve bends at two workers on this machine.

An ordered run with 8 workers on the same file landed at 379k records/s, in line with unordered. The heap stays near empty when every batch costs the same. Ordering costs parallelism when one batch is late: the reader blocks at `--reorder-cap`, which the slow-batch test checks directly.

## Design decisions

**Fixed pool.** A goroutine per batch would track the number of batches in the file. With a slow sink that number is the whole input, and each goroutine carries a stack. The pool size is the concurrency limit. The queues, not the pool, decide how much data sits in memory.

**Batch size and queue depth.** 1000 lines keeps a channel send and a sink transaction off the per-record path, and it keeps one slow batch from pinning a huge buffer. Two batches of slack per worker let the reader stay a step ahead of a brief stall. The stall test blocks the writer and checks that batches in flight stop at `2 * queue + workers + 1` while an 80,000-line file is still unread, and that the heap stays under 40MB.

**Backpressure.** The writer is the only consumer of the results channel. When it slows down, that channel fills, workers block in their send, the batch channel fills, and the reader blocks in its send. No stage drops data and no stage buffers the rest of the file. `--sink-delay` makes the same path visible without a custom sink.

**Ordering.** Sequence numbers go on at read time. The writer holds a min-heap and emits while the smallest sequence equals the next expected one. Capping the heap by refusing to receive would deadlock: the missing batch can be stuck behind a full results channel, including inside the worker that is trying to deliver it. The cap is an in-flight window the reader acquires before it sends a batch and the writer releases after that batch is written. The heap cannot exceed the window. Unordered mode skips the window and writes a batch as soon as it arrives.

**Shutdown.** Every send selects on `ctx.Done()`. On the first interrupt the reader stops after the batch it is already sending, workers finish what they have, and the writer flushes. The second interrupt cancels the context. `go.uber.org/goleak` checks the pipeline tests, including fail-fast and cancel. A subprocess test sends SIGINT to the real binary and expects a partial file and exit 0.

**CPU or I/O.** The profile above is the evidence: JSON decode, JSON encode, and allocation. The worker count is not the bottleneck once the single writer is saturated. A slower sink, such as Postgres `COPY` over the network, would move the limit to the writer again, and the same queues would keep memory flat while it catches up.

## Development

```bash
go test ./...
go test -race ./...
./scripts/bench.sh          # LINES=1000000 by default
docker compose up -d        # then: GOFETL_PG_URL=postgres://gofetl:gofetl@localhost:5432/gofetl?sslmode=disable go test ./internal/sink/
```

Dependencies stay on the standard library plus `errgroup`, `cobra`, `pgx`, `modernc.org/sqlite`, `yaml.v3`, and `goleak` in tests.
