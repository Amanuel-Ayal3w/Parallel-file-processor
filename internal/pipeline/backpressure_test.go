package pipeline

import (
	"bytes"
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"gofetl/internal/gendata"
	"gofetl/internal/record"
	"gofetl/internal/sink"
	"gofetl/internal/source"
)

// gateSink blocks each write until gate is closed.
// The pipeline has one writer, so the first write stalls the rest of the run.
type gateSink struct {
	gate <-chan struct{}
}

func (g *gateSink) Write(ctx context.Context, _ []record.Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.gate:
		return nil
	}
}

func (g *gateSink) Flush(context.Context) error { return nil }
func (g *gateSink) Close() error                { return nil }

var _ sink.Sink = (*gateSink)(nil)

func TestReaderBlocksWhenWriterStalls(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "in.jsonl")
	const (
		lines     = 80_000
		batchSize = 1000
		workers   = 2
		queue     = 2
	)
	if err := gendata.WriteFile(path, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	in, err := source.Open(path, "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stats := &Stats{}
	errCh := make(chan error, 1)
	go func() {
		_, runErr := Run(ctx, in, &gateSink{gate: gate}, Params{
			BatchSize: batchSize,
			Workers:   workers,
			QueueSize: queue,
			Stats:     stats,
		})
		errCh <- runErr
	}()

	// batches channel + workers holding a batch + results channel + the batch inside Write.
	bound := int64(2*queue + workers + 1)
	lower := int64(queue + workers)
	deadline := time.Now().Add(15 * time.Second)
	var last int64
	stable := 0
	for {
		if time.Now().After(deadline) {
			t.Fatalf("reader did not block; batches read %d", stats.BatchesRead.Load())
		}
		select {
		case err := <-errCh:
			t.Fatalf("pipeline ended early: %v (batches read %d)", err, stats.BatchesRead.Load())
		default:
		}
		got := stats.BatchesRead.Load()
		if got >= lower && got == last {
			stable++
			if stable >= 8 {
				break
			}
		} else {
			stable = 0
			last = got
		}
		time.Sleep(5 * time.Millisecond)
	}

	got := stats.BatchesRead.Load()
	totalBatches := int64((lines + batchSize - 1) / batchSize)
	if got > bound {
		t.Fatalf("reader ran ahead: %d batches in flight, bound %d (file has %d)", got, bound, totalBatches)
	}
	if got >= totalBatches {
		t.Fatalf("reader consumed the file while the writer was stalled (%d batches)", got)
	}

	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	const heapLimit = 40 << 20
	if mem.HeapAlloc > heapLimit {
		t.Fatalf("heap alloc %d bytes while writer stalled, limit %d", mem.HeapAlloc, heapLimit)
	}

	release()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("pipeline did not finish after the writer was released")
	}
	if stats.Written.Load() != lines {
		t.Fatalf("wrote %d, want %d", stats.Written.Load(), lines)
	}
	if stats.BatchesRead.Load() != stats.BatchesWritten.Load() {
		t.Fatalf("batches read %d written %d", stats.BatchesRead.Load(), stats.BatchesWritten.Load())
	}
	if stats.MaxInFlight.Load() > bound {
		t.Fatalf("max in flight %d, bound %d", stats.MaxInFlight.Load(), bound)
	}
}

func TestSinkDelayBoundsInFlight(t *testing.T) {
	var raw bytes.Buffer
	const lines = 2000
	if err := gendata.Write(&raw, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	in, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	const workers, queue = 2, 1
	stats := &Stats{}
	_, err = Run(context.Background(), in, &memSink{}, Params{
		BatchSize: 100,
		Workers:   workers,
		QueueSize: queue,
		SinkDelay: 2 * time.Millisecond,
		Stats:     stats,
	})
	if err != nil {
		t.Fatal(err)
	}
	bound := int64(2*queue + workers + 1)
	if stats.MaxInFlight.Load() > bound {
		t.Fatalf("max in flight %d, bound %d", stats.MaxInFlight.Load(), bound)
	}
	if stats.Written.Load() != lines {
		t.Fatalf("wrote %d", stats.Written.Load())
	}
}
