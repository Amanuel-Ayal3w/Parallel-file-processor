package pipeline

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"gofetl/internal/record"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

// Params configures a pipeline run.
type Params struct {
	Chain      transform.Chain
	BatchSize  int
	Workers    int
	QueueSize  int // channel capacity for batches and results; 0 means 2*workers
	Sequential bool
	// SinkDelay sleeps in the writer before each batch write. It is a debug knob
	// for watching backpressure: a slow sink fills the queues and blocks the reader.
	SinkDelay time.Duration
	// Ordered writes batches by sequence number. ReorderCap is the maximum number
	// of batches allowed in flight; the reader blocks once that many are waiting
	// to be written. Zero means QueueSize+Workers. Capping at the reader, rather
	// than refusing results in the writer, avoids a deadlock when the next batch
	// is the one stuck behind a full results channel.
	Ordered    bool
	ReorderCap int
	// BeforeBatch, if set, runs on the worker before the batch is parsed.
	// Tests use it to stall one sequence number.
	BeforeBatch func(seq int)
	// OnError is fail, skip, or dlq. Empty means fail.
	OnError string
	// DLQ receives dead-letter JSONL when OnError is dlq.
	DLQ io.Writer
	// Stop, when closed, tells the reader to stop and drain work already queued.
	Stop <-chan struct{}
	// Stats, when set, receives counters. Otherwise Run allocates one.
	Stats *Stats
}

// batchResult is one worker's output for a single batch.
type batchResult struct {
	Seq     int
	Records []record.Record
	Bad     []badLine
}

// Run reads, transforms, and writes records.
// Sequential uses one goroutine. Otherwise a reader, a fixed worker pool, and one writer
// move batches through buffered channels. Output order follows completion, not input order.
func Run(ctx context.Context, in *source.Reader, out sink.Sink, p Params) (*Stats, error) {
	policy, err := normalizeOnError(p.OnError)
	if err != nil {
		return nil, err
	}
	p.OnError = policy
	if p.OnError == OnErrorDLQ && p.DLQ == nil {
		return nil, fmt.Errorf("on-error %s requires a dead-letter writer", OnErrorDLQ)
	}
	if p.Sequential {
		return runSequential(ctx, in, out, p)
	}
	return runParallel(ctx, in, out, p)
}

func runParallel(ctx context.Context, in *source.Reader, out sink.Sink, p Params) (*Stats, error) {
	if p.BatchSize <= 0 {
		p.BatchSize = defaultBatchSize
	}
	if p.Workers <= 0 {
		p.Workers = runtime.NumCPU()
	}
	if p.QueueSize <= 0 {
		p.QueueSize = 2 * p.Workers
	}

	stats := p.Stats
	if stats == nil {
		stats = &Stats{}
	}
	var dlq *bufio.Writer
	if p.DLQ != nil {
		dlq = bufio.NewWriterSize(p.DLQ, 64*1024)
	}
	g, ctx := errgroup.WithContext(ctx)
	// Both queues are bounded so a slow writer stops the reader instead of buffering the file.
	batches := make(chan record.Batch, p.QueueSize)
	results := make(chan batchResult, p.QueueSize)

	var slots chan struct{}
	if p.Ordered {
		capN := p.ReorderCap
		if capN <= 0 {
			capN = p.QueueSize + p.Workers
		}
		slots = make(chan struct{}, capN)
	}

	g.Go(func() error {
		defer close(batches)
		return readBatches(ctx, in, batches, p.BatchSize, stats, slots, p.Stop)
	})

	var wg sync.WaitGroup
	wg.Add(p.Workers)
	for range p.Workers {
		g.Go(func() error {
			defer wg.Done()
			return runWorker(ctx, in.Format(), in.Header(), p.Chain, batches, results, stats, p.BeforeBatch, p.OnError)
		})
	}
	g.Go(func() error {
		wg.Wait()
		close(results)
		return nil
	})
	g.Go(func() error {
		if p.Ordered {
			return writeOrdered(ctx, out, results, stats, p.SinkDelay, slots, dlq)
		}
		return writeResults(ctx, out, results, stats, p.SinkDelay, dlq)
	})

	err := g.Wait()
	if dlq != nil {
		if ferr := dlq.Flush(); err == nil {
			err = ferr
		}
	}
	if ferr := out.Flush(context.Background()); err == nil {
		err = ferr
	}
	return stats, err
}

func readBatches(ctx context.Context, in *source.Reader, out chan<- record.Batch, batchSize int, stats *Stats, slots chan struct{}, stop <-chan struct{}) error {
	seq := 0
	batch := record.Batch{}
	send := func() error {
		if len(batch.Lines) == 0 {
			return nil
		}
		batch.Seq = seq
		if slots != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case slots <- struct{}{}:
			}
		}
		select {
		case <-ctx.Done():
			if slots != nil {
				<-slots
			}
			return ctx.Err()
		case out <- batch:
		}
		stats.BatchesRead.Add(1)
		stats.ObserveInFlight()
		seq++
		batch = record.Batch{}
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if stop != nil {
			select {
			case <-stop:
				return send()
			default:
			}
		}
		lineNo, line, err := in.Next()
		if err == io.EOF {
			return send()
		}
		if err != nil {
			return err
		}
		if len(batch.Lines) == 0 {
			batch.StartLine = lineNo
		}
		batch.Lines = append(batch.Lines, record.Line{No: lineNo, Raw: line})
		if len(batch.Lines) >= batchSize {
			if err := send(); err != nil {
				return err
			}
		}
	}
}

func runWorker(ctx context.Context, format string, header []string, chain transform.Chain, batches <-chan record.Batch, results chan<- batchResult, stats *Stats, before func(int), onError string) error {
	for batch := range batches {
		if before != nil {
			before(batch.Seq)
		}
		recs := make([]record.Record, 0, len(batch.Lines))
		var bad []badLine
		for _, line := range batch.Lines {
			stats.Read.Add(1)
			rec, keep, err := decodeLine(format, header, line.No, line.Raw, chain)
			if err != nil {
				if herr := handleBad(onError, line.No, line.Raw, err, stats, &bad); herr != nil {
					return herr
				}
				continue
			}
			if keep {
				recs = append(recs, rec)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case results <- batchResult{Seq: batch.Seq, Records: recs, Bad: bad}:
		}
	}
	return nil
}

func writeResults(ctx context.Context, out sink.Sink, results <-chan batchResult, stats *Stats, delay time.Duration, dlq *bufio.Writer) error {
	for res := range results {
		if err := emitBatch(ctx, out, res, stats, delay, dlq); err != nil {
			return err
		}
	}
	return nil
}

func emitBatch(ctx context.Context, out sink.Sink, res batchResult, stats *Stats, delay time.Duration, dlq *bufio.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := writeDeadLetters(dlq, res.Bad); err != nil {
		return err
	}
	if len(res.Records) > 0 {
		if err := out.Write(ctx, res.Records); err != nil {
			return err
		}
		stats.Written.Add(int64(len(res.Records)))
	}
	stats.BatchesWritten.Add(1)
	return nil
}
