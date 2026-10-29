package pipeline

import (
	"context"
	"io"
	"runtime"
	"sync"

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
	QueueSize  int
	Sequential bool
}

// batchResult is one worker's output for a single batch.
type batchResult struct {
	Seq     int
	Records []record.Record
}

// Run reads, transforms, and writes records.
// Sequential uses one goroutine. Otherwise a reader, a fixed worker pool, and one writer
// move batches through buffered channels. Output order follows completion, not input order.
func Run(ctx context.Context, in *source.Reader, out sink.Sink, p Params) (*Stats, error) {
	if p.Sequential {
		return RunSequential(ctx, in, out, p.Chain, p.BatchSize)
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

	stats := &Stats{}
	g, ctx := errgroup.WithContext(ctx)
	batches := make(chan record.Batch, p.QueueSize)
	results := make(chan batchResult, p.QueueSize)

	g.Go(func() error {
		defer close(batches)
		return readBatches(ctx, in, batches, p.BatchSize)
	})

	var wg sync.WaitGroup
	wg.Add(p.Workers)
	for range p.Workers {
		g.Go(func() error {
			defer wg.Done()
			return runWorker(ctx, in.Format(), in.Header(), p.Chain, batches, results, stats)
		})
	}
	g.Go(func() error {
		wg.Wait()
		close(results)
		return nil
	})
	g.Go(func() error {
		return writeResults(ctx, out, results, stats)
	})

	err := g.Wait()
	if ferr := out.Flush(context.Background()); err == nil {
		err = ferr
	}
	return stats, err
}

func readBatches(ctx context.Context, in *source.Reader, out chan<- record.Batch, batchSize int) error {
	seq := 0
	batch := record.Batch{}
	send := func() error {
		if len(batch.Lines) == 0 {
			return nil
		}
		batch.Seq = seq
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- batch:
		}
		seq++
		batch = record.Batch{}
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
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

func runWorker(ctx context.Context, format string, header []string, chain transform.Chain, batches <-chan record.Batch, results chan<- batchResult, stats *Stats) error {
	for batch := range batches {
		recs := make([]record.Record, 0, len(batch.Lines))
		for _, line := range batch.Lines {
			stats.Read.Add(1)
			rec, keep, err := decodeLine(format, header, line.No, line.Raw, chain)
			if err != nil {
				return err
			}
			if keep {
				recs = append(recs, rec)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case results <- batchResult{Seq: batch.Seq, Records: recs}:
		}
	}
	return nil
}

func writeResults(ctx context.Context, out sink.Sink, results <-chan batchResult, stats *Stats) error {
	for res := range results {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(res.Records) == 0 {
			continue
		}
		if err := out.Write(ctx, res.Records); err != nil {
			return err
		}
		stats.Written.Add(int64(len(res.Records)))
	}
	return nil
}
