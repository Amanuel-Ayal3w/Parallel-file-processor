package pipeline

import (
	"bufio"
	"context"
	"io"

	"gofetl/internal/record"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

const defaultBatchSize = 1000

// RunSequential reads, transforms, and writes in one goroutine.
// It is the baseline later milestones are compared against.
// Bad records fail the run.
func RunSequential(ctx context.Context, in *source.Reader, out sink.Sink, chain transform.Chain, batchSize int) (*Stats, error) {
	return runSequential(ctx, in, out, Params{
		Chain:     chain,
		BatchSize: batchSize,
		OnError:   OnErrorFail,
	})
}

func runSequential(ctx context.Context, in *source.Reader, out sink.Sink, p Params) (stats *Stats, err error) {
	if p.BatchSize <= 0 {
		p.BatchSize = defaultBatchSize
	}
	stats = p.Stats
	if stats == nil {
		stats = &Stats{}
	}
	var dlq *bufio.Writer
	if p.DLQ != nil {
		dlq = bufio.NewWriterSize(p.DLQ, 64*1024)
	}
	defer func() {
		if dlq != nil {
			if ferr := dlq.Flush(); err == nil {
				err = ferr
			}
		}
		if ferr := out.Flush(context.Background()); err == nil {
			err = ferr
		}
	}()

	batch := make([]record.Record, 0, p.BatchSize)
	var bad []badLine
	flush := func() error {
		if werr := writeDeadLetters(dlq, bad); werr != nil {
			return werr
		}
		bad = bad[:0]
		if len(batch) == 0 {
			return nil
		}
		if werr := out.Write(ctx, batch); werr != nil {
			return werr
		}
		stats.Written.Add(int64(len(batch)))
		batch = batch[:0]
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if p.Stop != nil {
			select {
			case <-p.Stop:
				if ferr := flush(); ferr != nil {
					return stats, ferr
				}
				return stats, nil
			default:
			}
		}
		lineNo, line, nerr := in.Next()
		if nerr == io.EOF {
			if ferr := flush(); ferr != nil {
				return stats, ferr
			}
			return stats, nil
		}
		if nerr != nil {
			return stats, nerr
		}
		stats.Read.Add(1)
		rec, keep, derr := decodeLine(in.Format(), in.Header(), lineNo, line, p.Chain)
		if derr != nil {
			if herr := handleBad(p.OnError, lineNo, line, derr, stats, &bad); herr != nil {
				return stats, herr
			}
			continue
		}
		if !keep {
			continue
		}
		batch = append(batch, rec)
		if len(batch) >= p.BatchSize {
			if ferr := flush(); ferr != nil {
				return stats, ferr
			}
		}
	}
}
