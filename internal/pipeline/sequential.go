package pipeline

import (
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
func RunSequential(ctx context.Context, in *source.Reader, out sink.Sink, chain transform.Chain, batchSize int) (stats *Stats, err error) {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	stats = &Stats{}
	defer func() {
		if ferr := out.Flush(context.Background()); err == nil {
			err = ferr
		}
	}()

	batch := make([]record.Record, 0, batchSize)
	flush := func() error {
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
		lineNo, line, nerr := in.Next()
		if nerr == io.EOF {
			if err := flush(); err != nil {
				return stats, err
			}
			return stats, nil
		}
		if nerr != nil {
			return stats, nerr
		}
		stats.Read.Add(1)
		rec, keep, derr := decodeLine(in.Format(), in.Header(), lineNo, line, chain)
		if derr != nil {
			return stats, derr
		}
		if !keep {
			continue
		}
		batch = append(batch, rec)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
}
