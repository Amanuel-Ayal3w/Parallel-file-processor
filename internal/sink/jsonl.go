package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"gofetl/internal/record"
)

// JSONL writes one JSON object per line.
type JSONL struct {
	w *bufio.Writer
	c io.Closer
}

var _ Sink = (*JSONL)(nil)

// NewJSONL writes JSONL to w. Close flushes but does not close w.
func NewJSONL(w io.Writer) *JSONL {
	return &JSONL{w: bufio.NewWriterSize(w, 256*1024)}
}

// OpenJSONL creates or truncates path. "-" writes to stdout and does not close it.
func OpenJSONL(path string) (*JSONL, error) {
	if path == "-" {
		return NewJSONL(os.Stdout), nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	j := NewJSONL(f)
	j.c = f
	return j, nil
}

// Write encodes each record as a single JSON line.
func (j *JSONL) Write(ctx context.Context, recs []record.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := json.Marshal(rec.Data)
		if err != nil {
			return err
		}
		if _, err := j.w.Write(raw); err != nil {
			return err
		}
		if err := j.w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

// Flush pushes buffered lines to the underlying writer.
func (j *JSONL) Flush(context.Context) error {
	return j.w.Flush()
}

// Close flushes and closes the file when OpenJSONL created one.
func (j *JSONL) Close() error {
	err := j.Flush(context.Background())
	if j.c != nil {
		err = errors.Join(err, j.c.Close())
	}
	return err
}
