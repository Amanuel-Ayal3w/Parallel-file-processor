// Package sink writes batches of records to an output.
package sink

import (
	"context"

	"gofetl/internal/record"
)

// Sink accepts batches of records.
// Write must finish with recs before returning; the caller may reuse the backing array.
type Sink interface {
	Write(ctx context.Context, recs []record.Record) error
	Flush(ctx context.Context) error
	Close() error
}
