package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"gofetl/internal/record"
)

// Postgres loads batches with COPY into gofetl_records(line, payload).
type Postgres struct {
	conn *pgx.Conn
}

var _ Sink = (*Postgres)(nil)

// OpenPostgres connects and creates the records table when it is missing.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS gofetl_records (
		line BIGINT NOT NULL,
		payload TEXT NOT NULL
	)`); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return &Postgres{conn: conn}, nil
}

// Write copies the batch into Postgres.
func (p *Postgres) Write(ctx context.Context, recs []record.Record) error {
	if len(recs) == 0 {
		return nil
	}
	n, err := p.conn.CopyFrom(ctx,
		pgx.Identifier{"gofetl_records"},
		[]string{"line", "payload"},
		pgx.CopyFromSlice(len(recs), func(i int) ([]any, error) {
			raw, err := json.Marshal(recs[i].Data)
			if err != nil {
				return nil, err
			}
			return []any{int64(recs[i].Line), string(raw)}, nil
		}),
	)
	if err != nil {
		return err
	}
	if int(n) != len(recs) {
		return fmt.Errorf("postgres copy wrote %d of %d records", n, len(recs))
	}
	return nil
}

// Flush is a no-op. COPY commits with the batch.
func (p *Postgres) Flush(context.Context) error { return nil }

// Close closes the connection.
func (p *Postgres) Close() error { return p.conn.Close(context.Background()) }
