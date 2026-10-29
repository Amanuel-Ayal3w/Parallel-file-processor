package sink

import (
	"context"
	"database/sql"
	"encoding/json"

	"gofetl/internal/record"

	_ "modernc.org/sqlite"
)

// SQLite inserts batches into gofetl_records(line, payload).
type SQLite struct {
	db *sql.DB
}

var _ Sink = (*SQLite)(nil)

// OpenSQLite creates the database file and the records table.
func OpenSQLite(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS gofetl_records (
		line INTEGER NOT NULL,
		payload TEXT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLite{db: db}, nil
}

// Write inserts the batch in one transaction.
func (s *SQLite) Write(ctx context.Context, recs []record.Record) error {
	if len(recs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO gofetl_records(line, payload) VALUES (?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, rec := range recs {
		raw, err := json.Marshal(rec.Data)
		if err != nil {
			tx.Rollback()
			return err
		}
		if _, err := stmt.ExecContext(ctx, rec.Line, string(raw)); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// Flush is a no-op. Each Write commits its own transaction.
func (s *SQLite) Flush(context.Context) error { return nil }

// Close closes the database.
func (s *SQLite) Close() error { return s.db.Close() }
