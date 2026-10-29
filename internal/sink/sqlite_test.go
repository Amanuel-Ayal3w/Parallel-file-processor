package sink

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"gofetl/internal/record"
)

func TestSQLitePath(t *testing.T) {
	abs, err := sqlitePath("sqlite:///tmp/events.db")
	if err != nil || abs != "/tmp/events.db" {
		t.Fatalf("%q %v", abs, err)
	}
	rel, err := sqlitePath("sqlite://events.db")
	if err != nil || rel != "events.db" {
		t.Fatalf("%q %v", rel, err)
	}
}

func TestSQLiteRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.db")
	s, err := Open(context.Background(), "sqlite://"+path)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Write(context.Background(), []record.Record{
		{Line: 2, Data: map[string]any{"user_id": "ada"}},
		{Line: 3, Data: map[string]any{"user_id": "bea"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM gofetl_records`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows %d", n)
	}
	var payload string
	if err := db.QueryRow(`SELECT payload FROM gofetl_records WHERE line = 2`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != `{"user_id":"ada"}` {
		t.Fatalf("%s", payload)
	}
}

func TestPostgresRoundTrip(t *testing.T) {
	dsn := os.Getenv("GOFETL_PG_URL")
	if dsn == "" {
		t.Skip("set GOFETL_PG_URL to run the postgres sink test")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pg := s.(*Postgres)
	if _, err := pg.conn.Exec(ctx, `TRUNCATE gofetl_records`); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, []record.Record{{Line: 1, Data: map[string]any{"event": "click"}}}); err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := pg.conn.QueryRow(ctx, `SELECT payload FROM gofetl_records WHERE line = 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != `{"event":"click"}` {
		t.Fatalf("%s", payload)
	}
}
