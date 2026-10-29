package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestRunSQLiteTransform(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.jsonl")
	dbPath := filepath.Join(dir, "out.db")
	cfg := filepath.Join(dir, "t.yaml")
	if err := os.WriteFile(in, []byte("{\"user\":\"ada\",\"n\":1}\n{\"user\":\"bea\",\"n\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	yaml := "transforms:\n  - type: rename\n    from: user\n    to: user_id\n  - type: filter\n    field: n\n    op: gte\n    value: 2\n"
	if err := os.WriteFile(cfg, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	root := newRoot()
	root.SetArgs([]string{"run", "--input", in, "--output", "sqlite://" + dbPath, "--transform", cfg, "--workers", "2"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	var payload string
	if err := db.QueryRow(`SELECT COUNT(*), MIN(payload) FROM gofetl_records`).Scan(&n, &payload); err != nil {
		t.Fatal(err)
	}
	if n != 1 || payload != `{"n":2,"user_id":"bea"}` {
		t.Fatalf("n=%d payload=%s", n, payload)
	}
}
