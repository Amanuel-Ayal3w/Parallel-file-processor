package transform

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gofetl/internal/record"
)

func TestFilterDropsNonMatch(t *testing.T) {
	chain := NewChain(
		Filter{Field: "event", Op: "eq", Value: "click"},
		Rename{From: "user", To: "user_id"},
	)
	rec, keep, err := chain.Apply(record.Record{Data: map[string]any{"event": "view", "user": "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if keep {
		t.Fatal("view should be filtered out")
	}
	if _, ok := rec.Data["user_id"]; ok {
		t.Fatal("rename ran after a drop")
	}
}

func TestFilterKeepsMatch(t *testing.T) {
	rec, keep, err := Filter{Field: "n", Op: "gte", Value: 2}.Transform(record.Record{
		Data: map[string]any{"n": int64(2)},
	})
	if err != nil || !keep || rec.Data["n"] != int64(2) {
		t.Fatalf("keep %v err %v data %#v", keep, err, rec.Data)
	}
	_, keep, err = Filter{Field: "event", Op: "contains", Value: "cli"}.Transform(record.Record{
		Data: map[string]any{"event": "click"},
	})
	if err != nil || !keep {
		t.Fatal(err, keep)
	}
}

func TestCastTimeAndBool(t *testing.T) {
	rec, keep, err := Cast{Field: "ts", To: "time"}.Transform(record.Record{
		Data: map[string]any{"ts": "2024-01-01T00:00:00Z"},
	})
	if err != nil || !keep {
		t.Fatal(err, keep)
	}
	got, ok := rec.Data["ts"].(time.Time)
	if !ok || !got.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("%#v", rec.Data["ts"])
	}
	rec, keep, err = Cast{Field: "ok", To: "bool"}.Transform(record.Record{Data: map[string]any{"ok": "true"}})
	if err != nil || !keep || rec.Data["ok"] != true {
		t.Fatalf("%#v %v", rec.Data["ok"], err)
	}
}

func TestAggregate(t *testing.T) {
	agg, err := NewAggregator("event",
		Metric{Type: "count", As: "n"},
		Metric{Type: "sum", Field: "value", As: "total"},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range []record.Record{
		{Line: 1, Data: map[string]any{"event": "click", "value": 1.5}},
		{Line: 2, Data: map[string]any{"event": "view", "value": int64(2)}},
		{Line: 3, Data: map[string]any{"event": "click", "value": 0.5}},
	} {
		if err := agg.Add(rec); err != nil {
			t.Fatal(err)
		}
	}
	rows := agg.Rows()
	if len(rows) != 2 {
		t.Fatalf("%d rows", len(rows))
	}
	if rows[0].Data["event"] != "click" || rows[0].Data["n"] != int64(2) || rows[0].Data["total"] != 2.0 {
		t.Fatalf("%#v", rows[0].Data)
	}
	if rows[1].Data["event"] != "view" || rows[1].Data["n"] != int64(1) {
		t.Fatalf("%#v", rows[1].Data)
	}
}

func TestLoadExample(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.yaml")
	body := []byte(`
transforms:
  - type: filter
    field: event
    op: eq
    value: click
  - type: aggregate
    group_by: event
    metrics:
      - type: count
        as: n
`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	chain, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if chain.Aggregator() == nil || len(chain.Steps()) != 1 {
		t.Fatalf("steps %d agg %v", len(chain.Steps()), chain.Aggregator())
	}
	_, keep, err := chain.Apply(record.Record{Data: map[string]any{"event": "view"}})
	if err != nil || keep {
		t.Fatal(err, keep)
	}
}

func TestAggregateMustBeLast(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.yaml")
	body := []byte("transforms:\n  - type: aggregate\n    group_by: event\n    metrics:\n      - {type: count, as: n}\n  - type: rename\n    from: a\n    to: b\n")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error")
	}
}
