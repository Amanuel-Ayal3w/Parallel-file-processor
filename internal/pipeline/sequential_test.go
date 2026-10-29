package pipeline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"gofetl/internal/record"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

type memSink struct {
	batches [][]record.Record
	delay   func()
}

func (m *memSink) Write(_ context.Context, recs []record.Record) error {
	if m.delay != nil {
		m.delay()
	}
	cp := make([]record.Record, len(recs))
	copy(cp, recs)
	m.batches = append(m.batches, cp)
	return nil
}

func (m *memSink) Flush(context.Context) error { return nil }
func (m *memSink) Close() error                { return nil }

func (m *memSink) records() []record.Record {
	var out []record.Record
	for _, b := range m.batches {
		out = append(out, b...)
	}
	return out
}

func TestSequentialRenameAndBatches(t *testing.T) {
	in, err := source.New(strings.NewReader("{\"user\":\"ada\",\"n\":1}\n{\"user\":\"bea\",\"n\":2}\n{\"user\":\"cy\",\"n\":3}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	out := &memSink{}
	chain := transform.NewChain(transform.Rename{From: "user", To: "user_id"})
	stats, err := RunSequential(context.Background(), in, out, chain, 2)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Read.Load() != 3 || stats.Written.Load() != 3 {
		t.Fatalf("stats %+v", stats)
	}
	if len(out.batches) != 2 || len(out.batches[0]) != 2 || len(out.batches[1]) != 1 {
		t.Fatalf("batches %#v", out.batches)
	}
	if out.records()[0].Data["user_id"] != "ada" {
		t.Fatalf("%#v", out.records()[0].Data)
	}
}

func TestSequentialBadJSON(t *testing.T) {
	in, err := source.New(strings.NewReader("{\"user\":\"ada\"}\nnot-json\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	out := sink.NewJSONL(&buf)
	_, err = RunSequential(context.Background(), in, out, transform.Chain{}, 10)
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err %v", err)
	}
}

func TestSequentialCancel(t *testing.T) {
	in, err := source.New(strings.NewReader("{\"n\":1}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = RunSequential(ctx, in, &memSink{}, transform.Chain{}, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestSequentialCSV(t *testing.T) {
	in, err := source.New(strings.NewReader("user,event\nada,click\n"), "csv")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	out := sink.NewJSONL(&buf)
	stats, err := RunSequential(context.Background(), in, out, transform.NewChain(transform.Rename{From: "user", To: "user_id"}), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.Written.Load() != 1 {
		t.Fatal(stats.Written.Load())
	}
	if strings.TrimSpace(buf.String()) != `{"event":"click","user_id":"ada"}` {
		t.Fatalf("%s", buf.String())
	}
}

func TestSequentialEmpty(t *testing.T) {
	in, err := source.New(strings.NewReader(""), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := RunSequential(context.Background(), in, &memSink{}, transform.Chain{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Read.Load() != 0 || stats.Written.Load() != 0 {
		t.Fatalf("%+v", stats)
	}
	if _, _, err := in.Next(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
