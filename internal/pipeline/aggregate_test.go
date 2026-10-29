package pipeline

import (
	"bytes"
	"context"
	"testing"

	"gofetl/internal/gendata"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func TestAggregateInWriter(t *testing.T) {
	var raw bytes.Buffer
	if err := gendata.Write(&raw, gendata.Options{Lines: 6, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	agg, err := transform.NewAggregator("event", transform.Metric{Type: "count", As: "n"})
	if err != nil {
		t.Fatal(err)
	}
	in, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	out := &memSink{}
	stats, err := Run(context.Background(), in, out, Params{
		Chain:     transform.NewChain(transform.Filter{Field: "event", Op: "ne", Value: "purchase"}).WithAggregator(agg),
		BatchSize: 2,
		Workers:   3,
		Ordered:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Read.Load() != 6 || stats.Written.Load() != 2 {
		t.Fatalf("read %d wrote %d", stats.Read.Load(), stats.Written.Load())
	}
	recs := out.records()
	if len(recs) != 2 {
		t.Fatalf("%d groups", len(recs))
	}
	if recs[0].Data["event"] != "click" || recs[0].Data["n"] != int64(2) {
		t.Fatalf("%#v", recs[0].Data)
	}
	if recs[1].Data["event"] != "view" || recs[1].Data["n"] != int64(2) {
		t.Fatalf("%#v", recs[1].Data)
	}
}
