package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"gofetl/internal/gendata"
	"gofetl/internal/record"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func TestParallelMatchesSequential(t *testing.T) {
	var raw bytes.Buffer
	if err := gendata.Write(&raw, gendata.Options{Lines: 250, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	chain := transform.NewChain(transform.Rename{From: "user", To: "user_id"})
	seqIn, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	seqOut := &memSink{}
	seqStats, err := Run(context.Background(), seqIn, seqOut, Params{
		Chain: chain, BatchSize: 16, Sequential: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	parIn, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	parOut := &memSink{}
	parStats, err := Run(context.Background(), parIn, parOut, Params{
		Chain: chain, BatchSize: 16, Workers: 4, QueueSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if seqStats.Read.Load() != parStats.Read.Load() || seqStats.Written.Load() != parStats.Written.Load() {
		t.Fatalf("sequential read=%d wrote=%d parallel read=%d wrote=%d",
			seqStats.Read.Load(), seqStats.Written.Load(), parStats.Read.Load(), parStats.Written.Load())
	}
	if seqStats.Written.Load() != 250 {
		t.Fatalf("wrote %d", seqStats.Written.Load())
	}
	if strings.Join(canon(seqOut.records()), "\n") != strings.Join(canon(parOut.records()), "\n") {
		t.Fatal("parallel output differs from sequential")
	}
}

func TestParallelErrorUnblocks(t *testing.T) {
	body := "{\"id\":1}\nnot-json\n" + strings.Repeat("{\"id\":2}\n", 100)
	in, err := source.New(strings.NewReader(body), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Run(ctx, in, &memSink{}, Params{BatchSize: 1, Workers: 4, QueueSize: 1})
	if err == nil || !strings.Contains(err.Error(), "line") {
		t.Fatalf("err %v", err)
	}
}

func TestParallelEmpty(t *testing.T) {
	in, err := source.New(strings.NewReader(""), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Run(context.Background(), in, &memSink{}, Params{Workers: 2, BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Read.Load() != 0 || stats.Written.Load() != 0 {
		t.Fatalf("read %d wrote %d", stats.Read.Load(), stats.Written.Load())
	}
}

func TestParallelCancel(t *testing.T) {
	in, err := source.New(strings.NewReader("{\"id\":1}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Run(ctx, in, &memSink{}, Params{Workers: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func canon(recs []record.Record) []string {
	out := make([]string, len(recs))
	for i, rec := range recs {
		raw, err := json.Marshal(rec.Data)
		if err != nil {
			out[i] = err.Error()
			continue
		}
		out[i] = string(raw)
	}
	sort.Strings(out)
	return out
}
