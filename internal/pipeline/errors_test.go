package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"gofetl/internal/gendata"
	"gofetl/internal/source"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSkipBadRecords(t *testing.T) {
	defer goleak.VerifyNone(t)
	in, err := source.New(strings.NewReader("{\"id\":1}\nbad\n{\"id\":3}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	out := &memSink{}
	stats, err := Run(context.Background(), in, out, Params{
		OnError:   OnErrorSkip,
		BatchSize: 10,
		Workers:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Errors.Load() != 1 || stats.Written.Load() != 2 {
		t.Fatalf("errors %d written %d", stats.Errors.Load(), stats.Written.Load())
	}
	recs := out.records()
	if len(recs) != 2 {
		t.Fatalf("%d records", len(recs))
	}
}

func TestDeadLetterFile(t *testing.T) {
	defer goleak.VerifyNone(t)
	in, err := source.New(strings.NewReader("{\"id\":1}\nbad\n{\"id\":3}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var dlq bytes.Buffer
	out := &memSink{}
	stats, err := Run(context.Background(), in, out, Params{
		OnError:   OnErrorDLQ,
		DLQ:       &dlq,
		BatchSize: 1,
		Workers:   2,
		Ordered:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Errors.Load() != 1 || stats.Written.Load() != 2 {
		t.Fatalf("errors %d written %d", stats.Errors.Load(), stats.Written.Load())
	}
	var entry dlqEntry
	if err := json.Unmarshal([]byte(strings.TrimSpace(dlq.String())), &entry); err != nil {
		t.Fatal(err, dlq.String())
	}
	if entry.Line != 2 || entry.Raw != "bad" || entry.Error == "" {
		t.Fatalf("%+v", entry)
	}
	recs := out.records()
	if id, _ := recs[0].Data["id"].(int64); id != 1 {
		t.Fatalf("first id %v", recs[0].Data["id"])
	}
	if id, _ := recs[1].Data["id"].(int64); id != 3 {
		t.Fatalf("second id %v", recs[1].Data["id"])
	}
}

func TestFailFastNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)
	body := "{\"id\":1}\nnot-json\n" + strings.Repeat("{\"id\":2}\n", 200)
	in, err := source.New(strings.NewReader(body), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = Run(ctx, in, &memSink{}, Params{BatchSize: 1, Workers: 4, QueueSize: 2, OnError: OnErrorFail})
	if err == nil || !strings.Contains(err.Error(), "line") {
		t.Fatalf("err %v", err)
	}
}

func TestGracefulStopDrains(t *testing.T) {
	defer goleak.VerifyNone(t)
	var raw bytes.Buffer
	const lines = 5000
	if err := gendata.Write(&raw, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	in, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	stop := make(chan struct{})
	stats := &Stats{}
	errCh := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), in, &gateSink{gate: gate}, Params{
			BatchSize: 100,
			Workers:   2,
			QueueSize: 2,
			Stats:     stats,
			Stop:      stop,
		})
		errCh <- runErr
	}()

	deadline := time.Now().Add(10 * time.Second)
	for stats.BatchesRead.Load() == 0 {
		if time.Now().After(deadline) {
			close(gate)
			t.Fatal("writer never stalled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	close(gate)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pipeline did not drain")
	}
	wrote := stats.Written.Load()
	if wrote == 0 || wrote >= lines {
		t.Fatalf("wrote %d, want a partial drain of %d", wrote, lines)
	}
	if stats.Read.Load() != wrote {
		t.Fatalf("read %d wrote %d", stats.Read.Load(), wrote)
	}
}

func TestCancelNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t)
	in, err := source.New(strings.NewReader("{\"id\":1}\n"), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Run(ctx, in, &memSink{}, Params{Workers: 2})
	if err == nil {
		t.Fatal("expected cancel")
	}
}
