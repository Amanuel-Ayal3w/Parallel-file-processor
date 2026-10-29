package pipeline

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"gofetl/internal/gendata"
	"gofetl/internal/source"
)

func TestOrderedOutputWaitsForSlowBatch(t *testing.T) {
	var raw bytes.Buffer
	const lines = 40
	if err := gendata.Write(&raw, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	const capN = 4
	hold := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	defer release()

	in, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	stats := &Stats{}
	out := &memSink{}
	errCh := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), in, out, Params{
			BatchSize:  1,
			Workers:    4,
			QueueSize:  4,
			Ordered:    true,
			ReorderCap: capN,
			Stats:      stats,
			BeforeBatch: func(seq int) {
				if seq == 0 {
					<-hold
				}
			},
		})
		errCh <- runErr
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("reorder window did not fill: read %d maxReorder %d", stats.BatchesRead.Load(), stats.MaxReorder.Load())
		}
		if stats.BatchesRead.Load() >= int64(capN) && stats.MaxReorder.Load() >= 2 {
			break
		}
		select {
		case err := <-errCh:
			t.Fatalf("pipeline ended early: %v", err)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := stats.BatchesRead.Load(); got > int64(capN) {
		t.Fatalf("reader passed the reorder cap: %d batches in flight, cap %d", got, capN)
	}
	if stats.MaxReorder.Load() > int64(capN) {
		t.Fatalf("reorder buffer %d exceeds cap %d", stats.MaxReorder.Load(), capN)
	}

	release()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ordered pipeline did not finish")
	}
	recs := out.records()
	if len(recs) != lines {
		t.Fatalf("got %d records", len(recs))
	}
	for i, rec := range recs {
		id, _ := rec.Data["id"].(int64)
		if id != int64(i+1) {
			t.Fatalf("position %d has id %d", i, id)
		}
	}
	if stats.MaxReorder.Load() > int64(capN) {
		t.Fatalf("max reorder %d", stats.MaxReorder.Load())
	}
}

func TestUnorderedEmitsSlowBatchLate(t *testing.T) {
	var raw bytes.Buffer
	if err := gendata.Write(&raw, gendata.Options{Lines: 20, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	hold := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	defer release()

	in, err := source.New(bytes.NewReader(raw.Bytes()), "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	out := &memSink{}
	errCh := make(chan error, 1)
	go func() {
		_, runErr := Run(context.Background(), in, out, Params{
			BatchSize: 1,
			Workers:   4,
			QueueSize: 4,
			BeforeBatch: func(seq int) {
				if seq == 0 {
					<-hold
				}
			},
		})
		errCh <- runErr
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("unordered writer did not emit a later batch")
		}
		if len(out.records()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	first, _ := out.records()[0].Data["id"].(int64)
	if first == 1 {
		t.Fatal("unordered writer waited for the slow first batch")
	}
	release()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline did not finish")
	}
}
