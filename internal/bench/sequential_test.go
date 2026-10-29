package bench

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"gofetl/internal/gendata"
	"gofetl/internal/pipeline"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func BenchmarkSequential(b *testing.B) {
	dir := b.TempDir()
	inPath := filepath.Join(dir, "in.jsonl")
	const lines = 4000
	if err := gendata.WriteFile(inPath, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		b.Fatal(err)
	}
	chain := transform.NewChain(transform.Rename{From: "user", To: "user_id"})
	b.ResetTimer()
	b.ReportAllocs()
	var written int64
	for i := 0; i < b.N; i++ {
		in, err := source.Open(inPath, "jsonl")
		if err != nil {
			b.Fatal(err)
		}
		out := sink.NewJSONL(io.Discard)
		stats, err := pipeline.RunSequential(context.Background(), in, out, chain, 500)
		in.Close()
		out.Close()
		if err != nil {
			b.Fatal(err)
		}
		written += stats.Written.Load()
	}
	b.StopTimer()
	if sec := b.Elapsed().Seconds(); sec > 0 {
		b.ReportMetric(float64(written)/sec, "records/s")
	}
}
