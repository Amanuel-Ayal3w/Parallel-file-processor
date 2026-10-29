// Package gendata writes synthetic event files for benchmarks and tests.
package gendata

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Options controls generated data.
type Options struct {
	Lines  int
	Format string // jsonl or csv
	Gzip   bool
}

// WriteFile creates path and writes synthetic records.
// Paths ending in .gz are compressed even when Gzip is false.
func WriteFile(path string, opts Options) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	out := io.Writer(f)
	if opts.Gzip || strings.HasSuffix(strings.ToLower(path), ".gz") {
		gz := gzip.NewWriter(f)
		defer gz.Close()
		out = gz
	}
	return Write(out, opts)
}

// Write emits opts.Lines records. CSV output includes a header even when Lines is 0.
func Write(w io.Writer, opts Options) error {
	format := opts.Format
	if format == "" {
		format = "jsonl"
	}
	if format != "jsonl" && format != "csv" {
		return fmt.Errorf("unsupported format %q", format)
	}
	if opts.Lines < 0 {
		return fmt.Errorf("lines must be >= 0")
	}
	if format == "csv" {
		if _, err := io.WriteString(w, "id,event,user,value,ts\n"); err != nil {
			return err
		}
	}
	events := [...]string{"click", "view", "purchase"}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= opts.Lines; i++ {
		event := events[(i-1)%len(events)]
		user := fmt.Sprintf("u%04d", (i-1)%1000)
		value := float64((i%50)+1) / 2
		ts := base.Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		var err error
		if format == "csv" {
			_, err = fmt.Fprintf(w, "%d,%s,%s,%.1f,%s\n", i, event, user, value, ts)
		} else {
			_, err = fmt.Fprintf(w, "{\"id\":%d,\"event\":%q,\"user\":%q,\"value\":%.1f,\"ts\":%q}\n", i, event, user, value, ts)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
