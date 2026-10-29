// Package source streams CSV and JSONL inputs one line at a time.
package source

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxLine = 10 * 1024 * 1024

// Reader streams lines from a CSV or JSONL input.
// CSV inputs yield data lines only; the header is available from Header.
type Reader struct {
	sc      *bufio.Scanner
	format  string
	header  []string
	line    int
	closers []io.Closer
}

// Open opens path and detects the format from the extension when format is empty.
// A "-" path reads JSONL from stdin unless format is set.
// Gzip is used when the path ends in .gz.
func Open(path, format string) (*Reader, error) {
	if path == "-" {
		if format == "" {
			format = "jsonl"
		}
		return New(os.Stdin, format)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	closers := []io.Closer{f}
	var r io.Reader = f

	name := path
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("gzip: %w", err)
		}
		closers = append(closers, gz)
		r = gz
		name = strings.TrimSuffix(path, filepath.Ext(path))
	}
	if format == "" {
		format = Detect(name)
	}
	if format == "" {
		closeAll(closers)
		return nil, fmt.Errorf("cannot detect format of %q; pass --format jsonl or csv", path)
	}

	rd, err := New(r, format)
	if err != nil {
		closeAll(closers)
		return nil, err
	}
	rd.closers = closers
	return rd, nil
}

// New wraps r as a line reader. format is "jsonl" or "csv".
// For CSV, the first line is consumed as the header.
func New(r io.Reader, format string) (*Reader, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "jsonl" && format != "csv" {
		return nil, fmt.Errorf("unsupported format %q", format)
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	rd := &Reader{sc: sc, format: format}
	if format == "csv" {
		if !sc.Scan() {
			if err := sc.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("csv: missing header")
		}
		rd.line = 1
		header, err := csvFields(sc.Bytes())
		if err != nil {
			return nil, fmt.Errorf("csv header: %w", err)
		}
		if len(header) == 0 {
			return nil, fmt.Errorf("csv: empty header")
		}
		seen := make(map[string]struct{}, len(header))
		for _, h := range header {
			if h == "" {
				return nil, fmt.Errorf("csv: empty header column")
			}
			if _, ok := seen[h]; ok {
				return nil, fmt.Errorf("csv: duplicate header %q", h)
			}
			seen[h] = struct{}{}
		}
		rd.header = header
	}
	return rd, nil
}

// Format returns "jsonl" or "csv".
func (r *Reader) Format() string { return r.format }

// Header returns the CSV header, or nil for JSONL.
func (r *Reader) Header() []string { return r.header }

// Next returns the next non-empty data line and its 1-based line number.
// The returned buffer is owned by the caller.
func (r *Reader) Next() (int, []byte, error) {
	for r.sc.Scan() {
		r.line++
		b := r.sc.Bytes()
		if len(bytesTrimSpace(b)) == 0 {
			continue
		}
		cp := make([]byte, len(b))
		copy(cp, b)
		return r.line, cp, nil
	}
	if err := r.sc.Err(); err != nil {
		return 0, nil, err
	}
	return 0, nil, io.EOF
}

// Close closes the underlying file and gzip reader, if any.
func (r *Reader) Close() error {
	return closeAll(r.closers)
}

// Detect reports jsonl or csv from a file name, ignoring a trailing .gz.
func Detect(path string) string {
	name := strings.ToLower(path)
	name = strings.TrimSuffix(name, ".gz")
	switch strings.ToLower(filepath.Ext(name)) {
	case ".csv":
		return "csv"
	case ".jsonl", ".ndjson":
		return "jsonl"
	default:
		return ""
	}
}

func closeAll(cs []io.Closer) error {
	var err error
	for i := len(cs) - 1; i >= 0; i-- {
		err = errors.Join(err, cs[i].Close())
	}
	return err
}

func bytesTrimSpace(b []byte) []byte {
	i := 0
	j := len(b)
	for i < j && (b[i] == ' ' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\t' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}
