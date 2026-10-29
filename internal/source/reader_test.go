package source

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gofetl/internal/gendata"
)

func TestJSONLLinesAndNumbers(t *testing.T) {
	in := strings.NewReader("\n{\"id\":1,\"value\":1.5}\n\n{\"id\":2}\n")
	r, err := New(in, "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	n1, line, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 2 {
		t.Fatalf("line number %d", n1)
	}
	rec, err := Parse("jsonl", nil, n1, line)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Data["id"] != int64(1) {
		t.Fatalf("id %#v", rec.Data["id"])
	}
	if rec.Data["value"] != 1.5 {
		t.Fatalf("value %#v", rec.Data["value"])
	}
	n2, _, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 4 {
		t.Fatalf("line number %d", n2)
	}
}

func TestCSVHeaderAndRow(t *testing.T) {
	r, err := New(strings.NewReader("user,event\nada,click\n"), "csv")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(r.Header(), ","); got != "user,event" {
		t.Fatalf("header %s", got)
	}
	n, line, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("line %d", n)
	}
	rec, err := Parse("csv", r.Header(), n, line)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Data["user"] != "ada" || rec.Data["event"] != "click" {
		t.Fatalf("%#v", rec.Data)
	}
}

func TestCSVFieldCount(t *testing.T) {
	_, err := Parse("csv", []string{"a", "b"}, 3, []byte("only"))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("err %v", err)
	}
}

func TestGzipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl.gz")
	if err := gendata.WriteFile(path, gendata.Options{Lines: 5, Format: "jsonl", Gzip: true}); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Format() != "jsonl" {
		t.Fatalf("format %s", r.Format())
	}
	var n int
	for {
		_, _, err := r.Next()
		if err != nil {
			break
		}
		n++
	}
	if n != 5 {
		t.Fatalf("lines %d", n)
	}
}

func TestDetect(t *testing.T) {
	if Detect("a.csv.gz") != "csv" || Detect("a.jsonl") != "jsonl" || Detect("a.txt") != "" {
		t.Fatal(Detect("a.csv.gz"), Detect("a.jsonl"), Detect("a.txt"))
	}
	var buf bytes.Buffer
	if err := gendata.Write(&buf, gendata.Options{Lines: 1, Format: "csv"}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(buf.String(), "id,event,user,value,ts\n") {
		t.Fatalf("%q", buf.String())
	}
	if _, err := os.Stat(os.DevNull); err != nil {
		t.Fatal(err)
	}
}
