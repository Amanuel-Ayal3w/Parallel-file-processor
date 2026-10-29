package sink

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"gofetl/internal/record"
)

func TestJSONLWrite(t *testing.T) {
	var buf bytes.Buffer
	j := NewJSONL(&buf)
	err := j.Write(context.Background(), []record.Record{
		{Line: 1, Data: map[string]any{"b": int64(1), "a": "x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(buf.String())
	if got != `{"a":"x","b":1}` {
		t.Fatalf("%s", got)
	}
}
