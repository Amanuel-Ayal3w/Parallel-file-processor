package transform

import (
	"testing"

	"gofetl/internal/record"
)

func TestRename(t *testing.T) {
	chain := NewChain(Rename{From: "user", To: "user_id"})
	in := record.Record{Line: 1, Data: map[string]any{"user": "ada", "n": int64(1)}}
	out, keep, err := chain.Apply(in)
	if err != nil || !keep {
		t.Fatal(err, keep)
	}
	if _, ok := out.Data["user"]; ok {
		t.Fatal("old key remains")
	}
	if out.Data["user_id"] != "ada" || out.Data["n"] != int64(1) {
		t.Fatalf("%#v", out.Data)
	}
}

func TestRenameMissing(t *testing.T) {
	out, keep, err := Rename{From: "user", To: "user_id"}.Transform(record.Record{
		Data: map[string]any{"n": 1},
	})
	if err != nil || !keep {
		t.Fatal(err, keep)
	}
	if _, ok := out.Data["user_id"]; ok {
		t.Fatal("invented key")
	}
}
