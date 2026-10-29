package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRename(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.jsonl")
	out := filepath.Join(dir, "out.jsonl")
	if err := os.WriteFile(in, []byte("{\"user\":\"ada\",\"n\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := newRoot()
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	root.SetArgs([]string{"run", "--input", in, "--output", out, "--rename", "user=user_id", "--batch-size", "10"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != `{"n":1,"user_id":"ada"}` {
		t.Fatalf("%s", got)
	}
}

func TestGenAndRunCSV(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "events.csv")
	out := filepath.Join(dir, "out.jsonl")
	root := newRoot()
	root.SetArgs([]string{"gen", "--lines", "4", "--format", "csv", "--output", in})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	root = newRoot()
	root.SetArgs([]string{"run", "--input", in, "--output", out, "--rename", "user=user_id"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 4 {
		t.Fatalf("lines %d\n%s", len(lines), got)
	}
	if !strings.Contains(lines[0], `"user_id":"u0000"`) {
		t.Fatalf("%s", lines[0])
	}
}

func TestRenameFlag(t *testing.T) {
	root := newRoot()
	root.SetArgs([]string{"run", "--input", "x", "--output", "y", "--rename", "nope"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "old=new") {
		t.Fatalf("err %v", err)
	}
}
