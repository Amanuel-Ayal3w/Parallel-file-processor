package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPprofWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.jsonl")
	out := filepath.Join(dir, "out.jsonl")
	prof := filepath.Join(dir, "prof")
	if err := os.WriteFile(in, []byte("{\"id\":1}\n{\"id\":2}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := newRoot()
	root.SetArgs([]string{"run", "--input", in, "--output", out, "--quiet", "--pprof", prof, "--workers", "2"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cpu.pprof", "heap.pprof"} {
		info, err := os.Stat(filepath.Join(prof, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
	}
}
