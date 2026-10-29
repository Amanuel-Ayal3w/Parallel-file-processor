package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gofetl/internal/gendata"
)

func TestGracefulInterrupt(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "gofetl")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	in := filepath.Join(dir, "in.jsonl")
	out := filepath.Join(dir, "out.jsonl")
	const lines = 8000
	if err := gendata.WriteFile(in, gendata.Options{Lines: lines, Format: "jsonl"}); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "run",
		"--input", in,
		"--output", out,
		"--batch-size", "100",
		"--workers", "2",
		"--queue-size", "2",
		"--sink-delay", "30ms",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	select {
	case err := <-wait:
		if err != nil {
			t.Fatalf("exit %v\n%s", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("process did not exit after interrupt\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "draining") {
		t.Fatalf("stderr %s", stderr.String())
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	n := bytes.Count(got, []byte("\n"))
	if n == 0 || n >= lines {
		t.Fatalf("wrote %d records of %d after interrupt\n%s", n, lines, stderr.String())
	}
}
