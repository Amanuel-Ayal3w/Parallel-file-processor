package pipeline

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestReporterEmitsRate(t *testing.T) {
	defer goleak.VerifyNone(t)
	stats := &Stats{}
	stats.Read.Store(10)
	stats.Written.Store(4)
	stats.Errors.Store(1)
	var buf lockedBuf
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runReporter(&buf, stats, 15*time.Millisecond, stop, func() (int, int) { return 2, 1 })
	}()
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(buf.String(), "batches_q=2") || !strings.Contains(buf.String(), "results_q=1") {
		if time.Now().After(deadline) {
			t.Fatalf("report %q", buf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	<-done
}
