package pipeline

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func monitor(p Params, stats *Stats, depth func() (int, int)) func() {
	if !p.Progress {
		return func() {}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReporter(os.Stderr, stats, time.Second, stop, depth)
	}()
	return func() {
		close(stop)
		wg.Wait()
	}
}

func runReporter(w io.Writer, stats *Stats, every time.Duration, stop <-chan struct{}, depth func() (int, int)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	lastWritten := stats.Written.Load()
	lastAt := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			written := stats.Written.Load()
			sec := now.Sub(lastAt).Seconds()
			rate := 0.0
			if sec > 0 {
				rate = float64(written-lastWritten) / sec
			}
			batches, results := 0, 0
			if depth != nil {
				batches, results = depth()
			}
			fmt.Fprintf(w, "read=%d wrote=%d errors=%d batches_q=%d results_q=%d rate=%.0f/s\n",
				stats.Read.Load(), written, stats.Errors.Load(), batches, results, rate)
			lastWritten = written
			lastAt = now
		}
	}
}
