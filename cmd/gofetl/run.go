package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"gofetl/internal/pipeline"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func newRunCmd() *cobra.Command {
	var (
		input         string
		output        string
		format        string
		batchSize     int
		workers       int
		queueSize     int
		reorderCap    int
		sinkDelay     time.Duration
		sequential    bool
		ordered       bool
		onError       string
		dlqPath       string
		transformPath string
		pprofDir      string
		progress      bool
		quiet         bool
		renames       []string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Read, transform, and write records",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			chain, err := chainFromRenames(renames)
			if transformPath != "" {
				if len(renames) > 0 {
					return fmt.Errorf("pass either --transform or --rename")
				}
				chain, err = transform.Load(transformPath)
			}
			if err != nil {
				return err
			}
			if quiet {
				progress = false
			}
			if pprofDir != "" {
				if err := os.MkdirAll(pprofDir, 0o755); err != nil {
					return err
				}
				cpu, err := os.Create(filepath.Join(pprofDir, "cpu.pprof"))
				if err != nil {
					return err
				}
				defer cpu.Close()
				if err := pprof.StartCPUProfile(cpu); err != nil {
					return err
				}
				defer pprof.StopCPUProfile()
				defer func() {
					heap, err := os.Create(filepath.Join(pprofDir, "heap.pprof"))
					if err != nil {
						fmt.Fprintln(os.Stderr, "gofetl: heap profile:", err)
						return
					}
					defer heap.Close()
					if err := pprof.WriteHeapProfile(heap); err != nil {
						fmt.Fprintln(os.Stderr, "gofetl: heap profile:", err)
					}
				}()
			}

			runCtx, cancelRun := context.WithCancel(cmd.Context())
			stop := make(chan struct{})
			sigCh := make(chan os.Signal, 2)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigCh)
			sigDone := make(chan struct{})
			go func() {
				defer close(sigDone)
				select {
				case <-sigCh:
					fmt.Fprintln(os.Stderr, "gofetl: interrupt, draining in-flight work")
					close(stop)
				case <-runCtx.Done():
					return
				}
				select {
				case <-sigCh:
					fmt.Fprintln(os.Stderr, "gofetl: interrupt, canceling")
					cancelRun()
				case <-runCtx.Done():
				}
			}()
			defer func() {
				cancelRun()
				<-sigDone
			}()

			in, err := source.Open(input, format)
			if err != nil {
				return err
			}
			defer in.Close()
			out, err := sink.Open(runCtx, output)
			if err != nil {
				return err
			}
			defer out.Close()

			var dlq io.WriteCloser
			if onError == pipeline.OnErrorDLQ {
				path := dlqPath
				if path == "" {
					if input == "-" {
						path = "gofetl.dlq.jsonl"
					} else {
						path = input + ".dlq.jsonl"
					}
				}
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				dlq = f
				defer f.Close()
			}

			start := time.Now()
			stats, err := pipeline.Run(runCtx, in, out, pipeline.Params{
				Chain:      chain,
				BatchSize:  batchSize,
				Workers:    workers,
				QueueSize:  queueSize,
				Sequential: sequential,
				SinkDelay:  sinkDelay,
				Ordered:    ordered,
				ReorderCap: reorderCap,
				OnError:    onError,
				DLQ:        dlq,
				Stop:       stop,
				Progress:   progress,
			})
			elapsed := time.Since(start)
			if stats != nil {
				printSummary(stats, elapsed)
			}
			return err
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "input CSV or JSONL file, or - for stdin")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output: JSONL path, file://, sqlite://, or postgres://")
	cmd.Flags().StringVar(&format, "format", "", "input format: jsonl or csv (default: from extension)")
	cmd.Flags().StringVar(&transformPath, "transform", "", "YAML file of filter, rename, cast, and aggregate steps")
	cmd.Flags().IntVar(&batchSize, "batch-size", 1000, "lines per batch and sink write")
	cmd.Flags().IntVar(&workers, "workers", runtime.NumCPU(), "worker goroutines")
	cmd.Flags().IntVar(&queueSize, "queue-size", 0, "capacity of the batch and result queues (default 2*workers)")
	cmd.Flags().DurationVar(&sinkDelay, "sink-delay", 0, "debug delay before each sink write, to surface backpressure")
	cmd.Flags().BoolVar(&sequential, "sequential", false, "use the single-loop baseline instead of the worker pool")
	cmd.Flags().BoolVar(&ordered, "ordered", false, "write batches in input order")
	cmd.Flags().IntVar(&reorderCap, "reorder-cap", 0, "max batches in flight when --ordered (default queue-size+workers)")
	cmd.Flags().StringVar(&onError, "on-error", pipeline.OnErrorFail, "what to do with a bad record: fail, skip, or dlq")
	cmd.Flags().StringVar(&dlqPath, "dlq-file", "", "dead-letter JSONL path used when --on-error=dlq")
	cmd.Flags().BoolVar(&progress, "progress", true, "print read, write, error, and queue counters once a second")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "disable the progress line")
	cmd.Flags().StringVar(&pprofDir, "pprof", "", "write cpu.pprof and heap.pprof into this directory")
	cmd.Flags().StringArrayVar(&renames, "rename", nil, "rename a field, as old=new (repeatable)")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func chainFromRenames(renames []string) (transform.Chain, error) {
	steps := make([]transform.Transformer, 0, len(renames))
	for _, spec := range renames {
		from, to, ok := strings.Cut(spec, "=")
		if !ok || from == "" || to == "" {
			return transform.Chain{}, fmt.Errorf("rename %q: want old=new", spec)
		}
		steps = append(steps, transform.Rename{From: from, To: to})
	}
	return transform.NewChain(steps...), nil
}

func printSummary(stats *pipeline.Stats, elapsed time.Duration) {
	written := stats.Written.Load()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(written) / elapsed.Seconds()
	}
	fmt.Fprintf(os.Stderr, "read %d wrote %d errors %d in %s (%.0f records/s)\n",
		stats.Read.Load(), written, stats.Errors.Load(), elapsed.Round(time.Millisecond), rate)
}
