package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"gofetl/internal/pipeline"
	"gofetl/internal/sink"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func newRunCmd() *cobra.Command {
	var (
		input     string
		output    string
		format    string
		batchSize int
		renames   []string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Read, transform, and write records sequentially",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			chain, err := chainFromRenames(renames)
			if err != nil {
				return err
			}
			in, err := source.Open(input, format)
			if err != nil {
				return err
			}
			defer in.Close()
			out, err := sink.OpenJSONL(output)
			if err != nil {
				return err
			}
			defer out.Close()

			start := time.Now()
			stats, err := pipeline.RunSequential(cmd.Context(), in, out, chain, batchSize)
			elapsed := time.Since(start)
			if stats != nil {
				printSummary(stats, elapsed)
			}
			return err
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "input CSV or JSONL file, or - for stdin")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output JSONL file, or - for stdout")
	cmd.Flags().StringVar(&format, "format", "", "input format: jsonl or csv (default: from extension)")
	cmd.Flags().IntVar(&batchSize, "batch-size", 1000, "records per sink write")
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
