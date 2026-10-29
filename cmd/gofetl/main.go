// Command gofetl streams CSV and JSONL files through a transform pipeline.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "gofetl:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "gofetl",
		Short: "Stream a large CSV or JSONL file through a parallel transform pipeline",
		Long: `gofetl reads a CSV or JSONL file, transforms each record, and writes the results.

The sequential runner is the baseline. Later modes add a bounded worker pool.`,
		SilenceErrors: true,
	}
	root.AddCommand(newRunCmd(), newGenCmd())
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	return root
}
