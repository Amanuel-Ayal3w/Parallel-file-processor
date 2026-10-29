package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"gofetl/internal/gendata"
)

func newGenCmd() *cobra.Command {
	var (
		lines  int
		output string
		format string
		gz     bool
	)
	cmd := &cobra.Command{
		Use:   "gen",
		Short: "Write a synthetic CSV or JSONL file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			if err := gendata.WriteFile(output, gendata.Options{
				Lines:  lines,
				Format: format,
				Gzip:   gz,
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "wrote %d %s records to %s\n", lines, format, output)
			return nil
		},
	}
	cmd.Flags().IntVar(&lines, "lines", 1000, "number of records")
	cmd.Flags().StringVarP(&output, "output", "o", "", "destination path")
	cmd.Flags().StringVar(&format, "format", "jsonl", "jsonl or csv")
	cmd.Flags().BoolVar(&gz, "gzip", false, "gzip the output")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}
