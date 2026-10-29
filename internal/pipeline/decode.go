package pipeline

import (
	"fmt"

	"gofetl/internal/record"
	"gofetl/internal/source"
	"gofetl/internal/transform"
)

func decodeLine(format string, header []string, lineNo int, line []byte, chain transform.Chain) (record.Record, bool, error) {
	rec, err := source.Parse(format, header, lineNo, line)
	if err != nil {
		return record.Record{}, false, err
	}
	out, keep, err := chain.Apply(rec)
	if err != nil {
		return record.Record{}, false, fmt.Errorf("line %d: %w", lineNo, err)
	}
	return out, keep, nil
}
