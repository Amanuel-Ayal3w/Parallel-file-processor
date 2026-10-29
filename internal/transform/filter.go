package transform

import (
	"fmt"
	"strings"

	"gofetl/internal/record"
)

// Filter keeps a record when field op value is true.
// Ops: eq, ne, gt, lt, gte, lte, contains.
type Filter struct {
	Field string
	Op    string
	Value any
}

// Transform drops records that do not match.
func (f Filter) Transform(rec record.Record) (record.Record, bool, error) {
	got, ok := rec.Data[f.Field]
	if !ok {
		return rec, false, nil
	}
	match, err := matchOp(f.Op, got, f.Value)
	if err != nil {
		return rec, false, fmt.Errorf("filter %s: %w", f.Field, err)
	}
	return rec, match, nil
}

func matchOp(op string, got, want any) (bool, error) {
	switch op {
	case "eq":
		return same(got, want), nil
	case "ne":
		return !same(got, want), nil
	case "contains":
		return strings.Contains(fmt.Sprint(got), fmt.Sprint(want)), nil
	case "gt", "lt", "gte", "lte":
		gf, ok := asFloat(got)
		if !ok {
			return false, fmt.Errorf("value %v is not numeric", got)
		}
		wf, ok := asFloat(want)
		if !ok {
			return false, fmt.Errorf("comparison %v is not numeric", want)
		}
		switch op {
		case "gt":
			return gf > wf, nil
		case "lt":
			return gf < wf, nil
		case "gte":
			return gf >= wf, nil
		default:
			return gf <= wf, nil
		}
	default:
		return false, fmt.Errorf("unknown op %q", op)
	}
}
