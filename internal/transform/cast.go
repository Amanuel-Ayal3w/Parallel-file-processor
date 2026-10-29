package transform

import (
	"fmt"
	"time"

	"gofetl/internal/record"
)

// Cast converts one field to int, float, bool, or time (RFC3339).
type Cast struct {
	Field string
	To    string
}

// Transform replaces the field in place.
func (c Cast) Transform(rec record.Record) (record.Record, bool, error) {
	got, ok := rec.Data[c.Field]
	if !ok {
		return rec, true, nil
	}
	switch c.To {
	case "int":
		f, ok := asFloat(got)
		if !ok {
			return rec, false, fmt.Errorf("cast %s to int: %v", c.Field, got)
		}
		rec.Data[c.Field] = int64(f)
	case "float":
		f, ok := asFloat(got)
		if !ok {
			return rec, false, fmt.Errorf("cast %s to float: %v", c.Field, got)
		}
		rec.Data[c.Field] = f
	case "bool":
		b, ok := asBool(got)
		if !ok {
			return rec, false, fmt.Errorf("cast %s to bool: %v", c.Field, got)
		}
		rec.Data[c.Field] = b
	case "time":
		parsed, err := time.Parse(time.RFC3339, fmt.Sprint(got))
		if err != nil {
			return rec, false, fmt.Errorf("cast %s to time: %w", c.Field, err)
		}
		rec.Data[c.Field] = parsed.UTC()
	default:
		return rec, false, fmt.Errorf("cast %s: unknown type %q", c.Field, c.To)
	}
	return rec, true, nil
}
