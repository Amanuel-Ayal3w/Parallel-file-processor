package transform

import "gofetl/internal/record"

// Rename moves Data[From] to Data[To] when From is present.
type Rename struct {
	From string
	To   string
}

// Transform renames one field.
func (r Rename) Transform(rec record.Record) (record.Record, bool, error) {
	if v, ok := rec.Data[r.From]; ok {
		rec.Data[r.To] = v
		if r.From != r.To {
			delete(rec.Data, r.From)
		}
	}
	return rec, true, nil
}
