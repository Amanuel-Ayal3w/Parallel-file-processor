package source

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"gofetl/internal/record"
)

// Parse decodes one raw line into a record.
func Parse(format string, header []string, lineNo int, line []byte) (record.Record, error) {
	switch format {
	case "jsonl":
		return parseJSONL(lineNo, line)
	case "csv":
		return parseCSV(header, lineNo, line)
	default:
		return record.Record{}, fmt.Errorf("line %d: unsupported format %q", lineNo, format)
	}
}

func parseJSONL(lineNo int, line []byte) (record.Record, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var data map[string]any
	if err := dec.Decode(&data); err != nil {
		return record.Record{}, fmt.Errorf("line %d: %w", lineNo, err)
	}
	if data == nil {
		data = map[string]any{}
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return record.Record{}, fmt.Errorf("line %d: extra JSON value after record", lineNo)
	}
	data, err := normalizeNumbers(data)
	if err != nil {
		return record.Record{}, fmt.Errorf("line %d: %w", lineNo, err)
	}
	return record.Record{Line: lineNo, Data: data}, nil
}

func normalizeNumbers(data map[string]any) (map[string]any, error) {
	for k, v := range data {
		n, ok := v.(json.Number)
		if !ok {
			continue
		}
		if i, err := n.Int64(); err == nil {
			data[k] = i
			continue
		}
		f, err := n.Float64()
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", k, err)
		}
		data[k] = f
	}
	return data, nil
}

func parseCSV(header []string, lineNo int, line []byte) (record.Record, error) {
	fields, err := csvFields(line)
	if err != nil {
		return record.Record{}, fmt.Errorf("line %d: %w", lineNo, err)
	}
	if len(fields) != len(header) {
		return record.Record{}, fmt.Errorf("line %d: got %d fields, header has %d", lineNo, len(fields), len(header))
	}
	data := make(map[string]any, len(header))
	for i, h := range header {
		data[h] = fields[i]
	}
	return record.Record{Line: lineNo, Data: data}, nil
}

func csvFields(line []byte) ([]string, error) {
	r := csv.NewReader(bytes.NewReader(line))
	r.FieldsPerRecord = -1
	r.ReuseRecord = true
	rec, err := r.Read()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rec))
	copy(out, rec)
	return out, nil
}
