package transform

import (
	"fmt"
	"sort"

	"gofetl/internal/record"
)

// Metric is one aggregate measurement. Type is "count" or "sum".
type Metric struct {
	Type  string
	Field string
	As    string
}

// Aggregator folds records into group rows. Only the writer goroutine may call Add.
type Aggregator struct {
	GroupBy string
	Metrics []Metric
	groups  map[string]*acc
}

type acc struct {
	key   any
	count int64
	sums  map[string]float64
}

// NewAggregator builds a group-by aggregator.
func NewAggregator(groupBy string, metrics ...Metric) (*Aggregator, error) {
	if groupBy == "" {
		return nil, fmt.Errorf("aggregate group_by is required")
	}
	if len(metrics) == 0 {
		return nil, fmt.Errorf("aggregate requires a metric")
	}
	seen := map[string]struct{}{}
	for _, m := range metrics {
		if m.As == "" {
			return nil, fmt.Errorf("aggregate metric is missing a name")
		}
		if _, ok := seen[m.As]; ok {
			return nil, fmt.Errorf("aggregate duplicate metric %q", m.As)
		}
		seen[m.As] = struct{}{}
		switch m.Type {
		case "count":
		case "sum":
			if m.Field == "" {
				return nil, fmt.Errorf("sum metric %q requires a field", m.As)
			}
		default:
			return nil, fmt.Errorf("unknown aggregate metric %q", m.Type)
		}
	}
	return &Aggregator{GroupBy: groupBy, Metrics: metrics, groups: map[string]*acc{}}, nil
}

// Add folds one record into its group.
func (a *Aggregator) Add(rec record.Record) error {
	keyVal, ok := rec.Data[a.GroupBy]
	if !ok {
		return fmt.Errorf("line %d: missing group field %s", rec.Line, a.GroupBy)
	}
	key := fmt.Sprint(keyVal)
	sums := make(map[string]float64, len(a.Metrics))
	for _, m := range a.Metrics {
		if m.Type != "sum" {
			continue
		}
		f, ok := asFloat(rec.Data[m.Field])
		if !ok {
			return fmt.Errorf("line %d: field %s is not numeric", rec.Line, m.Field)
		}
		sums[m.As] = f
	}
	g := a.groups[key]
	if g == nil {
		g = &acc{key: keyVal, sums: map[string]float64{}}
		a.groups[key] = g
	}
	g.count++
	for name, v := range sums {
		g.sums[name] += v
	}
	return nil
}

// Rows returns one record per group, sorted by the group key.
func (a *Aggregator) Rows() []record.Record {
	keys := make([]string, 0, len(a.groups))
	for k := range a.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([]record.Record, 0, len(keys))
	for _, k := range keys {
		g := a.groups[k]
		data := map[string]any{a.GroupBy: g.key}
		for _, m := range a.Metrics {
			switch m.Type {
			case "count":
				data[m.As] = g.count
			case "sum":
				data[m.As] = g.sums[m.As]
			}
		}
		rows = append(rows, record.Record{Data: data})
	}
	return rows
}
