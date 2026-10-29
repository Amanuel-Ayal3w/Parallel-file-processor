package transform

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type fileConfig struct {
	Transforms []stepConfig `yaml:"transforms"`
}

type stepConfig struct {
	Type    string         `yaml:"type"`
	Field   string         `yaml:"field"`
	Op      string         `yaml:"op"`
	Value   any            `yaml:"value"`
	From    string         `yaml:"from"`
	To      string         `yaml:"to"`
	GroupBy string         `yaml:"group_by"`
	Metrics []metricConfig `yaml:"metrics"`
}

type metricConfig struct {
	Type  string `yaml:"type"`
	Field string `yaml:"field"`
	As    string `yaml:"as"`
}

// Load reads a transform chain from a YAML file.
// An aggregate step must be last. It runs in the writer, after per-record steps.
func Load(path string) (Chain, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Chain{}, err
	}
	var cfg fileConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Chain{}, fmt.Errorf("transform config: %w", err)
	}
	return build(cfg.Transforms)
}

func build(steps []stepConfig) (Chain, error) {
	var rec []Transformer
	var agg *Aggregator
	for i, step := range steps {
		switch step.Type {
		case "rename":
			if step.From == "" || step.To == "" {
				return Chain{}, fmt.Errorf("transform %d: rename requires from and to", i)
			}
			rec = append(rec, Rename{From: step.From, To: step.To})
		case "filter":
			if step.Field == "" || step.Op == "" {
				return Chain{}, fmt.Errorf("transform %d: filter requires field and op", i)
			}
			rec = append(rec, Filter{Field: step.Field, Op: step.Op, Value: step.Value})
		case "cast":
			if step.Field == "" || step.To == "" {
				return Chain{}, fmt.Errorf("transform %d: cast requires field and to", i)
			}
			rec = append(rec, Cast{Field: step.Field, To: step.To})
		case "aggregate":
			if i != len(steps)-1 {
				return Chain{}, fmt.Errorf("aggregate must be the last transform")
			}
			metrics := make([]Metric, len(step.Metrics))
			for j, m := range step.Metrics {
				metrics[j] = Metric{Type: m.Type, Field: m.Field, As: m.As}
			}
			var err error
			agg, err = NewAggregator(step.GroupBy, metrics...)
			if err != nil {
				return Chain{}, err
			}
		default:
			return Chain{}, fmt.Errorf("transform %d: unknown type %q", i, step.Type)
		}
	}
	return NewChain(rec...).WithAggregator(agg), nil
}
