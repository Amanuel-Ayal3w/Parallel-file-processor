// Package transform applies per-record steps such as rename, filter, and cast.
package transform

import "gofetl/internal/record"

// Transformer maps one record to another.
// A false keep value drops the record. Transformers must not retain rec.Data.
type Transformer interface {
	Transform(rec record.Record) (record.Record, bool, error)
}

// Chain applies steps in order. The zero value passes records through.
type Chain struct {
	steps []Transformer
}

// NewChain returns a chain of steps.
func NewChain(steps ...Transformer) Chain {
	return Chain{steps: steps}
}

// Steps returns the per-record steps.
func (c Chain) Steps() []Transformer { return c.steps }

// Apply runs every step. The first drop or error stops the chain.
func (c Chain) Apply(rec record.Record) (record.Record, bool, error) {
	cur := rec
	for _, step := range c.steps {
		next, keep, err := step.Transform(cur)
		if err != nil || !keep {
			return next, keep, err
		}
		cur = next
	}
	return cur, true, nil
}
