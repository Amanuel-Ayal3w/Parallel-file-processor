// Package record holds the values that move through the pipeline.
package record

// Record is one parsed input line.
type Record struct {
	Line int
	Data map[string]any
}

// Line is a raw input line tagged with its 1-based file line number.
type Line struct {
	No  int
	Raw []byte
}

// Batch is a sequenced group of raw lines produced by the reader.
type Batch struct {
	Seq       int
	StartLine int
	Lines     []Line
}
