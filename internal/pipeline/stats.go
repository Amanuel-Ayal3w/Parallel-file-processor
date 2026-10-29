package pipeline

import "sync/atomic"

// Stats counts records observed by a run.
// Counters are atomic so workers can update them without a lock.
type Stats struct {
	Read    atomic.Int64
	Written atomic.Int64
	Errors  atomic.Int64
}
