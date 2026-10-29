package pipeline

import "sync/atomic"

// Stats counts records observed by a run.
// Counters are atomic so workers can update them without a lock.
type Stats struct {
	Read           atomic.Int64
	Written        atomic.Int64
	Errors         atomic.Int64
	BatchesRead    atomic.Int64
	BatchesWritten atomic.Int64
	MaxInFlight    atomic.Int64
	MaxReorder     atomic.Int64
}

// ObserveInFlight records the high-water mark of batches sent but not yet written.
func (s *Stats) ObserveInFlight() {
	inFlight := s.BatchesRead.Load() - s.BatchesWritten.Load()
	for {
		cur := s.MaxInFlight.Load()
		if inFlight <= cur || s.MaxInFlight.CompareAndSwap(cur, inFlight) {
			return
		}
	}
}
