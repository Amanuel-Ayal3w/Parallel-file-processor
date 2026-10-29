package pipeline

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"time"

	"gofetl/internal/sink"
	"gofetl/internal/transform"
)

// reorderHeap is a min-heap of batch results ordered by sequence number.
type reorderHeap []batchResult

func (h reorderHeap) Len() int           { return len(h) }
func (h reorderHeap) Less(i, j int) bool { return h[i].Seq < h[j].Seq }
func (h reorderHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *reorderHeap) Push(x any) {
	*h = append(*h, x.(batchResult))
}

func (h *reorderHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// writeOrdered emits batches in sequence order.
// slots is the in-flight window acquired by the reader; one token is released
// per batch after it is written. The heap therefore never grows past the cap,
// including when one sequence is much slower than the others.
func writeOrdered(ctx context.Context, out sink.Sink, results <-chan batchResult, stats *Stats, delay time.Duration, slots chan struct{}, dlq *bufio.Writer, agg *transform.Aggregator) error {
	var pending reorderHeap
	next := 0
	for res := range results {
		heap.Push(&pending, res)
		if n := int64(pending.Len()); n > stats.MaxReorder.Load() {
			stats.MaxReorder.Store(n)
		}
		for pending.Len() > 0 && pending[0].Seq == next {
			item := heap.Pop(&pending).(batchResult)
			if err := emitBatch(ctx, out, item, stats, delay, dlq, agg); err != nil {
				return err
			}
			next++
			if slots != nil {
				<-slots
			}
		}
	}
	if pending.Len() > 0 {
		return fmt.Errorf("ordered write ended while waiting for seq %d (%d batches buffered)", next, pending.Len())
	}
	return writeAggregate(ctx, out, agg, stats)
}
