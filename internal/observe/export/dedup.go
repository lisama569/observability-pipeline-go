package export

import (
	"container/list"
	"context"
	"sync"

	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

// DedupExporter wraps an exporter so that a retried batch carrying a
// previously accepted batch id is acknowledged without being delivered twice.
// The accepted-id cache is bounded (oldest evicted); a re-delivery of an
// already-evicted id may reach the backend again, which is why the batch id
// must remain stable across all in-flight retries.
type DedupExporter struct {
	mu       sync.Mutex
	inner    Exporter
	capacity int
	ll       *list.List
	index    map[string]*list.Element
}

// NewDedupExporter wraps exporter with a bounded accepted-id cache.
func NewDedupExporter(exporter Exporter, seenCapacity int) *DedupExporter {
	if seenCapacity < 1 {
		seenCapacity = 1
	}
	return &DedupExporter{
		inner:    exporter,
		capacity: seenCapacity,
		ll:       list.New(),
		index:    make(map[string]*list.Element),
	}
}

// Export decodes just enough of the envelope to read batch id, deduplicates
// accepted ids, and forwards unseen batches to the inner exporter.
func (d *DedupExporter) Export(ctx context.Context, data []byte) error {
	b, err := wire.Decode(data)
	if err != nil {
		// Malformed/unsupported payloads are permanently rejected.
		return Permanent(err)
	}
	d.mu.Lock()
	if _, ok := d.index[b.BatchID]; ok {
		d.mu.Unlock()
		// Previously accepted: acknowledge as success, do not re-deliver.
		return nil
	}
	d.mu.Unlock()

	if err := d.inner.Export(ctx, data); err != nil {
		// Only remember the id once the backend accepted it.
		return err
	}
	d.remember(b.BatchID)
	return nil
}

func (d *DedupExporter) remember(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.index[id]; ok {
		d.ll.MoveToBack(el)
		return
	}
	el := d.ll.PushBack(id)
	d.index[id] = el
	if d.ll.Len() > d.capacity {
		front := d.ll.Front()
		d.ll.Remove(front)
		delete(d.index, front.Value.(string))
	}
}

// SeenCount returns the number of distinct accepted batch ids cached.
func (d *DedupExporter) SeenCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ll.Len()
}
