package traffic

import (
	"container/heap"
	"container/list"
	"context"
	"slices"
	"sync"
)

type boundedQueueElement struct {
	priority int64
	cancel   func()
}

type byBQEPriority []*list.Element

func (a byBQEPriority) Len() int      { return len(a) }
func (a byBQEPriority) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byBQEPriority) Less(i, j int) bool {
	return a[i].Value.(boundedQueueElement).priority < a[j].Value.(boundedQueueElement).priority
}
func (a *byBQEPriority) Push(x any) { *a = append(*a, x.(*list.Element)) }
func (a *byBQEPriority) Pop() any {
	n := a.Len() - 1
	x := (*a)[n]
	*a = (*a)[:n]
	return x
}

type BoundedQueue struct {
	slots    *list.List
	capacity int
	minHeap  []*list.Element
	mu       sync.Mutex
}

func NewBoundedQueue(capacity int) *BoundedQueue {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	return &BoundedQueue{
		slots:    list.New(),
		capacity: capacity,
	}
}

// precondition: x.slots.Len < x.capacity
// locks_excluded: mu
func (q *BoundedQueue) startTask(f func(context.Context), e *list.Element) (cancel func()) {
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		f(ctx)
		done <- struct{}{}
	}()
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
		// Release slot here.
		q.mu.Lock()
		q.slots.Remove(e)
		if i := slices.Index(q.minHeap, e); i >= 0 { // Scan for e in linear time.
			heap.Remove((*byBQEPriority)(&q.minHeap), i)
		}
		q.mu.Unlock()
	}()
	return cancel
}

// TryInsert tries to add x into an available empty slot if available.
//
// It returns the cancel function and true or nil and false.
func (q *BoundedQueue) TryInsert(f func(context.Context), priority int64) (cancel func(), ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.tryInsert(f, priority)
}

// locks_excluded: mu
func (q *BoundedQueue) tryInsert(f func(context.Context), priority int64) (cancel func(), ok bool) {
	if q.slots.Len() >= q.capacity {
		return nil, false // Insert failed, at capacity.
	}
	// x.slots.Len() < x.capacity

	e := q.slots.PushBack(boundedQueueElement{})

	cancel = q.startTask(f, e)

	e.Value = boundedQueueElement{
		cancel:   cancel,
		priority: priority,
	}
	heap.Push((*byBQEPriority)(&q.minHeap), e)

	return cancel, true
}

// Insert x into an available empty slot if available or preempt a lower priority element.
func (q *BoundedQueue) Insert(f func(context.Context), priority int64) (cancel func(), ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cancel, ok := q.tryInsert(f, priority); ok {
		return cancel, true
	}
	// x.slots.Len() >= x.capacity

	e := q.minHeap[0]

	if e.Value.(boundedQueueElement).priority >= priority {
		// We lack the priority to enqueue.
		return nil, false
	}
	// priority > minPriority

	q.cancelMin()

	cancel = q.startTask(f, e)

	e.Value = boundedQueueElement{
		cancel:   cancel,
		priority: priority,
	}

	// Fix min.
	heap.Fix((*byBQEPriority)(&q.minHeap), 0)

	return cancel, true
}

// precondition: x.slots.Len > 0
// locks_excluded: mu
func (q *BoundedQueue) cancelMin() {
	e := q.minHeap[0]                      // Peek min.
	e.Value.(boundedQueueElement).cancel() // Cancel
}
