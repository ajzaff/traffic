package traffic

import (
	"container/heap"
	"container/list"
	"context"
	"sync"
)

type boundedQueueElem struct {
	priority int64
	cancel   func()
}

type byMinBQP []*list.Element

func (a byMinBQP) Len() int      { return len(a) }
func (a byMinBQP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byMinBQP) Less(i, j int) bool {
	return a[i].Value.(boundedQueueElem).priority < a[j].Value.(boundedQueueElem).priority
}
func (a *byMinBQP) Push(x any) { *a = append(*a, x.(*list.Element)) }
func (a *byMinBQP) Pop() any {
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
func (q *BoundedQueue) startTask(f func(context.Context)) (cancel func()) {
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

	cancel = q.startTask(f)

	e := q.slots.PushBack(boundedQueueElem{
		cancel:   cancel,
		priority: priority,
	})

	heap.Push((*byMinBQP)(&q.minHeap), e)

	return cancel, true
}

// Insert x into an available empty slot if available or preempt a lower priority element.
//
// Insert may fail if priority is lower than the current lowest priority task.
func (q *BoundedQueue) Insert(f func(context.Context), priority int64) (cancel func(), ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if cancel, ok := q.tryInsert(f, priority); ok {
		return cancel, true
	}
	// x.slots.Len() >= x.capacity

	e := q.minHeap[0] // Peek min element

	if e.Value.(boundedQueueElem).priority >= priority {
		// We lack the priority to enqueue.
		return nil, false
	}
	// priority > minPriority

	// Cancel min priority task.
	e.Value.(boundedQueueElem).cancel()
	// We can reuse e for the new task and avoid generating garbage list elements.
	// Note: the old e.cancel cannot be called in the case f finishes early
	//       because we hold the lock q.mu.
	cancel = q.startTask(f)

	q.minHeap[0].Value = boundedQueueElem{
		cancel:   cancel,
		priority: priority,
	}

	// Fix min.
	heap.Fix((*byMinBQP)(&q.minHeap), 0)

	return cancel, true
}
