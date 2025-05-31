package traffic

import (
	"sync"
	"sync/atomic"
	"time"
)

type LeakingQueue[E any] struct {
	mu      sync.Mutex
	running atomic.Bool
	rate    time.Duration
	queue   chan E
	C       chan E
}

func NewLeakingQueue[E any](rate time.Duration, capacity int) LeakingQueue[E] {
	if rate <= 0 {
		panic("rate must be positive")
	}
	return LeakingQueue[E]{
		queue: make(chan E, capacity),
		C:     make(chan E),
	}
}

func (q *LeakingQueue[E]) Stop() bool { return q.running.CompareAndSwap(true, false) }

func (q *LeakingQueue[E]) Start() bool {
	if !q.running.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		t := time.Tick(q.rate)
		for q.running.Load() {
			<-t
			if e, ok := <-q.queue; ok {
				q.C <- e
			}
		}
	}()
	return true
}

func (q *LeakingQueue[E]) TryEnqueue(e E) bool {
	enqueued := false
	q.mu.Lock()
	if len(q.queue) < cap(q.queue) {
		q.queue <- e
		enqueued = true
	}
	q.mu.Unlock()
	return enqueued
}
