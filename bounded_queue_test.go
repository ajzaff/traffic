package traffic

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBoundedQueue(t *testing.T) {
	q := NewBoundedQueue(10)

	var cancelCount atomic.Int64

	var wg sync.WaitGroup
	wg.Add(10)

	for i := range 10 {
		go func(i int) {
			defer wg.Done()
			_, ok := q.Insert(func(ctx context.Context) {
				<-ctx.Done()
				t.Logf("P0 task #%d cancelled", i)
				cancelCount.Add(1)
			}, 0)
			if !ok {
				t.Error("TryInsert failed to insert initial P0 task")
			}
			t.Logf("P0 task #%d started...", i)
		}(i)
	}

	wg.Wait()

	_, ok := q.TryInsert(func(ctx context.Context) {}, 10)
	if ok {
		t.Error("TryInsert unexpectedly succeeded inserting P0 task #11, beyond capacity of 10")
	}

	wg.Add(10)

	for i := range 10 {
		go func(i int) {
			defer wg.Done()
			_, ok := q.Insert(func(ctx context.Context) {
				<-ctx.Done() // Never done!
				t.Error("a P100 task was cancelled unexpectedly")
			}, 100)
			if !ok {
				t.Error("TryInsert failed to insert a P100 task")
			}
			t.Logf("P100 task #%d started...", i)
		}(i)
	}

	wg.Wait()

	_, ok = q.TryInsert(func(ctx context.Context) {}, 0)
	if ok {
		t.Error("TryInsert unexpectedly succeeded inserting P100 task #11, beyond capacity of 10")
	}

	// Check 10 original tasks cancelled!
	if got, want := cancelCount.Load(), int64(10); got != want {
		t.Errorf("cancel count is not correct, wanted %d, got %d", want, got)
	}

	// Check expected P sum
	sum := int64(0)
	for e := q.slots.Front(); e != nil; e = e.Next() {
		sum += e.Value.(boundedQueueElem).priority
	}
	if gotSum, wantSum := sum, 10*int64(100); gotSum != wantSum {
		t.Errorf("P sum is not correct, wanted %d, got %d", wantSum, gotSum)
	}
}
