package traffic

import (
	"sync"
	"testing"
)

func TestKeyedBucketTrySpendSynchronous(t *testing.T) {
	m := NewKeyedBucket[struct{}]()
	b := m.GetOrCreateBucket(struct{}{},
		0,
		1,
		1,
		1000,
		1000,
	)

	var wg sync.WaitGroup
	wg.Add(1000)

	for range 1000 {
		go func() {
			defer wg.Done()
			if !b.TrySpend(1) {
				t.Fail()
			}
		}()
	}

	wg.Wait()

	if b.TrySpend(1) {
		t.Fail()
	}
}
