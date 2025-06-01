package traffic

import (
	"sync"
	"sync/atomic"
	"time"
)

type keyedBucket struct {
	delta           int64
	minCost         int64
	maxCost         int64
	capacity        int64
	availableTokens int64
	mu              sync.Mutex // for availableTokens
}

func (b *keyedBucket) refillBucket() {
	b.mu.Lock()
	b.availableTokens = min(
		b.availableTokens+b.delta,
		b.capacity,
	)
	b.mu.Unlock()
}

func (b *keyedBucket) TrySpend(tokens int64) bool {
	tokens = max(tokens, b.minCost)
	if tokens > b.maxCost {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if tokens > b.availableTokens {
		return false
	}
	b.availableTokens -= tokens
	return true
}

type KeyedBucket[K comparable] struct {
	m       sync.Map // K => keyBucket
	running atomic.Bool
}

func NewKeyedBucket[K comparable]() *KeyedBucket[K] {
	return &KeyedBucket[K]{
		m: sync.Map{},
	}
}

func (b *KeyedBucket[K]) refillBuckets() {
	b.m.Range(func(_, value any) bool {
		v := value.(*keyedBucket)
		v.refillBucket()
		return true
	})
}

func (b *KeyedBucket[K]) Start() {
	if !b.running.CompareAndSwap(false, true) {
		return
	}
	go func() {
		t := time.Tick(time.Minute)
		for b.running.Load() {
			b.refillBuckets()
			<-t
		}
	}()
}

func (b *KeyedBucket[K]) Stop() bool {
	return b.running.CompareAndSwap(true, false)
}

type BucketInterface interface {
	TrySpend(token int64) bool
}

func (b *KeyedBucket[K]) GetOrCreateBucket(key K, tokensPerMin, minCost, maxCost, initCapacity, capacity int64) BucketInterface {
	if v, ok := b.m.Load(key); ok {
		return v.(*keyedBucket)
	}

	tokensPerMin = max(0, tokensPerMin)
	minCost = max(0, min(minCost, capacity))
	maxCost = max(0, min(maxCost, capacity))
	initCapacity = min(initCapacity, capacity)
	v := &keyedBucket{
		delta:           tokensPerMin,
		minCost:         minCost,
		maxCost:         maxCost,
		capacity:        capacity,
		availableTokens: initCapacity,
	}
	value, _ := b.m.LoadOrStore(key, v)
	return value.(*keyedBucket)
}

func (b *KeyedBucket[K]) GetBucket(key K) BucketInterface {
	e, ok := b.m.Load(key)
	if !ok {
		return nil
	}
	return e.(*keyedBucket)
}

func (b *KeyedBucket[K]) TrySpend(key K, tokens int64) bool {
	e := b.GetBucket(key)
	if e == nil {
		return false
	}
	return e.TrySpend(tokens)
}
