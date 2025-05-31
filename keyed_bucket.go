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
	availableTokens atomic.Int64
}

func (b *keyedBucket) TrySpend(tokens int64) bool {
	availableTokens := b.availableTokens.Load()
	if tokens > availableTokens {
		return false
	}
	return b.availableTokens.CompareAndSwap(availableTokens, availableTokens-tokens)
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
		v.availableTokens.Add(v.delta)
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
	minCost = max(0, minCost)
	maxCost = max(0, maxCost)
	maxCost = min(maxCost, capacity)
	initCapacity = min(initCapacity, capacity)
	v := &keyedBucket{
		delta:    tokensPerMin,
		minCost:  minCost,
		maxCost:  maxCost,
		capacity: capacity,
	}
	v.availableTokens.Store(initCapacity)
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
