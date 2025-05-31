package traffic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Bucket struct {
	availableTokens int64
	newTokensPerSec int64
	capacity        int64
	minCost         int64
	maxCost         int64
	cond            sync.Cond
	running         atomic.Bool
}

func (b *Bucket) Stop() bool {
	return b.running.CompareAndSwap(true, false)
}

func (b *Bucket) Start() bool {
	if !b.running.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		ch := time.Tick(time.Second)
		for b.running.Load() {
			<-ch
			b.cond.L.Lock()
			b.availableTokens = min(b.capacity, b.availableTokens+b.newTokensPerSec)
			b.cond.Broadcast()
			b.cond.L.Unlock()
		}
	}()
	return true
}

func (b *Bucket) validateSpend(tokens int64) (int64, bool) {
	if b.maxCost > 0 && tokens > b.maxCost {
		return 0, false
	}
	if tokens < b.minCost {
		tokens = b.minCost
	}
	return tokens, true
}

func (b *Bucket) TrySpend(tokens int64) bool {
	tokens, ok := b.validateSpend(tokens)
	if !ok {
		return false
	}
	b.cond.L.Lock()
	spent := false
	if tokens < b.availableTokens {
		b.availableTokens -= tokens
		spent = true
	}
	b.cond.L.Unlock()
	return spent
}

func (b *Bucket) Spend(tokens int64) bool {
	tokens, ok := b.validateSpend(tokens)
	if !ok {
		return false
	}
	b.cond.L.Lock()
	for b.availableTokens < tokens {
		b.cond.Wait()
	}
	b.availableTokens -= tokens
	b.cond.L.Unlock()
	return true
}

func (b *Bucket) SpendContext(ctx context.Context, tokens int64) bool {
	tokens, ok := b.validateSpend(tokens)
	if !ok {
		return false
	}
	b.cond.L.Lock()
	for b.availableTokens < tokens {
		b.cond.Wait()
		select {
		case <-ctx.Done():
			b.cond.L.Unlock()
			return false
		default:
		}
	}
	b.availableTokens -= tokens
	b.cond.L.Unlock()
	return true
}
