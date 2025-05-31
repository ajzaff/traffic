package traffic

import (
	"sync"
	"time"
)

type approxWindow struct {
	start time.Time
	count int64
}

type ApproxWindow struct {
	duration        time.Duration
	tokensPerWindow int64
	prevWindow      approxWindow
	currCount       int64
	mu              sync.Mutex
}

func NewApproxWindow(duration time.Duration, tokensPerWindow int64) ApproxWindow {
	now := time.Now()
	return ApproxWindow{
		duration:        duration,
		tokensPerWindow: tokensPerWindow,
		prevWindow: approxWindow{
			start: now,
		},
	}
}

func (w *ApproxWindow) newWindow(start time.Time) {
	w.prevWindow.start = start
	w.prevWindow.count = w.currCount
	w.currCount = 0
}

func (w *ApproxWindow) TrySpend(tokens int64) (spent bool) {
	w.mu.Lock()
	now := time.Now()
	overlap := now.Sub(w.prevWindow.start).Seconds() / w.duration.Seconds()
	if overlap >= 1 {
		overlap = 1
		w.newWindow(now)
	}
	count := float64(w.currCount) + float64(w.prevWindow.count)*(1-overlap)
	count += float64(tokens)
	if int64(count) < w.tokensPerWindow {
		w.currCount += tokens
		spent = true
	}
	w.mu.Unlock()
	return spent
}
