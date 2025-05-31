package traffic

import (
	"container/ring"
	"sync"
)

// Hash64 represents all types with [Sum64].
//
// See [hash.Hash64].
type Hash64 interface {
	Sum64() uint64
}

// Ring implements a data structure for consistently hashing requests to backends.
//
// The zero ring is ready to use.
// Ring is safe to use for concurrent use.
type Ring struct {
	ring *ring.Ring
	mu   sync.RWMutex
}

func (r *Ring) InsertBackend(backend Hash64) {
	h64 := backend.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	root := r.ring
	if root == nil {
		r.ring = ring.New(1)
		r.ring.Value = h64
		return
	}

	for e := root; ; {
		next := e.Next()
		if next == root || next.Value.(uint64) > h64 {
			e.Link(&ring.Ring{Value: h64}) // Insert backend in correct position.
			break
		}
		e = next
	}
}

func (r *Ring) RemoveBackend(backend Hash64) (removed bool) {
	h64 := backend.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	root := r.ring
	if root == nil {
		return false // No backends.
	}

	for e := root; ; {
		next := e.Next()
		if next == root {
			return false // No backends found.
		}
		if next.Value.(uint64) == h64 {
			next.Unlink(1) // Remove backend.
			break
		}
		e = next
	}
	return true
}

func (r *Ring) RouteRequest(request Hash64) (backend uint64, ok bool) {
	h64 := request.Sum64()

	r.mu.RLock()
	defer r.mu.RUnlock()

	root := r.ring
	if root == nil {
		return 0, false // No backends.
	}

	for e := root; ; {
		next := e.Next()
		if next == root || next.Value.(uint64) > h64 {
			return e.Value.(uint64), true // Backend found.
		}
		e = next
	}
}
