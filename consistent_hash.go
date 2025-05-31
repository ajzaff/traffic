package traffic

import (
	"container/ring"
	"hash/maphash"
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

	r.insertBackend(h64)
}

// locks_excluded: mu
func (r *Ring) insertBackend(backend uint64) {
	root := r.ring
	if root == nil {
		r.ring = ring.New(1)
		r.ring.Value = backend
		return
	}

	for e := root; ; {
		next := e.Next()
		if next == root || next.Value.(uint64) > backend {
			e.Link(&ring.Ring{Value: backend}) // Insert backend in correct position.
			break
		}
		e = next
	}
}

func (r *Ring) RemoveBackend(backend Hash64) (removed bool) {
	h64 := backend.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.removeBackend(h64)
}

// locks_excluded: mu
func (r *Ring) removeBackend(backend uint64) bool {
	root := r.ring
	if root == nil {
		return false // No backends.
	}

	for e := root; ; {
		next := e.Next()
		if next == root {
			return false // No backends found.
		}
		if next.Value.(uint64) == backend {
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

	return r.routeRequest(h64)
}

// locks_excluded: mu
func (r *Ring) routeRequest(req uint64) (backend uint64, ok bool) {
	root := r.ring
	if root == nil {
		return 0, false // No backends.
	}

	for e := root; ; {
		next := e.Next()
		if next == root || next.Value.(uint64) > req {
			return e.Value.(uint64), true // Backend found.
		}
		e = next
	}
}

type StableRing struct {
	// number of virtual nodes in the ring used to represent a sigle backend.
	// Ring is a special case of StableRing with numVirtualNodes = 0.
	// The more virtual nodes, the more balanced partitions result at the cost of
	// a factor of memory and lookup time.
	numVirtualNodes uint64

	// unique map hasher for generating virtual node hashes.
	h maphash.Hash

	// maps virtual nodes back to backends
	// for use in RouteRequest.
	reverseLookup map[uint64]uint64

	Ring
}

// NewStableRing constructs a StableRing with the given number of virtual nodes.
func NewStableRing(virtualNodes int) *StableRing {
	if virtualNodes < 0 {
		panic("virtual nodes must be positive")
	}
	return &StableRing{
		reverseLookup:   map[uint64]uint64{},
		numVirtualNodes: uint64(virtualNodes),
	}
}

func (r *StableRing) hashBackendNonce(backend, nonce uint64) uint64 {
	r.h.Reset()
	for backend > 0 {
		r.h.WriteByte(byte(backend))
		backend >>= 8
	}
	for nonce > 0 {
		r.h.WriteByte(byte(nonce))
		nonce >>= 8
	}
	return r.h.Sum64()
}

func (r *StableRing) InsertBackend(backend Hash64) {
	h64 := backend.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := uint64(0); i < r.numVirtualNodes; i++ {
		b := r.hashBackendNonce(h64, i)
		r.insertBackend(b)
		r.reverseLookup[b] = h64
	}
}

func (r *StableRing) RemoveBackend(backend Hash64) (removed bool) {
	h64 := backend.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := uint64(0); i < r.numVirtualNodes; i++ {
		b := r.hashBackendNonce(h64, i)
		removed = r.removeBackend(b) || removed
		delete(r.reverseLookup, b)
	}

	return removed
}

func (r *StableRing) RouteRequest(request Hash64) (backend uint64, ok bool) {
	h64 := request.Sum64()

	r.mu.RLock()
	defer r.mu.RUnlock()

	b, ok := r.routeRequest(h64)
	if !ok {
		return 0, false // No backend found.
	}
	b, ok = r.reverseLookup[b]
	return b, ok
}
