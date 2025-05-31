package traffic

import (
	"container/ring"
	"hash/maphash"
	"iter"
	"slices"
	"sync"
	"sync/atomic"
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

	ring Ring
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

// locks_excluded: ring.mu
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

// iterNodes iterates over virtual nodes for the backend.
func (r *StableRing) iterNodes(backend uint64) iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for i := uint64(0); i < r.numVirtualNodes; i++ {
			b := r.hashBackendNonce(backend, i)
			if !yield(b) {
				return
			}
		}
	}
}

func (r *StableRing) InsertBackend(backend Hash64) {
	h64 := backend.Sum64()

	r.ring.mu.Lock()
	defer r.ring.mu.Unlock()

	r.insertBackend(h64)
}

// locks_excluded: ring.mu
func (r *StableRing) insertBackend(backend uint64) {
	for n := range r.iterNodes(backend) {
		r.ring.insertBackend(n)
		r.reverseLookup[n] = backend
	}
}

func (r *StableRing) RemoveBackend(backend Hash64) (removed bool) {
	h64 := backend.Sum64()

	r.ring.mu.Lock()
	defer r.ring.mu.Unlock()

	return r.removeBackend(h64)
}

// locks_excluded: ring.mu
func (r *StableRing) removeBackend(backend uint64) (removed bool) {
	for n := range r.iterNodes(backend) {
		removed = r.ring.removeBackend(n) || removed
		delete(r.reverseLookup, n)
	}

	return removed
}

func (r *StableRing) RouteRequest(request Hash64) (backend uint64, ok bool) {
	h64 := request.Sum64()

	r.ring.mu.RLock()
	defer r.ring.mu.RUnlock()

	return r.routeRequest(h64)
}

// locks_excluded: ring.mu
func (r *StableRing) routeRequest(req uint64) (backend uint64, ok bool) {
	b, ok := r.ring.routeRequest(req)
	if ok {
		b, ok = r.reverseLookup[b]
	}
	return b, ok
}

type nodeEntry struct {
	node    uint64
	backend uint64
}

func cmpNodeEntry(a, b nodeEntry) int {
	if a.node < b.node {
		return -1
	}
	if b.node < a.node {
		return +1
	}
	return 0
}

// CachedStableRing wraps StableRing with a cache which improves the
// amortized performance of the key routing function [RouteRequest].
type CachedStableRing struct {
	// Cache nodes to a slice to enable a more performant binary search algorithm.
	cache []nodeEntry
	gen   atomic.Uint64 // generation counter
	mu    sync.RWMutex  // for cache

	ring *StableRing
}

func NewCachedStableRing(numVirtualNodes int) *CachedStableRing {
	r := &CachedStableRing{}
	r.ring = NewStableRing(numVirtualNodes) // may panic.
	return r
}

func (r *CachedStableRing) nextGen(currGen uint64) bool {
	return r.gen.CompareAndSwap(currGen, currGen+1)
}

// precondition: nextGen.
// locked_excluded: ring.ring.mu
func (r *CachedStableRing) rebuildCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = r.cache[:0]
	r.ring.ring.ring.Do(func(v any) {
		node := v.(uint64)
		r.cache = append(r.cache, nodeEntry{
			node:    node,
			backend: r.ring.reverseLookup[node],
		})
	})
}

func (r *CachedStableRing) InsertBackend(backend Hash64) {
	h64 := backend.Sum64()

	r.ring.ring.mu.Lock()
	defer r.ring.ring.mu.Unlock()
	r.insertBackend(h64)
}

// locks_excluded: ring.ring.mu
func (r *CachedStableRing) insertBackend(backend uint64) {
	currGen := r.gen.Load()
	r.ring.insertBackend(backend)
	if r.nextGen(currGen) {
		r.rebuildCache()
	}
}

func (r *CachedStableRing) RemoveBackend(backend Hash64) (removed bool) {
	h64 := backend.Sum64()

	r.ring.ring.mu.Lock()
	defer r.ring.ring.mu.Unlock()
	return r.removeBackend(h64)
}

// locks_excluded: ring.ring.mu
func (r *CachedStableRing) removeBackend(backend uint64) (removed bool) {
	currGen := r.gen.Load()
	removed = r.ring.removeBackend(backend)
	if removed && r.nextGen(currGen) {
		r.rebuildCache()
	}
	return removed
}

func (r *CachedStableRing) RouteRequest(request Hash64) (backend uint64, ok bool) {
	h64 := request.Sum64()

	r.ring.ring.mu.RLock()
	defer r.ring.ring.mu.RUnlock()

	r.mu.RLock()
	defer r.mu.RUnlock()

	// fast path always runs in logarithmic time.
	n := len(r.cache)
	if n == 0 {
		return 0, false // No nodes.
	}
	dummy := nodeEntry{h64, 0}
	i, _ := slices.BinarySearchFunc(r.cache, dummy, cmpNodeEntry)
	if i == n {
		i-- // When insert position = n, use last node.
	}
	return r.cache[i].backend, true
}
