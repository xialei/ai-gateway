// Package scheduler implements Prefix-aware scheduling: a consistent-hash
// ring with virtual nodes and instance affinity, backed by an Instance
// Registry that tracks membership, health and load.
package scheduler

import (
	"crypto/sha1"
	"encoding/binary"
	"sort"
	"sync"
)

// ring is a consistent-hash ring of virtual nodes. Membership churn rebuilds
// the ring; ring rebuild is cheap (virtual-node sort) and rate-limited by the
// scheduler's thrash suppression so brief flaps do not destroy affinity.
type ring struct {
	mu        sync.RWMutex
	hashes    []uint64          // sorted virtual-node hashes
	vnodeTo   map[uint64]string // virtual-node hash → instance id
	instances map[string]int    // instance id → weight (0 = removed)
	vnodes    int               // virtual nodes per weight unit
}

func newRing(vnodes int) *ring {
	if vnodes <= 0 {
		vnodes = 160
	}
	return &ring{
		vnodeTo:   make(map[uint64]string),
		instances: make(map[string]int),
		vnodes:    vnodes,
	}
}

// SetInstances replaces the ring membership. Instances with weight 0 are
// omitted (treated as removed). The ring is rebuilt deterministically.
func (r *ring) SetInstances(weights map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hashes = r.hashes[:0]
	r.vnodeTo = make(map[uint64]string, len(weights)*r.vnodes)
	r.instances = make(map[string]int, len(weights))
	for id, w := range weights {
		if w <= 0 {
			continue
		}
		r.instances[id] = w
		for i := 0; i < r.vnodes*w; i++ {
			h := hashKey(id + "#" + itoa(i))
			r.vnodeTo[h] = id
			r.hashes = append(r.hashes, h)
		}
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// Members returns the live instance ids currently on the ring.
func (r *ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.instances))
	for id := range r.instances {
		out = append(out, id)
	}
	return out
}

// Empty reports whether the ring has any live member.
func (r *ring) Empty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.hashes) == 0
}

// Get returns the instance id responsible for key. ok is false when the ring
// is empty.
func (r *ring) Get(key string) (id string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return "", false
	}
	h := hashKey(key)
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if idx == len(r.hashes) {
		idx = 0 // wrap around
	}
	return r.vnodeTo[r.hashes[idx]], true
}

// GetNext returns the instance id responsible for key, skipping any id in
// exclude. It walks the ring clockwise. ok is false if every member is
// excluded (or the ring is empty).
func (r *ring) GetNext(key string, exclude map[string]struct{}) (id string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return "", false
	}
	h := hashKey(key)
	start := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if start == len(r.hashes) {
		start = 0
	}
	for i := 0; i < len(r.hashes); i++ {
		idx := (start + i) % len(r.hashes)
		cand := r.vnodeTo[r.hashes[idx]]
		if _, skip := exclude[cand]; skip {
			continue
		}
		return cand, true
	}
	return "", false
}

// GetCandidates returns up to max distinct instance ids responsible for key
// (the prefix-affinity neighbors, in ring order), skipping any id in exclude.
// The first element is the primary affinity owner; subsequent elements are the
// next clockwise owners — the set of instances whose KV cache is most likely
// to already hold the request's prefix (nearest replication). This is the
// candidate set the load-aware scheduler selects among.
func (r *ring) GetCandidates(key string, exclude map[string]struct{}, max int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 || max <= 0 {
		return nil
	}
	h := hashKey(key)
	start := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if start == len(r.hashes) {
		start = 0
	}
	out := make([]string, 0, max)
	seen := make(map[string]struct{}, max)
	for i := 0; i < len(r.hashes) && len(out) < max; i++ {
		idx := (start + i) % len(r.hashes)
		cand := r.vnodeTo[r.hashes[idx]]
		if _, skip := exclude[cand]; skip {
			continue
		}
		if _, dup := seen[cand]; dup {
			continue
		}
		seen[cand] = struct{}{}
		out = append(out, cand)
	}
	return out
}

// hashKey produces a 64-bit hash of key by taking the top 8 bytes of SHA-1.
// It is fully deterministic across processes, restarts, and architectures — a
// consistent-hash ring MUST hash a given key to the same virtual node every
// time, otherwise a gateway restart re-shards every prefix and destroys the
// KV-cache affinity the scheduler exists to preserve.
//
// SHA-1 is chosen over the faster hash/maphash and hash/fnv for correctness:
//   - maphash.MakeSeed is random per process and there is no public way to
//     construct a fixed maphash.Seed, so a "fixed seed" was never actually
//     fixed — the prior code's comment contradicted its behavior.
//   - FNV-1a is deterministic but has poor high-bit diffusion on the
//     sequential inputs the ring generates (id#0, id#1, …), which clusters
//     virtual nodes and skews the distribution badly.
//
// Cost is negligible: ~50 ns per hash, and hashing is once per Pick on the
// request path (vs hundreds of ms for the LLM generation) plus 160·N at ring
// build time (~80 µs for 10 instances), with zero allocations.
func hashKey(key string) uint64 {
	sum := sha1.Sum([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

func itoa(i int) string {
	// inline to avoid strconv import in this hot file's surface; it's just
	// for vnode id construction which is not on the per-request path.
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
