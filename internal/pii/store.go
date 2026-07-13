package pii

import (
	"sync"
	"time"
)

// MapStore persists id → original-entity mappings for the redact→restore
// cycle. The default in-memory implementation has a short TTL; a Redis
// adapter would satisfy this interface for cross-replica sharing and
// survivability across gateway restarts.
type MapStore interface {
	// Put stores id → entity with a TTL.
	Put(id, entity string, ttl time.Duration)
	// Get returns the entity for id. ok is false if missing or expired.
	Get(id string) (entity string, ok bool)
	// Delete removes the mapping for id (called when the request completes).
	Delete(id string)
}

// memoryMapStore is the default in-process MapStore.
type memoryMapStore struct {
	mu   sync.Mutex
	data map[string]entry
}

type entry struct {
	value   string
	expires time.Time
}

// NewMemoryMapStore returns an in-process MapStore.
func NewMemoryMapStore() MapStore {
	return &memoryMapStore{data: make(map[string]entry)}
}

func (m *memoryMapStore) Put(id, entity string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[id] = entry{value: entity, expires: time.Now().Add(ttl)}
}

func (m *memoryMapStore) Get(id string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.data[id]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expires) {
		delete(m.data, id)
		return "", false
	}
	return e.value, true
}

func (m *memoryMapStore) Delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, id)
}
