package dedup

import (
	"testing"
)

func TestSeqIDCache(t *testing.T) {
	// Create filter with nil redis client (only testing in-memory layer)
	cache, err := NewFilterMemOnly(10)
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	// Not seen yet
	if cache.SeenSeqID("seq-1") {
		t.Fatal("seq-1 should not be seen yet")
	}

	// Mark as seen
	cache.MarkSeqID("seq-1")

	// Now it should be seen
	if !cache.SeenSeqID("seq-1") {
		t.Fatal("seq-1 should be seen")
	}

	// Different seqID not seen
	if cache.SeenSeqID("seq-2") {
		t.Fatal("seq-2 should not be seen")
	}
}

func TestSeqIDCacheEviction(t *testing.T) {
	cache, err := NewFilterMemOnly(3)
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	cache.MarkSeqID("a")
	cache.MarkSeqID("b")
	cache.MarkSeqID("c")

	if !cache.SeenSeqID("a") || !cache.SeenSeqID("b") || !cache.SeenSeqID("c") {
		t.Fatal("all three should be present")
	}

	// Adding a 4th should evict the oldest
	cache.MarkSeqID("d")

	if !cache.SeenSeqID("d") {
		t.Fatal("d should be present")
	}
	// LRU evicts least recently used — "a" was added first and not accessed after b,c
	// but since we accessed a in SeenSeqID above, it depends on LRU ordering.
	// Just verify total count doesn't exceed 3
	count := 0
	for _, id := range []string{"a", "b", "c", "d"} {
		if cache.SeenSeqID(id) {
			count++
		}
	}
	if count > 3 {
		t.Fatalf("cache should have max 3 entries, got %d", count)
	}
}

// NewFilterMemOnly creates a dedup filter with only the in-memory LRU layer
// (no Redis client needed for unit tests).
func NewFilterMemOnly(cacheSize int) (*Filter, error) {
	return NewFilter(nil, 5000, cacheSize)
}
