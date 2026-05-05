package dedup

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/redis/go-redis/v9"
)

const (
	shadowPrefix = "__repl:applying:"
)

// Filter provides two-layer loop prevention:
// Layer 1: Redis-backed shadow keys (__repl:applying:{key})
// Layer 2: In-memory LRU cache keyed on SeqID
type Filter struct {
	client    redis.UniversalClient
	seqCache  *lru.Cache[string, struct{}]
	ttlMs     int
	shadowTTL time.Duration
}

// NewFilter creates a new dedup filter.
func NewFilter(client redis.UniversalClient, ttlMs int, cacheSize int) (*Filter, error) {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("create LRU cache: %w", err)
	}
	// shadowTTL must cover the full keyspace-event round-trip at peak load.
	// At 38k+ writes/s, PubSub delivery can be delayed several seconds due to
	// Redis server load. Using the configured dedup_ttl_seconds (default 5s)
	// ensures loopback detection works even under heavy burst loads.
	shTTL := time.Duration(ttlMs) * time.Millisecond
	return &Filter{
		client:    client,
		seqCache:  cache,
		ttlMs:     ttlMs,
		shadowTTL: shTTL,
	}, nil
}

// IsApplying checks if a key is currently being written by the replication applier.
// Returns true if the shadow key exists (meaning this is a replication write, skip it).
func (f *Filter) IsApplying(ctx context.Context, key string) (bool, error) {
	val, err := f.client.Exists(ctx, shadowPrefix+key).Result()
	if err != nil {
		return false, fmt.Errorf("EXISTS %s: %w", shadowPrefix+key, err)
	}
	return val > 0, nil
}

// MarkApplying stores a CRC32 hash of the applied value as the shadow key.
// The producer reads the current key value, hashes it, and compares:
//   - Match → the current value IS the applied value → Apply event → skip
//   - Mismatch → a new write overwrote the applied value → publish
func (f *Filter) MarkApplying(ctx context.Context, key string, value []byte) error {
	hash := crc32.ChecksumIEEE(value)
	return f.client.Set(ctx, shadowPrefix+key, strconv.FormatUint(uint64(hash), 10), f.shadowTTL).Err()
}

// ApplyingHash returns the CRC32 hash stored in the shadow key, or 0 if none.
// The producer uses this to compare against the current key value's hash.
func (f *Filter) ApplyingHash(ctx context.Context, key string) (uint32, error) {
	val, err := f.client.Get(ctx, shadowPrefix+key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", shadowPrefix+key, err)
	}
	n, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return 0, nil // legacy or corrupt → treat as no shadow
	}
	return uint32(n), nil
}

// ValueHash returns CRC32 of a byte slice.
func ValueHash(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

// SeenSeqID checks if a SeqID has already been processed.
func (f *Filter) SeenSeqID(seqID string) bool {
	_, ok := f.seqCache.Get(seqID)
	return ok
}

// MarkSeqID marks a SeqID as processed.
func (f *Filter) MarkSeqID(seqID string) {
	f.seqCache.Add(seqID, struct{}{})
}
