package dedup

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/redis/go-redis/v9"
)

const (
	shadowPrefix = "__repl:applying:"
	// shadowTTL is the lifetime of the loop-prevention shadow key.
	// It only needs to outlive the keyspace notification round-trip
	// (~1ms locally). 200ms is generous while not blocking user writes.
	shadowTTL = 200 * time.Millisecond
)

// Filter provides two-layer loop prevention:
// Layer 1: Redis-backed shadow keys (__repl:applying:{key})
// Layer 2: In-memory LRU cache keyed on SeqID
type Filter struct {
	client   redis.UniversalClient
	seqCache *lru.Cache[string, struct{}]
	ttlMs    int
}

// NewFilter creates a new dedup filter.
func NewFilter(client redis.UniversalClient, ttlMs int, cacheSize int) (*Filter, error) {
	cache, err := lru.New[string, struct{}](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("create LRU cache: %w", err)
	}
	return &Filter{
		client:   client,
		seqCache: cache,
		ttlMs:    ttlMs,
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

// MarkApplying sets the shadow key to indicate a replication write is in progress.
// Uses a fixed short TTL (shadowTTL) so legitimate user writes to the same key
// are not blocked after the replication write settles.
func (f *Filter) MarkApplying(ctx context.Context, key string) error {
	return f.client.Set(ctx, shadowPrefix+key, "1", shadowTTL).Err()
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
