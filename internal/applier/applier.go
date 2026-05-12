package applier

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/metrics"
	"github.com/nagaraju/redibridge/internal/reader"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const metaPrefix = "__meta:"

// Applier applies accepted deltas to the local Redis cluster.
type Applier struct {
	client redis.UniversalClient
	dedup  *dedup.Filter
	siteID string
	logger *zap.Logger
}

// New creates a new Applier.
func New(client redis.UniversalClient, dedup *dedup.Filter, logger *zap.Logger) *Applier {
	return &Applier{
		client: client,
		dedup:  dedup,
		logger: logger,
	}
}

// WithSiteID sets the local site ID for metric labels.
func (a *Applier) WithSiteID(siteID string) *Applier {
	a.siteID = siteID
	return a
}

// Apply writes an accepted delta to the local cluster.
// It sets a dedup shadow key, applies the value via pipeline, preserves TTL,
// and updates the __meta:{key} hash.
func (a *Applier) Apply(ctx context.Context, delta bus.Delta) error {
	// Check if key expired in transit.
	// Use ExpiresAtMs (absolute epoch) if set; fall back to CapturedAt+TTLMs for
	// backward-compatibility with older agents that don't send ExpiresAtMs.
	if delta.ExpiresAtMs > 0 {
		if time.Now().UnixMilli() >= delta.ExpiresAtMs {
			a.logger.Debug("key expired in transit (ExpiresAtMs), skipping",
				zap.String("key", delta.Key),
				zap.Int64("expires_at_ms", delta.ExpiresAtMs))
			if a.siteID != "" {
				metrics.TTLExpiredInTransitTotal.WithLabelValues(a.siteID).Inc()
			}
			return nil
		}
	} else if delta.TTLMs > 0 {
		elapsed := time.Now().UnixMilli() - delta.CapturedAt
		remaining := delta.TTLMs - elapsed
		if remaining <= 0 {
			a.logger.Debug("key expired in transit (TTLMs), skipping",
				zap.String("key", delta.Key),
				zap.Int64("ttl_ms", delta.TTLMs),
				zap.Int64("elapsed", elapsed))
			if a.siteID != "" {
				metrics.TTLExpiredInTransitTotal.WithLabelValues(a.siteID).Inc()
			}
			return nil
		}
	}

	// Set dedup shadow key with value hash so the producer can distinguish
	// Apply events (value matches hash) from original writes (value differs).
	if err := a.dedup.MarkApplying(ctx, delta.Key, delta.Value); err != nil {
		return fmt.Errorf("mark applying: %w", err)
	}

	// Phase 1: apply value using direct client calls (not pipeline).
	// ClusterClient.Pipeline().Exec() can silently swallow errors in cluster
	// mode, so we use direct calls that surface errors immediately.
	switch delta.KeyType {
	case "string":
		if err := a.client.Set(ctx, delta.Key, delta.Value, 0).Err(); err != nil {
			return fmt.Errorf("SET %s: %w", delta.Key, err)
		}

	case "hash":
		var fields map[string]string
		if err := json.Unmarshal(delta.Value, &fields); err != nil {
			return fmt.Errorf("unmarshal hash: %w", err)
		}
		if err := a.client.Del(ctx, delta.Key).Err(); err != nil {
			return fmt.Errorf("DEL %s: %w", delta.Key, err)
		}
		if len(fields) > 0 {
			flat := make([]interface{}, 0, len(fields)*2)
			for k, v := range fields {
				flat = append(flat, k, v)
			}
			if err := a.client.HSet(ctx, delta.Key, flat...).Err(); err != nil {
				return fmt.Errorf("HSET %s: %w", delta.Key, err)
			}
		}

	case "list":
		var elements []string
		if err := json.Unmarshal(delta.Value, &elements); err != nil {
			return fmt.Errorf("unmarshal list: %w", err)
		}
		if err := a.client.Del(ctx, delta.Key).Err(); err != nil {
			return fmt.Errorf("DEL %s: %w", delta.Key, err)
		}
		if len(elements) > 0 {
			ifaces := make([]interface{}, len(elements))
			for i, e := range elements {
				ifaces[i] = e
			}
			if err := a.client.RPush(ctx, delta.Key, ifaces...).Err(); err != nil {
				return fmt.Errorf("RPUSH %s: %w", delta.Key, err)
			}
		}

	case "set":
		var members []string
		if err := json.Unmarshal(delta.Value, &members); err != nil {
			return fmt.Errorf("unmarshal set: %w", err)
		}
		if err := a.client.Del(ctx, delta.Key).Err(); err != nil {
			return fmt.Errorf("DEL %s: %w", delta.Key, err)
		}
		if len(members) > 0 {
			ifaces := make([]interface{}, len(members))
			for i, m := range members {
				ifaces[i] = m
			}
			if err := a.client.SAdd(ctx, delta.Key, ifaces...).Err(); err != nil {
				return fmt.Errorf("SADD %s: %w", delta.Key, err)
			}
		}

	case "zset":
		var entries []reader.ZSetEntry
		if err := json.Unmarshal(delta.Value, &entries); err != nil {
			return fmt.Errorf("unmarshal zset: %w", err)
		}
		if err := a.client.Del(ctx, delta.Key).Err(); err != nil {
			return fmt.Errorf("DEL %s: %w", delta.Key, err)
		}
		if len(entries) > 0 {
			zMembers := make([]redis.Z, len(entries))
			for i, e := range entries {
				zMembers[i] = redis.Z{Score: e.Score, Member: e.Member}
			}
			if err := a.client.ZAdd(ctx, delta.Key, zMembers...).Err(); err != nil {
				return fmt.Errorf("ZADD %s: %w", delta.Key, err)
			}
		}

	case "none":
		if err := a.client.Del(ctx, delta.Key).Err(); err != nil {
			return fmt.Errorf("DEL %s: %w", delta.Key, err)
		}

	default:
		return fmt.Errorf("unsupported key type: %s", delta.KeyType)
	}

	// Apply TTL using the absolute expiry epoch.
	// ExpiresAtMs (set by producer as capturedAt+ttlMs) takes priority; fall back
	// to the legacy CapturedAt+TTLMs calculation for older agent compatibility.
	if delta.KeyType != "none" {
		var expireAt int64
		if delta.ExpiresAtMs > 0 {
			expireAt = delta.ExpiresAtMs
		} else if delta.TTLMs > 0 {
			expireAt = delta.CapturedAt + delta.TTLMs
		}
		if expireAt > 0 {
			if err := a.client.PExpireAt(ctx, delta.Key, time.UnixMilli(expireAt)).Err(); err != nil {
				a.logger.Warn("PExpireAt failed", zap.String("key", delta.Key), zap.Error(err))
			}
		}
	}

	// Phase 2: update __meta:{key} with HLC, site_id, and val_hash after the
	// value write committed. val_hash must be written here so the reconciler
	// does not treat consumer-applied values as drifted on the next scan.
	// If this fails the consumer will not ACK; on retry ReadMeta returns the
	// old (lower) HLC so the delta is accepted and both writes are re-attempted.
	metaKey := metaPrefix + delta.Key
	args := []interface{}{"hlc", strconv.FormatUint(delta.HLC, 10), "site", delta.SiteID}
	if delta.KeyType != "none" && len(delta.Value) > 0 {
		args = append(args, "val_hash", hashValue(delta.Value))
	}
	if err := a.client.HSet(ctx, metaKey, args...).Err(); err != nil {
		return fmt.Errorf("meta write: %w", err)
	}

	return nil
}

// hashValue returns the FNV-1a 64-bit hex fingerprint of a value blob.
// Identical to the hashValue helper in producer so reconciler comparisons stay consistent.
func hashValue(b []byte) string {
	h := fnv.New64a()
	h.Write(b)
	return strconv.FormatUint(h.Sum64(), 16)
}

// ReadMeta reads the __meta:{key} hash for LWW comparison.
// Returns hlc=0 and empty siteID if meta does not exist.
func ReadMeta(ctx context.Context, client redis.UniversalClient, key string) (hlc uint64, siteID string, err error) {
	metaKey := metaPrefix + key
	vals, err := client.HGetAll(ctx, metaKey).Result()
	if err != nil {
		return 0, "", fmt.Errorf("HGETALL %s: %w", metaKey, err)
	}
	if len(vals) == 0 {
		return 0, "", nil // No meta = first write, always accept
	}

	hlcStr, ok := vals["hlc"]
	if !ok {
		return 0, "", nil
	}
	hlc, err = strconv.ParseUint(hlcStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse hlc: %w", err)
	}

	siteID = vals["site"]
	return hlc, siteID, nil
}
