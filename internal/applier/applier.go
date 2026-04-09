package applier

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/reader"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const metaPrefix = "__meta:"

// Applier applies accepted deltas to the local Redis cluster.
type Applier struct {
	client *redis.Client
	dedup  *dedup.Filter
	logger *zap.Logger
}

// New creates a new Applier.
func New(client *redis.Client, dedup *dedup.Filter, logger *zap.Logger) *Applier {
	return &Applier{
		client: client,
		dedup:  dedup,
		logger: logger,
	}
}

// Apply writes an accepted delta to the local cluster.
// It sets a dedup shadow key, applies the value via pipeline, preserves TTL,
// and updates the __meta:{key} hash.
func (a *Applier) Apply(ctx context.Context, delta bus.Delta) error {
	// Check if key expired in transit
	if delta.TTLMs > 0 {
		elapsed := time.Now().UnixMilli() - delta.CapturedAt
		remaining := delta.TTLMs - elapsed
		if remaining <= 0 {
			a.logger.Debug("key expired in transit, skipping",
				zap.String("key", delta.Key),
				zap.Int64("ttl_ms", delta.TTLMs),
				zap.Int64("elapsed", elapsed))
			return nil
		}
	}

	// Set dedup shadow key
	if err := a.dedup.MarkApplying(ctx, delta.Key); err != nil {
		return fmt.Errorf("mark applying: %w", err)
	}

	pipe := a.client.Pipeline()

	// Apply value based on key type
	switch delta.KeyType {
	case "string":
		pipe.Set(ctx, delta.Key, delta.Value, 0)

	case "hash":
		var fields map[string]string
		if err := json.Unmarshal(delta.Value, &fields); err != nil {
			return fmt.Errorf("unmarshal hash: %w", err)
		}
		pipe.Del(ctx, delta.Key)
		if len(fields) > 0 {
			flat := make([]interface{}, 0, len(fields)*2)
			for k, v := range fields {
				flat = append(flat, k, v)
			}
			pipe.HSet(ctx, delta.Key, flat...)
		}

	case "list":
		var elements []string
		if err := json.Unmarshal(delta.Value, &elements); err != nil {
			return fmt.Errorf("unmarshal list: %w", err)
		}
		pipe.Del(ctx, delta.Key)
		if len(elements) > 0 {
			ifaces := make([]interface{}, len(elements))
			for i, e := range elements {
				ifaces[i] = e
			}
			pipe.RPush(ctx, delta.Key, ifaces...)
		}

	case "set":
		var members []string
		if err := json.Unmarshal(delta.Value, &members); err != nil {
			return fmt.Errorf("unmarshal set: %w", err)
		}
		pipe.Del(ctx, delta.Key)
		if len(members) > 0 {
			ifaces := make([]interface{}, len(members))
			for i, m := range members {
				ifaces[i] = m
			}
			pipe.SAdd(ctx, delta.Key, ifaces...)
		}

	case "zset":
		var entries []reader.ZSetEntry
		if err := json.Unmarshal(delta.Value, &entries); err != nil {
			return fmt.Errorf("unmarshal zset: %w", err)
		}
		pipe.Del(ctx, delta.Key)
		if len(entries) > 0 {
			zMembers := make([]redis.Z, len(entries))
			for i, e := range entries {
				zMembers[i] = redis.Z{Score: e.Score, Member: e.Member}
			}
			pipe.ZAdd(ctx, delta.Key, zMembers...)
		}

	case "none":
		// Key was deleted at source
		pipe.Del(ctx, delta.Key)

	default:
		return fmt.Errorf("unsupported key type: %s", delta.KeyType)
	}

	// Apply TTL if present and key type is not "none"
	if delta.KeyType != "none" && delta.TTLMs > 0 {
		expireAt := delta.CapturedAt + delta.TTLMs
		pipe.PExpireAt(ctx, delta.Key, time.UnixMilli(expireAt))
	}

	// Update __meta:{key} with HLC and site_id
	metaKey := metaPrefix + delta.Key
	pipe.HSet(ctx, metaKey, "hlc", strconv.FormatUint(delta.HLC, 10), "site", delta.SiteID)

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline exec: %w", err)
	}

	return nil
}

// ReadMeta reads the __meta:{key} hash for LWW comparison.
// Returns hlc=0 and empty siteID if meta does not exist.
func ReadMeta(ctx context.Context, client *redis.Client, key string) (hlc uint64, siteID string, err error) {
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
