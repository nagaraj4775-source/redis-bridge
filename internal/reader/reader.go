package reader

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ZSetEntry represents a sorted set member with score.
type ZSetEntry struct {
	Member string  `json:"m"`
	Score  float64 `json:"s"`
}

// ReadValue reads the full value of a key based on its type.
// Returns the key type and JSON-encoded value bytes.
func ReadValue(ctx context.Context, client redis.UniversalClient, key string) (keyType string, value []byte, err error) {
	t, err := client.Type(ctx, key).Result()
	if err != nil {
		return "", nil, fmt.Errorf("TYPE %s: %w", key, err)
	}

	switch t {
	case "string":
		val, err := client.Get(ctx, key).Bytes()
		if err != nil {
			return "", nil, fmt.Errorf("GET %s: %w", key, err)
		}
		return "string", val, nil

	case "hash":
		val, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return "", nil, fmt.Errorf("HGETALL %s: %w", key, err)
		}
		b, err := json.Marshal(val)
		if err != nil {
			return "", nil, fmt.Errorf("marshal hash: %w", err)
		}
		return "hash", b, nil

	case "list":
		val, err := client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return "", nil, fmt.Errorf("LRANGE %s: %w", key, err)
		}
		b, err := json.Marshal(val)
		if err != nil {
			return "", nil, fmt.Errorf("marshal list: %w", err)
		}
		return "list", b, nil

	case "set":
		val, err := client.SMembers(ctx, key).Result()
		if err != nil {
			return "", nil, fmt.Errorf("SMEMBERS %s: %w", key, err)
		}
		b, err := json.Marshal(val)
		if err != nil {
			return "", nil, fmt.Errorf("marshal set: %w", err)
		}
		return "set", b, nil

	case "zset":
		val, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return "", nil, fmt.Errorf("ZRANGEWITHSCORES %s: %w", key, err)
		}
		entries := make([]ZSetEntry, len(val))
		for i, z := range val {
			entries[i] = ZSetEntry{
				Member: z.Member.(string),
				Score:  z.Score,
			}
		}
		b, err := json.Marshal(entries)
		if err != nil {
			return "", nil, fmt.Errorf("marshal zset: %w", err)
		}
		return "zset", b, nil

	case "none":
		return "none", nil, nil

	default:
		return "", nil, fmt.Errorf("unsupported key type: %s", t)
	}
}

// ReadTTL reads the remaining TTL of a key in milliseconds.
// Returns -1 if the key has no expiry, -2 if the key does not exist.
func ReadTTL(ctx context.Context, client redis.UniversalClient, key string) (int64, error) {
	ttl, err := client.PTTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("PTTL %s: %w", key, err)
	}
	return ttl.Milliseconds(), nil
}
