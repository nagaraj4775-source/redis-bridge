package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Delta is the replication event transmitted over the bus.
type Delta struct {
	SiteID      string `json:"site_id"`
	Key         string `json:"key"`
	KeyType     string `json:"key_type"`
	Value       []byte `json:"value"`
	HLC         uint64 `json:"hlc"`
	CapturedAt  int64  `json:"captured_at"`
	TTLMs       int64  `json:"ttl_ms"`
	ExpiresAtMs int64  `json:"expires_at_ms"` // absolute expiry epoch ms (0 = no expiry); supersedes CapturedAt+TTLMs
	Cmd         string `json:"cmd"`
	SeqID       string `json:"seq_id"`
}

// LagInfo captures consumer-group lag for one peer stream.
type LagInfo struct {
	StreamLen       int64  `json:"stream_len"`
	Lag             int64  `json:"lag"`              // undelivered messages
	Pending         int64  `json:"pending"`           // delivered but unacked
	LastDeliveredID string `json:"last_delivered_id"` // last acked message ID
	Consumers       int64  `json:"consumers"`
}

// Bus abstracts the replication transport so Kafka can be swapped in later.
type Bus interface {
	Publish(ctx context.Context, siteID string, delta Delta) error
	Consume(ctx context.Context, peerSiteID string, group string, consumer string, batchSize int, startID string) ([]Message, error)
	Ack(ctx context.Context, peerSiteID string, group string, ids []string) error
	StreamLen(ctx context.Context, siteID string) (int64, error)
	GroupLag(ctx context.Context, peerSiteID, group string) (LagInfo, error)
	EnsureGroup(ctx context.Context, peerSiteID, group string) error
	Ping(ctx context.Context) error
	Close() error
}

// Message wraps a Delta with its stream message ID.
type Message struct {
	ID    string
	Delta Delta
}

// RedisStreamsBus implements Bus using Redis Streams.
type RedisStreamsBus struct {
	client       redis.UniversalClient
	streamPrefix string
	maxLen       int64
	streamTTL    time.Duration // when > 0, MINID trimming is used instead of MAXLEN
	logger       *zap.Logger
}

// NewRedisStreamsBus creates a new Redis Streams-backed bus for a standalone instance.
// streamTTL sets time-based retention (MINID trimming); when > 0 it takes priority over maxLen.
func NewRedisStreamsBus(addr, password, streamPrefix string, maxLen int64, streamTTL time.Duration, logger *zap.Logger) *RedisStreamsBus {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		PoolSize:     32,
		MinIdleConns: 4,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	return &RedisStreamsBus{
		client:       client,
		streamPrefix: streamPrefix,
		maxLen:       maxLen,
		streamTTL:    streamTTL,
		logger:       logger,
	}
}

// NewRedisStreamsBusSentinel creates a bus backed by a Redis Sentinel cluster.
// streamTTL sets time-based retention (MINID trimming); when > 0 it takes priority over maxLen.
func NewRedisStreamsBusSentinel(masterName string, sentinelAddrs []string, password, streamPrefix string, maxLen int64, streamTTL time.Duration, logger *zap.Logger) *RedisStreamsBus {
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       masterName,
		SentinelAddrs:    sentinelAddrs,
		Password:         password,
		PoolSize:         32,
		MinIdleConns:     4,
		ReadTimeout:      5 * time.Second,
		WriteTimeout:     5 * time.Second,
		RouteByLatency:   false,
		RouteRandomly:    false,
	})
	return &RedisStreamsBus{
		client:       client,
		streamPrefix: streamPrefix,
		maxLen:       maxLen,
		streamTTL:    streamTTL,
		logger:       logger,
	}
}

// NewRedisStreamsBusCluster creates a bus backed by a Redis Cluster.
// streamTTL sets time-based retention (MINID trimming); when > 0 it takes priority over maxLen.
func NewRedisStreamsBusCluster(addrs []string, password, streamPrefix string, maxLen int64, streamTTL time.Duration, logger *zap.Logger) *RedisStreamsBus {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		PoolSize:     32,
		MinIdleConns: 4,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	return &RedisStreamsBus{
		client:       client,
		streamPrefix: streamPrefix,
		maxLen:       maxLen,
		streamTTL:    streamTTL,
		logger:       logger,
	}
}

// Ping checks connectivity to the underlying Redis instance.
func (b *RedisStreamsBus) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisStreamsBus) streamName(siteID string) string {
	return b.streamPrefix + siteID
}

// Publish writes a delta to the site's replication stream.
// Trimming strategy (applied on every XADD):
//   - streamTTL > 0 → MINID: trim entries whose ID (ms timestamp) is older than now-TTL.
//   - maxLen > 0    → MAXLEN: keep at most maxLen entries exactly (count-based).
//   - neither set   → no trimming (unlimited growth).
func (b *RedisStreamsBus) Publish(ctx context.Context, siteID string, delta Delta) error {
	valBytes, err := json.Marshal(delta)
	if err != nil {
		return fmt.Errorf("marshal delta: %w", err)
	}

	args := &redis.XAddArgs{
		Stream: b.streamName(siteID),
		Approx: false, // exact MAXLEN: never trim entries the consumer hasn't read yet
		Values: map[string]interface{}{
			"data": string(valBytes),
		},
	}
	if b.streamTTL > 0 {
		// MINID uses the stream entry ID (which is "<unix-ms>-<seq>") as the
		// trim boundary. Any entry with ID < minID is removed.
		minMS := time.Now().Add(-b.streamTTL).UnixMilli()
		args.MinID = strconv.FormatInt(minMS, 10) + "-0"
	} else {
		args.MaxLen = b.maxLen
	}
	return b.client.XAdd(ctx, args).Err()
}

// EnsureGroup creates the consumer group if it does not exist.
func (b *RedisStreamsBus) EnsureGroup(ctx context.Context, peerSiteID, group string) error {
	stream := b.streamName(peerSiteID)
	err := b.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

// Consume reads a batch of deltas from a peer's stream using consumer groups.
// startID=">" reads new undelivered messages; startID="0" re-reads the PEL
// (messages delivered but not yet ACKed — used for crash recovery).
func (b *RedisStreamsBus) Consume(ctx context.Context, peerSiteID string, group string, consumer string, batchSize int, startID string) ([]Message, error) {
	stream := b.streamName(peerSiteID)

	results, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, startID},
		Count:    int64(batchSize),
		Block:    100 * time.Millisecond,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	var msgs []Message
	for _, stream := range results {
		for _, xmsg := range stream.Messages {
			data, ok := xmsg.Values["data"].(string)
			if !ok {
				b.logger.Warn("skipping message with missing data field", zap.String("id", xmsg.ID))
				continue
			}
			var d Delta
			if err := json.Unmarshal([]byte(data), &d); err != nil {
				b.logger.Error("failed to unmarshal delta, sending to DLQ",
					zap.String("id", xmsg.ID), zap.Error(err))
				b.deadLetter(ctx, peerSiteID, xmsg.ID, data)
				continue
			}
			msgs = append(msgs, Message{ID: xmsg.ID, Delta: d})
		}
	}
	return msgs, nil
}

// Ack acknowledges processed messages.
func (b *RedisStreamsBus) Ack(ctx context.Context, peerSiteID string, group string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	stream := b.streamName(peerSiteID)
	return b.client.XAck(ctx, stream, group, ids...).Err()
}

// StreamLen returns the length of a site's replication stream.
func (b *RedisStreamsBus) StreamLen(ctx context.Context, siteID string) (int64, error) {
	stream := b.streamName(siteID)
	return b.client.XLen(ctx, stream).Result()
}

// GroupLag returns consumer-group lag stats for a peer's stream.
func (b *RedisStreamsBus) GroupLag(ctx context.Context, peerSiteID, group string) (LagInfo, error) {
	stream := b.streamName(peerSiteID)

	// XLEN routes by key slot (position 1) — always correct.
	streamLen, _ := b.client.XLen(ctx, stream).Result()

	// XInfoGroups args: ["xinfo", "groups", <stream>] — key is at position 2.
	// go-redis defaults to position 1 ("groups"), causing wrong-shard routing in
	// ClusterClient. SetFirstKeyPos(2) fixes the routing before Process is called.
	cmd := redis.NewXInfoGroupsCmd(ctx, stream)
	cmd.SetFirstKeyPos(2)
	if err := b.client.Process(ctx, cmd); err != nil {
		if err == redis.Nil || strings.Contains(err.Error(), "no such key") {
			return LagInfo{StreamLen: streamLen}, nil
		}
		return LagInfo{StreamLen: streamLen}, fmt.Errorf("xinfo groups %s: %w", stream, err)
	}

	for _, g := range cmd.Val() {
		if g.Name == group {
			return LagInfo{
				StreamLen:       streamLen,
				Lag:             g.Lag,
				Pending:         g.Pending,
				LastDeliveredID: g.LastDeliveredID,
				Consumers:       g.Consumers,
			}, nil
		}
	}
	return LagInfo{StreamLen: streamLen}, nil
}

// deadLetter sends unprocessable messages to a DLQ stream.
func (b *RedisStreamsBus) deadLetter(ctx context.Context, peerSiteID, msgID, data string) {
	dlqStream := "repl:dlq:" + peerSiteID
	b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]interface{}{
			"original_id": msgID,
			"data":        data,
			"ts":          strconv.FormatInt(time.Now().UnixMilli(), 10),
		},
	})
}

// Close shuts down the bus client.
func (b *RedisStreamsBus) Close() error {
	return b.client.Close()
}
