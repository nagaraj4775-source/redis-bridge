package producer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/hlc"
	"github.com/nagaraju/redibridge/internal/metrics"
	"github.com/nagaraju/redibridge/internal/reader"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// metaCAS writes __meta:{key} only if our HLC is strictly greater than the
// currently stored one. This prevents the producer from overwriting a more
// recent meta value that the applier may have written between the producer's
// publish call and this meta-write (the race that breaks LWW convergence).
var metaCAS = redis.NewScript(`
local existing = redis.call('HGET', KEYS[1], 'hlc')
if existing == false then
    redis.call('HSET', KEYS[1], 'hlc', ARGV[1], 'site', ARGV[2])
    return 1
end
-- uint64 safe comparison: tonumber() loses precision above 2^53.
-- Decimal strings of different lengths: longer = larger.
-- Same length: lexicographic order equals numeric order.
local function u64lt(a, b)
    if #a ~= #b then return #a < #b end
    return a < b
end
if u64lt(existing, ARGV[1]) then
    redis.call('HSET', KEYS[1], 'hlc', ARGV[1], 'site', ARGV[2])
    return 1
end
return 0
`)

// Tracked keyspace commands
var trackedCommands = map[string]bool{
	"set": true, "setex": true, "psetex": true, "mset": true,
	"hset": true, "hmset": true,
	"lpush": true, "rpush": true,
	"sadd": true,
	"zadd": true,
	"del": true, "expire": true, "persist": true,
	"rename": true, "copy": true,
}

// Producer listens for keyspace events on Redis masters and publishes deltas.
type Producer struct {
	siteID     string
	masters    []*redis.Client      // per-node clients for PSubscribe only
	dataClient redis.UniversalClient // for meta/shadow/reader ops (auto-routes in cluster)
	bus        bus.Bus
	clock      *hlc.HLC
	dedup      *dedup.Filter
	logger     *zap.Logger
	seqNo      atomic.Uint64
}

// New creates a new Producer.
// masters are per-node standalone clients used only for PSubscribe (keyspace notifications
// must be received on the specific node that owns the key's slot).
// dataClient is used for all data operations and can be a ClusterClient for automatic routing.
func New(siteID string, masters []*redis.Client, dataClient redis.UniversalClient, b bus.Bus, clock *hlc.HLC, dd *dedup.Filter, logger *zap.Logger) *Producer {
	return &Producer{
		siteID:     siteID,
		masters:    masters,
		dataClient: dataClient,
		bus:        b,
		clock:      clock,
		dedup:      dd,
		logger:     logger,
	}
}

// Run starts keyspace listeners on all masters. Blocks until ctx is cancelled.
func (p *Producer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i, master := range p.masters {
		// Enable keyspace notifications
		if err := master.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
			p.logger.Warn("failed to set notify-keyspace-events, may already be set",
				zap.Int("master", i), zap.Error(err))
		}

		wg.Add(1)
		go func(idx int, client *redis.Client) {
			defer wg.Done()
			p.listenMaster(ctx, idx, client)
		}(i, master)
	}

	wg.Wait()
	return nil
}

func (p *Producer) listenMaster(ctx context.Context, idx int, client *redis.Client) {
	logger := p.logger.With(zap.Int("master", idx))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := p.subscribeAndProcess(ctx, idx, client, logger)
		if err != nil && ctx.Err() == nil {
			logger.Error("subscription error, reconnecting in 1s", zap.Error(err))
			time.Sleep(1 * time.Second)
		}
	}
}

func (p *Producer) subscribeAndProcess(ctx context.Context, idx int, client *redis.Client, logger *zap.Logger) error {
	pubsub := client.PSubscribe(ctx, "__keyevent@*__:*")
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("pubsub channel closed")
			}
			p.handleEvent(ctx, msg, logger)
		}
	}
}

func (p *Producer) handleEvent(ctx context.Context, msg *redis.Message, logger *zap.Logger) {
	// msg.Channel: __keyevent@0__:set
	// msg.Payload: the key name
	key := msg.Payload
	cmd := extractCommand(msg.Channel)

	// Skip internal keys (meta, dedup shadow, replication streams, DLQ)
	if strings.HasPrefix(key, "__repl:") || strings.HasPrefix(key, "__meta:") ||
		strings.HasPrefix(key, "repl:stream:") || strings.HasPrefix(key, "repl:dlq:") {
		return
	}

	// Skip untracked commands
	if !trackedCommands[cmd] {
		return
	}

	// Check if this is a replication write (loop prevention).
	// Uses dataClient so shadow key lookup auto-routes in cluster mode.
	applying, err := p.dedup.IsApplying(ctx, key)
	if err != nil {
		logger.Warn("dedup check failed", zap.String("key", key), zap.Error(err))
		return
	}
	if applying {
		return
	}

	// Read key value and TTL via dataClient (auto-routes to correct shard in cluster mode).
	keyType, value, err := reader.ReadValue(ctx, p.dataClient, key)
	if err != nil {
		// Key might have been deleted between notification and read
		if cmd == "del" {
			keyType = "none"
			value = nil
		} else {
			logger.Debug("failed to read key value", zap.String("key", key), zap.Error(err))
			return
		}
	}

	ttlMs, err := reader.ReadTTL(ctx, p.dataClient, key)
	if err != nil {
		logger.Debug("failed to read TTL", zap.String("key", key), zap.Error(err))
		ttlMs = -1
	}

	// Handle "none" type from TYPE command (key was deleted)
	if keyType == "none" {
		value = nil
		ttlMs = -1
	}

	// Stamp with HLC
	ts := p.clock.Now()
	capturedAt := time.Now().UnixMilli()

	// Build SeqID
	seq := p.seqNo.Add(1)
	seqID := fmt.Sprintf("%s-%d", p.siteID, seq)

	delta := bus.Delta{
		SiteID:     p.siteID,
		Key:        key,
		KeyType:    keyType,
		Value:      value,
		HLC:        ts,
		CapturedAt: capturedAt,
		TTLMs:      ttlMs,
		Cmd:        strings.ToUpper(cmd),
		SeqID:      seqID,
	}

	// Write meta BEFORE publishing so the LWW anchor is visible to our own
	// consumer before any peer can receive and process the delta.
	// Uses dataClient so the meta key auto-routes to its correct shard in cluster mode.
	metaKey := "__meta:" + key
	if keyType == "none" {
		p.dataClient.Del(ctx, metaKey)
	} else {
		metaCAS.Run(ctx, p.dataClient, []string{metaKey}, strconv.FormatUint(ts, 10), p.siteID)
	}

	// Publish to bus
	if err := p.bus.Publish(ctx, p.siteID, delta); err != nil {
		logger.Error("failed to publish delta",
			zap.String("key", key), zap.Error(err))
		return
	}

	metrics.EventsCaptured.WithLabelValues(p.siteID, keyType).Inc()
	metrics.EventsPublished.WithLabelValues(p.siteID).Inc()
}

// extractCommand extracts the command from a keyevent channel name.
// e.g. "__keyevent@0__:set" → "set"
func extractCommand(channel string) string {
	idx := strings.LastIndex(channel, ":")
	if idx < 0 {
		return ""
	}
	return channel[idx+1:]
}
