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
	siteID        string
	masters       []*redis.Client      // per-node clients for PSubscribe only
	clusterClient *redis.ClusterClient // non-nil in cluster mode; used for topology watch
	nodeOpts      *redis.Options       // base opts for creating dynamic per-node clients
	dataClient    redis.UniversalClient
	bus           bus.Bus
	clock         *hlc.HLC
	dedup         *dedup.Filter
	logger        *zap.Logger
	seqNo         atomic.Uint64

	subMu  sync.Mutex
	subSet map[string]context.CancelFunc // master addr → cancel func
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
		subSet:     make(map[string]context.CancelFunc),
	}
}

// WithClusterTopologyWatch enables automatic re-subscription when a replica is
// promoted to master. The producer polls CLUSTER topology every 10 s:
//   - new master detected → ConfigSet KEA + start PubSub goroutine
//   - master no longer in cluster → cancel its subscription context
//
// Call this immediately after New() for cluster-mode deployments.
func (p *Producer) WithClusterTopologyWatch(cc *redis.ClusterClient, baseOpts *redis.Options) *Producer {
	p.clusterClient = cc
	p.nodeOpts = baseOpts
	return p
}

// Run starts keyspace listeners on all masters. Blocks until ctx is cancelled.
func (p *Producer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i, master := range p.masters {
		if err := master.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
			p.logger.Warn("failed to set notify-keyspace-events, may already be set",
				zap.Int("master", i), zap.Error(err))
		}

		addr := master.Options().Addr
		subCtx, cancel := context.WithCancel(ctx)

		p.subMu.Lock()
		p.subSet[addr] = cancel
		p.subMu.Unlock()

		wg.Add(1)
		go func(idx int, client *redis.Client, sCtx context.Context, a string) {
			defer wg.Done()
			defer func() {
				p.subMu.Lock()
				delete(p.subSet, a)
				p.subMu.Unlock()
			}()
			p.listenMaster(sCtx, idx, client)
		}(i, master, subCtx, addr)
	}

	// Topology watcher runs alongside subscriptions in cluster mode and
	// re-subscribes to promoted replicas within one reconcile interval (~10s).
	if p.clusterClient != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.watchTopology(ctx)
		}()
	}

	wg.Wait()
	return nil
}

// watchTopology polls CLUSTER topology every 10 s and reconciles subscriptions.
func (p *Producer) watchTopology(ctx context.Context) {
	p.reconcileMasters(ctx) // immediate first pass
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reconcileMasters(ctx)
		}
	}
}

// reconcileMasters compares the cluster's current master set with active
// subscriptions. Promoted replicas get a new PubSub goroutine; masters that
// are no longer in the cluster have their subscription context cancelled.
func (p *Producer) reconcileMasters(ctx context.Context) {
	// Force the ClusterClient to re-read CLUSTER SLOTS so we see any
	// replica promotions that happened since the last refresh.
	p.clusterClient.ReloadState(ctx)

	var addrMu sync.Mutex
	currentAddrs := make(map[string]struct{})
	if err := p.clusterClient.ForEachMaster(ctx, func(_ context.Context, c *redis.Client) error {
		addrMu.Lock()
		currentAddrs[c.Options().Addr] = struct{}{}
		addrMu.Unlock()
		return nil
	}); err != nil {
		p.logger.Warn("topology reconcile: ForEachMaster failed", zap.Error(err))
		return
	}

	p.subMu.Lock()

	// Cancel subscriptions for masters that left the cluster
	for addr, cancel := range p.subSet {
		if _, active := currentAddrs[addr]; !active {
			p.logger.Info("master left cluster, cancelling subscription", zap.String("addr", addr))
			cancel()
			delete(p.subSet, addr)
		}
	}

	// Collect new masters (promoted replicas) and register placeholder to
	// prevent a second reconcile from double-starting the same address.
	var toStart []string
	for addr := range currentAddrs {
		if _, exists := p.subSet[addr]; !exists {
			toStart = append(toStart, addr)
			p.subSet[addr] = func() {} // placeholder
		}
	}

	p.subMu.Unlock()

	for _, addr := range toStart {
		optsCopy := *p.nodeOpts
		optsCopy.Addr = addr
		c := redis.NewClient(&optsCopy)

		if err := c.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err(); err != nil {
			p.logger.Warn("failed to set notify-keyspace-events on promoted master",
				zap.String("addr", addr), zap.Error(err))
		}

		subCtx, cancel := context.WithCancel(ctx)

		p.subMu.Lock()
		p.subSet[addr] = cancel // replace placeholder with real cancel
		p.subMu.Unlock()

		p.logger.Info("replica promoted to master, starting subscription", zap.String("addr", addr))

		go func(a string, cl *redis.Client, sCtx context.Context) {
			p.listenMaster(sCtx, -1, cl)
			cl.Close()
			p.subMu.Lock()
			delete(p.subSet, a)
			p.subMu.Unlock()
		}(addr, c, subCtx)
	}
}

func (p *Producer) listenMaster(ctx context.Context, idx int, client *redis.Client) {
	var logger *zap.Logger
	if idx >= 0 {
		logger = p.logger.With(zap.Int("master", idx))
	} else {
		logger = p.logger.With(zap.String("master_addr", client.Options().Addr))
	}

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

	ch := pubsub.Channel(redis.WithChannelSize(100000))

	// Semaphore limits concurrent event handlers so events are dispatched
	// immediately (not queued behind serial processing). This keeps the
	// shadow-key TTL check valid even during 100k-key bulk operations.
	const workerConcurrency = 50
	sem := make(chan struct{}, workerConcurrency)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("pubsub channel closed")
			}
			sem <- struct{}{}
			go func(m *redis.Message) {
				defer func() { <-sem }()
				p.handleEvent(ctx, m, logger)
			}(msg)
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

	// Value-hash loop prevention: the shadow stores a CRC32 of the value the
	// consumer just applied. If the current key value matches that hash, this
	// keyspace event was triggered by the Apply's SET → skip (prevents infinite
	// replication loops). If the hash differs, a new user write overwrote the
	// applied value → publish so all sites learn about it.
	shadowHash, err := p.dedup.ApplyingHash(ctx, key)
	if err != nil {
		logger.Warn("dedup check failed", zap.String("key", key), zap.Error(err))
		return
	}
	if shadowHash > 0 && dedup.ValueHash(value) == shadowHash {
		return
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
		if err := metaCAS.Run(ctx, p.dataClient, []string{metaKey}, strconv.FormatUint(ts, 10), p.siteID).Err(); err != nil {
			logger.Warn("metaCAS failed", zap.String("key", key), zap.String("metaKey", metaKey), zap.Error(err))
		}
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
