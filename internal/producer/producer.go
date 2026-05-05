package producer

import (
	"context"
	"fmt"
	"path"
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

// ReconcileStatus is a JSON-serialisable snapshot of the last reconciler cycle.
type ReconcileStatus struct {
	Enabled      bool      `json:"enabled"`
	LastRunAt    time.Time `json:"last_run_at,omitempty"`
	LastScanMs   int64     `json:"last_scan_ms"`
	LastRepaired int       `json:"last_repaired"`
	TotalRuns    int64     `json:"total_runs"`
}

// BootstrapStatus is a JSON-serialisable snapshot of bootstrap progress.
type BootstrapStatus struct {
	State      string    `json:"state"` // idle | running | done | done_with_errors | cancelled
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Published  int64     `json:"published"`
	Errors     int64     `json:"errors"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
}

// KeyStatus is a JSON-serialisable snapshot of a single key's replication state.
type KeyStatus struct {
	Key        string `json:"key"`
	Published  bool   `json:"published"`
	HLC        uint64 `json:"hlc"`
	OriginSite string `json:"origin_site"`
}

// bootstrapProgress holds live counters for an in-flight bootstrap.
// Atomic fields allow the status endpoint to read them without a lock.
type bootstrapProgress struct {
	state      string
	startedAt  time.Time
	finishedAt time.Time
	published  atomic.Int64
	errors     atomic.Int64
	errMsg     string
}

// Producer listens for keyspace events on Redis masters and publishes deltas.
type Producer struct {
	siteID            string
	masters           []*redis.Client      // per-node clients for PSubscribe only
	clusterClient     *redis.ClusterClient // non-nil in cluster mode; used for topology watch
	nodeOpts          *redis.Options       // base opts for creating dynamic per-node clients
	dataClient        redis.UniversalClient
	bus               bus.Bus
	clock             *hlc.HLC
	dedup             *dedup.Filter
	logger            *zap.Logger
	seqNo             atomic.Uint64
	reconcileInterval time.Duration // 0 = disabled

	// Pattern filtering (Feature 5)
	includeGlobs []string // if non-empty, key must match at least one
	excludeGlobs []string // key must not match any

	// On-demand reconcile trigger and status (Feature 1)
	reconcileTrigger  chan struct{}
	reconcileStatusMu sync.Mutex
	reconcileStatus   ReconcileStatus

	// Bootstrap sync state (Feature 6)
	bootstrapMu     sync.Mutex
	bootstrapProg   *bootstrapProgress
	bootstrapCancel context.CancelFunc

	subMu  sync.Mutex
	subSet map[string]context.CancelFunc // master addr → cancel func
}

// New creates a new Producer.
// masters are per-node standalone clients used only for PSubscribe (keyspace notifications
// must be received on the specific node that owns the key's slot).
// dataClient is used for all data operations and can be a ClusterClient for automatic routing.
func New(siteID string, masters []*redis.Client, dataClient redis.UniversalClient, b bus.Bus, clock *hlc.HLC, dd *dedup.Filter, logger *zap.Logger) *Producer {
	return &Producer{
		siteID:           siteID,
		masters:          masters,
		dataClient:       dataClient,
		bus:              b,
		clock:            clock,
		dedup:            dd,
		logger:           logger,
		subSet:           make(map[string]context.CancelFunc),
		reconcileInterval: 30 * time.Second, // default; override with WithReconcileInterval
		reconcileTrigger: make(chan struct{}, 1),
		reconcileStatus:  ReconcileStatus{Enabled: true},
		bootstrapProg:    &bootstrapProgress{state: "idle"},
	}
}

// WithReconcileInterval sets how often the reconciler scans for PubSub-missed keys.
// Set to 0 to disable the reconciler entirely.
func (p *Producer) WithReconcileInterval(d time.Duration) *Producer {
	p.reconcileInterval = d
	return p
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

// WithPatternFilter sets the include/exclude glob patterns for key filtering.
// If includeGlobs is non-empty, only keys matching at least one pattern are replicated.
// Keys matching any excludeGlob are always skipped, regardless of includeGlobs.
// Uses standard glob syntax: * matches any sequence, ? matches one character.
// Example: WithPatternFilter([]string{"users:*","orders:*"}, []string{"cache:*"})
func (p *Producer) WithPatternFilter(includeGlobs, excludeGlobs []string) *Producer {
	p.includeGlobs = includeGlobs
	p.excludeGlobs = excludeGlobs
	return p
}

// TriggerReconcile requests an immediate reconciler cycle without waiting for
// the next scheduled tick. Returns true if the trigger was accepted, false if
// a trigger is already queued (idempotent — safe to call multiple times).
func (p *Producer) TriggerReconcile() bool {
	select {
	case p.reconcileTrigger <- struct{}{}:
		return true
	default:
		return false
	}
}

// SyncKey force-publishes a specific key to the replication stream regardless
// of its __meta: anchor state. Bypasses dedup and pattern filters.
// Useful for post-incident recovery of a known missing key.
func (p *Producer) SyncKey(ctx context.Context, key string) {
	p.handleEvent(ctx, &redis.Message{
		Channel: "__keyevent@0__:set",
		Payload: key,
	}, 0, p.logger)
}

// GetReconcileStatus returns a snapshot of the last reconciler cycle.
func (p *Producer) GetReconcileStatus() ReconcileStatus {
	p.reconcileStatusMu.Lock()
	defer p.reconcileStatusMu.Unlock()
	return p.reconcileStatus
}

// KeyStatus reads the __meta:{key} hash and returns the key's replication state.
// Returns Published=false if the key has never been replicated.
func (p *Producer) KeyStatus(ctx context.Context, key string) (KeyStatus, error) {
	vals, err := p.dataClient.HGetAll(ctx, "__meta:"+key).Result()
	if err != nil {
		return KeyStatus{Key: key}, fmt.Errorf("HGETALL __meta:%s: %w", key, err)
	}
	if len(vals) == 0 {
		return KeyStatus{Key: key, Published: false}, nil
	}
	ks := KeyStatus{Key: key, Published: true, OriginSite: vals["site"]}
	if hlcStr, ok := vals["hlc"]; ok {
		if v, err := strconv.ParseUint(hlcStr, 10, 64); err == nil {
			ks.HLC = v
		}
	}
	return ks, nil
}

// StartBootstrap triggers a one-time full-scan publish of all data keys.
// Only one bootstrap can run at a time; returns error if one is already running.
// The bootstrap respects pattern filters. Use GetBootstrapStatus to track progress.
func (p *Producer) StartBootstrap(ctx context.Context) error {
	p.bootstrapMu.Lock()
	defer p.bootstrapMu.Unlock()
	if p.bootstrapProg.state == "running" {
		return fmt.Errorf("bootstrap already running (published=%d)", p.bootstrapProg.published.Load())
	}
	bCtx, cancel := context.WithCancel(ctx)
	p.bootstrapCancel = cancel
	prog := &bootstrapProgress{state: "running", startedAt: time.Now()}
	p.bootstrapProg = prog
	metrics.BootstrapInProgress.WithLabelValues(p.siteID).Set(1)
	go p.runBootstrap(bCtx, prog)
	return nil
}

// StopBootstrap cancels a running bootstrap. No-op if not running.
func (p *Producer) StopBootstrap() {
	p.bootstrapMu.Lock()
	defer p.bootstrapMu.Unlock()
	if p.bootstrapCancel != nil {
		p.bootstrapCancel()
	}
}

// GetBootstrapStatus returns a snapshot of the current or last bootstrap.
func (p *Producer) GetBootstrapStatus() BootstrapStatus {
	p.bootstrapMu.Lock()
	prog := p.bootstrapProg
	p.bootstrapMu.Unlock()
	return BootstrapStatus{
		State:      prog.state,
		StartedAt:  prog.startedAt,
		FinishedAt: prog.finishedAt,
		Published:  prog.published.Load(),
		Errors:     prog.errors.Load(),
		ErrorMsg:   prog.errMsg,
	}
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

	// Reconciler catches keys whose PubSub event was silently dropped.
	// PubSub is at-most-once; the reconciler is the reliability safety net.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runReconciler(ctx)
	}()

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

	// Log address sets so we can detect hostname vs IP format mismatches.
	{
		cur := make([]string, 0, len(currentAddrs))
		for a := range currentAddrs {
			cur = append(cur, a)
		}
		p.subMu.Lock()
		sub := make([]string, 0, len(p.subSet))
		for a := range p.subSet {
			sub = append(sub, a)
		}
		p.subMu.Unlock()
		p.logger.Info("topology reconcile",
			zap.Strings("cluster_masters", cur),
			zap.Strings("active_subscriptions", sub),
		)
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

// runReconciler periodically scans all data keys and re-publishes any that
// were never captured by PubSub (at-most-once delivery gap). The scan interval
// is configured via WithReconcileInterval (default 30 s; 0 = disabled).
// Each cycle waits a settle delay (1/3 of the interval, min 5 s, max 15 s)
// before re-checking candidates, allowing in-flight PubSub events to finish.
func (p *Producer) runReconciler(ctx context.Context) {
	if p.reconcileInterval <= 0 {
		p.logger.Info("reconciler: disabled (reconcile_interval_seconds=0)")
		p.reconcileStatusMu.Lock()
		p.reconcileStatus.Enabled = false
		p.reconcileStatusMu.Unlock()
		return
	}
	settleDelay := p.reconcileInterval / 3
	if settleDelay < 5*time.Second {
		settleDelay = 5 * time.Second
	}
	if settleDelay > 15*time.Second {
		settleDelay = 15 * time.Second
	}
	p.logger.Info("reconciler: started",
		zap.Duration("interval", p.reconcileInterval),
		zap.Duration("settle", settleDelay))
	ticker := time.NewTicker(p.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.reconcileTrigger:
			p.logger.Info("reconciler: manual trigger received")
			p.reconcileMissedEvents(ctx, settleDelay)
		case <-ticker.C:
			p.reconcileMissedEvents(ctx, settleDelay)
		}
	}
}

func (p *Producer) reconcileMissedEvents(ctx context.Context, settleDelay time.Duration) {
	scanStart := time.Now()
	candidates, err := p.scanMissingMeta(ctx)
	scanMs := time.Since(scanStart).Milliseconds()

	if err != nil {
		p.logger.Warn("reconciler: scan failed", zap.Error(err))
		return
	}
	metrics.ReconcilerScanDurationMs.WithLabelValues(p.siteID).Observe(float64(scanMs))

	if len(candidates) == 0 {
		p.updateReconcileStatus(scanMs, 0)
		return
	}
	p.logger.Info("reconciler: keys without meta (settling)",
		zap.Int("count", len(candidates)), zap.Duration("settle", settleDelay))

	select {
	case <-ctx.Done():
		return
	case <-time.After(settleDelay):
	}

	republished := 0
	for _, key := range candidates {
		if ctx.Err() != nil {
			return
		}
		exists, err := p.dataClient.Exists(ctx, "__meta:"+key).Result()
		if err != nil || exists > 0 {
			continue // PubSub caught up during settle window
		}
		p.logger.Info("reconciler: re-publishing missed event", zap.String("key", key))
		p.handleEvent(ctx, &redis.Message{
			Channel: "__keyevent@0__:set",
			Payload: key,
		}, 0, p.logger)
		republished++
	}
	if republished > 0 {
		p.logger.Info("reconciler: done", zap.Int("republished", republished))
		metrics.ReconcilerRepairedTotal.WithLabelValues(p.siteID).Add(float64(republished))
	}
	metrics.ReconcilerRunsTotal.WithLabelValues(p.siteID).Inc()
	p.updateReconcileStatus(scanMs, republished)
}

func (p *Producer) updateReconcileStatus(scanMs int64, repaired int) {
	p.reconcileStatusMu.Lock()
	defer p.reconcileStatusMu.Unlock()
	p.reconcileStatus.LastRunAt = time.Now()
	p.reconcileStatus.LastScanMs = scanMs
	p.reconcileStatus.LastRepaired = repaired
	p.reconcileStatus.TotalRuns++
}

// scanMissingMeta returns all non-internal keys that have no __meta: entry.
// Phase 1: SCAN all masters to collect data keys (respecting pattern filter).
// Phase 2: Pipeline EXISTS checks in batches of 100 (100x fewer round trips
// vs one EXISTS per key).
func (p *Producer) scanMissingMeta(ctx context.Context) ([]string, error) {
	var (
		mu      sync.Mutex
		allKeys []string
	)

	// Phase 1: SCAN each master for data keys (skipping internal keys and filtered keys).
	scanNode := func(scanCtx context.Context, c *redis.Client) error {
		var cursor uint64
		for {
			keys, next, err := c.Scan(scanCtx, cursor, "*", 500).Result()
			if err != nil {
				return err
			}
			var local []string
			for _, key := range keys {
				if isInternalKey(key) {
					continue
				}
				if !p.matchesFilter(key) {
					continue
				}
				local = append(local, key)
			}
			if len(local) > 0 {
				mu.Lock()
				allKeys = append(allKeys, local...)
				mu.Unlock()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return nil
	}

	if cc, ok := p.dataClient.(*redis.ClusterClient); ok {
		if err := cc.ForEachMaster(ctx, scanNode); err != nil {
			return nil, fmt.Errorf("reconciler ForEachMaster: %w", err)
		}
	} else if c, ok := p.dataClient.(*redis.Client); ok {
		if err := scanNode(ctx, c); err != nil {
			return nil, err
		}
	}

	if len(allKeys) == 0 {
		return nil, nil
	}

	// Phase 2: pipeline EXISTS __meta:{key} in batches of 100.
	// This reduces N round trips to N/100 round trips.
	const pipelineBatch = 100
	var missing []string
	for i := 0; i < len(allKeys); i += pipelineBatch {
		end := i + pipelineBatch
		if end > len(allKeys) {
			end = len(allKeys)
		}
		batch := allKeys[i:end]

		pipe := p.dataClient.Pipeline()
		cmds := make([]*redis.IntCmd, len(batch))
		for j, key := range batch {
			cmds[j] = pipe.Exists(ctx, "__meta:"+key)
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, fmt.Errorf("reconciler pipeline EXISTS: %w", err)
		}
		for j, cmd := range cmds {
			if cmd.Val() == 0 {
				missing = append(missing, batch[j])
			}
		}
	}
	return missing, nil
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
	const workerConcurrency = 200
	sem := make(chan struct{}, workerConcurrency)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return fmt.Errorf("pubsub channel closed")
			}
			go func(m *redis.Message) {
				// Capture the applying-shadow BEFORE entering the semaphore queue.
				shadowAtFire, _ := p.dedup.ApplyingHash(ctx, m.Payload)

				sem <- struct{}{}
				defer func() { <-sem }()
				p.handleEvent(ctx, m, shadowAtFire, logger)
			}(msg)
		}
	}
}

func (p *Producer) handleEvent(ctx context.Context, msg *redis.Message, shadowAtFire uint32, logger *zap.Logger) {
	// msg.Channel: __keyevent@0__:set
	// msg.Payload: the key name
	key := msg.Payload
	cmd := extractCommand(msg.Channel)

	// Skip internal keys (meta, dedup shadow, replication streams, DLQ)
	if isInternalKey(key) {
		return
	}

	// Skip untracked commands
	if !trackedCommands[cmd] {
		return
	}

	// Apply pattern filter (Feature 5). SyncKey bypasses this via shadowAtFire=0
	// check below, but pattern filter still applies to PubSub events.
	if len(p.includeGlobs) > 0 || len(p.excludeGlobs) > 0 {
		if !p.matchesFilter(key) {
			metrics.PatternFilteredTotal.WithLabelValues(p.siteID, "pattern").Inc()
			return
		}
	}

	// Read key value and TTL via dataClient (auto-routes to correct shard in cluster mode).
	keyType, value, err := reader.ReadValue(ctx, p.dataClient, key)
	if err != nil {
		if cmd == "del" {
			keyType = "none"
			value = nil
		} else {
			// Retry with exponential backoff — pool exhaustion at burst rates
			// can cause transient failures that resolve within a few ms.
			const maxAttempts = 5
			var retryErr error
			for attempt := 1; attempt < maxAttempts; attempt++ {
				time.Sleep(time.Duration(10*(1<<(attempt-1))) * time.Millisecond) // 10, 20, 40, 80 ms
				keyType, value, retryErr = reader.ReadValue(ctx, p.dataClient, key)
				if retryErr == nil {
					break
				}
			}
			if retryErr != nil {
				logger.Warn("failed to read key value after retries, dropping",
					zap.String("key", key), zap.Int("attempts", maxAttempts), zap.Error(retryErr))
				return
			}
		}
	}

	// Value-hash loop prevention: use the shadow snapshot taken at event-fire
	// time (before any semaphore delay) to distinguish Apply-triggered events
	// (shadow set BEFORE the triggering SET) from original user writes (no shadow).
	if shadowAtFire > 0 && dedup.ValueHash(value) == shadowAtFire {
		logger.Warn("shadow-dedup drop",
			zap.String("key", key),
			zap.String("cmd", cmd),
			zap.Uint32("shadowAtFire", shadowAtFire),
			zap.Uint32("valueHash", dedup.ValueHash(value)),
		)
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

	// Compute absolute expiry epoch so the consumer can call PExpireAt directly
	// without relying on clock synchronisation between sites (Feature 7).
	var expiresAtMs int64
	if ttlMs > 0 {
		expiresAtMs = capturedAt + ttlMs
	}

	delta := bus.Delta{
		SiteID:      p.siteID,
		Key:         key,
		KeyType:     keyType,
		Value:       value,
		HLC:         ts,
		CapturedAt:  capturedAt,
		TTLMs:       ttlMs,
		ExpiresAtMs: expiresAtMs,
		Cmd:         strings.ToUpper(cmd),
		SeqID:       seqID,
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

	// Publish to bus — retry once on transient failure.
	if err := p.bus.Publish(ctx, p.siteID, delta); err != nil {
		time.Sleep(100 * time.Millisecond)
		if err2 := p.bus.Publish(ctx, p.siteID, delta); err2 != nil {
			logger.Error("failed to publish delta after retry",
				zap.String("key", key), zap.Error(err2))
			return
		}
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

// isInternalKey returns true for RedisBridge-managed keys that must never be replicated.
func isInternalKey(key string) bool {
	return strings.HasPrefix(key, "__repl:") ||
		strings.HasPrefix(key, "__meta:") ||
		strings.HasPrefix(key, "repl:stream:") ||
		strings.HasPrefix(key, "repl:dlq:")
}

// matchesFilter returns true if key should be replicated given the configured
// include/exclude glob patterns. An empty includeGlobs list means all keys are
// included. Exclude patterns take priority over include patterns.
// Glob syntax: * matches any sequence, ? matches one character.
// Note: path.Match treats / as a separator but Redis keys rarely contain /;
// for keys with / use explicit patterns.
func (p *Producer) matchesFilter(key string) bool {
	// Step 1: include filter — key must match at least one include pattern
	if len(p.includeGlobs) > 0 {
		included := false
		for _, pat := range p.includeGlobs {
			if m, _ := path.Match(pat, key); m {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}
	// Step 2: exclude filter — key must NOT match any exclude pattern
	for _, pat := range p.excludeGlobs {
		if m, _ := path.Match(pat, key); m {
			return false
		}
	}
	return true
}

// runBootstrap performs a full-scan publish of all data keys to the replication
// stream. Used to seed a new site or recover after a prolonged outage.
// Progress is tracked in prog; metrics are updated throughout.
func (p *Producer) runBootstrap(ctx context.Context, prog *bootstrapProgress) {
	defer func() {
		prog.finishedAt = time.Now()
		if ctx.Err() != nil {
			prog.state = "cancelled"
		} else if prog.errors.Load() > 0 {
			prog.state = "done_with_errors"
		} else {
			prog.state = "done"
		}
		metrics.BootstrapInProgress.WithLabelValues(p.siteID).Set(0)
		p.logger.Info("bootstrap: finished",
			zap.String("state", prog.state),
			zap.Int64("published", prog.published.Load()),
			zap.Int64("errors", prog.errors.Load()),
		)
	}()

	p.logger.Info("bootstrap: started",
		zap.Strings("include", p.includeGlobs),
		zap.Strings("exclude", p.excludeGlobs),
	)

	publishKey := func(key string) {
		if ctx.Err() != nil {
			return
		}
		p.handleEvent(ctx, &redis.Message{
			Channel: "__keyevent@0__:set",
			Payload: key,
		}, 0, p.logger)
		prog.published.Add(1)
		metrics.BootstrapKeysPublishedTotal.WithLabelValues(p.siteID).Inc()
	}

	scanNode := func(scanCtx context.Context, c *redis.Client) error {
		var cursor uint64
		for {
			if scanCtx.Err() != nil {
				return nil
			}
			keys, next, err := c.Scan(scanCtx, cursor, "*", 500).Result()
			if err != nil {
				return err
			}
			for _, key := range keys {
				if isInternalKey(key) {
					continue
				}
				if !p.matchesFilter(key) {
					continue
				}
				publishKey(key)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return nil
	}

	var scanErr error
	if cc, ok := p.dataClient.(*redis.ClusterClient); ok {
		scanErr = cc.ForEachMaster(ctx, scanNode)
	} else if c, ok := p.dataClient.(*redis.Client); ok {
		scanErr = scanNode(ctx, c)
	}

	if scanErr != nil {
		prog.errors.Add(1)
		prog.errMsg = scanErr.Error()
		p.logger.Error("bootstrap: scan error", zap.Error(scanErr))
	}
}
