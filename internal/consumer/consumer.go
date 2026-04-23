package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/nagaraju/redibridge/internal/applier"
	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/hlc"
	"github.com/nagaraju/redibridge/internal/metrics"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Consumer reads delta events from remote peers and applies them locally.
type Consumer struct {
	siteID       string
	consumerID   string // unique XREADGROUP consumer name — set per agent instance
	peers        []string
	bus          bus.Bus
	localClient  redis.UniversalClient
	applier      *applier.Applier
	dedup        *dedup.Filter
	clock        *hlc.HLC
	group        string
	batchSize    int
	concurrency  int
	logger       *zap.Logger

	mu       sync.Mutex
	paused   map[string]bool
	cancelFn map[string]context.CancelFunc

	// keyMu provides per-key serialization so concurrent peer goroutines
	// do not race on LWW check + apply for the same key (TOCTOU prevention).
	keyMu sync.Map // map[string]*sync.Mutex
}

// New creates a new Consumer.
// consumerID must be unique per agent instance on the same site so that
// XREADGROUP tracks each instance's PEL (pending entry list) independently.
// Use hostname or a numbered suffix (e.g. "ec-site-a-consumer-2") when
// running multiple consumer-only agents on the same site.
func New(
	siteID string,
	consumerID string,
	peers []string,
	b bus.Bus,
	localClient redis.UniversalClient,
	app *applier.Applier,
	dd *dedup.Filter,
	clock *hlc.HLC,
	group string,
	batchSize int,
	concurrency int,
	logger *zap.Logger,
) *Consumer {
	return &Consumer{
		siteID:      siteID,
		consumerID:  consumerID,
		peers:       peers,
		bus:         b,
		localClient: localClient,
		applier:     app,
		dedup:       dd,
		clock:       clock,
		group:       group,
		batchSize:   batchSize,
		concurrency: concurrency,
		logger:      logger,
		paused:      make(map[string]bool),
		cancelFn:    make(map[string]context.CancelFunc),
	}
}

// Run starts consumer goroutines for all peers. Blocks until ctx is cancelled.
// groupName returns a site-scoped consumer group name.
// Each agent gets its own group per peer stream so every agent
// receives an independent copy of every message.
func (c *Consumer) groupName() string {
	return c.group + "-" + c.siteID
}

func (c *Consumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for _, peer := range c.peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			// Retry EnsureGroup with backoff — peer cluster may still be initialising.
			backoff := 500 * time.Millisecond
			for {
				if err := c.bus.EnsureGroup(ctx, peerID, c.groupName()); err != nil {
					c.logger.Error("failed to ensure consumer group, retrying",
						zap.String("peer", peerID),
						zap.Duration("backoff", backoff),
						zap.Error(err))
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					if backoff < 10*time.Second {
						backoff *= 2
					}
					continue
				}
				break
			}
			c.consumePeer(ctx, peerID)
		}(peer)
	}

	wg.Wait()
	return nil
}

// Pause stops consuming from a specific peer.
func (c *Consumer) Pause(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused[peer] = true
	if cancel, ok := c.cancelFn[peer]; ok {
		cancel()
	}
	c.logger.Info("paused replication from peer", zap.String("peer", peer))
}

// Resume resumes consuming from a specific peer.
func (c *Consumer) Resume(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused[peer] = false
	c.logger.Info("resumed replication from peer", zap.String("peer", peer))
}

// IsPaused returns whether replication from a peer is paused.
func (c *Consumer) IsPaused(peer string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused[peer]
}

func (c *Consumer) consumePeer(ctx context.Context, peerID string) {
	logger := c.logger.With(zap.String("peer", peerID))
	consumerName := c.consumerID

	// Start by draining the PEL (messages delivered but not ACKed in a prior
	// run). Once the PEL is empty, switch to ">" for new messages.
	startID := "0"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check if paused
		if c.IsPaused(peerID) {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Create a cancellable sub-context for this peer
		peerCtx, cancel := context.WithCancel(ctx)
		c.mu.Lock()
		c.cancelFn[peerID] = cancel
		c.mu.Unlock()

		msgs, err := c.bus.Consume(peerCtx, peerID, c.groupName(), consumerName, c.batchSize, startID)
		cancel()

		// When reading PEL ("0") returns empty, the backlog is drained —
		// switch to new messages (">").
		if err == nil && len(msgs) == 0 && startID == "0" {
			startID = ">"
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("consume error", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		// Process messages with worker pool
		ackIDs := c.processMessages(ctx, peerID, msgs, logger)

		// ACK processed messages
		if len(ackIDs) > 0 {
			if err := c.bus.Ack(ctx, peerID, c.groupName(), ackIDs); err != nil {
				logger.Error("ack error", zap.Error(err))
			}
		}

		// If every message in the batch failed and the local cluster is
		// unreachable, stop consuming new messages and wait for recovery.
		// Once healthy, reset startID to "0" to re-drain the PEL.
		if len(msgs) > 0 && len(ackIDs) == 0 {
			if pingErr := c.localClient.Ping(ctx).Err(); pingErr != nil {
				logger.Warn("local cluster unreachable, pausing consumption until recovery",
					zap.Error(pingErr))
				c.waitForLocalCluster(ctx, logger)
				startID = "0" // re-drain PEL after recovery
			}
		}

		// Any message that failed to apply (processOne returned false) stays in
		// the PEL un-ACKed. Switch back to "0" so the next Consume call
		// re-delivers those entries and retries them. This handles transient
		// errors such as a partial cross-shard pipeline failure in the applier.
		if len(ackIDs) < len(msgs) && startID == ">" {
			startID = "0"
		}
	}
}

// waitForLocalCluster blocks with exponential backoff until the local Redis
// cluster responds to PING or the context is cancelled.
func (c *Consumer) waitForLocalCluster(ctx context.Context, logger *zap.Logger) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err := c.localClient.Ping(ctx).Err(); err == nil {
			logger.Info("local cluster recovered, resuming consumption")
			return
		}
		logger.Warn("local cluster still unreachable", zap.Duration("retry_in", backoff))
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Consumer) processMessages(ctx context.Context, peerID string, msgs []bus.Message, logger *zap.Logger) []string {
	var mu sync.Mutex
	var ackIDs []string
	var wg sync.WaitGroup

	sem := make(chan struct{}, c.concurrency)

	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return ackIDs
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(m bus.Message) {
			defer wg.Done()
			defer func() { <-sem }()

			if c.processOne(ctx, peerID, m, logger) {
				mu.Lock()
				ackIDs = append(ackIDs, m.ID)
				mu.Unlock()
			}
		}(msg)
	}

	wg.Wait()
	return ackIDs
}

// PeerLag is the lag snapshot for a single peer, returned by PeerLags.
type PeerLag struct {
	SiteID string       `json:"site_id"`
	Paused bool         `json:"paused"`
	Stream bus.LagInfo  `json:"stream"`
	Err    string       `json:"error,omitempty"`
}

// PeerLags returns a lag snapshot for every configured peer.
func (c *Consumer) PeerLags(ctx context.Context) []PeerLag {
	result := make([]PeerLag, 0, len(c.peers))
	for _, peer := range c.peers {
		info, err := c.bus.GroupLag(ctx, peer, c.groupName())
		pl := PeerLag{
			SiteID: peer,
			Paused: c.IsPaused(peer),
			Stream: info,
		}
		if err != nil {
			pl.Err = err.Error()
		}
		result = append(result, pl)
	}
	return result
}

// keyLock returns the per-key mutex, creating it on first access.
func (c *Consumer) keyLock(key string) *sync.Mutex {
	mu, _ := c.keyMu.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (c *Consumer) processOne(ctx context.Context, peerID string, msg bus.Message, logger *zap.Logger) bool {
	delta := msg.Delta

	// Loop-back guard: skip if delta originated from this site
	if delta.SiteID == c.siteID {
		metrics.WritesDiscarded.WithLabelValues(c.siteID, peerID, "loopback").Inc()
		return true // ACK it so we don't re-process
	}

	// SeqID dedup check (fast path, before acquiring lock)
	if c.dedup.SeenSeqID(delta.SeqID) {
		metrics.WritesDiscarded.WithLabelValues(c.siteID, peerID, "dedup_hit").Inc()
		return true
	}

	// Serialize LWW check + apply for this key across all peer goroutines.
	// Without this, two goroutines consuming different peer streams can both
	// read meta=0, both accept, and the last writer wins arbitrarily.
	lock := c.keyLock(delta.Key)
	lock.Lock()
	defer lock.Unlock()

	// Re-check dedup under lock in case another goroutine just processed the same SeqID
	if c.dedup.SeenSeqID(delta.SeqID) {
		metrics.WritesDiscarded.WithLabelValues(c.siteID, peerID, "dedup_hit").Inc()
		return true
	}

	// LWW resolution
	localHLC, localSite, err := applier.ReadMeta(ctx, c.localClient, delta.Key)
	if err != nil {
		logger.Error("read meta failed", zap.String("key", delta.Key), zap.Error(err))
		return false // Don't ACK, retry later
	}

	// If meta is absent but the key already exists locally, the producer may not
	// have written __meta yet (keyspace-event processing is async). Wait a short
	// grace period and re-read so we don't incorrectly overwrite a newer local
	// write that hasn't been stamped yet.
	if localHLC == 0 {
		exists, _ := c.localClient.Exists(ctx, delta.Key).Result()
		if exists > 0 {
			time.Sleep(40 * time.Millisecond)
			localHLC, localSite, err = applier.ReadMeta(ctx, c.localClient, delta.Key)
			if err != nil {
				logger.Error("read meta retry failed", zap.String("key", delta.Key), zap.Error(err))
				return false
			}
		}
	}

	accepted := false
	var lwwReason string
	if localHLC == 0 {
		// No meta = genuinely first write, always accept
		accepted = true
		lwwReason = "no_meta"
	} else if delta.HLC > localHLC {
		accepted = true
		lwwReason = "hlc_newer"
	} else if delta.HLC == localHLC && delta.SiteID > localSite {
		// Deterministic tie-break: higher site_id wins
		accepted = true
		lwwReason = "tie_break"
	} else {
		lwwReason = "lww_lost"
	}

	logger.Info("LWW decision",
		zap.String("key", delta.Key),
		zap.String("peer", peerID),
		zap.Uint64("delta_hlc", delta.HLC),
		zap.Uint64("local_hlc", localHLC),
		zap.String("delta_site", delta.SiteID),
		zap.String("local_site", localSite),
		zap.Bool("accepted", accepted),
		zap.String("reason", lwwReason),
	)

	if !accepted {
		metrics.WritesDiscarded.WithLabelValues(c.siteID, peerID, "lww_lost").Inc()
		c.dedup.MarkSeqID(delta.SeqID)
		return true
	}

	// Apply delta BEFORE advancing the clock. The applier sets the dedup
	// shadow internally (MarkApplying) right before writing the value, so the
	// producer's receipt-time shadow snapshot (taken in subscribeAndProcess)
	// correctly distinguishes the original write event (shadow not yet set)
	// from the Apply's write event (shadow set). Advancing the clock after
	// Apply ensures producer goroutines that are stamping concurrent original-
	// write events use the real wall clock, not an HLC inflated by Update().
	start := time.Now()
	if err := c.applier.Apply(ctx, delta); err != nil {
		logger.Error("apply failed",
			zap.String("key", delta.Key),
			zap.String("peer", peerID),
			zap.Error(err))
		return false
	}

	// Advance HLC after apply so concurrent producer goroutines don't
	// get an artificially higher timestamp for unrelated original writes.
	c.clock.Update(delta.HLC)

	elapsed := float64(time.Since(start).Milliseconds())
	metrics.ApplyDuration.WithLabelValues(c.siteID, peerID).Observe(elapsed)
	metrics.WritesApplied.WithLabelValues(c.siteID, peerID).Inc()
	metrics.EventsConsumed.WithLabelValues(c.siteID, peerID).Inc()

	// Track lag
	lag := float64(time.Now().UnixMilli() - delta.CapturedAt)
	metrics.LagMs.WithLabelValues(c.siteID, peerID).Set(lag)

	// Mark SeqID as seen
	c.dedup.MarkSeqID(delta.SeqID)

	return true
}
