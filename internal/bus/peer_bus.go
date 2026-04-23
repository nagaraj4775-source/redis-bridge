package bus

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// PerPeerBus implements Bus for embedded mode where each site's replication
// stream lives on that site's own Redis instance.
//
// Publish  → writes to the local bus (this site's Redis)
// Consume  → reads from the peer's own Redis (keyed by peerSiteID)
// Ack      → acks on the peer's Redis
// StreamLen → queries the local bus
//
// This allows each agent to host its own stream while peers pull from it
// directly, with no dedicated shared bus instance.
type PerPeerBus struct {
	local  *RedisStreamsBus            // publish + streamLen
	peers  map[string]*RedisStreamsBus // consume + ack per peer
	logger *zap.Logger
}

// NewPerPeerBus builds a PerPeerBus.
//   - localAddr  : this site's Redis address (bus for publishing)
//   - peerAddrs  : map[siteID]redisAddr for peer streams
//   - password   : shared password (or "" for none)
//   - streamPrefix: e.g. "repl:stream:"
//   - streamTTL  : time-based retention (0 = disabled, use maxLen instead)
func NewPerPeerBus(localAddr string, peerAddrs map[string]string, password, streamPrefix string, maxLen int64, streamTTL time.Duration, logger *zap.Logger) *PerPeerBus {
	peers := make(map[string]*RedisStreamsBus, len(peerAddrs))
	for siteID, addr := range peerAddrs {
		peers[siteID] = NewRedisStreamsBus(addr, password, streamPrefix, maxLen, streamTTL, logger)
	}
	return &PerPeerBus{
		local:  NewRedisStreamsBus(localAddr, password, streamPrefix, maxLen, streamTTL, logger),
		peers:  peers,
		logger: logger,
	}
}

// NewPerPeerBusDirect builds a PerPeerBus from pre-constructed bus instances.
// Used by main.go when the caller needs to choose standalone vs cluster bus clients.
func NewPerPeerBusDirect(local *RedisStreamsBus, peers map[string]*RedisStreamsBus, logger *zap.Logger) *PerPeerBus {
	return &PerPeerBus{
		local:  local,
		peers:  peers,
		logger: logger,
	}
}

// Ping checks connectivity to the local bus Redis instance.
func (p *PerPeerBus) Ping(ctx context.Context) error {
	return p.local.Ping(ctx)
}

// Publish writes the delta to this site's local stream.
func (p *PerPeerBus) Publish(ctx context.Context, siteID string, delta Delta) error {
	return p.local.Publish(ctx, siteID, delta)
}

// Consume reads from the named peer's Redis.
func (p *PerPeerBus) Consume(ctx context.Context, peerSiteID string, group string, consumer string, batchSize int, startID string) ([]Message, error) {
	peer, ok := p.peers[peerSiteID]
	if !ok {
		return nil, fmt.Errorf("PerPeerBus: no bus configured for peer %q", peerSiteID)
	}
	return peer.Consume(ctx, peerSiteID, group, consumer, batchSize, startID)
}

// Ack acknowledges messages on the peer's Redis.
func (p *PerPeerBus) Ack(ctx context.Context, peerSiteID string, group string, ids []string) error {
	peer, ok := p.peers[peerSiteID]
	if !ok {
		return fmt.Errorf("PerPeerBus: no bus configured for peer %q", peerSiteID)
	}
	return peer.Ack(ctx, peerSiteID, group, ids)
}

// EnsureGroup creates consumer groups on the peer's Redis.
func (p *PerPeerBus) EnsureGroup(ctx context.Context, peerSiteID, group string) error {
	peer, ok := p.peers[peerSiteID]
	if !ok {
		return fmt.Errorf("PerPeerBus: no bus configured for peer %q", peerSiteID)
	}
	return peer.EnsureGroup(ctx, peerSiteID, group)
}

// StreamLen returns the length of this site's local stream.
func (p *PerPeerBus) StreamLen(ctx context.Context, siteID string) (int64, error) {
	return p.local.StreamLen(ctx, siteID)
}

// GroupLag returns consumer-group lag stats from the named peer's Redis.
func (p *PerPeerBus) GroupLag(ctx context.Context, peerSiteID, group string) (LagInfo, error) {
	peer, ok := p.peers[peerSiteID]
	if !ok {
		return LagInfo{}, fmt.Errorf("PerPeerBus: no bus configured for peer %q", peerSiteID)
	}
	return peer.GroupLag(ctx, peerSiteID, group)
}

// Close shuts down all bus clients.
func (p *PerPeerBus) Close() error {
	var firstErr error
	if err := p.local.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, peer := range p.peers {
		if err := peer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
