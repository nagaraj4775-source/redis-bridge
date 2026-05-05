package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Agent identity / config info — value always 1; metadata carried in labels.
	// Displayed in Grafana as a table or stat panel to show site name, peers, role, TTL.
	AgentInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_agent_info",
		Help: "Static agent configuration info (value=1). Use labels for site, role, peers, stream TTL.",
	}, []string{"site_id", "role", "peers", "stream_ttl_hours", "consumer_id"})

	AgentUptimeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_agent_uptime_seconds",
		Help: "Seconds since agent started",
	}, []string{"site_id"})

	// Peer connectivity — 1 = GroupLag call succeeded (peer reachable), 0 = error
	PeerUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_peer_up",
		Help: "1 if the peer's stream is reachable, 0 if the last GroupLag call failed",
	}, []string{"site_id", "peer_site"})

	// Counters
	EventsCaptured = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_events_captured_total",
		Help: "Total keyspace events captured by the producer",
	}, []string{"site_id", "key_type"})

	EventsPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_events_published_total",
		Help: "Total events published to the replication bus",
	}, []string{"site_id"})

	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_events_consumed_total",
		Help: "Total events consumed from peer streams",
	}, []string{"site_id", "peer_site"})

	WritesApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_writes_applied_total",
		Help: "Total writes applied to local cluster",
	}, []string{"site_id", "peer_site"})

	WritesDiscarded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_writes_discarded_total",
		Help: "Total writes discarded during replication",
	}, []string{"site_id", "peer_site", "reason"})

	// Gauges
	LagMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_lag_ms",
		Help: "Replication lag in milliseconds per peer",
	}, []string{"site_id", "peer_site"})

	BusStreamLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_bus_stream_length",
		Help: "Length of each replication stream",
	}, []string{"stream"})

	PendingEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_pending_entries",
		Help: "Delivered but unACKed stream entries per peer (in-flight)",
	}, []string{"site_id", "peer_site"})

	ConsumerGroupLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_consumer_group_lag",
		Help: "Undelivered stream entries per peer (consumer group lag)",
	}, []string{"site_id", "peer_site"})

	// Histograms
	ApplyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "repl_apply_duration_ms",
		Help:    "Duration of apply operations in milliseconds",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	}, []string{"site_id", "peer_site"})

	// Reconciler metrics
	ReconcilerRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_reconciler_runs_total",
		Help: "Total number of reconciler cycles completed",
	}, []string{"site_id"})

	ReconcilerRepairedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_reconciler_repaired_total",
		Help: "Total number of keys re-published by the reconciler (PubSub drops recovered)",
	}, []string{"site_id"})

	ReconcilerScanDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "repl_reconciler_scan_duration_ms",
		Help:    "Duration of each reconciler full-scan cycle in milliseconds",
		Buckets: []float64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000},
	}, []string{"site_id"})

	// Bootstrap metrics
	BootstrapInProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "repl_bootstrap_in_progress",
		Help: "1 if a bootstrap scan is currently running, 0 otherwise",
	}, []string{"site_id"})

	BootstrapKeysPublishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_bootstrap_keys_published_total",
		Help: "Total keys published during bootstrap syncs",
	}, []string{"site_id"})

	// Pattern filter metrics
	PatternFilteredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_pattern_filtered_total",
		Help: "Total keyspace events skipped by include/exclude pattern filters",
	}, []string{"site_id", "reason"})

	// TTL metrics
	TTLExpiredInTransitTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "repl_ttl_expired_in_transit_total",
		Help: "Total deltas skipped because the key TTL expired before the consumer applied it",
	}, []string{"site_id"})
)
