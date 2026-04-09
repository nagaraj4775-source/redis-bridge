package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
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

	// Histograms
	ApplyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "repl_apply_duration_ms",
		Help:    "Duration of apply operations in milliseconds",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250},
	}, []string{"site_id", "peer_site"})
)
