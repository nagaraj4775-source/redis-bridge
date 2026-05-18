package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nagaraju/redibridge/internal/applier"
	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/config"
	"github.com/nagaraju/redibridge/internal/consumer"
	"github.com/nagaraju/redibridge/internal/coordinator"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/hlc"
	"github.com/nagaraju/redibridge/internal/metrics"
	"github.com/nagaraju/redibridge/internal/producer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	configPath := flag.String("config", "config/agent.yaml", "Path to config file")
	flag.Parse()

	// Structured logger — human-readable timestamps in the container's local timezone.
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Local().Format("2006-01-02T15:04:05.000Z07:00"))
	}
	zapCfg := zap.NewProductionConfig()
	zapCfg.EncoderConfig = encoderCfg
	logger, err := zapCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}
	// Resolve consumer ID: use config value → hostname → random UUID fallback.
	// Must be unique per agent instance so XREADGROUP tracks each PEL separately.
	consumerID := cfg.ConsumerID
	if consumerID == "" {
		if h, err := os.Hostname(); err == nil {
			consumerID = h
		} else {
			consumerID = fmt.Sprintf("%s-%d", cfg.SiteID, time.Now().UnixNano())
		}
	}

	logger.Info("config loaded",
		zap.String("site_id", cfg.SiteID),
		zap.String("role", cfg.Role),
		zap.String("consumer_id", consumerID),
		zap.Strings("masters", cfg.Cluster.Masters),
		zap.Strings("peers", cfg.Peers))

	// Publish static agent info metric (value=1, metadata in labels).
	metrics.AgentInfo.WithLabelValues(
		cfg.SiteID,
		cfg.Role,
		strings.Join(cfg.Peers, ","),
		fmt.Sprintf("%d", cfg.Bus.StreamTTLHours),
		consumerID,
	).Set(1)

	// Context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	// Create Redis clients based on cluster mode.
	// masters: per-node *redis.Client used only for PSubscribe (keyspace notifications).
	// localClient: redis.UniversalClient for all data operations; ClusterClient in cluster mode.
	masters, localClient, err := buildClusterClients(ctx, cfg, logger)
	if err != nil {
		logger.Fatal("failed to build cluster clients", zap.Error(err))
	}

	// Create HLC
	clock := hlc.New()

	// Create bus based on bus mode
	replBus, err := buildBusClient(cfg, logger)
	if err != nil {
		logger.Fatal("failed to build bus client", zap.Error(err))
	}
	if err := pingBus(ctx, replBus); err != nil {
		logger.Fatal("failed to connect to bus", zap.Error(err))
	}
	logger.Info("connected to bus",
		zap.String("mode", string(cfg.Bus.Mode)),
		zap.String("addr", resolvedBusAddr(cfg)))

	// Create dedup filter
	dedupTTLMs := cfg.Replication.DedupTTLSeconds * 1000
	dd, err := dedup.NewFilter(localClient, dedupTTLMs, 100000)
	if err != nil {
		logger.Fatal("failed to create dedup filter", zap.Error(err))
	}

	// Create applier
	app := applier.New(localClient, dd, logger).WithSiteID(cfg.SiteID)

	// Create producer
	prod := producer.New(cfg.SiteID, masters, localClient, replBus, clock, dd, logger).
		WithReconcileInterval(time.Duration(cfg.Replication.ReconcileIntervalSeconds) * time.Second).
		WithReconcileStartupDelay(time.Duration(cfg.Replication.ReconcileStartupDelaySeconds) * time.Second).
		WithReconcileScanBatch(cfg.Replication.ReconcileScanBatchSize).
		WithAutoBootstrapThreshold(cfg.Replication.AutoBootstrapThreshold).
		WithPubDedup(cfg.Replication.PubDedup).
		WithPatternFilter(cfg.Replication.IncludePatterns, cfg.Replication.ExcludePatterns)

	// In cluster mode, enable automatic re-subscription when a replica is
	// promoted to master. The producer polls CLUSTER topology every 10 s and
	// starts a new PubSub goroutine for any newly elected master.
	if cfg.Cluster.Mode == config.ModeCluster {
		if cc, ok := localClient.(*redis.ClusterClient); ok {
			nodeOpts := &redis.Options{
				Password:     cfg.Cluster.Password,
				PoolSize:     4,
				MinIdleConns: 1,
				ReadTimeout:  3 * time.Second,
				WriteTimeout: 3 * time.Second,
			}
			prod.WithClusterTopologyWatch(cc, nodeOpts)
		}
	}

	// Create consumer
	cons := consumer.New(
		cfg.SiteID,
		consumerID,
		cfg.Peers,
		replBus,
		localClient,
		app,
		dd,
		clock,
		cfg.Bus.ConsumerGroup,
		cfg.Replication.BatchSize,
		cfg.Replication.ApplyConcurrency,
		logger,
	)

	// Create coordinator (prod may be nil in consumer-only role; coordinator handles nil gracefully)
	var coordProd *producer.Producer
	if cfg.Role != "consumer" {
		coordProd = prod
	}
	coord := coordinator.New(cfg, cons, coordProd, logger)

	// Start all components
	var wg sync.WaitGroup

	// Start Prometheus metrics server
	wg.Add(1)
	go func() {
		defer wg.Done()
		metricsAddr := fmt.Sprintf(":%d", cfg.Metrics.Port)
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: metricsAddr, Handler: mux}
		logger.Info("metrics server listening", zap.String("addr", metricsAddr))

		go func() {
			<-ctx.Done()
			srv.Close()
		}()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	// Start coordinator
	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			<-ctx.Done()
			coord.Stop()
		}()
		if err := coord.Run(); err != http.ErrServerClosed {
			logger.Error("coordinator error", zap.Error(err))
		}
	}()

	// Start producer (skipped in consumer-only role)
	if cfg.Role != "consumer" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := prod.Run(ctx); err != nil {
				logger.Error("producer error", zap.Error(err))
			}
		}()
	} else {
		logger.Info("role=consumer: producer disabled")
	}

	// Start consumer (skipped in producer-only role)
	if cfg.Role != "producer" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cons.Run(ctx); err != nil {
				logger.Error("consumer error", zap.Error(err))
			}
		}()
	} else {
		logger.Info("role=producer: consumer disabled")
	}

	// Background uptime gauge — updated every 5s
	startedAt := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics.AgentUptimeSeconds.WithLabelValues(cfg.SiteID).
					Set(time.Since(startedAt).Seconds())
			}
		}
	}()

	logger.Info("redibridge started",
		zap.String("site_id", cfg.SiteID),
		zap.Int("masters", len(masters)),
		zap.Int("peers", len(cfg.Peers)))

	wg.Wait()

	// Cleanup — close per-node pubsub clients
	for _, m := range masters {
		m.Close()
	}
	// Close localClient only when it is not one of the per-node clients (cluster mode)
	if len(masters) == 0 || localClient != redis.UniversalClient(masters[0]) {
		localClient.Close()
	}
	if err := replBus.Close(); err != nil {
		logger.Warn("bus close error", zap.Error(err))
	}

	logger.Info("redibridge stopped")
}

// buildClusterClients creates Redis clients based on the cluster mode in config.
//
//   - standalone: one *redis.Client; localClient is the same instance
//   - sentinel:   one FailoverClient that auto-follows the master; localClient is the same
//   - cluster:    one *redis.Client per shard master for PSubscribe + a ClusterClient as localClient
func buildClusterClients(ctx context.Context, cfg *config.Config, logger *zap.Logger) ([]*redis.Client, redis.UniversalClient, error) {
	opts := &redis.Options{
		Password:     cfg.Cluster.Password,
		PoolSize:     32,
		MinIdleConns: 4,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	switch cfg.Cluster.Mode {
	case config.ModeStandalone:
		opts.Addr = cfg.Cluster.Addr
		c := redis.NewClient(opts)
		if err := c.Ping(ctx).Err(); err != nil {
			return nil, nil, fmt.Errorf("ping standalone %s: %w", cfg.Cluster.Addr, err)
		}
		logger.Info("connected to standalone redis", zap.String("addr", cfg.Cluster.Addr))
		return []*redis.Client{c}, c, nil

	case config.ModeSentinel:
		c := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.Cluster.SentinelMaster,
			SentinelAddrs: cfg.Cluster.SentinelAddrs,
			Password:      cfg.Cluster.Password,
			PoolSize:      32,
			MinIdleConns:  4,
			ReadTimeout:   3 * time.Second,
			WriteTimeout:  3 * time.Second,
		})
		if err := c.Ping(ctx).Err(); err != nil {
			return nil, nil, fmt.Errorf("ping sentinel master %s: %w", cfg.Cluster.SentinelMaster, err)
		}
		logger.Info("connected via sentinel",
			zap.String("master", cfg.Cluster.SentinelMaster),
			zap.Strings("sentinels", cfg.Cluster.SentinelAddrs))
		return []*redis.Client{c}, c, nil

	case config.ModeCluster:
		// Per-node standalone clients for PSubscribe (keyspace notifications are
		// node-local in cluster mode — the event is emitted by the owning shard).
		pubsubClients := make([]*redis.Client, len(cfg.Cluster.Masters))
		for i, addr := range cfg.Cluster.Masters {
			optsCopy := *opts
			optsCopy.Addr = addr
			c := redis.NewClient(&optsCopy)
			if err := c.Ping(ctx).Err(); err != nil {
				return nil, nil, fmt.Errorf("ping master %s: %w", addr, err)
			}
			pubsubClients[i] = c
			logger.Info("connected to cluster master (pubsub)", zap.String("addr", addr))
		}
		// ClusterClient for all data operations — handles MOVED redirects automatically.
		cc := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.Cluster.Masters,
			Password:     cfg.Cluster.Password,
			PoolSize:     200, // support 200 concurrent workers per node
			MinIdleConns: 8,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
		if err := cc.Ping(ctx).Err(); err != nil {
			return nil, nil, fmt.Errorf("ping cluster client: %w", err)
		}
		logger.Info("connected to redis cluster", zap.Strings("masters", cfg.Cluster.Masters))
		return pubsubClients, cc, nil

	default:
		return nil, nil, fmt.Errorf("unknown cluster mode: %s", cfg.Cluster.Mode)
	}
}

// buildBusClient creates the replication bus based on the bus mode in config.
func buildBusClient(cfg *config.Config, logger *zap.Logger) (bus.Bus, error) {
	streamTTL := time.Duration(cfg.Bus.StreamTTLHours) * time.Hour
	switch cfg.Bus.Mode {
	case config.ModeStandalone:
		return bus.NewRedisStreamsBus(cfg.Bus.Addr, cfg.Bus.Password, cfg.Bus.StreamPrefix, cfg.Bus.StreamMaxLen, streamTTL, logger), nil

	case config.ModeSentinel:
		return bus.NewRedisStreamsBusSentinel(
			cfg.Bus.SentinelMaster,
			cfg.Bus.SentinelAddrs,
			cfg.Bus.Password,
			cfg.Bus.StreamPrefix,
			cfg.Bus.StreamMaxLen,
			streamTTL,
			logger,
		), nil

	case config.ModeBusEmbedded:
		logger.Info("bus using embedded mode",
			zap.Any("peer_addrs", cfg.Bus.PeerBusAddrs))
		if cfg.Cluster.Mode == config.ModeCluster {
			// Cluster mode: use ClusterClient for the local bus and each peer bus.
			// This ensures XADD/XREADGROUP are routed to the correct shard automatically.
			localBus := bus.NewRedisStreamsBusCluster(cfg.Cluster.Masters, cfg.Bus.Password, cfg.Bus.StreamPrefix, cfg.Bus.StreamMaxLen, streamTTL, logger)
			peerBuses := make(map[string]*bus.RedisStreamsBus, len(cfg.Bus.PeerBusAddrs))
			for siteID, addr := range cfg.Bus.PeerBusAddrs {
				// addr is a bootstrap node; ClusterClient discovers remaining nodes.
				peerBuses[siteID] = bus.NewRedisStreamsBusCluster([]string{addr}, cfg.Bus.Password, cfg.Bus.StreamPrefix, cfg.Bus.StreamMaxLen, streamTTL, logger)
			}
			return bus.NewPerPeerBusDirect(localBus, peerBuses, logger), nil
		}
		// Standalone embedded: use the master at master_index as the local bus.
		localAddr := resolvedBusAddr(cfg)
		return bus.NewPerPeerBus(
			localAddr,
			cfg.Bus.PeerBusAddrs,
			cfg.Bus.Password,
			cfg.Bus.StreamPrefix,
			cfg.Bus.StreamMaxLen,
			streamTTL,
			logger,
		), nil

	default:
		return nil, fmt.Errorf("unknown bus mode: %s", cfg.Bus.Mode)
	}
}

// pingBus checks connectivity by delegating to the Bus.Ping method.
func pingBus(ctx context.Context, b bus.Bus) error {
	return b.Ping(ctx)
}

// resolvedBusAddr returns the effective Redis address for the bus.
// For embedded mode it derives the address from the local cluster masters;
// for all other modes it returns cfg.Bus.Addr directly.
func resolvedBusAddr(cfg *config.Config) string {
	if cfg.Bus.Mode != config.ModeBusEmbedded {
		return cfg.Bus.Addr
	}
	switch cfg.Cluster.Mode {
	case config.ModeCluster:
		return cfg.Cluster.Masters[cfg.Bus.MasterIndex]
	case config.ModeSentinel:
		// For sentinel, return the first sentinel addr as a hint (actual addr is dynamic)
		if len(cfg.Cluster.SentinelAddrs) > 0 {
			return cfg.Cluster.SentinelAddrs[0] + " (sentinel)"
		}
	case config.ModeStandalone:
		return cfg.Cluster.Addr
	}
	return "(embedded)"
}
