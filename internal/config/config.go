package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

// Config is the top-level configuration for the replication agent.
type Config struct {
	SiteID      string            `mapstructure:"site_id"`
	Role        string            `mapstructure:"role"`        // "full" (default) | "consumer" | "producer"
	ConsumerID  string            `mapstructure:"consumer_id"` // unique name for XREADGROUP; defaults to hostname
	Cluster     ClusterConfig     `mapstructure:"cluster"`
	Bus         BusConfig         `mapstructure:"bus"`
	Peers       []string          `mapstructure:"peers"`
	Replication ReplicationConfig `mapstructure:"replication"`
	Coordinator CoordinatorConfig `mapstructure:"coordinator"`
	Metrics     MetricsConfig     `mapstructure:"metrics"`
}

// ClusterMode defines how the agent connects to its local Redis cluster.
// Valid values: "standalone" | "sentinel" | "cluster"
type ClusterMode string

const (
	ModeStandalone ClusterMode = "standalone"
	ModeSentinel   ClusterMode = "sentinel"
	ModeCluster    ClusterMode = "cluster"

	// ModeBusEmbedded tells the agent to use one of its own local Redis masters
	// as the replication bus — no separate bus instance needed.
	// The master is selected by BusConfig.MasterIndex (default 0 = first master).
	ModeBusEmbedded ClusterMode = "embedded"
)

type ClusterConfig struct {
	// Mode selects the connection strategy.
	// "standalone" — single Redis instance (Addr)
	// "sentinel"   — Redis Sentinel HA (SentinelMaster + SentinelAddrs)
	// "cluster"    — Redis Cluster, one client per shard master (Masters)
	Mode           ClusterMode `mapstructure:"mode"`
	Addr           string      `mapstructure:"addr"`            // standalone only
	Masters        []string    `mapstructure:"masters"`         // cluster only
	SentinelMaster string      `mapstructure:"sentinel_master"` // sentinel only
	SentinelAddrs  []string    `mapstructure:"sentinel_addrs"`  // sentinel only
	Password       string      `mapstructure:"password"`
	TLS            bool        `mapstructure:"tls"`
}

type BusConfig struct {
	// Mode selects the bus connection strategy.
	// "standalone" — dedicated Redis instance (Addr required)
	// "sentinel"   — Redis Sentinel HA (SentinelMaster + SentinelAddrs required)
	// "embedded"   — reuse one of this site's own masters (no Addr needed;
	//                use MasterIndex to pick which master, default 0)
	Mode           ClusterMode        `mapstructure:"mode"`
	Addr           string             `mapstructure:"addr"`
	MasterIndex    int                `mapstructure:"master_index"`
	PeerBusAddrs   map[string]string  `mapstructure:"peer_bus_addrs"` // embedded mode: siteID → redisAddr
	SentinelMaster string             `mapstructure:"sentinel_master"`
	SentinelAddrs  []string           `mapstructure:"sentinel_addrs"`
	StreamPrefix   string             `mapstructure:"stream_prefix"`
	ConsumerGroup  string             `mapstructure:"consumer_group"`
	StreamMaxLen   int64              `mapstructure:"stream_max_len"` // max entries retained per stream (0 = unlimited)
	StreamTTLHours int                `mapstructure:"stream_ttl_hours"` // time-based retention: trim entries older than N hours (0 = disabled; takes priority over stream_max_len when set)
	Password       string             `mapstructure:"password"`
}

type ReplicationConfig struct {
	BatchSize                int `mapstructure:"batch_size"`
	ApplyConcurrency         int `mapstructure:"apply_concurrency"`
	DedupTTLSeconds          int `mapstructure:"dedup_ttl_seconds"`
	MaxInFlight              int `mapstructure:"max_in_flight"`
	ReconcileIntervalSeconds int `mapstructure:"reconcile_interval_seconds"` // 0 = disabled
}

type CoordinatorConfig struct {
	Port int `mapstructure:"port"`
}

type MetricsConfig struct {
	Port int `mapstructure:"port"`
}

// Load reads configuration from the given YAML file path.
func Load(path string) (*Config, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", path)
	}

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	// Defaults
	v.SetDefault("role", "full")
	v.SetDefault("consumer_id", "")
	v.SetDefault("bus.stream_prefix", "repl:stream:")
	v.SetDefault("bus.consumer_group", "repl-consumers")
	v.SetDefault("bus.stream_max_len", int64(1_000_000))
	v.SetDefault("bus.stream_ttl_hours", 0)
	v.SetDefault("replication.batch_size", 100)
	v.SetDefault("replication.apply_concurrency", 8)
	v.SetDefault("replication.dedup_ttl_seconds", 5)
	v.SetDefault("replication.max_in_flight", 1000)
	v.SetDefault("replication.reconcile_interval_seconds", 30)
	v.SetDefault("coordinator.port", 8080)
	v.SetDefault("metrics.port", 9090)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate checks that required fields are set and mode-specific fields are present.
func (c *Config) Validate() error {
	if c.SiteID == "" {
		return fmt.Errorf("site_id is required")
	}
	if len(c.Peers) == 0 {
		return fmt.Errorf("at least one peer is required")
	}

	// Default cluster mode to "cluster" for backward compatibility
	if c.Cluster.Mode == "" {
		c.Cluster.Mode = ModeCluster
	}

	switch c.Cluster.Mode {
	case ModeStandalone:
		if c.Cluster.Addr == "" {
			return fmt.Errorf("cluster.addr is required for standalone mode")
		}
	case ModeSentinel:
		if c.Cluster.SentinelMaster == "" {
			return fmt.Errorf("cluster.sentinel_master is required for sentinel mode")
		}
		if len(c.Cluster.SentinelAddrs) == 0 {
			return fmt.Errorf("cluster.sentinel_addrs is required for sentinel mode")
		}
	case ModeCluster:
		if len(c.Cluster.Masters) == 0 {
			return fmt.Errorf("cluster.masters is required for cluster mode")
		}
	default:
		return fmt.Errorf("cluster.mode must be one of: standalone, sentinel, cluster")
	}

	// Default bus mode to "standalone"
	if c.Bus.Mode == "" {
		c.Bus.Mode = ModeStandalone
	}

	switch c.Bus.Mode {
	case ModeStandalone:
		if c.Bus.Addr == "" {
			return fmt.Errorf("bus.addr is required for standalone bus mode")
		}
	case ModeSentinel:
		if c.Bus.SentinelMaster == "" {
			return fmt.Errorf("bus.sentinel_master is required for sentinel bus mode")
		}
		if len(c.Bus.SentinelAddrs) == 0 {
			return fmt.Errorf("bus.sentinel_addrs is required for sentinel bus mode")
		}
	case ModeBusEmbedded:
		// peer_bus_addrs must cover all declared peers
		if len(c.Bus.PeerBusAddrs) == 0 {
			return fmt.Errorf("bus.peer_bus_addrs is required for embedded bus mode")
		}
		for _, peer := range c.Peers {
			if _, ok := c.Bus.PeerBusAddrs[peer]; !ok {
				return fmt.Errorf("bus.peer_bus_addrs missing entry for peer %q", peer)
			}
		}
		// Validate that the chosen master index exists.
		switch c.Cluster.Mode {
		case ModeCluster:
			if c.Bus.MasterIndex < 0 || c.Bus.MasterIndex >= len(c.Cluster.Masters) {
				return fmt.Errorf("bus.master_index %d out of range (cluster has %d masters)",
					c.Bus.MasterIndex, len(c.Cluster.Masters))
			}
		default:
			if c.Bus.MasterIndex != 0 {
				return fmt.Errorf("bus.master_index must be 0 for standalone/sentinel cluster mode")
			}
		}
	default:
		return fmt.Errorf("bus.mode must be one of: standalone, sentinel, embedded")
	}

	return nil
}

// Redacted returns a copy of the config with passwords masked.
func (c *Config) Redacted() Config {
	r := *c
	if r.Cluster.Password != "" {
		r.Cluster.Password = "***"
	}
	if r.Bus.Password != "" {
		r.Bus.Password = "***"
	}
	return r
}
