# RedisBridge

Production-grade Redis Active-Active replication agent. Provides multi-master replication with Last-Write-Wins (LWW) conflict resolution using Hybrid Logical Clocks (HLC). Supports **standalone**, **Redis Sentinel**, **Redis Cluster**, and **embedded** (no dedicated bus) deployment modes.

---

## Architecture

```
┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
│      Cluster A      │   │      Cluster B      │   │      Cluster C      │
│  standalone / HA    │   │  standalone / HA    │   │  standalone / HA    │
└────────┬────────────┘   └────────┬────────────┘   └────────┬────────────┘
         │  keyspace events         │  keyspace events         │  keyspace events
         ▼                          ▼                          ▼
┌────────────────┐        ┌────────────────┐        ┌────────────────┐
│   Agent A      │        │   Agent B      │        │   Agent C      │
│  Producer      │        │  Producer      │        │  Producer      │
│  Consumer      │        │  Consumer      │        │  Consumer      │
└───────┬────────┘        └───────┬────────┘        └───────┬────────┘
        │  XADD                   │  XADD                   │  XADD
        ▼                         ▼                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Redis Streams (Replication Bus)                   │
│   repl:stream:cluster-a   repl:stream:cluster-b   repl:stream:cluster-c │
│                                                                      │
│   Each agent has its OWN consumer group per peer stream:             │
│     repl-consumers-cluster-b reads from repl:stream:cluster-a       │
│     repl-consumers-cluster-c reads from repl:stream:cluster-a       │
└──────────────────────────────────────────────────────────────────────┘
        │  XREADGROUP             │  XREADGROUP             │  XREADGROUP
        ▼                         ▼                         ▼
   Agent A applies           Agent B applies           Agent C applies
   peer deltas via LWW       peer deltas via LWW       peer deltas via LWW
```

**One agent per cluster.** Each agent runs two goroutine groups:
- **Producer** — subscribes to `__keyevent@*__:*`, captures writes, publishes deltas to the bus
- **Consumer** — reads deltas from each peer's stream, resolves conflicts with LWW, applies locally

---

## Features

- **Multi-master active-active replication** across N Redis clusters
- **LWW conflict resolution** with Hybrid Logical Clocks (monotonic, handles clock skew)
- **All Redis data types**: String, Hash, List, Set, Sorted Set, Delete
- **TTL preservation** with transit-time compensation (expired-in-transit skipped)
- **Two-layer dedup**: 200ms shadow key + in-memory LRU SeqID cache (no replication loops)
- **Dead Letter Queue** for unprocessable messages
- **Prometheus metrics** with per-peer labels
- **HTTP management API** (health, lag, pause/resume per peer, config dump)
- **Zero-downtime restartable** (XREADGROUP consumer groups resume from last ACK)
- **Three connection modes**: standalone, sentinel, cluster
- **Configurable concurrency**: worker pool for apply, backpressure via max-in-flight

---

## Deployment Modes

### Cluster topology

| Mode | Use Case | Config Example |
|---|---|---|
| `standalone` | Single Redis instance per site (dev, low-scale) | `config/standalone/agent-a.yaml` |
| `sentinel` | Redis Sentinel HA — 1 master + N replicas + sentinels | `config/sentinel/agent-a.yaml` |
| `cluster` | Redis Cluster (sharded, multi-master per site) | `config/cluster/agent-a.yaml` |

### Bus mode

| Bus mode | Use Case | How |
|---|---|---|
| `standalone` | Dedicated Redis bus instance (default) | `bus.addr: redis-bus:6379` |
| `sentinel` | HA bus via Redis Sentinel | `bus.sentinel_master` + `bus.sentinel_addrs` |
| `embedded` | **No separate bus container** — each site's own Redis master doubles as its stream host | `bus.master_index: 0` + `bus.peer_bus_addrs` |

**Embedded bus mode** eliminates the dedicated bus container. Each site publishes its replication stream to its own Redis (`master_index` selects which master). Consumers pull directly from each peer's Redis using `peer_bus_addrs`. This removes a potential single point of failure and reduces operational overhead.

---

## Getting Started with Existing Redis Clusters

This section covers how to add RedisBridge replication to Redis instances that are **already running in production** — no cluster tear-down, no downtime.

### Step 1 — Enable keyspace notifications on every Redis instance

RedisBridge's producer relies on Redis keyspace notifications to detect writes. This must be enabled on **every master** at every site.

**Check current setting:**
```bash
redis-cli -h <host> -p <port> CONFIG GET notify-keyspace-events
# If it returns an empty string, notifications are OFF
```

**Enable at runtime (takes effect immediately, no restart):**
```bash
redis-cli -h <host> -p <port> CONFIG SET notify-keyspace-events KEA
```

| Flag | Meaning |
|---|---|
| `K` | Keyspace events (key-name in channel) |
| `E` | Keyevent events (command in channel) |
| `A` | All commands (`g$lzxed` — alias for all generic + string + list + set + sorted set + stream + hash + expired + del) |

> **`KEA` is the minimum required.** If your Redis already has a custom value (e.g. `Kx`), **append** the missing flags rather than overriding:
> ```bash
> # Example: existing value is "Kx" → add the remaining flags
> redis-cli CONFIG SET notify-keyspace-events KExA
> ```

**Make it permanent** (so it survives Redis restart) by adding to `redis.conf`:
```
notify-keyspace-events KEA
```

---

### Step 2 — Decide on bus mode

| Scenario | Recommended bus mode | Extra Redis needed? |
|---|---|---|
| You have 1 Redis per site and want simplest setup | **embedded** | ❌ None — stream lives on your existing Redis |
| You have 3+ masters per site (sharded) | **embedded-cluster** | ❌ None — stream lives on master1 |
| You need the bus isolated from data traffic | **standalone** | ✅ 1 extra Redis per deployment |
| You need HA bus with automatic failover | **sentinel** | ✅ 1 master + N replicas + 3 sentinels |

---

### Step 3 — Write the agent config

Choose the template that matches your topology.

#### Option A — Embedded bus (1 Redis per site, recommended)

No extra Redis needed. Stream is hosted on the same Redis as your data.

```yaml
# /etc/redibridge/agent-site-a.yaml
site_id: "site-a"                # unique identifier for this site

cluster:
  mode: "standalone"
  addr: "<your-redis-host>:6379"
  password: "<redis-auth-password-or-blank>"
  tls: false                     # set true if Redis requires TLS

bus:
  mode: "embedded"
  master_index: 0                # only valid value for standalone mode
  peer_bus_addrs:                # one entry per peer site
    site-b: "<site-b-redis-host>:6379"
    site-c: "<site-c-redis-host>:6379"
  stream_prefix: "repl:stream:"  # stream key prefix on Redis
  consumer_group: "repl-consumers"
  password: "<redis-auth-password-or-blank>"

peers:
  - "site-b"
  - "site-c"

replication:
  batch_size: 100
  apply_concurrency: 8
  dedup_ttl_seconds: 5
  max_in_flight: 1000

coordinator:
  port: 8080      # management API (health, pause/resume)

metrics:
  port: 9090      # Prometheus /metrics endpoint
```

> **Network requirement**: the agent at site-A must be able to TCP-connect to `<site-b-redis-host>:6379` and `<site-c-redis-host>:6379` to pull their streams. Open firewall rules accordingly.

#### Option B — Embedded bus (3-master Redis Cluster per site)

```yaml
site_id: "site-a"

cluster:
  mode: "cluster"
  masters:
    - "<master1-host>:6379"
    - "<master2-host>:6379"
    - "<master3-host>:6379"
  password: "<password-or-blank>"

bus:
  mode: "embedded"
  master_index: 0              # master1 hosts the replication stream
  peer_bus_addrs:
    site-b: "<site-b-master1-host>:6379"
    site-c: "<site-c-master1-host>:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"
  password: "<password-or-blank>"

peers:
  - "site-b"
  - "site-c"

replication:
  batch_size: 100
  apply_concurrency: 8
  dedup_ttl_seconds: 5
  max_in_flight: 1000

coordinator:
  port: 8080
metrics:
  port: 9090
```

#### Option C — Standalone bus (dedicated bus Redis)

Use this when you want to isolate replication traffic from data traffic.

```yaml
site_id: "site-a"

cluster:
  mode: "standalone"
  addr: "<your-redis-host>:6379"
  password: "<password-or-blank>"

bus:
  mode: "standalone"
  addr: "<dedicated-bus-redis-host>:6379"   # shared bus reachable by all agents
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"
  password: "<bus-password-or-blank>"

peers:
  - "site-b"
  - "site-c"

replication:
  batch_size: 100
  apply_concurrency: 8
  dedup_ttl_seconds: 5
  max_in_flight: 1000

coordinator:
  port: 8080
metrics:
  port: 9090
```

> **One shared bus Redis** is required and must be reachable by **all** agents across all sites.

---

### Step 4 — Deploy the agent

#### Option A — Docker (recommended)

```bash
docker run -d \
  --name redibridge-site-a \
  --restart unless-stopped \
  -v /etc/redibridge/agent-site-a.yaml:/etc/redibridge/agent.yaml:ro \
  -p 8080:8080 \
  -p 9090:9090 \
  ghcr.io/nagaraju/redibridge:latest \
  --config /etc/redibridge/agent.yaml
```

#### Option B — Binary

```bash
# Build
git clone https://github.com/nagaraju/redibridge
cd redibridge
make build
# binary is at bin/redibridge

# Run
./bin/redibridge --config /etc/redibridge/agent-site-a.yaml
```

#### Option C — systemd service

```ini
# /etc/systemd/system/redibridge.service
[Unit]
Description=RedisBridge replication agent
After=network.target

[Service]
ExecStart=/usr/local/bin/redibridge --config /etc/redibridge/agent.yaml
Restart=always
RestartSec=5
User=redibridge

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now redibridge
journalctl -u redibridge -f
```

---

### Step 5 — Verify the agent is running

```bash
# Health check (replace 8080 with your coordinator port)
curl http://localhost:8080/health
# → {"site_id":"site-a","status":"ok","uptime_s":12}

# Check peer lag
curl http://localhost:8080/lag
# → {"peers":[{"site":"site-b","paused":false},{"site":"site-c","paused":false}]}

# Prometheus metrics
curl http://localhost:9090/metrics | grep repl_
```

---

### Step 6 — Smoke-test replication

```bash
# Write a key on site-A's Redis
redis-cli -h <site-a-redis> SET repl:test:1 "hello-from-A"

# Wait ~1 second, then verify on B and C
redis-cli -h <site-b-redis> GET repl:test:1   # → "hello-from-A"
redis-cli -h <site-c-redis> GET repl:test:1   # → "hello-from-A"

# Check captured/published metrics on agent-a
curl http://localhost:9090/metrics | grep repl_events_published_total
# Should be > 0
```

---

### Important: Keys to exclude from replication

RedisBridge automatically skips the following internal keys — do **not** set keyspace notification filters that exclude them:

| Key pattern | Managed by | Purpose |
|---|---|---|
| `__meta:{key}` | producer + applier | LWW timestamp per key |
| `__repl:applying:{key}` | dedup | Loop-prevention shadow key (200ms) |
| `repl:stream:{site_id}` | bus | Replication stream (NOT replicated to peers) |
| `repl:dlq:{site_id}` | bus | Dead Letter Queue for bad messages |

---

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| No keys replicated, `repl_events_captured_total` = 0 | Keyspace notifications not enabled | `CONFIG SET notify-keyspace-events KEA` on every master |
| Agent starts but peers timeout | Firewall blocks peer Redis ports | Open TCP access from each agent to each peer's Redis (bus port) |
| Keys replicate in one direction only | Only one agent is running | Deploy an agent at **every** site |
| LWW not converging | NTP clock drift > 1s between sites | Sync clocks (`chronyc`, `ntpd`); HLC tolerates minor drift but extreme skew causes ordering issues |
| `bus.peer_bus_addrs missing entry for peer` on startup | Config missing a peer's bus address | Add all peer site IDs to `bus.peer_bus_addrs` in the config |
| Existing keys not replicated after agent starts | Producer only captures **new** writes after startup | Bootstrap by re-writing existing keys, or use `redis-cli --scan \| xargs redis-cli DUMP/RESTORE` to seed peers |
| High replication lag | Batch size too small or `apply_concurrency` too low | Increase `replication.batch_size` and `replication.apply_concurrency` |

---

## Quick Start

### Prerequisites
- Docker ≥ 24, Docker Compose ≥ 2
- Go 1.23+ (for local development)

### Standalone (simplest — dev/test)

```bash
make up-standalone
# or
docker compose -f docker/standalone/docker-compose.yml up --build -d
```

Ports:
- Redis A → `localhost:6381`, B → `6382`, C → `6383`
- Bus → `localhost:6390`
- Agents coordinator → `8081 / 8082 / 8083`
- Agents metrics → `9091 / 9092 / 9093`

```bash
# Write on A, verify on B and C
redis-cli -p 6381 SET user:100 "Alice"
sleep 1
redis-cli -p 6382 GET user:100   # → "Alice"
redis-cli -p 6383 GET user:100   # → "Alice"
```

### Redis Sentinel HA

```bash
make up-sentinel
# or
docker compose -f docker/sentinel/docker-compose.yml up --build -d
```

### Redis Cluster (sharded)

```bash
make up-cluster
# or
docker compose -f docker/cluster/docker-compose.yml up --build -d
```

### Embedded bus — no dedicated bus container

```bash
# Embedded standalone: 3 single-master sites, stream on each site's Redis
make up-embedded
# or
docker compose -f docker/embedded/docker-compose.yml up --build -d
```

Ports:
- Redis A/B/C → `localhost:6401 / 6402 / 6403`
- Agent coordinators → `8191 / 8192 / 8193`
- Agent metrics → `9191 / 9192 / 9193`

```bash
# Embedded cluster: 3 sites × 3 masters each (9 Redis total), stream on master1 of each site
make up-embedded-cluster
# or
docker compose -f docker/embedded-cluster/docker-compose.yml up --build -d
```

Ports:
- Site A masters → `6411 / 6412 / 6413` | Site B → `6414–6416` | Site C → `6417–6419`
- Agent coordinators → `8291 / 8292 / 8293`
- Agent metrics → `9291 / 9292 / 9293`

```bash
# Quick smoke-test after bring-up
redis-cli -p 6401 SET user:100 "Alice"
sleep 1
redis-cli -p 6402 GET user:100   # → "Alice"
redis-cli -p 6403 GET user:100   # → "Alice"
```

### All Make targets

```bash
make up-standalone          / down-standalone          / logs-standalone
make up-sentinel            / down-sentinel            / logs-sentinel
make up-cluster             / down-cluster             / logs-cluster
make up-embedded            / down-embedded            / logs-embedded
make up-embedded-cluster    / down-embedded-cluster    / logs-embedded-cluster

make build            # build binary to bin/redibridge
make test-unit        # run all unit tests
make test             # run all tests (including integration)
make run              # run agent locally with standalone/agent-a.yaml
```

---

## Configuration

### Standalone mode (with dedicated bus)

```yaml
site_id: "cluster-a"

cluster:
  mode: "standalone"
  addr: "redis-a:6379"
  password: ""
  tls: false

bus:
  mode: "standalone"
  addr: "redis-bus:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"

peers:
  - "cluster-b"
  - "cluster-c"

replication:
  batch_size: 100
  apply_concurrency: 8
  dedup_ttl_seconds: 5
  max_in_flight: 1000

coordinator:
  port: 8080

metrics:
  port: 9090
```

### Sentinel mode

```yaml
site_id: "cluster-a"

cluster:
  mode: "sentinel"
  sentinel_master: "mymaster-a"
  sentinel_addrs:
    - "sentinel-a-1:26379"
    - "sentinel-a-2:26379"
    - "sentinel-a-3:26379"

bus:
  mode: "sentinel"
  sentinel_master: "bus-master"
  sentinel_addrs:
    - "bus-sentinel-1:26379"
    - "bus-sentinel-2:26379"
    - "bus-sentinel-3:26379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"

peers:
  - "cluster-b"
  - "cluster-c"
```

### Cluster mode (with dedicated bus)

```yaml
cluster:
  mode: "cluster"
  masters:
    - "redis-a-master1:6379"
    - "redis-a-master2:6379"
    - "redis-a-master3:6379"

bus:
  mode: "standalone"
  addr: "redis-bus:6379"
```

### Embedded bus — standalone cluster

Each site is a single Redis instance that also hosts its own replication stream. No separate bus container.

```yaml
site_id: "site-a"

cluster:
  mode: "standalone"
  addr: "redis-a:6379"

bus:
  mode: "embedded"          # redis-a:6379 is both data store and stream host
  master_index: 0           # always 0 for standalone cluster mode
  peer_bus_addrs:           # addresses to pull peer streams from
    site-b: "redis-b:6379"
    site-c: "redis-c:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"

peers:
  - "site-b"
  - "site-c"
```

### Embedded bus — 3-master cluster

Each site has 3 Redis masters. `master_index: 0` designates master1 as the stream host. Consumers pull directly from each peer's master1.

```yaml
site_id: "ec-site-a"

cluster:
  mode: "cluster"
  masters:
    - "redis-a-master1:6379"
    - "redis-a-master2:6379"
    - "redis-a-master3:6379"

bus:
  mode: "embedded"
  master_index: 0           # stream lives on redis-a-master1
  peer_bus_addrs:
    ec-site-b: "redis-b-master1:6379"
    ec-site-c: "redis-c-master1:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"

peers:
  - "ec-site-b"
  - "ec-site-c"
```

---

## How It Works — Step by Step

**1. App writes a key on Cluster A:**
```
redis-cli -p 6381 SET order:99 "shipped"
```

**2. Redis fires a keyspace notification:**
```
__keyevent@0__:set  →  payload: "order:99"
```

**3. Agent A Producer captures it:**
- Checks `__repl:applying:order:99` → nil (not a replication write) → proceed
- Reads value `"shipped"`, TTL `-1`
- Stamps with HLC: `hlc = wallClock<<20 | logicalCounter`
- Builds `Delta{site_id:"cluster-a", key:"order:99", value:"shipped", hlc:X, seq_id:"cluster-a-42"}`
- Publishes to `repl:stream:cluster-a` on the bus
- Writes `__meta:order:99 {hlc:X, site:cluster-a}` on Redis A (for LWW correctness)

**4. Bus stream receives the delta** (`repl:stream:cluster-a` has 1 new entry)

**5. Agent B and Agent C each have their own consumer group:**
- `repl-consumers-cluster-b` reads from `repl:stream:cluster-a` → B gets a copy
- `repl-consumers-cluster-c` reads from `repl:stream:cluster-a` → C gets a copy

**6. Agent B Consumer processes the delta:**
- Loopback guard: `delta.site_id = "cluster-a"` ≠ `"cluster-b"` → proceed
- SeqID dedup: `"cluster-a-42"` not in LRU → proceed
- LWW check: `ReadMeta("order:99")` → no meta on B → first write → **ACCEPT**
- Applier:
  1. `SET __repl:applying:order:99 1 PX 200` (shadow key, 200ms TTL)
  2. Pipeline: `SET order:99 "shipped"` + `HSET __meta:order:99 hlc=X site=cluster-a`
- ACKs message on bus stream
- Prometheus: `repl_writes_applied_total++`, `repl_lag_ms` updated

**7. Conflict scenario (same key written on A and B simultaneously):**
- A writes `"v1"` at HLC=1000, B writes `"v2"` at HLC=1001
- A receives B's delta: local HLC=1000, delta HLC=1001 → 1001 > 1000 → **B wins**
- B receives A's delta: local HLC=1001, delta HLC=1000 → 1000 < 1001 → **reject**
- C receives both → picks the one with higher HLC (1001 = B's)
- All 3 converge to `"v2"` ✅

**Tie-break:** equal HLC → lexicographically higher `site_id` wins (deterministic).

---

## Management API

All endpoints on coordinator port (default `:8080`):

```bash
# Health
curl http://localhost:8081/health
# → {"site_id":"cluster-a","status":"ok","uptime_s":171}

# Lag / pause status per peer
curl http://localhost:8081/lag
# → {"peers":[{"site":"cluster-b","paused":false},{"site":"cluster-c","paused":false}]}

# Stats summary
curl http://localhost:8081/stats

# Redacted config dump
curl http://localhost:8081/config

# Pause consuming from cluster-b (maintenance window)
curl -X POST http://localhost:8081/pause \
  -H 'Content-Type: application/json' \
  -d '{"site":"cluster-b"}'

# Resume
curl -X POST http://localhost:8081/resume \
  -H 'Content-Type: application/json' \
  -d '{"site":"cluster-b"}'
```

---

## Prometheus Metrics

Exposed on metrics port (default `:9090/metrics`):

| Metric | Type | Labels | Description |
|---|---|---|---|
| `repl_events_captured_total` | Counter | `site_id`, `key_type` | Keyspace events captured by producer |
| `repl_events_published_total` | Counter | `site_id` | Deltas successfully published to bus |
| `repl_events_consumed_total` | Counter | `site_id`, `peer_site` | Messages read from peer streams |
| `repl_writes_applied_total` | Counter | `site_id`, `peer_site` | Deltas accepted and applied (LWW won) |
| `repl_writes_discarded_total` | Counter | `site_id`, `peer_site`, `reason` | Rejected: `loopback`, `dedup_hit`, `lww_lost` |
| `repl_lag_ms` | Gauge | `site_id`, `peer_site` | Replication lag in milliseconds |
| `repl_apply_duration_ms` | Histogram | `site_id`, `peer_site` | Time to apply a single delta |
| `repl_bus_stream_length` | Gauge | `stream` | Current stream length on bus |

---

## Project Structure

```
RedisReplicator/
├── cmd/agent/main.go                  # Entry point, wires all components
├── internal/
│   ├── hlc/          hlc.go           # Hybrid Logical Clock
│   ├── config/       config.go        # Viper config loader + validation
│   ├── bus/          bus.go           # Redis Streams Bus interface + RedisStreamsBus
│   │                 peer_bus.go      # PerPeerBus — embedded mode (per-peer Redis clients)
│   ├── producer/     producer.go      # Keyspace event listener + delta publisher
│   ├── consumer/     consumer.go      # Stream consumer + LWW + worker pool + per-key mutex
│   ├── applier/      applier.go       # Apply delta to local Redis + meta update
│   ├── dedup/        dedup.go         # Shadow keys (200ms) + LRU SeqID cache
│   ├── reader/       reader.go        # Read key value + TTL by type
│   ├── metrics/      metrics.go       # Prometheus metric definitions
│   └── coordinator/  coordinator.go   # HTTP management API
├── config/
│   ├── cluster/           agent-a/b/c.yaml  # Cluster mode + dedicated bus
│   ├── sentinel/          agent-a/b/c.yaml  # Sentinel mode + HA bus
│   ├── standalone/        agent-a/b/c.yaml  # Standalone mode + dedicated bus
│   ├── embedded/          agent-a/b/c.yaml  # Standalone cluster + embedded bus
│   └── embedded-cluster/  agent-a/b/c.yaml  # 3-master cluster + embedded bus
├── docker/
│   ├── cluster/           docker-compose.yml        # 9 masters + bus + 3 agents
│   ├── sentinel/          docker-compose.yml         # HA: masters + replicas + sentinels
│   ├── standalone/        docker-compose.yml         # 3 Redis nodes + bus + 3 agents
│   ├── embedded/          docker-compose.yml         # 3 Redis nodes, NO bus, 3 agents
│   └── embedded-cluster/  docker-compose.yml         # 9 masters, NO bus, 3 agents
├── tests/
│   ├── integration_test.go
│   ├── e2e_test.go                    # E2E — cluster mode
│   ├── e2e_embedded_test.go           # E2E — embedded standalone
│   └── e2e_embedded_cluster_test.go   # E2E — embedded 3-master cluster
├── Dockerfile
└── Makefile
```

---

## Testing

```bash
# Unit tests (no Redis required)
make test-unit

# All tests
make test

# Specific package
go test -v ./internal/hlc/...
go test -v ./internal/consumer/...

# E2E — dedicated bus (cluster mode)
make test-e2e                    # bring up + run + keep alive
make test-e2e-ci                 # bring up + run + tear down

# E2E — embedded bus (standalone)
make test-e2e-embedded           # bring up + run + keep alive
make test-e2e-embedded-ci        # bring up + run + tear down

# E2E — embedded bus (3-master cluster)
make test-e2e-embedded-cluster   # bring up + run + keep alive
make test-e2e-embedded-cluster-ci  # bring up + run + tear down
```

Unit test coverage:
- `internal/hlc` — monotonicity, concurrent safety, tie-break determinism
- `internal/consumer` — LWW: higher HLC wins, lower loses, tie-break by site, first write always accepted
- `internal/applier` — Delta encoding/decoding for all types, TTL expired-in-transit
- `internal/dedup` — SeqID LRU cache, eviction behaviour

E2E test suites (12 tests each):
- `tests/e2e_test.go` — cluster mode with dedicated bus
- `tests/e2e_embedded_test.go` — embedded bus, standalone cluster
- `tests/e2e_embedded_cluster_test.go` — embedded bus, 3-master cluster; also validates writes on master2/master3 replicate correctly

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Redis Streams as bus | Durable, consumer-group semantics, ACK-based, supports DLQ via re-delivery |
| HLC over wall clock | Monotonic even with NTP drift; encodes causality across sites |
| Per-agent consumer groups | Each consuming agent needs its own group so every agent gets every message independently |
| 200ms shadow key TTL | Short enough not to block user re-writes; long enough to suppress the replication write's own keyspace event |
| Producer writes `__meta` before publish | Without this, locally-originated keys have no timestamp → any incoming delta triggers "first write" policy and overwrites them |
| Embedded bus mode | Eliminates the dedicated bus container — each site hosts its own stream on an existing Redis master, removing a separate SPOF |
| `PerPeerBus` for embedded mode | Each site's stream lives on a different Redis; `PerPeerBus` routes `Publish` to the local Redis and `Consume`/`Ack` to each peer's Redis |
| Per-key mutex in consumer (`keyMu`) | Prevents LWW TOCTOU race: two peer goroutines reading `meta=0` simultaneously would both accept; the mutex serializes `ReadMeta + Apply` per key |

---

## License

MIT License
