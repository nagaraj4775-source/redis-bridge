# CLAUDE.md — RedisBridge

This file provides context for AI assistants (Claude, Cascade, Copilot, etc.) working in this repository.

---

## What This Project Is

A production-grade **Redis Active-Active replication agent** written in Go. It synchronises data across multiple Redis deployments in different sites using:
- **Redis Keyspace Notifications** to detect writes
- **Redis Streams** as a durable replication bus
- **Hybrid Logical Clocks (HLC)** for causally-consistent timestamps
- **Last-Write-Wins (LWW)** conflict resolution

---

## Repository Layout

```
cmd/agent/main.go          Entry point — wires all internal packages
internal/
  hlc/        hlc.go       Hybrid Logical Clock (wall+logical, packed uint64)
  config/     config.go    Viper-based loader; ClusterMode/BusConfig incl. embedded; Validate()
  bus/        bus.go       Bus interface + RedisStreamsBus (Publish/Consume/EnsureGroup/Ack/DLQ)
              peer_bus.go  PerPeerBus — embedded mode: local publish, per-peer consume clients
  producer/   producer.go  PSubscribe keyspace events → publish Delta to bus (filters repl:stream:)
  consumer/   consumer.go  XREADGROUP per peer → LWW check → apply; per-key mutex (keyMu)
  applier/    applier.go   Write delta to Redis + shadow key + __meta:{key}
  dedup/      dedup.go     Shadow key (200ms) + LRU SeqID cache
  reader/     reader.go    ReadValue (type-aware) + ReadTTL
  metrics/    metrics.go   Prometheus counter/gauge/histogram definitions
  coordinator/ coordinator.go  HTTP API: /health /lag /stats /pause /resume /config
config/
  cluster/           agent-a/b/c.yaml   Cluster mode + dedicated bus
  sentinel/          agent-a/b/c.yaml   Sentinel mode + HA bus
  standalone/        agent-a/b/c.yaml   Standalone mode + dedicated bus
  embedded/          agent-a/b/c.yaml   Standalone cluster + embedded bus (bus.mode: embedded)
  embedded-cluster/  agent-a/b/c.yaml   3-master cluster + embedded bus
docker/
  cluster/           docker-compose.yml  9 masters + bus + 3 agents
  sentinel/          docker-compose.yml  HA: masters + replicas + sentinels
                     sentinel-{a,b,c,bus}.conf
  standalone/        docker-compose.yml  3 Redis nodes + bus + 3 agents
  embedded/          docker-compose.yml  3 Redis nodes, NO bus container, 3 agents
  embedded-cluster/  docker-compose.yml  9 masters (3/site), NO bus, 3 agents
tests/
  integration_test.go
  e2e_test.go                    E2E — cluster mode
  e2e_embedded_test.go           E2E — embedded bus, standalone cluster (12 tests)
  e2e_embedded_cluster_test.go   E2E — embedded bus, 3-master cluster (12 tests)
Dockerfile
Makefile
```

---

## Core Data Flow

```
App write on Redis A
  → keyspace event: __keyevent@0__:set  key=order:99
  → Producer.handleEvent()
      1. IsApplying(key)?  →  no  →  proceed
      2. ReadValue + ReadTTL
      3. clock.Now()  →  HLC timestamp
      4. bus.Publish(siteID, Delta{...})  →  repl:stream:cluster-a
      5. HSET __meta:order:99  hlc=X  site=cluster-a   ← CRITICAL: sets local LWW anchor
  → Bus stream has entry

Consumer on Agent B (peer of A):
  XREADGROUP repl:stream:cluster-a  group=repl-consumers-cluster-b  ← site-scoped group
  → processOne()
      1. loopback guard: delta.SiteID != mySiteID
      2. SeqID LRU dedup
      3. ReadMeta(key) → localHLC, localSite
      4. LWW:
           localHLC == 0  →  first write, always accept
           delta.HLC > localHLC  →  accept
           delta.HLC == localHLC && delta.SiteID > localSite  →  accept (tie-break)
           else  →  discard
      5. applier.Apply(delta):
           SET __repl:applying:{key} 1 PX 200   ← shadow key (200ms)
           pipeline: SET key value  +  HSET __meta:{key} hlc site
      6. bus.Ack(msgID)
```

---

## Configuration Modes

### Cluster mode (`cluster.mode`)

`ClusterMode` type (`internal/config/config.go`):

| Value | Client created | Field used |
|---|---|---|
| `"standalone"` | `redis.NewClient(Addr)` | `cluster.addr` |
| `"sentinel"` | `redis.NewFailoverClient(SentinelAddrs, MasterName)` | `cluster.sentinel_*` |
| `"cluster"` | one `redis.NewClient` per master | `cluster.masters[]` |

### Bus mode (`bus.mode`)

Independent of cluster mode:

| Value | Bus client created | Required fields |
|---|---|---|
| `"standalone"` | `RedisStreamsBus` (single client) | `bus.addr` |
| `"sentinel"` | `RedisStreamsBus` (FailoverClient) | `bus.sentinel_master`, `bus.sentinel_addrs` |
| `"embedded"` | `PerPeerBus` (local publish + per-peer consume) | `bus.master_index`, `bus.peer_bus_addrs` |

**Embedded bus mode** (`internal/bus/peer_bus.go`):
- `Publish` → writes to local Redis (`resolvedBusAddr` picks `cluster.masters[master_index]` or `cluster.addr`)
- `Consume` / `Ack` / `EnsureGroup` → routes to the peer's Redis from `bus.peer_bus_addrs[peerSiteID]`
- `peer_bus_addrs` must have an entry for every declared peer (validated in `config.Validate()`)

---

## Critical Implementation Details

### Consumer group naming (bug fixed)
Each agent creates its consumer group as `{consumer_group}-{site_id}`:
```go
func (c *Consumer) groupName() string {
    return c.group + "-" + c.siteID
}
```
**Why**: Redis delivers each stream message to ONE consumer per group. If two agents shared `repl-consumers`, only one would receive each message.

### Producer must write `__meta` BEFORE publish (bug fixed)
Before publishing a delta to the bus, the producer CAS-writes:
```go
metaCAS.Run(ctx, client, []string{"__meta:"+key}, strconv.FormatUint(ts, 10), p.siteID)
```
**Why**: Without this, locally-written keys have no `__meta` entry. The consumer treats them as "first write" and any incoming peer delta can overwrite them. Writing BEFORE publish closes the race window where a peer receives the delta before our meta is set.

### Shadow key TTL is 200ms (bug fixed)
```go
const shadowTTL = 200 * time.Millisecond
```
**Why**: The shadow key suppresses the keyspace event that the applier's own `SET` triggers (sub-millisecond). The original value of `dedup_ttl_seconds * 1000` (5000ms) blocked legitimate user re-writes to the same key within 5 seconds of a replication.

### Producer filters `repl:stream:` keys
```go
if strings.HasPrefix(key, "repl:stream:") || strings.HasPrefix(key, "repl:dlq:") {
    return
}
```
**Why**: In embedded mode, writing to the local stream triggers a keyspace event for `repl:stream:site-a`. Without this filter, the producer would republish that stream-write as a user-data delta and it would replicate to peers as a stream key.

### Per-key mutex in consumer (`keyMu sync.Map`)
```go
lock := c.keyLock(delta.Key)  // LoadOrStore &sync.Mutex{}
lock.Lock()
defer lock.Unlock()
// ReadMeta + LWW check + Apply happen under the lock
```
**Why**: In embedded mode (and generally with N peer goroutines), two goroutines can concurrently call `ReadMeta` on the same key, both see `meta=0`, both accept, and the last pipeline writer wins arbitrarily. The per-key mutex serializes the TOCTOU window.

### `PerPeerBus` for embedded mode
In dedicated bus mode, all streams live on one Redis — a single `RedisStreamsBus` client suffices. In embedded mode each site's stream lives on that site's own Redis:
```
Agent-B consuming from A: connects to redis-a:6379  (PerPeerBus.peers["site-a"])
Agent-B publishing:       connects to redis-b:6379  (PerPeerBus.local)
```
`PerPeerBus` satisfies the `bus.Bus` interface and is created in `buildBusClient` when `bus.mode == "embedded"`. The producer and consumer only use the `bus.Bus` interface — no concrete type dependency.

---

## Internal Key Namespace

| Key pattern | Purpose | Set by |
|---|---|---|
| `__repl:applying:{key}` | Loop-prevention shadow key (200ms TTL) | `dedup.MarkApplying` |
| `__meta:{key}` | LWW metadata: `{hlc, site}` | `applier.Apply` + `producer.handleEvent` |
| `repl:stream:{site_id}` | Per-site replication stream on the bus | `bus.Publish` |
| `repl:stream:{site_id}:dlq` | Dead Letter Queue for unprocessable msgs | `bus.SendToDLQ` |

---

## HLC Encoding

```go
// internal/hlc/hlc.go
// packed uint64: high 44 bits = wall clock ms, low 20 bits = logical counter
func pack(wallMs uint64, logical uint32) uint64 {
    return (wallMs << 20) | uint64(logical&0xFFFFF)
}
```

Comparison is direct `uint64` comparison. Tie-break by `site_id` string (lexicographic).

---

## Key Structs

```go
// internal/bus/bus.go
type Delta struct {
    SiteID     string          // originating site
    Key        string          // Redis key
    KeyType    string          // string / hash / list / set / zset / none
    Value      interface{}     // type-specific (string, map, []string, etc.)
    HLC        uint64          // packed Hybrid Logical Clock
    CapturedAt int64           // Unix ms when captured
    TTLMs      int64           // remaining TTL in ms (-1 = no TTL)
    Cmd        string          // SET / HSET / LPUSH / DEL etc.
    SeqID      string          // "{site_id}-{seqNo}" monotonic per agent
}
```

---

## Getting Started with Existing Redis Clusters

### What must be enabled on every Redis master

```bash
# Check
redis-cli -h <host> -p <port> CONFIG GET notify-keyspace-events

# Enable at runtime (no restart needed)
redis-cli -h <host> -p <port> CONFIG SET notify-keyspace-events KEA

# Make permanent in redis.conf
notify-keyspace-events KEA
```

| Flag | Meaning |
|---|---|
| `K` | Keyspace events |
| `E` | Keyevent events |
| `A` | All commands (alias for `g$lzxed`) |

If existing value is non-empty (e.g. `Kx`), **append** missing flags — do not overwrite:
```bash
redis-cli CONFIG SET notify-keyspace-events KExA
```

---

### Bus mode decision tree

| Existing topology | Bus mode | Extra Redis? |
|---|---|---|
| 1 Redis per site | `embedded` | ❌ No |
| N masters per site (sharded cluster) | `embedded` + `master_index` | ❌ No |
| Need isolated bus traffic | `standalone` | ✅ 1 extra Redis shared by all sites |
| Need HA bus failover | `sentinel` | ✅ bus master + replicas + sentinels |

---

### Config templates for existing clusters

#### Embedded (1 Redis per site — most common)

```yaml
# /etc/redibridge/agent.yaml   (deploy one per site)
site_id: "site-a"

cluster:
  mode: "standalone"
  addr: "<existing-redis>:6379"
  password: "<auth-or-blank>"
  tls: false

bus:
  mode: "embedded"
  master_index: 0
  peer_bus_addrs:
    site-b: "<site-b-redis>:6379"   # agent-a must TCP-reach this
    site-c: "<site-c-redis>:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"
  password: "<auth-or-blank>"

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

#### Embedded (N masters per site)

```yaml
site_id: "site-a"

cluster:
  mode: "cluster"
  masters:
    - "<master1>:6379"
    - "<master2>:6379"
    - "<master3>:6379"
  password: "<auth-or-blank>"

bus:
  mode: "embedded"
  master_index: 0           # master1 hosts repl:stream:site-a
  peer_bus_addrs:
    site-b: "<site-b-master1>:6379"
    site-c: "<site-c-master1>:6379"
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"
  password: "<auth-or-blank>"

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

#### Standalone bus (dedicated bus Redis)

```yaml
site_id: "site-a"

cluster:
  mode: "standalone"
  addr: "<existing-redis>:6379"
  password: "<auth-or-blank>"

bus:
  mode: "standalone"
  addr: "<shared-bus-redis>:6379"   # all agents at all sites must reach this
  stream_prefix: "repl:stream:"
  consumer_group: "repl-consumers"
  password: "<bus-auth-or-blank>"

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

---

### Deploy the agent

#### Docker

```bash
docker run -d \
  --name redibridge \
  --restart unless-stopped \
  -v /etc/redibridge/agent.yaml:/etc/redibridge/agent.yaml:ro \
  -p 8080:8080 \
  -p 9090:9090 \
  ghcr.io/nagaraju/redibridge:latest \
  --config /etc/redibridge/agent.yaml
```

#### Binary

```bash
git clone https://github.com/nagaraju/redibridge && cd redibridge
make build
./bin/redibridge --config /etc/redibridge/agent.yaml
```

#### systemd

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
systemctl daemon-reload && systemctl enable --now redibridge
journalctl -u redibridge -f
```

---

### Verify and smoke-test

```bash
# Health
curl http://localhost:8080/health
# → {"site_id":"site-a","status":"ok","uptime_s":12}

# Peer status
curl http://localhost:8080/lag

# Metrics
curl http://localhost:9090/metrics | grep repl_events_published_total
# Should be > 0 after writing keys

# Replication round-trip
redis-cli -h <site-a> SET repl:test "hello"
sleep 1
redis-cli -h <site-b> GET repl:test   # → "hello"
redis-cli -h <site-c> GET repl:test   # → "hello"
```

---

### Network requirements (firewall rules)

In **embedded mode**, the agent connects to BOTH local Redis AND peer Redis instances:

```
agent-a → redis-a:6379  (local data + publish stream)
agent-a → redis-b:6379  (pull repl:stream:site-b from peer)
agent-a → redis-c:6379  (pull repl:stream:site-c from peer)

agent-b → redis-b:6379
agent-b → redis-a:6379
agent-b → redis-c:6379
```

In **standalone bus mode**, every agent connects to a single shared bus Redis:

```
agent-a → redis-a:6379   (local data)
agent-a → redis-bus:6379 (publish + consume all peer streams)
agent-b → redis-b:6379
agent-b → redis-bus:6379
```

---

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| `repl_events_captured_total` stays 0 | Keyspace notifications not enabled | `CONFIG SET notify-keyspace-events KEA` on every master |
| Peers timeout at startup | Firewall blocks agent → peer Redis | Open TCP port 6379 between sites |
| One-directional replication only | Agent not deployed on peer sites | Deploy an agent at **every** site |
| LWW not converging | Extreme NTP clock skew | Sync clocks; `chronyc tracking` to verify |
| `peer_bus_addrs missing entry for peer` | Config gap | Every `peers` entry needs a matching `bus.peer_bus_addrs` entry |
| Existing keys not replicated on startup | Producer only captures new writes | Re-write existing keys to trigger capture, or use `redis-cli --scan \| xargs DUMP/RESTORE` to seed peers |
| High lag after startup | Default batch/concurrency too low | Increase `batch_size` (→200+) and `apply_concurrency` (→16+) |

---

## Running the Project

```bash
# Simplest (local dev)
make up-standalone

# Production HA
make up-sentinel

# Redis Cluster (sharded, dedicated bus)
make up-cluster

# Embedded bus — standalone (no bus container)
make up-embedded

# Embedded bus — 3-master cluster (no bus container)
make up-embedded-cluster

# Build binary
make build

# Tests (no Docker needed)
make test-unit

# E2E tests
make test-e2e                      # cluster mode
make test-e2e-embedded             # embedded standalone
make test-e2e-embedded-cluster     # embedded 3-master cluster
```

Docker port layout:

| Compose file | Redis ports | Coordinator ports | Metrics ports |
|---|---|---|---|
| standalone / sentinel / cluster | 6381–6389 (+ bus 6390) | 8081–8083 | 9091–9093 |
| embedded | 6401–6403 | 8191–8193 | 9191–9193 |
| embedded-cluster | 6411–6419 | 8291–8293 | 9291–9293 |

---

## Management API (coordinator port :8080 inside container, :8081/82/83 on host)

```
GET  /health   → {"site_id","status","uptime_s"}
GET  /lag      → per-peer paused/active
GET  /stats    → peer list + metrics port
GET  /config   → full config (passwords redacted)
POST /pause    body: {"site":"cluster-b"}
POST /resume   body: {"site":"cluster-b"}
```

---

## Prometheus Metrics (:9090 inside, :9091/92/93 on host)

All metrics prefixed `repl_`:
- `events_captured_total{site_id, key_type}`
- `events_published_total{site_id}`
- `events_consumed_total{site_id, peer_site}`
- `writes_applied_total{site_id, peer_site}`
- `writes_discarded_total{site_id, peer_site, reason}` — reasons: `loopback`, `dedup_hit`, `lww_lost`
- `lag_ms{site_id, peer_site}`
- `apply_duration_ms{site_id, peer_site}` (histogram)
- `bus_stream_length{stream}`

---

## Testing

Unit tests live in `internal/*/` alongside source:
- `hlc_test.go` — monotonicity, concurrency, tie-break
- `lww_test.go` — LWW accept/reject/tie-break scenarios
- `applier_test.go` — Delta encoding for all types, TTL expired-in-transit
- `dedup_test.go` — SeqID LRU cache and eviction

Integration tests in `tests/integration_test.go` (require live Redis).

E2E test suites (12 tests each, all passing):
- `tests/e2e_test.go` — cluster mode, dedicated bus
- `tests/e2e_embedded_test.go` — embedded bus, standalone cluster; asserts `repl:stream:` keys never leak to peers
- `tests/e2e_embedded_cluster_test.go` — embedded bus, 3-master cluster; validates writes on master2/master3 replicate; exercises concurrent LWW via `keyMu`

All three suites cover: string replication, hash, TTL, bidirectional, delete, LWW convergence, latency (p50/p95), Prometheus metrics increment, management API health, CRUD round-trip.

---

## Go Module

```
module github.com/nagaraju/redibridge
go 1.23.0
```

Key dependencies: `github.com/redis/go-redis/v9`, `github.com/spf13/viper`, `go.uber.org/zap`, `github.com/prometheus/client_golang`, `github.com/hashicorp/golang-lru/v2`
