# RedisBridge — How It Works

A complete beginner's guide to understanding the RedisBridge replication system.

---

## What Problem Does It Solve?

Imagine you have **3 offices** (Site-A, Site-B, Site-C), each with their own Redis database. When someone updates data in Site-A, the other two offices don't automatically know about it. RedisBridge **automatically synchronizes data across all sites in real-time**.

---

## Big Picture Architecture

```
Site-A Redis ──► Agent-A ──► Stream-A ──► Agent-B ──► Site-B Redis
                                    └──► Agent-C ──► Site-C Redis

Site-B Redis ──► Agent-B ──► Stream-B ──► Agent-A ──► Site-A Redis
                                    └──► Agent-C ──► Site-C Redis
```

Each site has:
- **A Redis Cluster** — the actual data store
- **An Agent** — a Go process that watches and syncs data
- **A Stream** — a Redis Stream (like a log/queue) that holds changes

---

## The Agent Has Two Parts

### 1. Producer — "Watches for changes"
- Subscribes to Redis **keyspace notifications** (`__keyevent@*__:set`)
- When you do `SET mykey hello` on Redis, Redis fires an event: *"hey, mykey was set!"*
- The producer reads the new value, stamps it with a **timestamp (HLC)**, and writes it to the stream

### 2. Consumer — "Applies changes from other sites"
- Reads the streams of **other sites** (not its own)
- For each entry: compares timestamps, decides whether to apply
- If accepted: writes the value to local Redis

---

## Agent Roles — Scaling Consumer Apply Throughput

By default every agent runs **both** producer and consumer (`role: full`). You can split them when you need to scale out the apply side independently.

### The Three Roles

| Role | What runs | When to use |
|------|-----------|-------------|
| `full` | Producer + Consumer | Default — one agent does everything |
| `consumer` | Consumer only | Add extra apply workers to drain lag faster |
| `producer` | Producer only | Dedicated watcher with no apply overhead (advanced) |

### How It Works With Multiple Consumers

Redis consumer groups natively partition work across consumers. When you add a second consumer agent:

```
repl:stream:ec-site-b  (100,000 pending entries)
  ┌──────────────────────────────────────────────────────┐
  │  XREADGROUP call by agent-a-1  → gets entries 1,3,5… │
  │  XREADGROUP call by agent-a-2  → gets entries 2,4,6… │
  └──────────────────────────────────────────────────────┘
  Each entry goes to exactly ONE consumer. 2× consumers = 2× apply speed.
```

Each consumer **must have a unique consumer ID** so Redis tracks their pending entry list (PEL) separately.

### Consumer ID — How It Is Assigned

RedisBridge resolves the consumer ID in this priority order:

```
1. consumer_id field in agent.yaml         (explicit, always wins)
2. OS hostname                              (auto — works in Docker/Kubernetes)
3. siteID + unix-nanoseconds               (last-resort fallback)
```

In Docker/Kubernetes, every container already has a unique hostname — so leaving `consumer_id: ""` is enough.

### Deploying a Scale-out Consumer Agent

Keep your main agent as `role: full` (it runs the producer). Add extra agents as `role: consumer`:

```yaml
# agent-a-consumer-2.yaml  ← copy of agent-a.yaml with these two lines changed
site_id: "ec-site-a"
role: "consumer"            # ← no producer started on this instance
consumer_id: ""             # ← hostname used automatically (must be unique)

# All other config (cluster, bus, peers, replication) stays identical
```

Start it alongside the main agent:

```bash
# With Docker Compose (add a second service in docker-compose.yml)
redibridge --config /etc/redibridge/agent-a-consumer-2.yaml

# Or in Docker directly
sudo docker run --name agent-a-consumer-2 \
  -v $(pwd)/config/cluster/embedded-bus/agent-a-consumer-2.yaml:/etc/redibridge/agent.yaml \
  redibridge:latest
```

### Visual Architecture (1 Full + 2 Consumer Agents)

```
Site-A Redis ──keyspace events──► Agent-A (role: full)
                                   │  Producer: publishes to Stream-A
                                   │  Consumer: reads Stream-B + Stream-C
                                   │
                              ╔════╧═══════════════════════════════╗
                              ║   repl:stream:ec-site-b            ║
                              ║   100,000 pending entries          ║
                              ╚════╤════════╤═══════════════════════╝
                                   │        │
                      ┌────────────┘        └────────────┐
                      ▼                                  ▼
              Agent-A-1 (full)                  Agent-A-2 (consumer)
              consumer-id: host-1               consumer-id: host-2
              applies entries 1,3,5…            applies entries 2,4,6…
              → Site-A Redis                    → Site-A Redis
```

---

## Step-by-Step Replication Flow

```
1. User writes:   SET price 100   on Site-A

2. Redis fires keyspace event on Site-A

3. Producer (Site-A) wakes up:
   - Reads current value: "100"
   - Stamps with timestamp: HLC=1000
   - Writes to Stream-A: { key=price, value=100, hlc=1000, site=A }

4. Consumer (Site-B) reads Stream-A:
   - Gets entry: { price=100, hlc=1000, site=A }
   - Checks: is HLC=1000 newer than what I have? Yes → APPLY
   - Writes SET price 100 on Site-B Redis

5. Consumer (Site-C) does the same → Site-C gets price=100

Result: All 3 sites have price=100 ✓
```

---

## What Happens When Two Sites Write at the Same Time?

This is **LWW (Last-Write-Wins)** conflict resolution:

```
Site-A: SET price 100  (timestamp = 1000)
Site-B: SET price 200  (timestamp = 1005)   ← slightly later
```

Both agents publish their deltas. Every site eventually sees both:

```
Site-A consumer receives delta from B: HLC=1005 > my HLC=1000 → APPLY price=200
Site-B consumer receives delta from A: HLC=1000 < my HLC=1005 → REJECT
Site-C consumer receives both: picks HLC=1005 → APPLY price=200
```

All 3 sites converge to `price=200`. The **higher timestamp wins**.

---

## The Timestamp: HLC (Hybrid Logical Clock)

Not a simple wall clock — an HLC combines:
- **Physical time** (milliseconds since epoch) — so it roughly matches real time
- **Logical counter** — increments if two events happen in the same millisecond

This guarantees:
- Timestamps are **always increasing** (no duplicates, no going backwards)
- Works correctly even if clocks on different servers drift slightly

---

## Loop Prevention (Shadow Keys)

Without protection, this loop would happen:

```
Site-B Consumer applies price=200 → writes SET price 200 to Site-B Redis
→ Site-B Redis fires keyspace event
→ Site-B Producer sees "price was set!" → publishes AGAIN to stream
→ Other sites apply AGAIN → fires AGAIN → infinite loop!
```

**Solution: Shadow keys**

When the consumer applies a write, it stores a shadow key:
```
__repl:applying:price = CRC32("200")
```

The producer checks: *"Is the current value's hash equal to the shadow hash?"*
- **Yes** → This event was triggered by my own Apply → **skip**
- **No** → This is a real new user write → **publish**

The shadow key expires after **200ms** — enough time to cover the Apply event round-trip.

---

## What Is a Redis Stream?

Think of it like a **commit log** or **Kafka topic**, but built into Redis:

```
Stream-A (repl:stream:ec-site-a):
  entry-1: { key=price,    value=100, hlc=1000 }
  entry-2: { key=username, value=alice, hlc=1001 }
  entry-3: { key=price,    value=200, hlc=1005 }
```

- Consumers use **consumer groups** to track which entries they've processed
- If Site-B goes offline for an hour, when it comes back it reads all the entries it missed → **automatic catch-up**
- Entries remain in the stream after being consumed — they are not deleted on read

### When Are Entries Deleted?

Entries are only removed if you configure a retention policy in `agent.yaml`:

```yaml
bus:
  stream_max_len: 1000000     # keep last 1 million entries (count-based)
  stream_ttl_hours: 72        # OR keep last 3 days (time-based, takes priority)
```

If both are `0`, the stream grows indefinitely.

---

## Key Components Summary

| Component | File | Role |
|-----------|------|------|
| **Producer** | `internal/producer/producer.go` | Watches keyspace events, stamps with HLC, publishes deltas to stream |
| **Consumer** | `internal/consumer/consumer.go` | Reads peer streams, performs LWW check, applies accepted deltas |
| **Applier** | `internal/applier/applier.go` | Writes the accepted key/value to local Redis |
| **Dedup/Shadow** | `internal/dedup/dedup.go` | Prevents replication loops via CRC32 shadow keys |
| **HLC Clock** | `internal/clock/` | Provides monotonic, distributed-safe timestamps |
| **Bus** | `internal/bus/` | Abstraction over Redis Streams (standalone / sentinel / cluster) |
| **Config** | `internal/config/config.go` | YAML config loader with validation |
| **Coordinator** | `internal/coordinator/` | HTTP API: `/health`, `/lag`, `/stats`, `/config`, `/pause`, `/resume`, `/reconcile`, `/sync`, `/key-status`, `/bootstrap` |

---

## Deployment Modes

RedisBridge supports three deployment topologies:

| Mode | When to use |
|------|-------------|
| **Standalone** | Single Redis instance per site (dev/test) |
| **Sentinel** | Redis Sentinel HA per site (production, no sharding) |
| **Cluster** | Redis Cluster per site (production, horizontal scaling) |

And two bus configurations:

| Bus Mode | Description |
|----------|-------------|
| **Embedded** | The replication stream lives on each site's own Redis. Peers pull directly from each other. No separate broker needed. |
| **Separate** | A dedicated Redis instance (or Sentinel cluster) hosts all streams. Useful when you want a central replication bus. |

---

## Docker Compose Layout (Cluster + Embedded Bus)

```
docker/cluster/embedded-bus/
  redis-a-master1,2,3     ← Site-A Redis Cluster (3 masters + 3 replicas)
  redis-a-replica1,2,3
  redis-b-master1,2,3     ← Site-B Redis Cluster
  redis-b-replica1,2,3
  redis-c-master1,2,3     ← Site-C Redis Cluster
  redis-c-replica1,2,3
  agent-a                 ← RedisBridge agent for Site-A
  agent-b                 ← RedisBridge agent for Site-B
  agent-c                 ← RedisBridge agent for Site-C
```

Each agent is configured via a YAML file (e.g. `config/cluster/embedded-bus/agent-a.yaml`) that specifies:
- Which Redis cluster to connect to
- Which peers to replicate from
- Bus mode and stream settings
- Replication tuning (batch size, concurrency, dedup TTL)

---

## Data Flow Diagram (Full Detail)

```
┌─────────────────────────────────────────────────────────────┐
│                          SITE-A                             │
│                                                             │
│  User Write                                                 │
│     │                                                       │
│     ▼                                                       │
│  Redis Cluster ──keyspace event──► Producer                 │
│     ▲                               │                       │
│     │                               │ 1. Read value         │
│     │                               │ 2. Stamp with HLC     │
│     │                               │ 3. Write __meta key   │
│     │                               │ 4. Publish to stream  │
│     │                               ▼                       │
│     │                         repl:stream:ec-site-a         │
│     │                                                       │
│     │  Consumer (reads streams of B and C)                  │
│     │     │                                                 │
│     │     │ 1. Read delta from repl:stream:ec-site-b        │
│     │     │ 2. Compare HLC vs local __meta                  │
│     │     │ 3. Accept if newer → Apply via Applier          │
│     └─────┘ 4. Mark shadow key (loop prevention)           │
│              5. ACK stream entry                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Reconciler — Safety Net for Missed PubSub Events

### The Problem: PubSub Is At-Most-Once

The producer detects key changes via Redis keyspace notifications (PubSub). This mechanism is **at-most-once** — Redis fires the event and immediately forgets it. There is no acknowledgment, no retry, no persistence.

When the go-redis PubSub connection performs its periodic health check (~every 100ms), it briefly disconnects and reconnects. Any keyspace events fired during this reconnect window (typically 1–10ms) are **permanently lost** — the producer never sees them, they are never published to the stream, and the key is never replicated.

```
t=0ms   PubSub connected and subscribed
t=100ms Health check → reconnecting...
t=101ms SET mykey fired → keyspace event emitted ← NOBODY LISTENING
t=102ms PubSub reconnected and re-subscribed
t=103ms SET otherkey fired → delivered normally ✓
```

`mykey` is silently dropped with no error, no log, no metric anywhere in the stack.

### How the Reconciler Fixes This

The reconciler is a background goroutine inside the producer that acts as a **periodic safety net**. It exploits the structural invariant that every successfully published key has a `__meta:{key}` hash written to Redis. A key without this anchor was never published.

**Each reconciler cycle:**

```
1. SCAN all data keys across all masters (ForEachMaster in cluster mode)
2. Collect candidates: keys with no __meta: entry
3. Wait settle delay (let any still in-flight PubSub events be processed)
4. Re-check each candidate:
   - __meta: now exists → PubSub caught up naturally → skip
   - __meta: still missing → confirmed PubSub drop → re-publish
5. Re-publish: synthesize a SET event and call handleEvent directly
   → reads current value, stamps with fresh HLC, publishes delta to stream
```

The consumer's **LWW + SeqID dedup** ensures re-published events are idempotent — even if a key is re-published multiple times, site-b applies it correctly without duplication.

### Performance Design

The reconciler is designed to add minimal overhead:

| Concern | Solution |
|---------|----------|
| N EXISTS round trips per scan | **Pipelined in batches of 100** → 100× fewer round trips |
| Scanning large key spaces | SCAN uses cursor-based iteration — non-blocking, won't stall Redis |
| Scanning too frequently | Configurable interval (`reconcile_interval_seconds`); set to 0 to disable |
| False positives during active writes | Settle delay (1/3 of interval, clamped 5–15 s) absorbs in-flight events |

**Overhead estimate by dataset size:**

| Key count | Round trips (pipelined) | Wall-clock time |
|-----------|------------------------|-----------------|
| 100k | ~1,000 | ~100ms |
| 1M | ~10,000 | ~1s |
| 10M | ~100,000 | ~10s |

For large deployments, increase `reconcile_interval_seconds` proportionally (e.g., 300 s for 10M keys).

### Configuration

```yaml
replication:
  reconcile_interval_seconds: 30   # Default: 30s. Set to 0 to disable.
```

**Tuning guide:**

| Deployment size | Recommended interval |
|-----------------|----------------------|
| < 500k keys | 30s (default) |
| 500k – 5M keys | 120s |
| 5M – 20M keys | 300s |
| > 20M keys | 600s or disable + rely on stream catch-up |

### Log Messages

| Message | Meaning |
|---------|---------|
| `reconciler: started interval=30s settle=10s` | Reconciler goroutine is active |
| `reconciler: keys without meta (settling) count=N` | N candidates found; waiting for settle |
| `reconciler: re-publishing missed event key=X` | PubSub drop confirmed; re-publishing |
| `reconciler: done republished=N` | Cycle complete; N keys recovered |
| `reconciler: disabled (reconcile_interval_seconds=0)` | Reconciler turned off by config |

---

## Replication Configuration Reference

All options live under the `replication:` key in `agent.yaml`.

```yaml
replication:
  batch_size: 100
  apply_concurrency: 8
  dedup_ttl_seconds: 5
  max_in_flight: 1000
  reconcile_interval_seconds: 30
```

---

### `batch_size`

How many stream entries the agent reads **per `XREADGROUP` call** per peer.

```
Stream has 500 pending entries from site-b.

batch_size=100 → agent reads 100 at a time:
  Round 1: reads entries 1–100   → applies → ACKs
  Round 2: reads entries 101–200 → applies → ACKs
  ... 5 rounds to drain 500 entries

batch_size=500 → 1 round, but one large blocking call
```

| Value | Best for |
|-------|----------|
| `50–100` | Memory-constrained agents or very large value sizes |
| `100–500` | General workloads (default `100` is safe) |
| `500–1000` | Catching up after long downtime, high-throughput writes |

---

### `apply_concurrency`

How many goroutines apply messages **in parallel** from a single peer stream.

```
100 entries arrive from site-b.

apply_concurrency=1 → applied one by one:   ~100ms (serial)
apply_concurrency=8 → applied 8 at a time:  ~13ms  (8× faster)

Key-level serialization still applies — two writes to the
same key are always applied in order, even at concurrency=8.
```

Cap this at your Redis connection pool size. The default `8` works well for most deployments. Scale-out consumers (role: consumer) can run higher concurrency safely.

---

### `dedup_ttl_seconds`

How long the **shadow key** lives after a consumer applies a write, used to suppress replication loops.

```
t=0ms   Consumer applies: SET price 200
         → writes shadow: __repl:applying:price = CRC32("200")  TTL=5s

t=1ms   Site-b Redis fires keyspace event: "price was set"
t=1ms   Producer checks shadow: hash matches → SKIP (not a new write)

t=5001ms Shadow key expires
t=5002ms User writes: SET price 300  ← a real new write
          Producer checks shadow: expired → PUBLISH ✓
```

| Value | Risk |
|-------|------|
| `< 1s` | Shadow expires before PubSub event arrives → replication loop |
| `5s` | Sweet spot for LAN / Docker deployments (default) |
| `10s` | Recommended over WAN or high-latency links |
| `> 30s` | Legitimate local writes within that window may be suppressed |

---

### `max_in_flight`

Max **unacknowledged** stream entries before the consumer pauses (backpressure control).

```
Site-b publishes a burst of 5,000 entries.
max_in_flight=1000, apply_concurrency=8

t=0s   Consumer reads batch of 100 → starts applying (8 at a time)
       in-flight count = 100
t=0.1s reads next batch → in-flight = 200
...
t=1s   in-flight reaches 1,000 → PAUSE reading new entries
       Apply workers keep draining...
t=1.5s in-flight drops to 800 → RESUME reading
```

This prevents the agent from reading faster than it can apply, stopping the Redis PEL (pending entry list) from growing unboundedly and consuming memory.

**Rule of thumb:** `batch_size × apply_concurrency × 2`

| batch_size | apply_concurrency | Suggested max_in_flight |
|------------|-------------------|------------------------|
| 100 | 8 | 1,000–2,000 |
| 200 | 16 | 4,000–8,000 |
| 500 | 32 | 16,000–32,000 |

---

### `reconcile_interval_seconds`

How often the background reconciler scans for keys **silently dropped by PubSub**.

```
t=0s    Reconciler starts: SCAN all keys on all masters
         key "bench:532" found → no __meta: anchor → candidate

t=10s   Settle delay elapses: re-check candidate
         bench:532 still has no __meta: → PubSub drop confirmed
         → re-publish bench:532 to stream → site-b receives it ✓

t=30s   Next reconciler cycle begins
```

| Value | Effect |
|-------|--------|
| `30` | Scan every 30s — suitable for < 500k keys |
| `120` | Every 2 min — recommended for 500k–5M keys |
| `300` | Every 5 min — recommended for 5M–20M keys |
| `0` | **Disabled** — rely on stream catch-up only |

The scan is non-blocking (cursor-based `SCAN`) and uses pipelined `EXISTS` checks in batches of 100, so it adds minimal load. For datasets > 5M keys, increase the interval proportionally so cycles don't overlap.

---

## Management HTTP API

Every agent exposes an HTTP management API on `coordinator.port` (default `8080`).

### Existing endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check — returns `{"status":"ok"}` |
| `GET` | `/lag` | Per-peer replication lag in milliseconds |
| `GET` | `/stats` | Consumer group lag, pending counts, stream lengths |
| `GET` | `/config` | Redacted view of the running config |
| `POST` | `/pause` | Pause all consumers on this agent |
| `POST` | `/resume` | Resume all consumers |

### On-Demand Reconcile (Feature 1)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/reconcile` | Trigger an immediate reconciler cycle (no wait for next tick) |
| `GET` | `/reconcile/status` | Last cycle stats: scan duration, keys repaired, total runs |
| `POST` | `/sync?key=<name>` | Force-publish one specific key, bypassing dedup |

**Example — trigger reconcile:**
```bash
curl -X POST http://localhost:8080/reconcile
# {"message":"reconciler cycle triggered","queued":true,"status":"ok"}
```

**Example — check reconcile status:**
```bash
curl http://localhost:8080/reconcile/status
# {
#   "enabled": true,
#   "last_run_at": "2024-11-01T12:34:56Z",
#   "last_scan_ms": 312,
#   "last_repaired": 2,
#   "total_runs": 47
# }
```

**Example — force-publish a single key:**
```bash
curl -X POST "http://localhost:8080/sync?key=users:42"
# {"key":"users:42","message":"force-published to replication stream","status":"ok"}
```

### Per-Key Replication Status (Feature 3)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/key-status?key=<name>` | Whether a key has been published and from which site |

The endpoint reads the `__meta:{key}` hash that the producer writes on every successful publish. If the hash is absent the key has never been replicated from this site.

```bash
curl "http://localhost:8080/key-status?key=users:42"
# {"key":"users:42","published":true,"hlc":1732100000123456,"origin_site":"ec-site-a"}

curl "http://localhost:8080/key-status?key=missing:key"
# {"key":"missing:key","published":false,"hlc":0,"origin_site":""}
```

### Bootstrap Sync (Feature 6)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/bootstrap` | Start a one-time full-scan publish of all data keys |
| `POST` | `/bootstrap?stop=1` | Cancel a running bootstrap |
| `GET` | `/bootstrap/status` | Live progress: state, keys published, errors |

Bootstrap is the answer to *"I just added a new site — how do I seed it with existing data?"*. It performs a cursor-based SCAN across all masters and publishes every matching key to the replication stream. Peer sites consume those events and apply via the normal LWW path, so existing newer local writes are never overwritten.

```bash
# 1. Start bootstrap on the source site
curl -X POST http://localhost:8080/bootstrap
# {"message":"bootstrap started — poll /bootstrap/status for progress","status":"ok"}

# 2. Poll progress
curl http://localhost:8080/bootstrap/status
# {"state":"running","started_at":"...","published":48213,"errors":0}

# Wait until state == "done" or "done_with_errors"
curl http://localhost:8080/bootstrap/status
# {"state":"done","published":100000,"errors":0}

# 3. Cancel if needed
curl -X POST "http://localhost:8080/bootstrap?stop=1"
```

**Bootstrap respects pattern filters** — if `include_patterns` / `exclude_patterns` are configured, only matching keys are published.

---

## Key Pattern Filtering (Feature 5)

By default, RedisBridge replicates **all** user keys. You can restrict replication to specific key patterns using glob-style include/exclude rules in `agent.yaml`.

```yaml
replication:
  include_patterns:       # if set, only keys matching at least one pattern are replicated
    - "users:*"
    - "orders:*"
    - "inventory:*"
  exclude_patterns:       # keys matching any pattern are NEVER replicated
    - "cache:*"
    - "tmp:*"
    - "session:*"
```

### Evaluation Order

```
1. Is the key an internal RedisBridge key (__meta:, repl:stream:, …)?  → always skip
2. include_patterns defined AND key does NOT match any → skip + increment metric
3. key matches any exclude_pattern                    → skip + increment metric
4. Otherwise                                          → replicate normally
```

### Glob Syntax

Patterns use standard Go `path.Match` glob syntax:

| Pattern | Matches | Does NOT match |
|---------|---------|----------------|
| `users:*` | `users:123`, `users:abc:profile` | `orders:123` |
| `orders:?` | `orders:1` | `orders:123` |
| `cache:[abc]:*` | `cache:a:foo`, `cache:b:bar` | `cache:d:foo` |

> **Note:** `*` matches any sequence of characters except `/`. Since Redis keys rarely contain `/`, this works as a general wildcard for `:` separated namespaces.

### Scope

Pattern filtering applies to:
- **PubSub events** — the hot path; filtered before any Redis read
- **Reconciler scans** — filtered during the SCAN phase so non-matching keys are never checked or re-published
- **Bootstrap syncs** — filtered during SCAN so only matching keys are seeded

### Observability

Filtered events increment the `repl_pattern_filtered_total{site_id, reason}` Prometheus counter. A steady rate here is normal when patterns are configured; a sudden spike on a previously unconfigured agent indicates a misconfiguration.

---

## TTL Fidelity (Feature 7)

### The Problem

When a key has a TTL (time-to-live), naively replicating the *remaining* TTL causes all peer sites to expire the key *later* than the origin:

```
t=0   Site-A writes:  SET session:42 EX 60   (expires at t=60)
t=5   Producer reads remaining TTL: 55s
t=5   Delta published: {key: session:42, ttl_ms: 55000}
t=6   Site-B consumer applies delta, sets TTL to 55s from NOW
      → Site-B expires session:42 at t=61   ← 1 second late!
      (worse if replication is lagged by 10s: expires at t=70)
```

With high lag or network jitter, sessions could outlive their intended expiry by seconds to minutes across sites.

### The Fix: Absolute Expiry Epoch

The producer computes the **absolute expiry epoch** at capture time and encodes it in the delta:

```
expiresAtMs = capturedAt + remainingTTLms
```

The consumer then calls `PEXPIREAT key expiresAtMs` — which sets the **same wall-clock expiry** on every site regardless of replication lag.

```
t=0   Site-A writes:  SET session:42 EX 60
t=5   Producer captures:  capturedAt=T+5, ttlMs=55000
                         expiresAtMs = T+5 + 55000 = T+60s  ← absolute epoch
t=5   Delta: {key: session:42, expires_at_ms: <T+60s>}
t=6   Site-B: PEXPIREAT session:42 <T+60s>  → expires at T+60s ✓
t=15  Site-C: PEXPIREAT session:42 <T+60s>  → expires at T+60s ✓
```

All sites expire the key at the same wall-clock instant, regardless of how long the delta spent in the stream.

### In-Transit Expiry Guard

If the key's TTL expires **while the delta is still in the stream** (e.g., very long lag + very short TTL), the consumer skips the apply entirely:

```
if now >= expiresAtMs → skip (key is already logically expired)
```

This prevents writing a key to the peer site only to have it immediately expire — a waste of a write and potentially confusing to applications.

### Wire Format

The `ExpiresAtMs` field is backward-compatible. Older agents that do not send it fall back to the legacy `CapturedAt + TTLMs` calculation in the applier. Both approaches produce the same result when clocks are synchronized.

### Observability

Keys skipped due to in-transit expiry increment `repl_ttl_expired_in_transit_total{site_id}`. A non-zero rate is normal for workloads with very short TTLs (< 1 s) under high lag conditions.

---

## Frequently Asked Questions

**Q: What if Site-B is down for an hour while Site-A writes 10,000 keys?**  
A: All 10,000 deltas accumulate in `repl:stream:ec-site-a`. When Site-B's agent restarts, it reads from where it left off and catches up automatically. You can monitor progress via `curl http://localhost:8292/lag`.

**Q: What happens if the same key is written on two sites at exactly the same time?**  
A: Both deltas are published to both streams. Every site picks the delta with the **higher HLC timestamp** and discards the other. All sites converge to the same value.

**Q: Does this work with existing Redis data already in the cluster?**  
A: Only **new writes** (after the agent starts) are replicated by default. Use `POST /bootstrap` to publish all existing keys to the replication stream so peer sites can catch up.

**Q: Is there any message broker required (Kafka, RabbitMQ, etc.)?**  
A: No. RedisBridge uses Redis Streams built into your existing Redis instances. In embedded bus mode, no additional infrastructure is needed at all.

**Q: What Redis data types are supported?**  
A: `string`, `hash`, `list`, `set`, and `zset`. `del` (deletion) is also replicated.

**Q: I just added a fourth site. How do I seed it with existing data?**  
A: Start the new agent normally (it will receive all new writes automatically). Then trigger a bootstrap on each existing source site:
```bash
curl -X POST http://site-a-agent:8080/bootstrap
curl -X POST http://site-b-agent:8080/bootstrap
curl -X POST http://site-c-agent:8080/bootstrap
```
Poll `GET /bootstrap/status` until `state == "done"`. The new site's consumer will apply all published deltas via LWW — no duplicate-write issues because LWW discards anything older than what the site already has.

**Q: I suspect a specific key is missing from Site-B. How do I confirm and fix it?**  
A:
```bash
# Check whether the key was ever published from Site-A
curl "http://site-a-agent:8080/key-status?key=mykey"
# {"published":false,...}  ← never published, confirming the gap

# Force-publish it immediately
curl -X POST "http://site-a-agent:8080/sync?key=mykey"
```

**Q: How do I trigger a reconcile without waiting for the next 30-second tick?**  
A:
```bash
curl -X POST http://localhost:8080/reconcile
# Check what it found/fixed
curl http://localhost:8080/reconcile/status
```

**Q: Can I replicate only certain namespaces (e.g., skip cache keys)?**  
A: Yes. Add pattern filters to `agent.yaml`:
```yaml
replication:
  exclude_patterns:
    - "cache:*"
    - "tmp:*"
```
Or to replicate only specific namespaces:
```yaml
replication:
  include_patterns:
    - "users:*"
    - "orders:*"
```

**Q: Will TTL-limited keys (sessions, tokens) expire at the same time on all sites?**  
A: Yes, after Feature 7. The producer encodes an absolute expiry epoch (`expiresAtMs = capturedAt + ttlMs`) in every delta. The consumer calls `PEXPIREAT` with that epoch, so every site expires the key at the same wall-clock instant regardless of replication lag.
