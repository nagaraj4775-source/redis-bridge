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
| **Coordinator** | `internal/coordinator/` | HTTP API: `/health`, `/lag`, `/stats`, `/config`, `/pause`, `/resume` |

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

## Frequently Asked Questions

**Q: What if Site-B is down for an hour while Site-A writes 10,000 keys?**  
A: All 10,000 deltas accumulate in `repl:stream:ec-site-a`. When Site-B's agent restarts, it reads from where it left off and catches up automatically. You can monitor progress via `curl http://localhost:8292/lag`.

**Q: What happens if the same key is written on two sites at exactly the same time?**  
A: Both deltas are published to both streams. Every site picks the delta with the **higher HLC timestamp** and discards the other. All sites converge to the same value.

**Q: Does this work with existing Redis data already in the cluster?**  
A: Only **new writes** (after the agent starts) are replicated. Keys written before the agent started will not be retroactively synced unless you trigger a write on each existing key.

**Q: Is there any message broker required (Kafka, RabbitMQ, etc.)?**  
A: No. RedisBridge uses Redis Streams built into your existing Redis instances. In embedded bus mode, no additional infrastructure is needed at all.

**Q: What Redis data types are supported?**  
A: `string`, `hash`, `list`, `set`, and `zset`. `del` (deletion) is also replicated.
