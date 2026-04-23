# RedisBridge — Pre-Go-Live Validation Guide

A step-by-step runbook to validate the full replication pipeline before going live. Follow these steps after deploying agents against your existing Redis clusters.

---

## Prerequisites

Agents are running on all sites and accessible via their HTTP coordinator port.  
All examples assume a 3-site cluster setup. Adjust container names, hostnames, and ports to match your deployment.

```bash
# Define shorthands for the examples below
REDIS_A="redis-cli -c -h redis-a-master1 -p 6379"
REDIS_B="redis-cli -c -h redis-b-master1 -p 6379"
REDIS_C="redis-cli -c -h redis-c-master1 -p 6379"

# Agent HTTP ports (set in coordinator.port in your agent config)
AGENT_A="http://localhost:8291"
AGENT_B="http://localhost:8292"
AGENT_C="http://localhost:8293"
```

> If running via Docker, prefix each `redis-cli` command with:  
> `sudo docker exec <container-name> redis-cli ...`

---

## Step 1 — Confirm Agents are Healthy

```bash
curl $AGENT_A/status
curl $AGENT_B/status
curl $AGENT_C/status
```

**Expected response:**
```json
{"site_id":"ec-site-a","status":"ok","uptime_s":42}
```

If `status` is not `ok`, check agent logs:
```bash
sudo docker logs <agent-container> --tail 30
```

---

## Step 2 — Confirm Keyspace Notifications are Active

The producer subscribes to `__keyevent@*__:set` (and other event types). Redis must have keyspace notifications enabled.

```bash
# Check configuration — should contain at least "K" and "E" flags
$REDIS_A CONFIG GET notify-keyspace-events
```

The agent sets `KEA` (all keyspace + keyevent notifications) on startup automatically.  
If the value is empty, the producer will not detect any writes.

```bash
# Confirm the producer's pattern subscription is active
$REDIS_A PUBSUB NUMPAT
```

**Expected:** `1` — the producer has one active `PSubscribe` pattern.  
If `0`, the agent's producer goroutine may not have started. Check agent logs.

---

## Step 3 — Insert a Test Key and Verify Replication

```bash
# Write a key on site-a
$REDIS_A SET validate:test1 "hello-from-site-a"

# Wait ~1 second for replication to complete
sleep 1

# Read from all 3 sites
echo -n "Site-A: "; $REDIS_A GET validate:test1
echo -n "Site-B: "; $REDIS_B GET validate:test1
echo -n "Site-C: "; $REDIS_C GET validate:test1
```

**Expected:** All 3 sites return `hello-from-site-a`.

---

## Step 4 — Check the Replication Stream

After the producer processes the write event, it publishes a delta to the site's replication stream.

```bash
# Check how many entries are in the stream
$REDIS_A XLEN repl:stream:ec-site-a

# Read the most recent entry
$REDIS_A XREVRANGE repl:stream:ec-site-a + - COUNT 1
```

**Expected output:**
```
1) 1) "1776877115814-0"           <- entry ID: <unix-ms>-<seq>
   2) 1) "data"
      2) "{\"site_id\":\"ec-site-a\",\"key\":\"validate:test1\",
           \"key_type\":\"string\",\"value\":\"aGVsbG8tZnJvbS1zaXRlLWE=\",
           \"hlc\":1863191939212378112,\"seq_id\":\"ec-site-a:1\",...}"
```

Key fields in the delta JSON:

| Field        | Meaning                                        |
|--------------|------------------------------------------------|
| `site_id`    | Which site produced this delta                 |
| `key`        | The Redis key that was written                 |
| `key_type`   | `string`, `hash`, `list`, `set`, or `zset`     |
| `value`      | Base64-encoded value at the time of capture    |
| `hlc`        | Hybrid Logical Clock timestamp (LWW ordering)  |
| `captured_at`| Unix-ms when the value was read by producer    |
| `ttl_ms`     | Remaining TTL at capture time (-1 = no expiry) |
| `seq_id`     | Dedup ID — prevents double-apply on consumers  |

---

## Step 5 — Check the Meta Key (LWW Record)

The producer writes a `__meta:{key}` hash to record the last-seen HLC for each key.  
Consumers compare incoming deltas against this to decide whether to accept or reject.

```bash
$REDIS_A HGETALL __meta:{validate:test1}
```

**Expected:**
```
1) "hlc"
2) "1863191939212378112"
3) "site"
4) "ec-site-a"
```

---

## Step 6 — Check Consumer Groups and Offsets

Each agent consumes the peer sites' streams using Redis consumer groups.  
To check site-b's and site-c's consumption progress on site-a's stream:

```bash
# Run on site-a's Redis — shows all consumers reading repl:stream:ec-site-a
$REDIS_A XINFO GROUPS repl:stream:ec-site-a
```

**Key fields to check:**

| Field               | Expected | Meaning                                         |
|---------------------|----------|-------------------------------------------------|
| `name`              | `repl-consumers` | Consumer group name                   |
| `consumers`         | 2        | Number of active consumers (one per peer site)  |
| `pending`           | 0        | Delivered but not yet ACKed (backlog)           |
| `last-delivered-id` | latest ID | Last entry handed to a consumer               |
| `lag`               | 0        | Entries in stream not yet delivered             |

```bash
# Drill into individual consumers within the group
$REDIS_A XINFO CONSUMERS repl:stream:ec-site-a repl-consumers
```

---

## Step 7 — Check Lag via the Agent API

The `/lag` endpoint gives real-time lag per peer stream consumed by that agent.

```bash
curl $AGENT_A/lag   # how far behind is site-a when reading site-b and site-c?
curl $AGENT_B/lag
curl $AGENT_C/lag
```

**Expected (healthy state):**
```json
{
  "ec-site-b": {"lag": 0, "pending": 0, "stream_len": 1500, "last_delivered_id": "..."},
  "ec-site-c": {"lag": 0, "pending": 0, "stream_len": 1500, "last_delivered_id": "..."}
}
```

| Field         | Healthy | Concern                                          |
|---------------|---------|--------------------------------------------------|
| `lag`         | 0       | > 0 means catching up or agent is slow/stuck     |
| `pending`     | 0       | > 0 means messages delivered but not ACKed yet   |
| `stream_len`  | any     | Total entries ever written to that stream        |

---

## Step 8 — Check Prometheus Metrics

```bash
curl http://localhost:9091/metrics | grep redibridge
```

Key metrics to watch:

| Metric                              | What it tells you                          |
|-------------------------------------|--------------------------------------------|
| `redibridge_producer_published_total` | Total deltas published to the stream     |
| `redibridge_consumer_applied_total`   | Total deltas accepted and applied        |
| `redibridge_consumer_rejected_total`  | Deltas rejected by LWW (stale writes)    |
| `redibridge_consumer_lag`             | Current lag per peer (gauge)             |
| `redibridge_consumer_pending`         | Unacked in-flight messages per peer      |

> Metrics port is set in `metrics.port` in your agent config (default: `9090`).  
> Each agent exposes its own metrics on its own port.

---

## Step 9 — Test Pause and Resume

Validate that pausing stops consumption and resuming catches up cleanly.

```bash
# Pause site-a's consumption (stops reading peer streams)
curl -X POST $AGENT_A/pause

# Write keys on site-b while site-a is paused
$REDIS_B SET validate:paused-key "written-while-paused"
sleep 1

# Confirm site-a has lag (it missed the write)
curl $AGENT_A/lag

# Resume — site-a will catch up automatically
curl -X POST $AGENT_A/resume
sleep 2

# Confirm site-a caught up
echo -n "Site-A: "; $REDIS_A GET validate:paused-key
```

---

## Step 10 — Reset Consumer Offset (if needed)

Use this if a consumer is stuck on a corrupt entry, or you need to replay history.

```bash
# Reset to only consume NEW entries going forward (skip all history)
$REDIS_A XGROUP SETID repl:stream:ec-site-b repl-consumers $

# Reset to replay ALL entries from the beginning
$REDIS_A XGROUP SETID repl:stream:ec-site-b repl-consumers 0

# Reset to a specific entry ID
$REDIS_A XGROUP SETID repl:stream:ec-site-b repl-consumers 1776877115814-0
```

> After resetting, restart the consuming agent so it picks up the new offset:
> ```bash
> sudo docker restart embedded-bus-agent-a-1
> ```

---

## Step 11 — LWW Conflict Resolution Test

Validate that concurrent writes on two sites converge to the same value.

```bash
# Write the same key on two sites at the same time
$REDIS_A SET validate:lww "from-site-a" &
$REDIS_B SET validate:lww "from-site-b" &
wait

# Wait for convergence (~3–5 seconds)
sleep 5

echo -n "Site-A: "; $REDIS_A GET validate:lww
echo -n "Site-B: "; $REDIS_B GET validate:lww
echo -n "Site-C: "; $REDIS_C GET validate:lww

# Check which site's write won
$REDIS_A HGETALL __meta:{validate:lww}
```

**Pass criteria:** All 3 sites show the **same value** (either `from-site-a` or `from-site-b`).  
The `__meta` hash shows the winning site and its HLC timestamp.

---

## Step 12 — Agent Failover / Catch-up Test

Validate that a restarted agent catches up on missed entries.

```bash
# Stop site-a's agent
sudo docker stop embedded-bus-agent-a-1

# Write 10 keys on site-b while site-a is down
for i in $(seq 1 10); do
  $REDIS_B SET validate:catchup:$i "val$i"
done

# Confirm site-a is missing them
echo -n "Site-A key 5: "; $REDIS_A GET validate:catchup:5   # should be (nil)

# Restart site-a's agent
sudo docker start embedded-bus-agent-a-1
sleep 5

# Confirm site-a caught up
echo -n "Site-A key 5: "; $REDIS_A GET validate:catchup:5   # should be val5

# Check lag is back to 0
curl $AGENT_A/lag
```

---

## Step 13 — Validate a Consumer-Only Agent

Use this when you have deployed a second agent with `role: consumer` to scale out apply throughput.

### 13a — Confirm the Consumer-Only Agent Started Correctly

```bash
# Check its startup logs — should say "role=consumer: producer disabled"
sudo docker logs agent-a-consumer-2 --tail 20 | grep -E "role|consumer_id|producer disabled"
```

**Expected log lines:**
```
{"level":"info","msg":"config loaded","role":"consumer","consumer_id":"agent-a-consumer-2-hostname",...}
{"level":"info","msg":"role=consumer: producer disabled"}
{"level":"info","msg":"redibridge started","site_id":"ec-site-a",...}
```

If you see `producer disabled` — the agent is running in consumer-only mode correctly.

### 13b — Confirm Both Consumer Names Are Registered in Redis

```bash
# List all consumers in the group that reads site-b's stream from site-a's perspective
# Group name format: <consumer_group>-<site_id>  e.g. repl-consumers-ec-site-a
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO CONSUMERS repl:stream:ec-site-b repl-consumers-ec-site-a
```

**Expected output — two distinct consumers:**
```
1)  1) "name"
    2) "agent-a-hostname-1"         ← main agent (role: full), consumer ID = its hostname
    3) "pending"
    4) (integer) 0                  ← no backlog when healthy
    5) "idle"
    6) (integer) 312                ← ms since it last fetched a message

2)  1) "name"
    2) "agent-a-consumer-2-host"    ← consumer-only agent, consumer ID = its hostname
    3) "pending"
    4) (integer) 0
    5) "idle"
    6) (integer) 289
```

> **Both names must be different.** If you see the same name twice, the consumer IDs collide — set `consumer_id` explicitly in one of the configs.

### 13c — Verify Work Is Being Partitioned (Under Load)

Write 1,000 keys rapidly on site-b and immediately inspect the PEL split:

```bash
# Generate 1,000 keys quickly on site-b
(
  for i in $(seq 1 1000); do
    echo "SET validate:consumer-split:$i val$i"
  done
) | sudo docker exec -i embedded-bus-redis-b-master1-1 \
    redis-cli -c -h redis-b-master1 --pipe

# Immediately check PEL distribution — catch it before ACKs complete
sleep 0.5
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO CONSUMERS repl:stream:ec-site-b repl-consumers-ec-site-a
```

**Expected:** Both consumers show non-zero `pending` counts that add up to approximately 1,000. The split is roughly equal but not guaranteed to be exactly 50/50 — Redis assigns entries to whichever consumer calls XREADGROUP first.

### 13d — Confirm No Duplicate Keys Applied

Each stream entry must be applied by **exactly one** consumer. Verify no key was written twice by checking its value is consistent:

```bash
# Wait for both consumers to finish
sleep 5

# Spot-check 5 keys on site-a — all should be present and correct
for i in 100 250 500 750 999; do
  echo -n "validate:consumer-split:$i = "
  sudo docker exec embedded-bus-redis-a-master1-1 \
    redis-cli -c -h redis-a-master1 GET validate:consumer-split:$i
done
```

**Expected:** `val100`, `val250`, `val500`, `val750`, `val999` — all correct, none missing.

### 13e — Confirm Only One Producer Is Running (Critical)

Running two producers on the same site creates duplicate stream entries. Verify only one producer is active:

```bash
# Count active PSubscribe pattern subscriptions on site-a's masters
# Should be exactly 1 per master regardless of how many consumer agents are running
sudo docker exec embedded-bus-redis-a-master1-1 \
  redis-cli -c -h redis-a-master1 PUBSUB NUMPAT

sudo docker exec embedded-bus-redis-a-master2-1 \
  redis-cli -c -h redis-a-master2 PUBSUB NUMPAT

sudo docker exec embedded-bus-redis-a-master3-1 \
  redis-cli -c -h redis-a-master3 PUBSUB NUMPAT
```

**Expected:** `1` on each master.  
**If you see `2`:** a consumer-only agent was misconfigured with `role: full` — it is running a second producer. Fix its config to `role: consumer` and restart.

---

## Step 14 — Remove a Consumer Agent Gracefully

When scaling down, Redis retains the consumer's PEL until it is cleaned up. Remove it explicitly to avoid orphaned pending entries:

```bash
# First, stop the consumer agent
sudo docker stop agent-a-consumer-2

# Wait for its PEL to drain to 0 naturally (messages it already fetched get re-delivered
# to the remaining consumer after the XAUTOCLAIM timeout)
# OR claim its pending messages immediately to the main consumer:

sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 \
  XAUTOCLAIM repl:stream:ec-site-b repl-consumers-ec-site-a agent-a-hostname-1 0 0-0 COUNT 10000

# Once PEL = 0, delete the consumer entry from the group
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 \
  XGROUP DELCONSUMER repl:stream:ec-site-b repl-consumers-ec-site-a agent-a-consumer-2-host

# Confirm it's gone
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO CONSUMERS repl:stream:ec-site-b repl-consumers-ec-site-a
```

---

## Quick Health Checklist

Run through this checklist before going live:

```
[ ] curl /status on all agents returns "ok"
[ ] PUBSUB NUMPAT = 1 on each Redis master (producer subscribed)
[ ] XLEN repl:stream:<site> grows after writes
[ ] XINFO GROUPS shows lag=0 and pending=0 for all consumer groups
[ ] curl /lag on all agents returns lag=0 for all peers
[ ] __meta:{key} hash exists on the originating site after a write
[ ] LWW test: all 3 sites converge to the same value after concurrent writes
[ ] Catch-up test: restarted agent replays all missed entries and reaches lag=0

# If running consumer-only scale-out agents:
[ ] Consumer-only agent logs show "role=consumer: producer disabled"
[ ] XINFO CONSUMERS shows 2+ unique consumer names (no duplicates)
[ ] PUBSUB NUMPAT = 1 per master (only one producer active)
[ ] PEL split visible under load (both consumers show non-zero pending)
[ ] All keys present and correct on the target site after split apply
```

---

## Troubleshooting Quick Reference

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Key not replicated | PUBSUB NUMPAT = 0 | Check agent logs; producer may have failed to subscribe |
| Lag stuck > 0 | Consumer group offset stuck | Check `XINFO CONSUMERS`; reset offset if needed (Step 10) |
| All sites diverge on LWW | Clock skew between sites | Check agent logs for HLC values; ensure NTP is running |
| Stream grows forever | `stream_max_len` and `stream_ttl_hours` both 0 | Set `stream_ttl_hours` in agent config (e.g. `72` for 3 days) |
| Agent crashes on startup | Bad config or Redis not reachable | Check `agent.yaml` and Redis connectivity |
| Replication loop | Shadow key TTL too long | Default is `dedup_ttl_seconds: 5`; reduce if needed |
| PUBSUB NUMPAT = 2 on a master | Two producers running on same site | Set `role: consumer` on the extra agent; restart it |
| Two consumers have same name | `consumer_id` collision | Set explicit `consumer_id` in each agent's config |
| Orphaned PEL after consumer removed | Consumer stopped without cleanup | Run `XAUTOCLAIM` + `XGROUP DELCONSUMER` (Step 14) |
| Second consumer not appearing in XINFO | Consumer agent not yet connected | Check agent-2 logs; verify it uses the same `bus` config |
