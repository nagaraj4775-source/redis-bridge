# RedisBridge — Benchmarking & Test Case Validation

This document covers:
1. What metrics to measure and how to interpret them
2. How to run each benchmark scenario manually
3. The full end-to-end test suite (`demo_cluster_embedded.sh`) and what each step validates

---

## Metrics Reference

| Metric | What it measures | Healthy target |
|--------|-----------------|----------------|
| **Replication latency** | Time from write on Site-A to read on Site-B | < 100ms (LAN) |
| **Throughput** | Keys replicated per second across all peers | > 5,000 keys/s |
| **Catch-up speed** | Keys/sec during lag recovery after downtime | > 3,000 keys/s |
| **LWW convergence time** | Time for all sites to agree on the same value | < 2s |
| **Stream lag** | Unread entries in a peer's stream | 0 at steady state |
| **Pending messages** | Delivered but unACKed entries | 0 at steady state |
| **Failover recovery** | Time to resume replication after a master dies | < 30s |

---

## Benchmark 1 — Replication Latency (Single Key)

Measures how long a single write takes to appear on all peer sites.

```bash
# Write the key and capture the timestamp
START=$(date +%s%3N)
sudo docker exec embedded-bus-redis-a-master1-1 \
  redis-cli -c -h redis-a-master1 SET bench:latency:1 "hello"

# Poll site-b until the key appears
while true; do
  VAL=$(sudo docker exec embedded-bus-redis-b-master1-1 \
    redis-cli -c -h redis-b-master1 GET bench:latency:1)
  if [ "$VAL" = "hello" ]; then
    END=$(date +%s%3N)
    echo "Replication latency: $((END - START)) ms"
    break
  fi
  sleep 0.01
done
```

**Expected:** < 100ms on a local machine, < 30ms typically.

Repeat 10 times and average the results to get a stable p50 latency.

---

## Benchmark 2 — Throughput (Bulk Insert)

Measures how many keys per second RedisBridge can replicate end-to-end.

```bash
# Insert 10,000 keys on site-a using redis-cli pipe mode
TIMESTAMP=$(date +%s%N)
(
  for i in $(seq 1 10000); do
    echo "SET bench:throughput:${TIMESTAMP}:$i val$i"
  done
) | sudo docker exec -i embedded-bus-redis-a-master1-1 \
    redis-cli -c -h redis-a-master1 --pipe

echo "Insert done. Waiting for replication..."

# Poll until the last key appears on site-b
START=$(date +%s%3N)
while true; do
  VAL=$(sudo docker exec embedded-bus-redis-b-master1-1 \
    redis-cli -c -h redis-b-master1 GET "bench:throughput:${TIMESTAMP}:10000")
  if [ "$VAL" = "val10000" ]; then
    END=$(date +%s%3N)
    ELAPSED=$(( (END - START) ))
    echo "10,000 keys replicated in ${ELAPSED}ms"
    echo "Throughput: $(( 10000 * 1000 / ELAPSED )) keys/s"
    break
  fi
  sleep 0.1
done
```

**Tuning levers:**

| Config | Effect |
|--------|--------|
| `apply_concurrency` | Increase to apply more deltas in parallel (default: 8) |
| `batch_size` | Increase for fewer round-trips per XREADGROUP call (default: 100) |
| `max_in_flight` | Increase to allow more unACKed in-flight messages (default: 1000) |

---

## Benchmark 3 — LWW Convergence Time

Measures how quickly conflicting concurrent writes converge to a single value.

```bash
KEY="{bench}:lww:$(date +%s)"

# Write from both sites simultaneously
sudo docker exec embedded-bus-redis-a-master1-1 \
  redis-cli -c -h redis-a-master1 SET $KEY "from-site-a" &
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 SET $KEY "from-site-b" &
wait

START=$(date +%s%3N)

# Poll until all 3 sites agree
while true; do
  VA=$(sudo docker exec embedded-bus-redis-a-master1-1 redis-cli -c -h redis-a-master1 GET $KEY)
  VB=$(sudo docker exec embedded-bus-redis-b-master1-1 redis-cli -c -h redis-b-master1 GET $KEY)
  VC=$(sudo docker exec embedded-bus-redis-c-master1-1 redis-cli -c -h redis-c-master1 GET $KEY)
  if [ "$VA" = "$VB" ] && [ "$VB" = "$VC" ] && [ -n "$VA" ]; then
    END=$(date +%s%3N)
    echo "Converged to '$VA' in $((END - START))ms"
    # Show which site won
    sudo docker exec embedded-bus-redis-a-master1-1 \
      redis-cli -c -h redis-a-master1 HGETALL "__meta:{${KEY#\{}}"
    break
  fi
  sleep 0.05
done
```

**Expected:** Convergence in < 500ms on local Docker. All sites must show the same value.

---

## Benchmark 4 — Catch-up Speed After Downtime

Measures how fast a site recovers lag after coming back online.

```bash
# Stop site-a's agent
sudo docker stop embedded-bus-agent-a-1
echo "Agent-a stopped"

# Insert 5,000 keys on site-b while site-a is down
TIMESTAMP=$(date +%s%N)
(
  for i in $(seq 1 5000); do
    echo "SET bench:catchup:${TIMESTAMP}:$i val$i"
  done
) | sudo docker exec -i embedded-bus-redis-b-master1-1 \
    redis-cli -c -h redis-b-master1 --pipe

echo "5,000 keys inserted while site-a was down"

# Check lag on site-a's stream (should be ~5000 pending)
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO GROUPS repl:stream:ec-site-b

# Restart site-a's agent and time catch-up
START=$(date +%s%3N)
sudo docker start embedded-bus-agent-a-1

# Poll until site-a receives the last key
while true; do
  VAL=$(sudo docker exec embedded-bus-redis-a-master1-1 \
    redis-cli -c -h redis-a-master1 GET "bench:catchup:${TIMESTAMP}:5000")
  if [ "$VAL" = "val5000" ]; then
    END=$(date +%s%3N)
    ELAPSED=$(( END - START ))
    echo "Caught up 5,000 keys in ${ELAPSED}ms"
    echo "Catch-up speed: $(( 5000 * 1000 / ELAPSED )) keys/s"
    break
  fi
  sleep 0.1
done
```

---

## Benchmark 5 — Master Failover Impact

Measures how replication behaves when a Redis master dies and a replica is promoted.

```bash
# Write a key before failover to confirm replication is working
sudo docker exec embedded-bus-redis-a-master1-1 \
  redis-cli -c -h redis-a-master1 SET bench:failover:before "pre-failover"
sleep 2

# Stop master1 on site-a (simulates hardware failure)
echo "Stopping redis-a-master1..."
sudo docker stop embedded-bus-redis-a-master1-1
FAILOVER_START=$(date +%s%3N)

# Wait for cluster to promote a replica (~15-25s with cluster-node-timeout=5s)
echo "Waiting for replica promotion..."
while true; do
  MASTERS=$(sudo docker exec embedded-bus-redis-a-master2-1 \
    redis-cli -c -h redis-a-master2 CLUSTER NODES 2>/dev/null | grep master | grep -v fail | wc -l)
  if [ "$MASTERS" -ge 3 ]; then
    PROMOTED_AT=$(date +%s%3N)
    echo "Replica promoted in $((PROMOTED_AT - FAILOVER_START))ms, $MASTERS masters online"
    break
  fi
  sleep 1
done

# Write a key after failover via master2 (cluster routes to correct shard)
sudo docker exec embedded-bus-redis-a-master2-1 \
  redis-cli -c -h redis-a-master2 SET bench:failover:after "post-failover"

# Confirm replication continues to site-b and site-c
sleep 5
echo -n "Site-B post-failover key: "
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 GET bench:failover:after
```

**Expected:** Replica promoted within 20–30s. New writes replicate normally after promotion.

---

## Benchmark 6 — Scale-out Consumer: 100k Keys Lag Drain

Demonstrates and measures how a second consumer-only agent accelerates lag recovery after a large backlog.

This test has **three phases**:
1. Build a 100k-entry backlog (stop agent, insert 100k keys, restart, measure solo drain speed)
2. Add a second consumer agent mid-drain, observe the acceleration
3. Compare drain time: single consumer vs dual consumer

### Phase 1 — Build the 100k Backlog

```bash
# Stop site-a's agent to let lag accumulate
sudo docker stop embedded-bus-agent-a-1
echo "Agent-a stopped. Inserting 100k keys on site-b..."

TIMESTAMP=$(date +%s%N)

# Insert 100,000 keys in batches of 500 (same rate as demo script)
for batch_start in $(seq 1 500 100000); do
  batch_end=$((batch_start + 499))
  (
    for i in $(seq $batch_start $batch_end); do
      echo "SET {bench}:scaleout:${TIMESTAMP}:$i val$i"
    done
  ) | sudo docker exec -i embedded-bus-redis-b-master1-1 \
      redis-cli -c -h redis-b-master1 --pipe > /dev/null
  sleep 0.05
done

echo "100k keys inserted. Checking stream lag..."

# Verify lag on site-b's stream before restarting site-a
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO GROUPS repl:stream:ec-site-b
```

**Expected output (before restart):** `lag` ≈ 100000, `pending` = 0

### Phase 2 — Restart Single Consumer and Measure Baseline

```bash
# Restart site-a's agent (single consumer)
START_SINGLE=$(date +%s%3N)
sudo docker start embedded-bus-agent-a-1
echo "Agent-a started (single consumer). Monitoring lag every 5 seconds..."

# Poll lag every 5 seconds and log it
for i in $(seq 1 12); do
  sleep 5
  LAG=$(curl -s http://localhost:8291/lag | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ec-site-b',{}).get('lag',0))" 2>/dev/null || echo "N/A")
  ELAPSED=$(( ($(date +%s%3N) - START_SINGLE) / 1000 ))
  echo "  [${ELAPSED}s] lag=ec-site-b: $LAG"
  if [ "$LAG" = "0" ]; then
    echo "✓ Single consumer drained lag in ${ELAPSED}s"
    break
  fi
done
```

Record the drain time — this is your **baseline** (single consumer).

### Phase 3 — Repeat with Dual Consumer and Compare

```bash
# Stop agent-a again and re-insert 100k keys to rebuild the backlog
sudo docker stop embedded-bus-agent-a-1

TIMESTAMP2=$(date +%s%N)
for batch_start in $(seq 1 500 100000); do
  batch_end=$((batch_start + 499))
  (
    for i in $(seq $batch_start $batch_end); do
      echo "SET {bench}:scaleout2:${TIMESTAMP2}:$i val$i"
    done
  ) | sudo docker exec -i embedded-bus-redis-b-master1-1 \
      redis-cli -c -h redis-b-master1 --pipe > /dev/null
  sleep 0.05
done
echo "100k keys inserted again. Starting DUAL consumers..."

# Start two consumer agents simultaneously
START_DUAL=$(date +%s%3N)
sudo docker start embedded-bus-agent-a-1

# Start a second consumer-only agent (uses hostname = unique consumer-id automatically)
sudo docker run -d --name agent-a-consumer-2 \
  --network embedded-bus_default \
  -e REDIBRIDGE_ROLE=consumer \
  -v $(pwd)/config/cluster/embedded-bus/agent-a.yaml:/etc/redibridge/agent.yaml \
  redibridge:latest \
  redibridge --config /etc/redibridge/agent.yaml

echo "Two consumers running. Monitoring lag..."

# Poll lag every 5 seconds
for i in $(seq 1 12); do
  sleep 5
  LAG=$(curl -s http://localhost:8291/lag | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ec-site-b',{}).get('lag',0))" 2>/dev/null || echo "N/A")
  ELAPSED=$(( ($(date +%s%3N) - START_DUAL) / 1000 ))
  echo "  [${ELAPSED}s] lag=ec-site-b: $LAG"
  if [ "$LAG" = "0" ]; then
    echo "✓ Dual consumer drained lag in ${ELAPSED}s"
    break
  fi
done

# Verify both consumers processed messages
echo ""
echo "Consumer group members on site-b's stream:"
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO CONSUMERS repl:stream:ec-site-b repl-consumers-ec-site-a

# Clean up
sudo docker stop agent-a-consumer-2
sudo docker rm agent-a-consumer-2
```

### Verifying Consumer Names During the Test

While both consumers are running, check their PEL and message counts:

```bash
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 XINFO CONSUMERS repl:stream:ec-site-b repl-consumers-ec-site-a
```

**Expected output — two consumers visible with split pending entries:**
```
1)  1) "name"
    2) "agent-a-hostname-1"       ← consumer 1 (from main agent's hostname)
    3) "pending"
    4) 47832                      ← messages assigned to consumer 1
    5) "idle"
    6) 123                        ← ms since last active

2)  1) "name"
    2) "agent-a-hostname-2"       ← consumer 2 (second container's hostname)
    3) "pending"
    4) 52168                      ← messages assigned to consumer 2
    5) "idle"
    6) 89
```

Total pending across both = 100,000. Split roughly 50/50.

### Expected Results

| Setup | 100k key drain time | Keys/s |
|-------|---------------------|--------|
| Single consumer (`apply_concurrency=8`) | ~30–40s | ~2,500–3,300 |
| Dual consumer (`apply_concurrency=8` each) | ~15–20s | ~5,000–6,600 |

> **Note:** Drain time scales approximately linearly with number of consumers. The bottleneck shifts to Redis write speed after ~4 consumer agents.

---

## Test Case Validation — End-to-End Demo Script

The script at `scripts/demo_cluster_embedded.sh` runs all test cases automatically.

```bash
bash scripts/demo_cluster_embedded.sh
```

Expected runtime: ~10–15 minutes (includes 100k bulk insert steps).  
Exit code 0 = all tests passed.

### What Each Step Validates

| Step | Scenario | Pass Criteria |
|------|----------|--------------|
| **1** | Basic single-key replication (A→B, A→C) | Key appears on all 3 sites within 5s |
| **2** | Bi-directional: write on Site-B, read on A and C | Key appears on all 3 sites within 5s |
| **3** | Multi-key: write 10 keys on Site-C | All 10 keys appear on A and B within 10s |
| **4** | LWW conflict: write same key on A and B simultaneously | All 3 sites converge to the **same value** within 10s |
| **5** | Bulk insert + catch-up: stop A, insert 100k on B, restart A | A recovers all 100k keys, lag reaches 0 |
| **6** | Lag drain: A catches up from B's 100k stream | Lag API returns 0 for peer ec-site-b |
| **7** | Bi-directional bulk: 100k keys written on A, replicated to B and C | Spot-check 5 random keys on both B and C |
| **8** | Lag drain after step-7 bulk | All agents report lag=0 across all peers |
| **9** | Full offline: stop A and B, insert 1000 keys on C | Keys exist only on C; A and B return connection error |
| **10** | Full recovery: restart A and B | Both recover all 1000 keys from C's stream; lag=0 |
| **11** | Master failover: stop master1 on site-A, write 3 keys, verify replication | Keys written post-failover replicate to B and C |

---

### Running Individual Steps in Isolation

To test just one scenario without running the full script:

**Test LWW only:**
```bash
KEY="{demo}:manual:lww:$(date +%s)"
sudo docker exec embedded-bus-redis-a-master1-1 redis-cli -c -h redis-a-master1 SET $KEY "from-a" &
sudo docker exec embedded-bus-redis-b-master1-1 redis-cli -c -h redis-b-master1 SET $KEY "from-b" &
wait
sleep 5
echo "A:$(sudo docker exec embedded-bus-redis-a-master1-1 redis-cli -c -h redis-a-master1 GET $KEY)"
echo "B:$(sudo docker exec embedded-bus-redis-b-master1-1 redis-cli -c -h redis-b-master1 GET $KEY)"
echo "C:$(sudo docker exec embedded-bus-redis-c-master1-1 redis-cli -c -h redis-c-master1 GET $KEY)"
```

**Test offline catch-up only:**
```bash
sudo docker stop embedded-bus-agent-a-1
sudo docker exec embedded-bus-redis-b-master1-1 \
  redis-cli -c -h redis-b-master1 SET manual:catchup:sentinel "sentinel-value"
sudo docker start embedded-bus-agent-a-1
sleep 10
sudo docker exec embedded-bus-redis-a-master1-1 \
  redis-cli -c -h redis-a-master1 GET manual:catchup:sentinel
# Expected: sentinel-value
```

---

## Interpreting Results

### Throughput

| Keys/s | Assessment |
|--------|-----------|
| > 5,000 | Excellent — adequate for most production workloads |
| 2,000–5,000 | Good — increase `apply_concurrency` to improve |
| < 2,000 | Investigate — check agent CPU, Redis latency, network |

### Replication Latency

| Latency | Assessment |
|---------|-----------|
| < 50ms | Excellent |
| 50–200ms | Normal — includes keyspace notification round-trip |
| 200ms–1s | Acceptable if writes are infrequent |
| > 1s | Investigate — agent may be overloaded or network congested |

### Catch-up Speed

Catch-up is faster than live replication because:
- No keyspace notification delay (reading directly from stream)
- Consumer batch reads (`batch_size` entries per XREADGROUP call)

Typical catch-up: **3,000–8,000 keys/s** depending on `apply_concurrency` and `batch_size`.

### LWW Convergence

Convergence requires **two replication round-trips**:
```
Write on A → Stream-A → Consumer on B applies → write fires event on B
                      → Consumer on C applies → write fires event on C
```

Expected convergence time = 2 × replication latency ≈ **100–400ms** on local Docker.

---

## Tuning for Higher Throughput

```yaml
# agent.yaml — replication section
replication:
  batch_size: 500          # default 100 — more entries per XREADGROUP call
  apply_concurrency: 16    # default 8  — more parallel apply goroutines
  max_in_flight: 5000      # default 1000 — more unACKed messages allowed
```

> **Note:** Increasing `apply_concurrency` beyond the number of CPU cores gives diminishing returns.  
> Increasing `max_in_flight` too high can cause memory pressure on the consumer.

---

## Known Throughput Limits

| Bottleneck | Limit | Fix |
|------------|-------|-----|
| Keyspace notification fan-out | ~10,000 events/s per master | Use concurrent producer goroutines (semaphore=50 built-in) |
| XREADGROUP round-trip | 1 call per batch | Increase `batch_size` |
| Apply goroutines | `apply_concurrency` × Redis write speed | Increase `apply_concurrency` |
| Shadow key TTL | Must expire before next write on same key | TTL=200ms; avoid writing same key > 5x/s for best results |
| Stream growth | Unbounded without retention policy | Set `stream_ttl_hours` or `stream_max_len` |

---

## Benchmark Checklist

```
[ ] Replication latency < 100ms for single key
[ ] Throughput > 5,000 keys/s for bulk insert
[ ] LWW convergence < 500ms (all sites same value)
[ ] Catch-up speed > 3,000 keys/s after agent restart
[ ] Lag returns to 0 within 60s after 100k bulk insert
[ ] Failover: replica promoted within 30s, replication resumes
[ ] Full demo script exits with code 0 (all 11 steps pass)
[ ] Scale-out consumer: dual consumer drains 100k in < 20s
[ ] XINFO CONSUMERS shows 2 unique consumer names, PEL split ~50/50
[ ] Drain time with 2 consumers is ≈ 50% of single-consumer baseline
```
