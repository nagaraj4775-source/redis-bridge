# config/ — Agent Configuration Reference

Each subfolder is a **complete, ready-to-use** set of agent configs for one deployment topology.
Pick the folder that matches your Redis setup, then copy and edit the three YAML files.

---

## How to choose

```
What does your Redis look like at each site?
│
├── Single Redis instance (no HA, no sharding)
│   │
│   ├── I have / can deploy a dedicated bus Redis  →  standalone/
│   └── I want NO extra Redis containers           →  embedded/
│
├── Redis Sentinel (master + replicas + sentinels, automatic failover)
│   └── (always uses a Sentinel bus)               →  sentinel/
│
└── Redis Cluster (multiple masters, data sharded)
    │
    ├── I have / can deploy a dedicated bus Redis  →  cluster/
    └── I want NO extra Redis containers           →  embedded-cluster/
```

---

## Folder index

| Folder | Redis topology per site | Bus | Extra Redis needed? | Docker Compose |
|---|---|---|---|---|
| `standalone/` | 1 single instance | Dedicated standalone Redis | ✅ Yes — 1 shared bus | `docker/standalone/` |
| `sentinel/` | 1 master + replicas + sentinels | Dedicated Sentinel Redis | ✅ Yes — HA bus cluster | `docker/sentinel/` |
| `cluster/` | 3 masters (sharded cluster) | Dedicated standalone Redis | ✅ Yes — 1 shared bus | `docker/cluster/` |
| `embedded/` | 1 single instance | **None — stream on site's own Redis** | ❌ No extra Redis | `docker/embedded/` |
| `embedded-cluster/` | 3 masters (sharded cluster) | **None — stream on master1** | ❌ No extra Redis | `docker/embedded-cluster/` |

---

## Port reference

| Folder | Redis ports | Coordinator API | Metrics |
|---|---|---|---|
| `standalone/` | 6381–6383 (+ bus 6390) | 8081 / 8082 / 8083 | 9091 / 9092 / 9093 |
| `sentinel/` | masters 6381–6383, bus 6390 | 8081 / 8082 / 8083 | 9091 / 9092 / 9093 |
| `cluster/` | 6381–6389 (+ bus 6390) | 8081 / 8082 / 8083 | 9091 / 9092 / 9093 |
| `embedded/` | 6401 / 6402 / 6403 | 8191 / 8192 / 8193 | 9191 / 9192 / 9193 |
| `embedded-cluster/` | 6411–6419 | 8291 / 8292 / 8293 | 9291 / 9292 / 9293 |

---

## Files in each folder

Every folder contains three files — one per site:

```
agent-a.yaml   ← Site A config  (copy and adapt for your first site)
agent-b.yaml   ← Site B config
agent-c.yaml   ← Site C config
```

To add a 4th site: copy `agent-c.yaml`, change `site_id`, add the new site to every
other agent's `peers:` list, and add its bus address to every other agent's
`bus.peer_bus_addrs` (embedded modes only).

---

## Connecting to existing Redis

If your Redis is already running, you only need to:

1. Set keyspace notifications on **every master**:
   ```bash
   redis-cli -h <host> CONFIG SET notify-keyspace-events KEA
   ```
2. Copy the matching config template, replace hostnames/ports/passwords.
3. Run the agent binary or Docker container pointed at your config file.

See the [Getting Started with Existing Redis Clusters](../README.md#getting-started-with-existing-redis-clusters)
section in the main README for full step-by-step instructions.
