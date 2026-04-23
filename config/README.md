# config/ — Agent Configuration Reference

Configs are organised by **Redis topology** then **bus type**.
Pick the folder matching your setup, copy and edit the three YAML files.

---

## How to choose

```
What does your Redis look like at each site?
│
├── standalone/   Single Redis instance (no HA, no sharding)
│   ├── separate-bus/   → dedicated shared bus Redis
│   └── embedded-bus/   → no extra Redis; stream on each site's own instance
│
├── sentinel/     Redis Sentinel (master + replicas + sentinels, auto-failover)
│   ├── separate-bus/   → dedicated HA Sentinel bus cluster
│   └── embedded-bus/   → no extra Redis; stream on each site's master
│
└── cluster/      Redis Cluster (sharded, multiple masters)
    ├── separate-bus/   → dedicated shared bus Redis
    └── embedded-bus/   → no extra Redis; stream on master1 of each site
```

---

## Folder index

| Config path | Redis topology | Bus | Extra Redis? | Docker Compose |
|---|---|---|---|---|
| `standalone/separate-bus/` | 1 instance | Standalone Redis | ✅ Yes | `docker/standalone/separate-bus/` |
| `standalone/embedded-bus/` | 1 instance | **None** | ❌ No | `docker/standalone/embedded-bus/` |
| `sentinel/separate-bus/` | Sentinel HA | Sentinel HA Redis | ✅ Yes | `docker/sentinel/separate-bus/` |
| `sentinel/embedded-bus/` | Sentinel HA | **None** | ❌ No | `docker/sentinel/embedded-bus/` |
| `cluster/separate-bus/` | Redis Cluster | Standalone Redis | ✅ Yes | `docker/cluster/separate-bus/` |
| `cluster/embedded-bus/` | Redis Cluster | **None** | ❌ No | `docker/cluster/embedded-bus/` |

---

## Port reference

| Config path | Redis ports | API ports | Metrics ports |
|---|---|---|---|
| `standalone/separate-bus/` | 6381–6383, bus 6390 | 8081–8083 | 9091–9093 |
| `standalone/embedded-bus/` | 6401–6403 | 8191–8193 | 9191–9193 |
| `sentinel/separate-bus/` | masters 6381/6384/6387, bus 6390 | 8081–8083 | 9091–9093 |
| `sentinel/embedded-bus/` | masters 6421/6424/6427 | 8391–8393 | 9391–9393 |
| `cluster/separate-bus/` | 6381–6389, bus 6390 | 8081–8083 | 9091–9093 |
| `cluster/embedded-bus/` | 6411–6419 | 8291–8293 | 9291–9293 |

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
`bus.peer_bus_addrs` (embedded-bus modes only).

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
