package tests

// End-to-end tests for RedisBridge in EMBEDDED-CLUSTER bus mode.
//
// Topology: 3 sites × 3 masters each (9 Redis containers total), NO dedicated
// bus container. Each site's master1 acts as the replication stream host.
//
// This validates embedded mode in a real 3-master-per-site layout — writes can
// arrive on any of the 3 masters and must still be replicated to all peers.
//
// Prerequisites:
//   make up-embedded-cluster
// 	 Ensure Go is installed in the system 
//
// Run manually:
//   go test -v -timeout 120s -run "^TestEmbeddedCluster" ./tests/

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ── port layout (embedded-cluster docker-compose) ─────────────────────────────

const (
	// Site A — masters on 6411/6412/6413
	ecAddrA1 = "localhost:6411"
	ecAddrA2 = "localhost:6412"
	ecAddrA3 = "localhost:6413"

	// Site B — masters on 6414/6415/6416
	ecAddrB1 = "localhost:6414"
	ecAddrB2 = "localhost:6415"
	ecAddrB3 = "localhost:6416"

	// Site C — masters on 6417/6418/6419
	ecAddrC1 = "localhost:6417"
	ecAddrC2 = "localhost:6418"
	ecAddrC3 = "localhost:6419"

	ecCoordA   = "http://localhost:8291"
	ecCoordB   = "http://localhost:8292"
	ecCoordC   = "http://localhost:8293"
	ecMetricsA = "http://localhost:9291"
	ecMetricsB = "http://localhost:9292"
	ecMetricsC = "http://localhost:9293"

	ecSiteA = "ec-site-a"
	ecSiteB = "ec-site-b"
	ecSiteC = "ec-site-c"

	ecLatencyBudget = 50 * time.Millisecond
	ecReplTimeout   = 5 * time.Second
)

// ── helpers ───────────────────────────────────────────────────────────────────

// ecAllMasters returns direct per-node clients (3 per site).
// Used for stream inspection (XLen) and waitOnAllMasters.
// NOTE: in Redis Cluster mode these clients get MOVED errors for wrong-slot keys;
// use ecClusterClients for all write/read operations.
func ecAllMasters(t *testing.T) (siteA, siteB, siteC []*redis.Client) {
	t.Helper()
	mk := func(addr string) *redis.Client {
		c := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { c.Close() })
		return c
	}
	siteA = []*redis.Client{mk(ecAddrA1), mk(ecAddrA2), mk(ecAddrA3)}
	siteB = []*redis.Client{mk(ecAddrB1), mk(ecAddrB2), mk(ecAddrB3)}
	siteC = []*redis.Client{mk(ecAddrC1), mk(ecAddrC2), mk(ecAddrC3)}
	return
}

// ecClusterClients returns a ClusterClient per site.
// These handle MOVED redirects automatically and should be used for all
// write/read operations in cluster-protocol mode.
func ecClusterClients(t *testing.T) (clA, clB, clC *redis.ClusterClient) {
	t.Helper()
	mkc := func(addrs ...string) *redis.ClusterClient {
		c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
		t.Cleanup(func() { c.Close() })
		return c
	}
	clA = mkc(ecAddrA1, ecAddrA2, ecAddrA3)
	clB = mkc(ecAddrB1, ecAddrB2, ecAddrB3)
	clC = mkc(ecAddrC1, ecAddrC2, ecAddrC3)
	return
}

func ecSkipIfNotRunning(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cl := redis.NewClient(&redis.Options{Addr: ecAddrA1})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("embedded-cluster not running (make up-embedded-cluster): %v", err)
	}
	for _, coord := range []string{ecCoordA, ecCoordB, ecCoordC} {
		resp, err := http.Get(coord + "/health")
		if err != nil || resp.StatusCode != 200 {
			t.Skipf("agent not healthy at %s — run make up-embedded-cluster first", coord)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
}

func ecUniqueKey(prefix string) string {
	return fmt.Sprintf("ec:%s:%d", prefix, time.Now().UnixNano())
}

// waitOnAllMasters polls all three masters of a site until value appears on at
// least one and returns the first latency observed.
func waitOnAllMasters(ctx context.Context, masters []*redis.Client, key, want string, timeout time.Duration) (time.Duration, error) {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	for time.Now().Before(deadline) {
		for _, m := range masters {
			v, err := m.Get(ctx, key).Result()
			if err == nil && v == want {
				return time.Since(start), nil
			}
		}
		time.Sleep(e2ePollInterval)
	}
	// Collect actual values for diagnosis
	var got []string
	for _, m := range masters {
		v, _ := m.Get(ctx, key).Result()
		got = append(got, fmt.Sprintf("%q", v))
	}
	return 0, fmt.Errorf("timeout after %v: want %q, got %v", timeout, want, got)
}

// ── Test 1: Stream lives on master1 of each site ──────────────────────────────

func TestEmbeddedClusterStreamOnMaster1(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	// Trigger events on each site via ClusterClient (auto-routes to correct shard).
	clA.Set(ctx, ecUniqueKey("init-a"), "v", 0)
	clB.Set(ctx, ecUniqueKey("init-b"), "v", 0)
	clC.Set(ctx, ecUniqueKey("init-c"), "v", 0)
	time.Sleep(300 * time.Millisecond)

	// In cluster mode the stream key hashes to one specific shard —
	// verify it exists on at least one master per site.
	for _, tc := range []struct {
		name    string
		siteID  string
		masters []*redis.Client
	}{
		{"site-a", ecSiteA, siteA},
		{"site-b", ecSiteB, siteB},
		{"site-c", ecSiteC, siteC},
	} {
		stream := "repl:stream:" + tc.siteID
		var total int64
		for _, m := range tc.masters {
			n, err := m.XLen(ctx, stream).Result()
			if err == nil {
				total += n
			}
		}
		if total == 0 {
			t.Errorf("stream %s not found on any master of %s", stream, tc.name)
		} else {
			t.Logf("stream %s on %s master1: %d entries \u2713", stream, tc.name, total)
		}
	}
}

// ── Test 2: Write on any master replicates to all peer masters ────────────────

func TestEmbeddedClusterReplicationFromAnyMaster(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	// Write multiple keys via ClusterClient — keys hash to different shards,
	// verifying that the producer captures events from every shard.
	keys := []struct{ key, val string }{
		{ecUniqueKey("any-m-1"), "from-a-shard1"},
		{ecUniqueKey("any-m-2"), "from-a-shard2"},
		{ecUniqueKey("any-m-3"), "from-a-shard3"},
	}
	for _, kv := range keys {
		clA.Set(ctx, kv.key, kv.val, 0)
	}

	for _, kv := range keys {
		lat, err := waitOnAllMasters(ctx, siteB, kv.key, kv.val, ecReplTimeout)
		if err != nil {
			t.Errorf("key %s from A\u2192B: %v", kv.key, err)
		} else {
			t.Logf("A(m%s)\u2192B latency: %v \u2713", kv.key[len(kv.key)-1:], lat)
		}
		lat, err = waitOnAllMasters(ctx, siteC, kv.key, kv.val, ecReplTimeout)
		if err != nil {
			t.Errorf("key %s from A\u2192C: %v", kv.key, err)
		} else {
			t.Logf("A(m%s)\u2192C latency: %v \u2713", kv.key[len(kv.key)-1:], lat)
		}
	}

	for _, kv := range keys {
		clA.Del(ctx, kv.key)
		clB.Del(ctx, kv.key)
		clC.Del(ctx, kv.key)
	}
}

// ── Test 3: String replication A → B, C ──────────────────────────────────────

func TestEmbeddedClusterStringReplication(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	key := ecUniqueKey("string")
	val := "hello-ec"

	clA.Set(ctx, key, val, 0)

	latB, err := waitOnAllMasters(ctx, siteB, key, val, ecReplTimeout)
	if err != nil {
		t.Fatalf("A\u2192B: %v", err)
	}
	latC, err := waitOnAllMasters(ctx, siteC, key, val, ecReplTimeout)
	if err != nil {
		t.Fatalf("A\u2192C: %v", err)
	}

	t.Logf("latency A\u2192B: %v  A\u2192C: %v", latB, latC)
	if latB > ecLatencyBudget {
		t.Errorf("A\u2192B %v exceeds %v budget", latB, ecLatencyBudget)
	}
	if latC > ecLatencyBudget {
		t.Errorf("A\u2192C %v exceeds %v budget", latC, ecLatencyBudget)
	}

	clA.Del(ctx, key)
	clB.Del(ctx, key)
	clC.Del(ctx, key)
}

// ── Test 4: Hash replication ──────────────────────────────────────────────────

func TestEmbeddedClusterHashReplication(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	clA, clB, clC := ecClusterClients(t)

	key := ecUniqueKey("hash")
	clA.HSet(ctx, key, "user", "alice", "role", "admin", "region", "APAC")

	for _, tc := range []struct {
		name string
		cl   *redis.ClusterClient
	}{{"B", clB}, {"C", clC}} {
		if err := waitForHash(ctx, tc.cl, key, "user", "alice", ecReplTimeout); err != nil {
			t.Errorf("hash to %s: %v", tc.name, err)
			continue
		}
		got, _ := tc.cl.HGetAll(ctx, key).Result()
		if got["role"] != "admin" || got["region"] != "APAC" {
			t.Errorf("site %s incomplete hash: %v", tc.name, got)
		} else {
			t.Logf("site %s hash ok %v \u2713", tc.name, got)
		}
	}

	clA.Del(ctx, key)
	clB.Del(ctx, key)
	clC.Del(ctx, key)
}

// ── Test 5: TTL preservation ──────────────────────────────────────────────────

func TestEmbeddedClusterTTLPreservation(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	key := ecUniqueKey("ttl")
	const ttl = 30 * time.Second
	clA.Set(ctx, key, "expiring", ttl)

	for _, tc := range []struct {
		name string
		ms   []*redis.Client
		cl   *redis.ClusterClient
	}{{"B", siteB, clB}, {"C", siteC, clC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "expiring", ecReplTimeout); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		rem, err := tc.cl.TTL(ctx, key).Result()
		if err != nil || rem <= 0 || rem > ttl {
			t.Errorf("site %s: TTL %v (orig %v) err=%v", tc.name, rem, ttl, err)
		} else {
			t.Logf("site %s: TTL %v \u2713", tc.name, rem)
		}
	}

	clA.Del(ctx, key)
	clB.Del(ctx, key)
	clC.Del(ctx, key)
}

// ── Test 6: Bidirectional replication ─────────────────────────────────────────

func TestEmbeddedClusterBidirectional(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	keyB := ecUniqueKey("bidir-B")
	clB.Set(ctx, keyB, "from-B", 0)
	if _, err := waitOnAllMasters(ctx, siteA, keyB, "from-B", ecReplTimeout); err != nil {
		t.Errorf("B\u2192A: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteC, keyB, "from-B", ecReplTimeout); err != nil {
		t.Errorf("B\u2192C: %v", err)
	}
	t.Log("B\u2192A,C \u2713")

	keyC := ecUniqueKey("bidir-C")
	clC.Set(ctx, keyC, "from-C", 0)
	if _, err := waitOnAllMasters(ctx, siteA, keyC, "from-C", ecReplTimeout); err != nil {
		t.Errorf("C\u2192A: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteB, keyC, "from-C", ecReplTimeout); err != nil {
		t.Errorf("C\u2192B: %v", err)
	}
	t.Log("C\u2192A,B \u2713")

	for _, k := range []string{keyB, keyC} {
		clA.Del(ctx, k)
		clB.Del(ctx, k)
		clC.Del(ctx, k)
	}
}

// ── Test 7: DELETE propagation ────────────────────────────────────────────────

func TestEmbeddedClusterDeletePropagation(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)

	key := ecUniqueKey("del")
	clA.Set(ctx, key, "to-delete", 0)
	if _, err := waitOnAllMasters(ctx, siteB, key, "to-delete", ecReplTimeout); err != nil {
		t.Fatalf("initial repl B: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteC, key, "to-delete", ecReplTimeout); err != nil {
		t.Fatalf("initial repl C: %v", err)
	}

	clA.Del(ctx, key)

	if err := waitForDeleted(ctx, clB, key, ecReplTimeout); err != nil {
		t.Errorf("DEL\u2192B: %v", err)
	} else {
		t.Log("DEL propagated to B \u2713")
	}
	if err := waitForDeleted(ctx, clC, key, ecReplTimeout); err != nil {
		t.Errorf("DEL\u2192C: %v", err)
	} else {
		t.Log("DEL propagated to C \u2713")
	}
}

// ── Test 8: LWW convergence across 3-master sites ────────────────────────────

func TestEmbeddedClusterLWWConvergence(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	clA, clB, clC := ecClusterClients(t)

	key := ecUniqueKey("lww")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); clA.Set(ctx, key, "from-A", 0) }()
	go func() { defer wg.Done(); clB.Set(ctx, key, "from-B", 0) }()
	wg.Wait()

	time.Sleep(3 * time.Second)

	valA, _ := clA.Get(ctx, key).Result()
	valB, _ := clB.Get(ctx, key).Result()
	valC, _ := clC.Get(ctx, key).Result()

	t.Logf("after convergence: A=%q  B=%q  C=%q", valA, valB, valC)

	if valA != valB || valB != valC {
		t.Errorf("LWW did not converge: A=%q B=%q C=%q", valA, valB, valC)
	} else {
		t.Logf("LWW converged to %q \u2713", valA)
	}

	metaA, _ := clA.HGetAll(ctx, "__meta:"+key).Result()
	metaB, _ := clB.HGetAll(ctx, "__meta:"+key).Result()
	metaC, _ := clC.HGetAll(ctx, "__meta:"+key).Result()
	t.Logf("meta: A=%v  B=%v  C=%v", metaA, metaB, metaC)
	if metaA["site"] != metaB["site"] || metaB["site"] != metaC["site"] {
		t.Errorf("LWW meta not consistent: A=%v B=%v C=%v", metaA, metaB, metaC)
	}

	clA.Del(ctx, key, "__meta:"+key)
	clB.Del(ctx, key, "__meta:"+key)
	clC.Del(ctx, key, "__meta:"+key)
}

// ── Test 9: Replication latency ───────────────────────────────────────────────

func TestEmbeddedClusterReplicationLatency(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, _ := ecAllMasters(t)
	clA, clB, _ := ecClusterClients(t)

	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	keys := make([]string, 0, samples)

	for i := 0; i < samples; i++ {
		key := ecUniqueKey(fmt.Sprintf("lat-%02d", i))
		val := fmt.Sprintf("v%d", i)
		keys = append(keys, key)

		clA.Set(ctx, key, val, 0)
		lat, err := waitOnAllMasters(ctx, siteB, key, val, ecReplTimeout)
		if err != nil {
			t.Errorf("sample %d: %v", i, err)
			continue
		}
		latencies = append(latencies, lat)
		time.Sleep(5 * time.Millisecond)
	}

	if len(latencies) == 0 {
		t.Fatal("no latency samples collected")
	}

	for i := 1; i < len(latencies); i++ {
		for j := i; j > 0 && latencies[j] < latencies[j-1]; j-- {
			latencies[j], latencies[j-1] = latencies[j-1], latencies[j]
		}
	}
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	t.Logf("Latency over %d samples \u2014 p50:%v p95:%v max:%v", len(latencies), p50, p95, latencies[len(latencies)-1])

	if p50 > ecLatencyBudget {
		t.Errorf("p50 %v exceeds %v budget", p50, ecLatencyBudget)
	}
	if p95 > ecLatencyBudget*2 {
		t.Errorf("p95 %v exceeds 2\u00d7 budget (%v)", p95, ecLatencyBudget*2)
	}

	for _, k := range keys {
		clA.Del(ctx, k)
		clB.Del(ctx, k)
	}
}

// ── Test 10: Metrics increment ────────────────────────────────────────────────

func TestEmbeddedClusterMetricsIncrement(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, _ := ecAllMasters(t)
	clA, clB, _ := ecClusterClients(t)

	before := fetchMetrics(t, ecMetricsB)
	appliedBefore := metricValue(before, `repl_writes_applied_total`)
	capturedBefore := metricValue(fetchMetrics(t, ecMetricsA), `repl_events_captured_total`)

	keys := make([]string, 5)
	for i := range keys {
		keys[i] = ecUniqueKey(fmt.Sprintf("metrics-%02d", i))
		clA.Set(ctx, keys[i], "v", 0)
	}
	for _, k := range keys {
		if _, err := waitOnAllMasters(ctx, siteB, k, "v", ecReplTimeout); err != nil {
			t.Errorf("%s not replicated: %v", k, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	after := fetchMetrics(t, ecMetricsB)
	appliedAfter := metricValue(after, `repl_writes_applied_total`)
	capturedAfter := metricValue(fetchMetrics(t, ecMetricsA), `repl_events_captured_total`)

	deltaApplied := appliedAfter - appliedBefore
	deltaCaptured := capturedAfter - capturedBefore

	t.Logf("writes_applied on B  +%.0f", deltaApplied)
	t.Logf("events_captured on A +%.0f", deltaCaptured)

	if deltaApplied < 5 {
		t.Errorf("expected \u22655 writes_applied on B, got +%.0f", deltaApplied)
	}
	if deltaCaptured < 5 {
		t.Errorf("expected \u22655 events_captured on A, got +%.0f", deltaCaptured)
	}

	for _, k := range keys {
		clA.Del(ctx, k)
		clB.Del(ctx, k)
	}
}

// ── Test 11: Management API health check ─────────────────────────────────────

func TestEmbeddedClusterManagementAPI(t *testing.T) {
	ecSkipIfNotRunning(t)

	for _, tc := range []struct {
		name  string
		coord string
	}{
		{"agent-a", ecCoordA},
		{"agent-b", ecCoordB},
		{"agent-c", ecCoordC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(tc.coord + "/health")
			if err != nil {
				t.Fatalf("GET /health: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("%s /health: want 200 got %d", tc.name, resp.StatusCode)
			} else {
				t.Logf("%s /health ok \u2713", tc.name)
			}
		})
	}
}

// ── Test 12: CRUD round-trip ──────────────────────────────────────────────────

func TestEmbeddedClusterCRUDRoundTrip(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	_, siteB, siteC := ecAllMasters(t)
	clA, clB, clC := ecClusterClients(t)
	key := ecUniqueKey("crud")

	clA.Set(ctx, key, "v1", 0)
	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "v1", ecReplTimeout); err != nil {
			t.Fatalf("CREATE\u2192%s: %v", tc.name, err)
		}
	}
	t.Log("CREATE \u2713")

	clA.Set(ctx, key, "v2", 0)
	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "v2", ecReplTimeout); err != nil {
			t.Fatalf("UPDATE\u2192%s: %v", tc.name, err)
		}
	}
	t.Log("UPDATE \u2713")

	clA.Del(ctx, key)
	for _, tc := range []struct {
		name string
		cl   *redis.ClusterClient
	}{{"B", clB}, {"C", clC}} {
		if err := waitForDeleted(ctx, tc.cl, key, ecReplTimeout); err != nil {
			t.Fatalf("DELETE\u2192%s: %v", tc.name, err)
		}
	}
	t.Log("DELETE \u2713")
}
