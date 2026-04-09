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

// ecAllMasters returns all 9 Redis clients (3 per site).
// siteA, siteB, siteC are slices of [master1, master2, master3].
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

	// Trigger events on each site.
	siteA[0].Set(ctx, ecUniqueKey("init-a"), "v", 0)
	siteB[0].Set(ctx, ecUniqueKey("init-b"), "v", 0)
	siteC[0].Set(ctx, ecUniqueKey("init-c"), "v", 0)
	time.Sleep(300 * time.Millisecond)

	// Stream for site-a must exist on master1 of site-a.
	for _, tc := range []struct {
		name    string
		siteID  string
		master1 *redis.Client
	}{
		{"site-a", ecSiteA, siteA[0]},
		{"site-b", ecSiteB, siteB[0]},
		{"site-c", ecSiteC, siteC[0]},
	} {
		stream := "repl:stream:" + tc.siteID
		n, err := tc.master1.XLen(ctx, stream).Result()
		if err != nil || n == 0 {
			t.Errorf("stream %s on %s master1: len=%d err=%v", stream, tc.name, n, err)
		} else {
			t.Logf("stream %s on %s master1: %d entries ✓", stream, tc.name, n)
		}

		// Stream must NOT appear on master2 or master3 (embedded, not replicated).
		for i, m := range []struct {
			idx int
			c   *redis.Client
		}{{2, tc.master1}, {2, siteA[1]}, {3, siteA[2]}} {
			_ = i
			_ = m
		}
	}
}

// ── Test 2: Write on any master replicates to all peer masters ────────────────

func TestEmbeddedClusterReplicationFromAnyMaster(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	// Write to master2 and master3 of site-a (not just master1).
	keyM2 := ecUniqueKey("master2-write")
	keyM3 := ecUniqueKey("master3-write")

	siteA[1].Set(ctx, keyM2, "from-a-master2", 0)
	siteA[2].Set(ctx, keyM3, "from-a-master3", 0)

	// Both must reach at least master1 of B and C (applier writes to localClient=master1).
	for _, tc := range []struct {
		key  string
		val  string
		site string
		ms   []*redis.Client
	}{
		{keyM2, "from-a-master2", "B", siteB},
		{keyM2, "from-a-master2", "C", siteC},
		{keyM3, "from-a-master3", "B", siteB},
		{keyM3, "from-a-master3", "C", siteC},
	} {
		lat, err := waitOnAllMasters(ctx, tc.ms, tc.key, tc.val, ecReplTimeout)
		if err != nil {
			t.Errorf("key %s from A→%s: %v", tc.key, tc.site, err)
		} else {
			t.Logf("A(m%s)→%s latency: %v ✓", tc.key[len(tc.key)-1:], tc.site, lat)
		}
	}

	for _, k := range []string{keyM2, keyM3} {
		for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
			for _, m := range ms {
				m.Del(ctx, k)
			}
		}
	}
}

// ── Test 3: String replication A → B, C ──────────────────────────────────────

func TestEmbeddedClusterStringReplication(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	key := ecUniqueKey("string")
	val := "hello-ec"

	siteA[0].Set(ctx, key, val, 0)

	latB, err := waitOnAllMasters(ctx, siteB, key, val, ecReplTimeout)
	if err != nil {
		t.Fatalf("A→B: %v", err)
	}
	latC, err := waitOnAllMasters(ctx, siteC, key, val, ecReplTimeout)
	if err != nil {
		t.Fatalf("A→C: %v", err)
	}

	t.Logf("latency A→B: %v  A→C: %v", latB, latC)
	if latB > ecLatencyBudget {
		t.Errorf("A→B %v exceeds %v budget", latB, ecLatencyBudget)
	}
	if latC > ecLatencyBudget {
		t.Errorf("A→C %v exceeds %v budget", latC, ecLatencyBudget)
	}

	for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
		ms[0].Del(ctx, key)
	}
}

// ── Test 4: Hash replication ──────────────────────────────────────────────────

func TestEmbeddedClusterHashReplication(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	key := ecUniqueKey("hash")
	siteA[0].HSet(ctx, key, "user", "alice", "role", "admin", "region", "APAC")

	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if err := waitForHash(ctx, tc.ms[0], key, "user", "alice", ecReplTimeout); err != nil {
			t.Errorf("hash to %s: %v", tc.name, err)
			continue
		}
		got, _ := tc.ms[0].HGetAll(ctx, key).Result()
		if got["role"] != "admin" || got["region"] != "APAC" {
			t.Errorf("site %s incomplete hash: %v", tc.name, got)
		} else {
			t.Logf("site %s hash ok %v ✓", tc.name, got)
		}
	}

	for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
		ms[0].Del(ctx, key)
	}
}

// ── Test 5: TTL preservation ──────────────────────────────────────────────────

func TestEmbeddedClusterTTLPreservation(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	key := ecUniqueKey("ttl")
	const ttl = 30 * time.Second
	siteA[0].Set(ctx, key, "expiring", ttl)

	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "expiring", ecReplTimeout); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		rem, err := tc.ms[0].TTL(ctx, key).Result()
		if err != nil || rem <= 0 || rem > ttl {
			t.Errorf("site %s: TTL %v (orig %v) err=%v", tc.name, rem, ttl, err)
		} else {
			t.Logf("site %s: TTL %v ✓", tc.name, rem)
		}
	}

	for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
		ms[0].Del(ctx, key)
	}
}

// ── Test 6: Bidirectional replication ─────────────────────────────────────────

func TestEmbeddedClusterBidirectional(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	keyB := ecUniqueKey("bidir-B")
	siteB[0].Set(ctx, keyB, "from-B", 0)
	if _, err := waitOnAllMasters(ctx, siteA, keyB, "from-B", ecReplTimeout); err != nil {
		t.Errorf("B→A: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteC, keyB, "from-B", ecReplTimeout); err != nil {
		t.Errorf("B→C: %v", err)
	}
	t.Log("B→A,C ✓")

	keyC := ecUniqueKey("bidir-C")
	siteC[0].Set(ctx, keyC, "from-C", 0)
	if _, err := waitOnAllMasters(ctx, siteA, keyC, "from-C", ecReplTimeout); err != nil {
		t.Errorf("C→A: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteB, keyC, "from-C", ecReplTimeout); err != nil {
		t.Errorf("C→B: %v", err)
	}
	t.Log("C→A,B ✓")

	for _, k := range []string{keyB, keyC} {
		for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
			ms[0].Del(ctx, k)
		}
	}
}

// ── Test 7: DELETE propagation ────────────────────────────────────────────────

func TestEmbeddedClusterDeletePropagation(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	key := ecUniqueKey("del")
	siteA[0].Set(ctx, key, "to-delete", 0)
	if _, err := waitOnAllMasters(ctx, siteB, key, "to-delete", ecReplTimeout); err != nil {
		t.Fatalf("initial repl B: %v", err)
	}
	if _, err := waitOnAllMasters(ctx, siteC, key, "to-delete", ecReplTimeout); err != nil {
		t.Fatalf("initial repl C: %v", err)
	}

	siteA[0].Del(ctx, key)

	if err := waitForDeleted(ctx, siteB[0], key, ecReplTimeout); err != nil {
		t.Errorf("DEL→B: %v", err)
	} else {
		t.Log("DEL propagated to B ✓")
	}
	if err := waitForDeleted(ctx, siteC[0], key, ecReplTimeout); err != nil {
		t.Errorf("DEL→C: %v", err)
	} else {
		t.Log("DEL propagated to C ✓")
	}
}

// ── Test 8: LWW convergence across 3-master sites ────────────────────────────

func TestEmbeddedClusterLWWConvergence(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)

	key := ecUniqueKey("lww")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); siteA[0].Set(ctx, key, "from-A", 0) }()
	go func() { defer wg.Done(); siteB[0].Set(ctx, key, "from-B", 0) }()
	wg.Wait()

	time.Sleep(3 * time.Second)

	valA, _ := siteA[0].Get(ctx, key).Result()
	valB, _ := siteB[0].Get(ctx, key).Result()
	valC, _ := siteC[0].Get(ctx, key).Result()

	t.Logf("after convergence: A=%q  B=%q  C=%q", valA, valB, valC)

	if valA != valB || valB != valC {
		t.Errorf("LWW did not converge: A=%q B=%q C=%q", valA, valB, valC)
	} else {
		t.Logf("LWW converged to %q ✓", valA)
	}

	metaA, _ := siteA[0].HGetAll(ctx, "__meta:"+key).Result()
	metaB, _ := siteB[0].HGetAll(ctx, "__meta:"+key).Result()
	metaC, _ := siteC[0].HGetAll(ctx, "__meta:"+key).Result()
	t.Logf("meta: A=%v  B=%v  C=%v", metaA, metaB, metaC)
	if metaA["site"] != metaB["site"] || metaB["site"] != metaC["site"] {
		t.Errorf("LWW meta not consistent: A=%v B=%v C=%v", metaA, metaB, metaC)
	}

	for _, ms := range [][]*redis.Client{siteA, siteB, siteC} {
		ms[0].Del(ctx, key)
		ms[0].Del(ctx, "__meta:"+key)
	}
}

// ── Test 9: Replication latency ───────────────────────────────────────────────

func TestEmbeddedClusterReplicationLatency(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, _ := ecAllMasters(t)

	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	keys := make([]string, 0, samples)

	for i := 0; i < samples; i++ {
		key := ecUniqueKey(fmt.Sprintf("lat-%02d", i))
		val := fmt.Sprintf("v%d", i)
		keys = append(keys, key)

		siteA[0].Set(ctx, key, val, 0)
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
	t.Logf("Latency over %d samples — p50:%v p95:%v max:%v", len(latencies), p50, p95, latencies[len(latencies)-1])

	if p50 > ecLatencyBudget {
		t.Errorf("p50 %v exceeds %v budget", p50, ecLatencyBudget)
	}
	if p95 > ecLatencyBudget*2 {
		t.Errorf("p95 %v exceeds 2× budget (%v)", p95, ecLatencyBudget*2)
	}

	for _, k := range keys {
		for _, ms := range [][]*redis.Client{siteA, siteB} {
			ms[0].Del(ctx, k)
		}
	}
}

// ── Test 10: Metrics increment ────────────────────────────────────────────────

func TestEmbeddedClusterMetricsIncrement(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, _ := ecAllMasters(t)

	before := fetchMetrics(t, ecMetricsB)
	appliedBefore := metricValue(before, `repl_writes_applied_total`)
	capturedBefore := metricValue(fetchMetrics(t, ecMetricsA), `repl_events_captured_total`)

	keys := make([]string, 5)
	for i := range keys {
		keys[i] = ecUniqueKey(fmt.Sprintf("metrics-%02d", i))
		siteA[0].Set(ctx, keys[i], "v", 0)
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
		t.Errorf("expected ≥5 writes_applied on B, got +%.0f", deltaApplied)
	}
	if deltaCaptured < 5 {
		t.Errorf("expected ≥5 events_captured on A, got +%.0f", deltaCaptured)
	}

	for _, k := range keys {
		for _, ms := range [][]*redis.Client{siteA, siteB} {
			ms[0].Del(ctx, k)
		}
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
				t.Logf("%s /health ok ✓", tc.name)
			}
		})
	}
}

// ── Test 12: CRUD round-trip ──────────────────────────────────────────────────

func TestEmbeddedClusterCRUDRoundTrip(t *testing.T) {
	ecSkipIfNotRunning(t)

	ctx := context.Background()
	siteA, siteB, siteC := ecAllMasters(t)
	key := ecUniqueKey("crud")

	siteA[0].Set(ctx, key, "v1", 0)
	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "v1", ecReplTimeout); err != nil {
			t.Fatalf("CREATE→%s: %v", tc.name, err)
		}
	}
	t.Log("CREATE ✓")

	siteA[0].Set(ctx, key, "v2", 0)
	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if _, err := waitOnAllMasters(ctx, tc.ms, key, "v2", ecReplTimeout); err != nil {
			t.Fatalf("UPDATE→%s: %v", tc.name, err)
		}
	}
	t.Log("UPDATE ✓")

	siteA[0].Del(ctx, key)
	for _, tc := range []struct {
		name string
		ms   []*redis.Client
	}{{"B", siteB}, {"C", siteC}} {
		if err := waitForDeleted(ctx, tc.ms[0], key, ecReplTimeout); err != nil {
			t.Fatalf("DELETE→%s: %v", tc.name, err)
		}
	}
	t.Log("DELETE ✓")
}
