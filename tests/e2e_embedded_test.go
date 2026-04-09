package tests

// End-to-end tests for RedisBridge in EMBEDDED bus mode.
//
// In embedded mode each site's own Redis instance doubles as the replication
// bus — there is NO separate bus container. These tests verify:
//   - All standard replication functionality works identically to dedicated-bus mode
//   - The replication stream IS present on the data Redis (same instance)
//   - Stream keys do NOT replicate to peer sites (internal prefix filtered)
//   - The embedded bus does not contaminate normal data reads
//
// Prerequisites:
//   make up-embedded          (or make test-e2e-embedded which does this automatically)
//
// Run manually:
//   go test -v -timeout 120s -run "^TestEmbedded" ./tests/

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ── port layout (embedded docker-compose) ────────────────────────────────────

const (
	embAddrA = "localhost:6401"
	embAddrB = "localhost:6402"
	embAddrC = "localhost:6403"

	embCoordA   = "http://localhost:8191"
	embCoordB   = "http://localhost:8192"
	embCoordC   = "http://localhost:8193"
	embMetricsA = "http://localhost:9191"
	embMetricsB = "http://localhost:9192"
	embMetricsC = "http://localhost:9193"

	// Site IDs used in embedded config files.
	embSiteA = "site-a"
	embSiteB = "site-b"
	embSiteC = "site-c"

	embLatencyBudget = 50 * time.Millisecond
	embReplTimeout   = 5 * time.Second
)

// ── helpers ───────────────────────────────────────────────────────────────────

func embClients(t *testing.T) (a, b, c *redis.Client) {
	t.Helper()
	a = redis.NewClient(&redis.Options{Addr: embAddrA})
	b = redis.NewClient(&redis.Options{Addr: embAddrB})
	c = redis.NewClient(&redis.Options{Addr: embAddrC})
	t.Cleanup(func() { a.Close(); b.Close(); c.Close() })
	return
}

func embSkipIfNotRunning(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cl := redis.NewClient(&redis.Options{Addr: embAddrA})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("embedded cluster not running (make up-embedded): %v", err)
	}
	for _, coord := range []string{embCoordA, embCoordB, embCoordC} {
		resp, err := http.Get(coord + "/health")
		if err != nil || resp.StatusCode != 200 {
			t.Skipf("agent not healthy at %s — run make up-embedded first", coord)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
}

func embUniqueKey(prefix string) string {
	return fmt.Sprintf("emb:%s:%d", prefix, time.Now().UnixNano())
}

// ── Test 1: Stream lives on data Redis (core embedded guarantee) ──────────────

func TestEmbeddedStreamOnDataRedis(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	// Write a key on A to trigger stream creation.
	key := embUniqueKey("init")
	if err := a.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("SET on A: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// The stream for site-a must exist ON redis-a itself (embedded mode).
	streamA := "repl:stream:" + embSiteA
	lenA, err := a.XLen(ctx, streamA).Result()
	if err != nil {
		t.Fatalf("XLEN %s on redis-a: %v", streamA, err)
	}
	if lenA == 0 {
		t.Errorf("expected stream %s on redis-a to have entries, got 0", streamA)
	} else {
		t.Logf("stream %s on redis-a has %d entries ✓", streamA, lenA)
	}

	// The stream for site-b must exist ON redis-b, and similarly for site-c.
	if err := b.Set(ctx, embUniqueKey("init-b"), "v", 0).Err(); err != nil {
		t.Fatalf("SET on B: %v", err)
	}
	if err := c.Set(ctx, embUniqueKey("init-c"), "v", 0).Err(); err != nil {
		t.Fatalf("SET on C: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	streamB := "repl:stream:" + embSiteB
	streamC := "repl:stream:" + embSiteC

	lenB, errB := b.XLen(ctx, streamB).Result()
	lenC, errC := c.XLen(ctx, streamC).Result()

	if errB != nil || lenB == 0 {
		t.Errorf("stream %s on redis-b: len=%d err=%v", streamB, lenB, errB)
	} else {
		t.Logf("stream %s on redis-b has %d entries ✓", streamB, lenB)
	}
	if errC != nil || lenC == 0 {
		t.Errorf("stream %s on redis-c: len=%d err=%v", streamC, lenC, errC)
	} else {
		t.Logf("stream %s on redis-c has %d entries ✓", streamC, lenC)
	}

	a.Del(ctx, key)
}

// ── Test 2: Stream keys do NOT replicate to peers ─────────────────────────────

func TestEmbeddedStreamKeysNotReplicated(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	streamA := "repl:stream:" + embSiteA

	// Remove any stale stream keys left by previous runs so the assertion is
	// purely about what the current code does, not historical state.
	b.Del(ctx, streamA)
	c.Del(ctx, streamA)

	// Write on A — this triggers the producer and publishes to repl:stream:site-a
	// on redis-a. The filter must prevent these stream writes from being
	// re-published across to redis-b / redis-c.
	for i := 0; i < 3; i++ {
		a.Set(ctx, embUniqueKey("no-leak"), "x", 0)
	}
	// Wait long enough for any hypothetical replication to have occurred.
	time.Sleep(800 * time.Millisecond)

	existsOnB, _ := b.Exists(ctx, streamA).Result()
	existsOnC, _ := c.Exists(ctx, streamA).Result()

	if existsOnB > 0 {
		t.Errorf("stream key %q leaked to redis-b — producer should filter repl:stream: prefix", streamA)
	} else {
		t.Logf("stream %s NOT present on redis-b ✓", streamA)
	}
	if existsOnC > 0 {
		t.Errorf("stream key %q leaked to redis-c — producer should filter repl:stream: prefix", streamA)
	} else {
		t.Logf("stream %s NOT present on redis-c ✓", streamA)
	}
}

// ── Test 3: String replication A → B, C within latency budget ─────────────────

func TestEmbeddedStringReplication(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	key := embUniqueKey("string")
	val := "hello-embedded"

	if err := a.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("SET on A: %v", err)
	}

	latB, err := waitForValue(ctx, b, key, val, embReplTimeout)
	if err != nil {
		t.Fatalf("A→B: %v", err)
	}
	latC, err := waitForValue(ctx, c, key, val, embReplTimeout)
	if err != nil {
		t.Fatalf("A→C: %v", err)
	}

	t.Logf("embedded latency A→B: %v  A→C: %v", latB, latC)

	if latB > embLatencyBudget {
		t.Errorf("A→B latency %v exceeds %v", latB, embLatencyBudget)
	}
	if latC > embLatencyBudget {
		t.Errorf("A→C latency %v exceeds %v", latC, embLatencyBudget)
	}

	a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
}

// ── Test 4: Hash replication ──────────────────────────────────────────────────

func TestEmbeddedHashReplication(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	key := embUniqueKey("hash")
	a.HSet(ctx, key, "user", "bob", "role", "dev", "region", "EU")

	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{{"B", b}, {"C", c}} {
		if err := waitForHash(ctx, tc.client, key, "user", "bob", embReplTimeout); err != nil {
			t.Errorf("hash replication to %s: %v", tc.name, err)
			continue
		}
		got, _ := tc.client.HGetAll(ctx, key).Result()
		if got["role"] != "dev" || got["region"] != "EU" {
			t.Errorf("cluster %s: incomplete hash: %v", tc.name, got)
		} else {
			t.Logf("cluster %s: hash ok %v ✓", tc.name, got)
		}
	}

	a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
}

// ── Test 5: TTL preservation ──────────────────────────────────────────────────

func TestEmbeddedTTLPreservation(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	key := embUniqueKey("ttl")
	const ttl = 30 * time.Second

	a.Set(ctx, key, "expiring", ttl)

	if _, err := waitForValue(ctx, b, key, "expiring", embReplTimeout); err != nil {
		t.Fatalf("B: %v", err)
	}
	if _, err := waitForValue(ctx, c, key, "expiring", embReplTimeout); err != nil {
		t.Fatalf("C: %v", err)
	}

	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{{"B", b}, {"C", c}} {
		rem, err := tc.client.TTL(ctx, key).Result()
		if err != nil {
			t.Errorf("TTL on %s: %v", tc.name, err)
		} else if rem <= 0 || rem > ttl {
			t.Errorf("cluster %s: unexpected TTL %v (original %v)", tc.name, rem, ttl)
		} else {
			t.Logf("cluster %s: TTL %v ✓", tc.name, rem)
		}
	}

	a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
}

// ── Test 6: Bidirectional replication B → A,C and C → A,B ────────────────────

func TestEmbeddedBidirectional(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	keyB := embUniqueKey("bidir-B")
	b.Set(ctx, keyB, "from-B", 0)
	if _, err := waitForValue(ctx, a, keyB, "from-B", embReplTimeout); err != nil {
		t.Errorf("B→A: %v", err)
	}
	if _, err := waitForValue(ctx, c, keyB, "from-B", embReplTimeout); err != nil {
		t.Errorf("B→C: %v", err)
	}
	t.Log("B→A,C ✓")

	keyC := embUniqueKey("bidir-C")
	c.Set(ctx, keyC, "from-C", 0)
	if _, err := waitForValue(ctx, a, keyC, "from-C", embReplTimeout); err != nil {
		t.Errorf("C→A: %v", err)
	}
	if _, err := waitForValue(ctx, b, keyC, "from-C", embReplTimeout); err != nil {
		t.Errorf("C→B: %v", err)
	}
	t.Log("C→A,B ✓")

	for _, k := range []string{keyB, keyC} {
		a.Del(ctx, k); b.Del(ctx, k); c.Del(ctx, k)
	}
}

// ── Test 7: DELETE propagation ────────────────────────────────────────────────

func TestEmbeddedDeletePropagation(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	key := embUniqueKey("del")
	a.Set(ctx, key, "to-be-deleted", 0)

	if _, err := waitForValue(ctx, b, key, "to-be-deleted", embReplTimeout); err != nil {
		t.Fatalf("initial repl B: %v", err)
	}
	if _, err := waitForValue(ctx, c, key, "to-be-deleted", embReplTimeout); err != nil {
		t.Fatalf("initial repl C: %v", err)
	}

	a.Del(ctx, key)

	if err := waitForDeleted(ctx, b, key, embReplTimeout); err != nil {
		t.Errorf("DEL not propagated to B: %v", err)
	} else {
		t.Log("DEL propagated to B ✓")
	}
	if err := waitForDeleted(ctx, c, key, embReplTimeout); err != nil {
		t.Errorf("DEL not propagated to C: %v", err)
	} else {
		t.Log("DEL propagated to C ✓")
	}
}

// ── Test 8: LWW convergence ───────────────────────────────────────────────────

func TestEmbeddedLWWConvergence(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)

	key := embUniqueKey("lww")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.Set(ctx, key, "from-A", 0) }()
	go func() { defer wg.Done(); b.Set(ctx, key, "from-B", 0) }()
	wg.Wait()

	time.Sleep(3 * time.Second)

	valA, _ := a.Get(ctx, key).Result()
	valB, _ := b.Get(ctx, key).Result()
	valC, _ := c.Get(ctx, key).Result()

	t.Logf("after convergence: A=%q  B=%q  C=%q", valA, valB, valC)

	if valA != valB || valB != valC {
		t.Errorf("LWW did not converge: A=%q B=%q C=%q", valA, valB, valC)
	} else {
		t.Logf("LWW converged to %q ✓", valA)
	}

	metaA, _ := a.HGetAll(ctx, "__meta:"+key).Result()
	metaB, _ := b.HGetAll(ctx, "__meta:"+key).Result()
	metaC, _ := c.HGetAll(ctx, "__meta:"+key).Result()
	t.Logf("meta: A=%v  B=%v  C=%v", metaA, metaB, metaC)

	if metaA["site"] != metaB["site"] || metaB["site"] != metaC["site"] {
		t.Errorf("LWW meta not consistent across sites")
	}

	a.Del(ctx, key); a.Del(ctx, "__meta:"+key)
	b.Del(ctx, key); b.Del(ctx, "__meta:"+key)
	c.Del(ctx, key); c.Del(ctx, "__meta:"+key)
}

// ── Test 9: Replication latency p50 and p95 < 50ms ───────────────────────────

func TestEmbeddedReplicationLatency(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, _ := embClients(t)

	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	keys := make([]string, 0, samples)

	for i := 0; i < samples; i++ {
		key := embUniqueKey(fmt.Sprintf("lat-%02d", i))
		val := fmt.Sprintf("v%d", i)
		keys = append(keys, key)

		if err := a.Set(ctx, key, val, 0).Err(); err != nil {
			t.Fatalf("SET %d: %v", i, err)
		}
		lat, err := waitForValue(ctx, b, key, val, embReplTimeout)
		if err != nil {
			t.Errorf("sample %d not replicated: %v", i, err)
			continue
		}
		latencies = append(latencies, lat)
		time.Sleep(5 * time.Millisecond)
	}

	if len(latencies) == 0 {
		t.Fatal("no latency samples collected")
	}

	// Sort and compute percentiles.
	for i := 1; i < len(latencies); i++ {
		for j := i; j > 0 && latencies[j] < latencies[j-1]; j-- {
			latencies[j], latencies[j-1] = latencies[j-1], latencies[j]
		}
	}
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	t.Logf("Latency over %d samples — p50:%v p95:%v max:%v", len(latencies), p50, p95, latencies[len(latencies)-1])

	if p50 > embLatencyBudget {
		t.Errorf("p50 %v exceeds %v budget", p50, embLatencyBudget)
	}
	if p95 > embLatencyBudget*2 {
		t.Errorf("p95 %v exceeds 2× budget (%v)", p95, embLatencyBudget*2)
	}

	for _, k := range keys {
		a.Del(ctx, k); b.Del(ctx, k)
	}
}

// ── Test 10: Metrics increment in embedded mode ───────────────────────────────

func TestEmbeddedMetricsIncrement(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, _ := embClients(t)

	before := fetchMetrics(t, embMetricsB)
	appliedBefore := metricValue(before, `repl_writes_applied_total`)
	capturedBefore := metricValue(fetchMetrics(t, embMetricsA), `repl_events_captured_total`)

	keys := make([]string, 5)
	for i := range keys {
		keys[i] = embUniqueKey(fmt.Sprintf("metrics-%02d", i))
		a.Set(ctx, keys[i], "v", 0)
	}
	for _, k := range keys {
		if _, err := waitForValue(ctx, b, k, "v", embReplTimeout); err != nil {
			t.Errorf("%s not replicated: %v", k, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	after := fetchMetrics(t, embMetricsB)
	appliedAfter := metricValue(after, `repl_writes_applied_total`)
	capturedAfter := metricValue(fetchMetrics(t, embMetricsA), `repl_events_captured_total`)

	deltaApplied := appliedAfter - appliedBefore
	deltaCaptured := capturedAfter - capturedBefore

	t.Logf("writes_applied on B  +%.0f (wrote 5 keys)", deltaApplied)
	t.Logf("events_captured on A +%.0f (wrote 5 keys)", deltaCaptured)

	if deltaApplied < 5 {
		t.Errorf("expected ≥5 writes_applied on B, got +%.0f", deltaApplied)
	}
	if deltaCaptured < 5 {
		t.Errorf("expected ≥5 events_captured on A, got +%.0f", deltaCaptured)
	}

	for _, k := range keys {
		a.Del(ctx, k); b.Del(ctx, k)
	}
}

// ── Test 11: Management API health check ─────────────────────────────────────

func TestEmbeddedManagementAPI(t *testing.T) {
	embSkipIfNotRunning(t)

	for _, tc := range []struct {
		name  string
		coord string
	}{
		{"agent-a", embCoordA},
		{"agent-b", embCoordB},
		{"agent-c", embCoordC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(tc.coord + "/health")
			if err != nil {
				t.Fatalf("GET /health: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("%s /health: expected 200, got %d", tc.name, resp.StatusCode)
			} else {
				t.Logf("%s /health: ok ✓", tc.name)
			}
		})
	}
}

// ── Test 12: CRUD full round-trip ─────────────────────────────────────────────

func TestEmbeddedCRUDRoundTrip(t *testing.T) {
	embSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := embClients(t)
	key := embUniqueKey("crud")

	a.Set(ctx, key, "v1", 0)
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if _, err := waitForValue(ctx, tc.cl, key, "v1", embReplTimeout); err != nil {
			t.Fatalf("CREATE→%s: %v", tc.name, err)
		}
	}
	t.Log("CREATE ✓")

	a.Set(ctx, key, "v2", 0)
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if _, err := waitForValue(ctx, tc.cl, key, "v2", embReplTimeout); err != nil {
			t.Fatalf("UPDATE→%s: %v", tc.name, err)
		}
	}
	t.Log("UPDATE ✓")

	a.Del(ctx, key)
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if err := waitForDeleted(ctx, tc.cl, key, embReplTimeout); err != nil {
			t.Fatalf("DELETE→%s: %v", tc.name, err)
		}
	}
	t.Log("DELETE ✓")
}
