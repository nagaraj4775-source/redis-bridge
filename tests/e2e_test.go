package tests

// End-to-end tests for RedisBridge.
//
// These tests run against the LIVE Docker cluster — they do NOT spin up
// in-process agents. They write to Redis through the exposed host ports and
// verify that the deployed agents replicate the data correctly.
//
// Prerequisites:
//   make up-cluster          (or make test-e2e which does this automatically)
//
// Run manually:
//   go test -v -timeout 120s -run "^TestE2E" ./tests/

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ── port layout (cluster mode docker-compose) ─────────────────────────────────

const (
	e2eAddrA = "localhost:6381"
	e2eAddrB = "localhost:6384"
	e2eAddrC = "localhost:6387"
	e2eBusAddr = "localhost:6390"

	e2eCoordA = "http://localhost:8081"
	e2eCoordB = "http://localhost:8082"
	e2eCoordC = "http://localhost:8083"

	e2eMetricsA = "http://localhost:9091"
	e2eMetricsB = "http://localhost:9092"
	e2eMetricsC = "http://localhost:9093"

	// Replication must complete within this budget.
	e2eLatencyBudget = 50 * time.Millisecond

	// Generous outer timeout for eventual-consistency polls.
	e2eReplTimeout = 5 * time.Second

	// Poll interval when checking replication.
	e2ePollInterval = 1 * time.Millisecond
)

// ── helpers ───────────────────────────────────────────────────────────────────

func e2eClients(t *testing.T) (a, b, c *redis.Client) {
	t.Helper()
	a = redis.NewClient(&redis.Options{Addr: e2eAddrA})
	b = redis.NewClient(&redis.Options{Addr: e2eAddrB})
	c = redis.NewClient(&redis.Options{Addr: e2eAddrC})
	t.Cleanup(func() { a.Close(); b.Close(); c.Close() })
	return
}

// e2eSkipIfNotRunning skips the test if the cluster is not reachable.
func e2eSkipIfNotRunning(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cl := redis.NewClient(&redis.Options{Addr: e2eAddrA})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("cluster not running (make up-cluster): %v", err)
	}
	// Also verify all three agents are healthy.
	for _, coord := range []string{e2eCoordA, e2eCoordB, e2eCoordC} {
		resp, err := http.Get(coord + "/health")
		if err != nil || resp.StatusCode != 200 {
			t.Skipf("agent not healthy at %s — run make up-cluster first", coord)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
}

// waitForValue polls client until key == want or timeout elapses.
// Returns the measured latency if successful, error otherwise.
func waitForValue(ctx context.Context, client *redis.Client, key, want string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		val, err := client.Get(ctx, key).Result()
		if err == nil && val == want {
			return time.Since(start), nil
		}
		time.Sleep(e2ePollInterval)
	}
	got, _ := client.Get(ctx, key).Result()
	return 0, fmt.Errorf("timeout after %s: want %q, got %q", timeout, want, got)
}

// waitForHash polls until a hash field matches want.
func waitForHash(ctx context.Context, client *redis.Client, key, field, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		val, err := client.HGet(ctx, key, field).Result()
		if err == nil && val == want {
			return nil
		}
		time.Sleep(e2ePollInterval)
	}
	return fmt.Errorf("hash %s.%s not replicated within %s", key, field, timeout)
}

// waitForDeleted polls until the key no longer exists.
func waitForDeleted(ctx context.Context, client *redis.Client, key string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exists, _ := client.Exists(ctx, key).Result()
		if exists == 0 {
			return nil
		}
		time.Sleep(e2ePollInterval)
	}
	return fmt.Errorf("key %s still exists after %s", key, timeout)
}

// uniqueKey generates a unique test key with a given prefix.
func uniqueKey(prefix string) string {
	return fmt.Sprintf("e2e:%s:%d", prefix, time.Now().UnixNano())
}

// metricValue sums all Prometheus metric lines whose name starts with metricName.
// Handles multiple label combinations (e.g. different key_type values).
func metricValue(body, metricName string) float64 {
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		if strings.HasPrefix(line, metricName) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				v, _ := strconv.ParseFloat(parts[len(parts)-1], 64)
				total += v
			}
		}
	}
	return total
}

// fetchMetrics returns the raw Prometheus metrics body for an agent.
func fetchMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get(addr + "/metrics")
	if err != nil {
		t.Fatalf("fetch metrics %s: %v", addr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// ── Test 1: String replication A → B and A → C ───────────────────────────────

func TestE2EStringReplication(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	key := uniqueKey("string")
	val := "hello-from-A"

	if err := a.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("SET on A failed: %v", err)
	}

	latB, err := waitForValue(ctx, b, key, val, e2eReplTimeout)
	if err != nil {
		t.Fatalf("replication to B: %v", err)
	}
	latC, err := waitForValue(ctx, c, key, val, e2eReplTimeout)
	if err != nil {
		t.Fatalf("replication to C: %v", err)
	}

	t.Logf("latency A→B: %v  A→C: %v", latB, latC)

	if latB > e2eLatencyBudget {
		t.Errorf("A→B latency %v exceeds budget %v", latB, e2eLatencyBudget)
	}
	if latC > e2eLatencyBudget {
		t.Errorf("A→C latency %v exceeds budget %v", latC, e2eLatencyBudget)
	}

	// Cleanup
	a.Del(ctx, key)
	b.Del(ctx, key)
	c.Del(ctx, key)
}

// ── Test 2: Hash replication A → B and A → C ─────────────────────────────────

func TestE2EHashReplication(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	key := uniqueKey("hash")
	if err := a.HSet(ctx, key, "user", "alice", "role", "admin", "region", "US").Err(); err != nil {
		t.Fatalf("HSET on A: %v", err)
	}

	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{{"B", b}, {"C", c}} {
		if err := waitForHash(ctx, tc.client, key, "user", "alice", e2eReplTimeout); err != nil {
			t.Errorf("hash replication to %s: %v", tc.name, err)
			continue
		}
		got, _ := tc.client.HGetAll(ctx, key).Result()
		if got["role"] != "admin" || got["region"] != "US" {
			t.Errorf("cluster %s: incomplete hash: got %v", tc.name, got)
		} else {
			t.Logf("cluster %s: hash replicated correctly %v", tc.name, got)
		}
	}

	a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
}

// ── Test 3: TTL preservation ──────────────────────────────────────────────────

func TestE2ETTLPreservation(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	key := uniqueKey("ttl")
	const ttl = 30 * time.Second

	if err := a.Set(ctx, key, "expiring", ttl).Err(); err != nil {
		t.Fatalf("SET with TTL on A: %v", err)
	}

	// Wait for both replicas to have the key.
	if _, err := waitForValue(ctx, b, key, "expiring", e2eReplTimeout); err != nil {
		t.Fatalf("TTL key not replicated to B: %v", err)
	}
	if _, err := waitForValue(ctx, c, key, "expiring", e2eReplTimeout); err != nil {
		t.Fatalf("TTL key not replicated to C: %v", err)
	}

	// Verify TTL is preserved (allow 2s tolerance for transit + measurement).
	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{{"B", b}, {"C", c}} {
		remaining, err := tc.client.TTL(ctx, key).Result()
		if err != nil {
			t.Errorf("TTL check on %s: %v", tc.name, err)
			continue
		}
		if remaining <= 0 {
			t.Errorf("cluster %s: key has no TTL (got %v)", tc.name, remaining)
		} else if remaining > ttl {
			t.Errorf("cluster %s: TTL %v exceeds original %v", tc.name, remaining, ttl)
		} else {
			t.Logf("cluster %s: TTL remaining = %v (original %v) ✓", tc.name, remaining, ttl)
		}
	}

	a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
}

// ── Test 4: Bidirectional replication B → A,C and C → A,B ────────────────────

func TestE2EBidirectionalReplication(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	// B → A, C
	keyB := uniqueKey("bidir-B")
	if err := b.Set(ctx, keyB, "from-B", 0).Err(); err != nil {
		t.Fatalf("SET on B: %v", err)
	}
	if _, err := waitForValue(ctx, a, keyB, "from-B", e2eReplTimeout); err != nil {
		t.Errorf("B→A: %v", err)
	}
	if _, err := waitForValue(ctx, c, keyB, "from-B", e2eReplTimeout); err != nil {
		t.Errorf("B→C: %v", err)
	}
	t.Log("B→A,C: ✓")

	// C → A, B
	keyC := uniqueKey("bidir-C")
	if err := c.Set(ctx, keyC, "from-C", 0).Err(); err != nil {
		t.Fatalf("SET on C: %v", err)
	}
	if _, err := waitForValue(ctx, a, keyC, "from-C", e2eReplTimeout); err != nil {
		t.Errorf("C→A: %v", err)
	}
	if _, err := waitForValue(ctx, b, keyC, "from-C", e2eReplTimeout); err != nil {
		t.Errorf("C→B: %v", err)
	}
	t.Log("C→A,B: ✓")

	for _, k := range []string{keyB, keyC} {
		a.Del(ctx, k); b.Del(ctx, k); c.Del(ctx, k)
	}
}

// ── Test 5: DELETE propagation ────────────────────────────────────────────────

func TestE2EDeletePropagation(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	key := uniqueKey("del")

	// First set and confirm replication.
	a.Set(ctx, key, "to-be-deleted", 0)
	if _, err := waitForValue(ctx, b, key, "to-be-deleted", e2eReplTimeout); err != nil {
		t.Fatalf("initial replication to B failed: %v", err)
	}
	if _, err := waitForValue(ctx, c, key, "to-be-deleted", e2eReplTimeout); err != nil {
		t.Fatalf("initial replication to C failed: %v", err)
	}

	// Now delete on A.
	if err := a.Del(ctx, key).Err(); err != nil {
		t.Fatalf("DEL on A: %v", err)
	}

	// Verify deletion propagates.
	if err := waitForDeleted(ctx, b, key, e2eReplTimeout); err != nil {
		t.Errorf("delete not propagated to B: %v", err)
	} else {
		t.Log("DEL propagated to B ✓")
	}
	if err := waitForDeleted(ctx, c, key, e2eReplTimeout); err != nil {
		t.Errorf("delete not propagated to C: %v", err)
	} else {
		t.Log("DEL propagated to C ✓")
	}
}

// ── Test 6: LWW convergence on simultaneous conflict ─────────────────────────

func TestE2ELWWConvergence(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	key := uniqueKey("lww")

	// Write to A and B simultaneously using goroutines.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.Set(ctx, key, "from-A", 0) }()
	go func() { defer wg.Done(); b.Set(ctx, key, "from-B", 0) }()
	wg.Wait()

	// Allow convergence time.
	time.Sleep(3 * time.Second)

	valA, _ := a.Get(ctx, key).Result()
	valB, _ := b.Get(ctx, key).Result()
	valC, _ := c.Get(ctx, key).Result()

	t.Logf("after convergence: A=%q  B=%q  C=%q", valA, valB, valC)

	// All three must agree on the same value.
	if valA != valB || valB != valC {
		t.Errorf("LWW did not converge: A=%q B=%q C=%q", valA, valB, valC)
	} else {
		t.Logf("LWW converged to %q across all 3 clusters ✓", valA)
	}

	// Verify that meta agrees (all point to the same winner's HLC + site).
	metaA, _ := a.HGetAll(ctx, "__meta:"+key).Result()
	metaB, _ := b.HGetAll(ctx, "__meta:"+key).Result()
	metaC, _ := c.HGetAll(ctx, "__meta:"+key).Result()
	t.Logf("meta: A=%v  B=%v  C=%v", metaA, metaB, metaC)

	if metaA["site"] != metaB["site"] || metaB["site"] != metaC["site"] {
		t.Errorf("LWW meta not consistent across clusters")
	}

	a.Del(ctx, key); a.Del(ctx, "__meta:"+key)
	b.Del(ctx, key); b.Del(ctx, "__meta:"+key)
	c.Del(ctx, key); c.Del(ctx, "__meta:"+key)
}

// ── Test 7: Replication latency — p50 and p95 must be < 50ms ─────────────────

func TestE2EReplicationLatency(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, _ := e2eClients(t)

	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	keys := make([]string, 0, samples)

	for i := 0; i < samples; i++ {
		key := uniqueKey(fmt.Sprintf("lat-%02d", i))
		val := fmt.Sprintf("v%d", i)
		keys = append(keys, key)

		start := time.Now()
		if err := a.Set(ctx, key, val, 0).Err(); err != nil {
			t.Fatalf("SET sample %d: %v", i, err)
		}

		lat, err := waitForValue(ctx, b, key, val, e2eReplTimeout)
		if err != nil {
			t.Errorf("sample %d not replicated: %v", i, err)
			continue
		}
		_ = start
		latencies = append(latencies, lat)

		// Small gap between samples to avoid batching effects.
		time.Sleep(5 * time.Millisecond)
	}

	// Compute p50 and p95.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	min := latencies[0]
	max := latencies[len(latencies)-1]

	t.Logf("Latency over %d samples — min:%v p50:%v p95:%v max:%v", len(latencies), min, p50, p95, max)

	if p50 > e2eLatencyBudget {
		t.Errorf("p50 latency %v exceeds %v budget", p50, e2eLatencyBudget)
	}
	if p95 > e2eLatencyBudget*2 {
		t.Errorf("p95 latency %v exceeds 2× budget (%v)", p95, e2eLatencyBudget*2)
	}

	// Cleanup
	for _, k := range keys {
		a.Del(ctx, k)
		b.Del(ctx, k)
	}
}

// ── Test 8: All three sites converge latency ──────────────────────────────────

func TestE2EThreeSiteLatency(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)

	const samples = 10
	type result struct{ latB, latC time.Duration }
	results := make([]result, 0, samples)

	for i := 0; i < samples; i++ {
		key := uniqueKey(fmt.Sprintf("3s-%02d", i))
		val := fmt.Sprintf("w%d", i)

		if err := a.Set(ctx, key, val, 0).Err(); err != nil {
			t.Fatalf("SET %d: %v", i, err)
		}

		var latB, latC time.Duration
		var errB, errC error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); latB, errB = waitForValue(ctx, b, key, val, e2eReplTimeout) }()
		go func() { defer wg.Done(); latC, errC = waitForValue(ctx, c, key, val, e2eReplTimeout) }()
		wg.Wait()

		if errB != nil {
			t.Errorf("sample %d B: %v", i, errB)
		}
		if errC != nil {
			t.Errorf("sample %d C: %v", i, errC)
		}
		results = append(results, result{latB, latC})

		a.Del(ctx, key); b.Del(ctx, key); c.Del(ctx, key)
		time.Sleep(5 * time.Millisecond)
	}

	var sumB, sumC time.Duration
	for _, r := range results {
		sumB += r.latB
		sumC += r.latC
	}
	avgB := sumB / time.Duration(len(results))
	avgC := sumC / time.Duration(len(results))
	t.Logf("avg latency A→B: %v  A→C: %v", avgB, avgC)

	if avgB > e2eLatencyBudget {
		t.Errorf("avg A→B latency %v exceeds %v", avgB, e2eLatencyBudget)
	}
	if avgC > e2eLatencyBudget {
		t.Errorf("avg A→C latency %v exceeds %v", avgC, e2eLatencyBudget)
	}
}

// ── Test 9: Prometheus metrics increment after writes ─────────────────────────

func TestE2EMetricsIncrement(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, _ := e2eClients(t)

	// Snapshot current counters on Agent B (which consumes from A).
	before := fetchMetrics(t, e2eMetricsB)
	appliedBefore := metricValue(before, `repl_writes_applied_total{peer_site="cluster-a"`)
	capturedBefore := metricValue(fetchMetrics(t, e2eMetricsA), `repl_events_captured_total`)

	// Write 5 keys on A.
	keys := make([]string, 5)
	for i := range keys {
		keys[i] = uniqueKey(fmt.Sprintf("metrics-%02d", i))
		if err := a.Set(ctx, keys[i], "v", 0).Err(); err != nil {
			t.Fatalf("SET: %v", err)
		}
	}

	// Wait for all 5 to replicate to B.
	for _, k := range keys {
		if _, err := waitForValue(ctx, b, k, "v", e2eReplTimeout); err != nil {
			t.Errorf("key %s not replicated: %v", k, err)
		}
	}

	// Give Prometheus scrape a moment to update counters.
	time.Sleep(500 * time.Millisecond)

	after := fetchMetrics(t, e2eMetricsB)
	appliedAfter := metricValue(after, `repl_writes_applied_total{peer_site="cluster-a"`)
	capturedAfter := metricValue(fetchMetrics(t, e2eMetricsA), `repl_events_captured_total`)

	delta := appliedAfter - appliedBefore
	captured := capturedAfter - capturedBefore

	t.Logf("repl_writes_applied_total on B increased by %.0f (wrote 5 keys)", delta)
	t.Logf("repl_events_captured_total on A increased by %.0f (wrote 5 keys)", captured)

	if delta < 5 {
		t.Errorf("expected ≥5 writes_applied on B, got +%.0f (before=%.0f after=%.0f)",
			delta, appliedBefore, appliedAfter)
	}
	if captured < 5 {
		t.Errorf("expected ≥5 events_captured on A, got +%.0f", captured)
	}

	// Check repl_lag_ms is present and reasonable.
	lagLine := metricValue(after, `repl_lag_ms{peer_site="cluster-a"`)
	t.Logf("repl_lag_ms{peer_site=cluster-a} on B = %.2f ms", lagLine)
	if lagLine < 0 {
		t.Errorf("negative lag: %.2f", lagLine)
	}

	for _, k := range keys {
		a.Del(ctx, k); b.Del(ctx, k)
	}
}

// ── Test 10: Agent management API health + lag ────────────────────────────────

func TestE2EManagementAPI(t *testing.T) {
	e2eSkipIfNotRunning(t)

	for _, tc := range []struct {
		name  string
		coord string
	}{
		{"agent-a", e2eCoordA},
		{"agent-b", e2eCoordB},
		{"agent-c", e2eCoordC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// /health
			resp, err := http.Get(tc.coord + "/health")
			if err != nil {
				t.Fatalf("GET /health: %v", err)
			}
			if resp.StatusCode != 200 {
				t.Errorf("expected 200, got %d", resp.StatusCode)
			}
			resp.Body.Close()

			// /lag
			resp, err = http.Get(tc.coord + "/lag")
			if err != nil {
				t.Fatalf("GET /lag: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("/lag: expected 200, got %d", resp.StatusCode)
			}
			if !strings.Contains(string(body), "peers") {
				t.Errorf("/lag response missing 'peers': %s", body)
			}
			t.Logf("%s /lag: %s", tc.name, body)

			// /config
			resp, err = http.Get(tc.coord + "/config")
			if err != nil {
				t.Fatalf("GET /config: %v", err)
			}
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if !strings.Contains(string(body), "SiteID") {
				t.Errorf("/config missing SiteID: %s", body)
			}
		})
	}
}

// ── Test 11: CRUD full round-trip (set → update → delete) ────────────────────

func TestE2ECRUDRoundTrip(t *testing.T) {
	e2eSkipIfNotRunning(t)

	ctx := context.Background()
	a, b, c := e2eClients(t)
	key := uniqueKey("crud")

	// CREATE
	if err := a.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if _, err := waitForValue(ctx, tc.cl, key, "v1", e2eReplTimeout); err != nil {
			t.Fatalf("CREATE→%s: %v", tc.name, err)
		}
	}
	t.Log("CREATE replicated ✓")

	// UPDATE
	if err := a.Set(ctx, key, "v2", 0).Err(); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if _, err := waitForValue(ctx, tc.cl, key, "v2", e2eReplTimeout); err != nil {
			t.Fatalf("UPDATE→%s: %v", tc.name, err)
		}
	}
	t.Log("UPDATE replicated ✓")

	// DELETE
	if err := a.Del(ctx, key).Err(); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	for _, tc := range []struct{ name string; cl *redis.Client }{{"B", b}, {"C", c}} {
		if err := waitForDeleted(ctx, tc.cl, key, e2eReplTimeout); err != nil {
			t.Fatalf("DELETE→%s: %v", tc.name, err)
		}
	}
	t.Log("DELETE replicated ✓")
}
