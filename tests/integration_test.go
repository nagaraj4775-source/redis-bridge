package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nagaraju/redibridge/internal/applier"
	"github.com/nagaraju/redibridge/internal/bus"
	"github.com/nagaraju/redibridge/internal/consumer"
	"github.com/nagaraju/redibridge/internal/dedup"
	"github.com/nagaraju/redibridge/internal/hlc"
	"github.com/nagaraju/redibridge/internal/producer"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// These tests require running Redis instances.
// Run with: docker compose -f docker/docker-compose.yml up -d
// Then: go test -v -run TestIntegration ./tests/

const (
	redisAAddr = "localhost:6381"
	redisBAddr = "localhost:6384"
	redisCAddr = "localhost:6387"
	busAddr    = "localhost:6390"
)

func skipIfNoRedis(t *testing.T) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: busAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping integration test: Redis not available at %s: %v", busAddr, err)
	}
	client.Close()
}

func TestIntegrationWriteReplicates(t *testing.T) {
	skipIfNoRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger, _ := zap.NewDevelopment()

	// Connect to cluster A master, cluster B master, and bus
	clientA := redis.NewClient(&redis.Options{Addr: redisAAddr})
	clientB := redis.NewClient(&redis.Options{Addr: redisBAddr})
	defer clientA.Close()
	defer clientB.Close()

	// Create bus
	replBus := bus.NewRedisStreamsBus(busAddr, "", "repl:stream:", logger)
	defer replBus.Close()

	// Setup agent A (producer)
	clockA := hlc.New()
	ddA, _ := dedup.NewFilter(clientA, 5000, 100000)
	mastersA := []*redis.Client{clientA}
	prodA := producer.New("cluster-a", mastersA, replBus, clockA, ddA, logger)

	// Setup agent B (consumer)
	clockB := hlc.New()
	ddB, _ := dedup.NewFilter(clientB, 5000, 100000)
	appB := applier.New(clientB, ddB, logger)
	consB := consumer.New("cluster-b", []string{"cluster-a"}, replBus, clientB, appB, ddB, clockB,
		"repl-consumers", 100, 8, logger)

	// Start producer A and consumer B
	go prodA.Run(ctx)
	go consB.Run(ctx)

	// Wait for subscriptions to be ready
	time.Sleep(2 * time.Second)

	// Write a key on cluster A
	testKey := fmt.Sprintf("test:integration:%d", time.Now().UnixNano())
	testVal := "hello-from-A"
	if err := clientA.Set(ctx, testKey, testVal, 0).Err(); err != nil {
		t.Fatalf("failed to SET on cluster A: %v", err)
	}

	// Wait for replication
	deadline := time.Now().Add(5 * time.Second)
	var replicated bool
	for time.Now().Before(deadline) {
		val, err := clientB.Get(ctx, testKey).Result()
		if err == nil && val == testVal {
			replicated = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !replicated {
		t.Fatalf("key %s was not replicated to cluster B within 5s", testKey)
	}

	// Cleanup
	clientA.Del(ctx, testKey)
	clientB.Del(ctx, testKey)

	t.Logf("key replicated successfully: %s = %s", testKey, testVal)
}

func TestIntegrationLWWConvergence(t *testing.T) {
	skipIfNoRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger, _ := zap.NewDevelopment()

	clientA := redis.NewClient(&redis.Options{Addr: redisAAddr})
	clientB := redis.NewClient(&redis.Options{Addr: redisBAddr})
	defer clientA.Close()
	defer clientB.Close()

	replBus := bus.NewRedisStreamsBus(busAddr, "", "repl:stream:", logger)
	defer replBus.Close()

	// Setup agents for both clusters
	clockA := hlc.New()
	ddA, _ := dedup.NewFilter(clientA, 5000, 100000)
	prodA := producer.New("cluster-a", []*redis.Client{clientA}, replBus, clockA, ddA, logger)
	appA := applier.New(clientA, ddA, logger)
	consA := consumer.New("cluster-a", []string{"cluster-b"}, replBus, clientA, appA, ddA, clockA,
		"repl-consumers", 100, 8, logger)

	clockB := hlc.New()
	ddB, _ := dedup.NewFilter(clientB, 5000, 100000)
	prodB := producer.New("cluster-b", []*redis.Client{clientB}, replBus, clockB, ddB, logger)
	appB := applier.New(clientB, ddB, logger)
	consB := consumer.New("cluster-b", []string{"cluster-a"}, replBus, clientB, appB, ddB, clockB,
		"repl-consumers", 100, 8, logger)

	go prodA.Run(ctx)
	go prodB.Run(ctx)
	go consA.Run(ctx)
	go consB.Run(ctx)

	time.Sleep(2 * time.Second)

	// Write same key on both clusters "simultaneously"
	conflictKey := fmt.Sprintf("test:conflict:%d", time.Now().UnixNano())
	clientA.Set(ctx, conflictKey, "from-A", 0)
	clientB.Set(ctx, conflictKey, "from-B", 0)

	// Wait for convergence
	time.Sleep(5 * time.Second)

	valA, _ := clientA.Get(ctx, conflictKey).Result()
	valB, _ := clientB.Get(ctx, conflictKey).Result()

	t.Logf("cluster A has: %s, cluster B has: %s", valA, valB)

	if valA != valB {
		t.Fatalf("clusters did not converge: A=%s B=%s", valA, valB)
	}

	// Cleanup
	clientA.Del(ctx, conflictKey)
	clientB.Del(ctx, conflictKey)
}

func TestIntegrationHashReplication(t *testing.T) {
	skipIfNoRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger, _ := zap.NewDevelopment()

	clientA := redis.NewClient(&redis.Options{Addr: redisAAddr})
	clientB := redis.NewClient(&redis.Options{Addr: redisBAddr})
	defer clientA.Close()
	defer clientB.Close()

	replBus := bus.NewRedisStreamsBus(busAddr, "", "repl:stream:", logger)
	defer replBus.Close()

	clockA := hlc.New()
	ddA, _ := dedup.NewFilter(clientA, 5000, 100000)
	prodA := producer.New("cluster-a", []*redis.Client{clientA}, replBus, clockA, ddA, logger)

	clockB := hlc.New()
	ddB, _ := dedup.NewFilter(clientB, 5000, 100000)
	appB := applier.New(clientB, ddB, logger)
	consB := consumer.New("cluster-b", []string{"cluster-a"}, replBus, clientB, appB, ddB, clockB,
		"repl-consumers", 100, 8, logger)

	go prodA.Run(ctx)
	go consB.Run(ctx)

	time.Sleep(2 * time.Second)

	testKey := fmt.Sprintf("test:hash:%d", time.Now().UnixNano())
	clientA.HSet(ctx, testKey, "name", "nagaraju", "role", "engineer")

	deadline := time.Now().Add(5 * time.Second)
	var replicated bool
	for time.Now().Before(deadline) {
		val, err := clientB.HGetAll(ctx, testKey).Result()
		if err == nil && val["name"] == "nagaraju" {
			replicated = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !replicated {
		t.Fatalf("hash %s was not replicated to cluster B within 5s", testKey)
	}

	clientA.Del(ctx, testKey)
	clientB.Del(ctx, testKey)
}
