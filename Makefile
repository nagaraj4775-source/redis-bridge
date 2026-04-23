.PHONY: build test test-unit run clean lint tidy \
        up-standalone down-standalone logs-standalone \
        up-sentinel   down-sentinel   logs-sentinel   \
        up-cluster    down-cluster    logs-cluster    \
        up-embedded   down-embedded   logs-embedded   \
        up-embedded-cluster down-embedded-cluster logs-embedded-cluster \
        test-e2e               test-e2e-ci               test-e2e-down \
        test-e2e-embedded      test-e2e-embedded-ci      test-e2e-embedded-down \
        test-e2e-embedded-cluster test-e2e-embedded-cluster-ci test-e2e-embedded-cluster-down

BINARY   = redibridge
GO       = go
GOFLAGS  = -ldflags="-s -w"

# ── Build ──────────────────────────────────────────────────────────────────────
build:
	$(GO) build $(GOFLAGS) -o bin/$(BINARY) ./cmd/agent

# ── Test ───────────────────────────────────────────────────────────────────────
test:
	$(GO) test -v -race -count=1 ./...

test-unit:
	$(GO) test -v -race -count=1 -short ./internal/...

# ── Run locally (standalone A for quick dev) ───────────────────────────────────
run: build
	./bin/$(BINARY) --config config/standalone/agent-a.yaml

# ── Standalone (3 single Redis instances + bus) ────────────────────────────────
up-standalone:
	docker compose -f docker/standalone/docker-compose.yml up --build -d

down-standalone:
	docker compose -f docker/standalone/docker-compose.yml down -v

logs-standalone:
	docker compose -f docker/standalone/docker-compose.yml logs -f

# ── Sentinel (3 clusters × 1M+2R+3S + HA bus) ────────────────────────────────
up-sentinel:
	docker compose -f docker/sentinel/docker-compose.yml up --build -d

down-sentinel:
	docker compose -f docker/sentinel/docker-compose.yml down -v

logs-sentinel:
	docker compose -f docker/sentinel/docker-compose.yml logs -f

# ── Embedded (3 standalone instances, NO dedicated bus) ──────────────────────
up-embedded:
	docker compose -f docker/embedded/docker-compose.yml up --build -d

down-embedded:
	docker compose -f docker/embedded/docker-compose.yml down -v

logs-embedded:
	docker compose -f docker/embedded/docker-compose.yml logs -f

# ── Cluster (3 clusters × 3 masters + bus) ────────────────────────────────────
up-cluster:
	docker compose -f docker/cluster/docker-compose.yml up --build -d

down-cluster:
	docker compose -f docker/cluster/docker-compose.yml down -v

logs-cluster:
	docker compose -f docker/cluster/docker-compose.yml logs -f

# ── E2E Tests (requires running cluster) ─────────────────────────────────────
#
# test-e2e      : start cluster, wait for agents, run tests, keep cluster up
# test-e2e-ci   : start cluster, run tests, tear down (CI-friendly)
# test-e2e-down : tear down cluster after manual test-e2e run

test-e2e: up-cluster
	@echo "==> Waiting 10s for agents to warm up..."
	@sleep 10
	@echo "==> Running E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestE2E" ./tests/

test-e2e-ci: up-cluster
	@echo "==> Waiting 10s for agents to warm up..."
	@sleep 10
	@echo "==> Running E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestE2E" ./tests/; \
	  EXIT=$$?; \
	  docker compose -f docker/cluster/docker-compose.yml down -v; \
	  exit $$EXIT

test-e2e-down:
	docker compose -f docker/cluster/docker-compose.yml down -v

# ── E2E Tests — Embedded bus mode ────────────────────────────────────────────
#
# test-e2e-embedded     : start embedded cluster, run tests, keep up
# test-e2e-embedded-ci  : start, run, tear down (CI-friendly)
# test-e2e-embedded-down: tear down embedded cluster

test-e2e-embedded: up-embedded
	@echo "==> Waiting 10s for agents to warm up..."
	@sleep 10
	@echo "==> Running Embedded E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestEmbedded" ./tests/

test-e2e-embedded-ci: up-embedded
	@echo "==> Waiting 10s for agents to warm up..."
	@sleep 10
	@echo "==> Running Embedded E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestEmbedded" ./tests/; \
	  EXIT=$$?; \
	  docker compose -f docker/embedded/docker-compose.yml down -v; \
	  exit $$EXIT

test-e2e-embedded-down:
	docker compose -f docker/embedded/docker-compose.yml down -v

# ── Embedded-Cluster (3 sites × 3 masters, NO dedicated bus) ────────────────
up-embedded-cluster:
	docker compose -f docker/cluster/embedded-bus/docker-compose.yml up --build -d

down-embedded-cluster:
	docker compose -f docker/cluster/embedded-bus/docker-compose.yml down -v

logs-embedded-cluster:
	docker compose -f docker/cluster/embedded-bus/docker-compose.yml logs -f

# ── E2E Tests — Embedded-Cluster mode ────────────────────────────────────────
#
# test-e2e-embedded-cluster     : start, run tests, keep up
# test-e2e-embedded-cluster-ci  : start, run, tear down (CI-friendly)
# test-e2e-embedded-cluster-down: tear down

test-e2e-embedded-cluster: up-embedded-cluster
	@echo "==> Waiting 20s for clusters to form and agents to warm up..."
	@sleep 20
	@echo "==> Running Embedded-Cluster E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestEmbeddedCluster" ./tests/

test-e2e-embedded-cluster-ci: up-embedded-cluster
	@echo "==> Waiting 20s for clusters to form and agents to warm up..."
	@sleep 20
	@echo "==> Running Embedded-Cluster E2E tests..."
	$(GO) test -v -timeout 120s -run "^TestEmbeddedCluster" ./tests/; \
	  EXIT=$$?; \
	  docker compose -f docker/cluster/embedded-bus/docker-compose.yml down -v; \
	  exit $$EXIT

test-e2e-embedded-cluster-down:
	docker compose -f docker/cluster/embedded-bus/docker-compose.yml down -v

# ── Misc ───────────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/

lint:
	golangci-lint run ./...

tidy:
	$(GO) mod tidy
