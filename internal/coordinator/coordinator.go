package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nagaraju/redibridge/internal/config"
	"github.com/nagaraju/redibridge/internal/consumer"
	"github.com/nagaraju/redibridge/internal/producer"
	"go.uber.org/zap"
)

// Coordinator serves the management HTTP API.
type Coordinator struct {
	cfg       *config.Config
	consumer  *consumer.Consumer
	producer  *producer.Producer // nil in consumer-only role
	startedAt time.Time
	logger    *zap.Logger
	server    *http.Server
}

// New creates a new Coordinator.
// prod may be nil when the agent runs in consumer-only role.
func New(cfg *config.Config, cons *consumer.Consumer, prod *producer.Producer, logger *zap.Logger) *Coordinator {
	return &Coordinator{
		cfg:       cfg,
		consumer:  cons,
		producer:  prod,
		startedAt: time.Now(),
		logger:    logger,
	}
}

// Run starts the HTTP server. Blocks until stopped.
func (c *Coordinator) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", c.handleHealth)
	mux.HandleFunc("/lag", c.handleLag)
	mux.HandleFunc("/stats", c.handleStats)
	mux.HandleFunc("/pause", c.handlePause)
	mux.HandleFunc("/resume", c.handleResume)
	mux.HandleFunc("/config", c.handleConfig)
	// Feature 1: on-demand reconcile
	mux.HandleFunc("/reconcile", c.handleReconcile)
	mux.HandleFunc("/reconcile/status", c.handleReconcileStatus)
	mux.HandleFunc("/sync", c.handleSync)
	// Feature 3: per-key status
	mux.HandleFunc("/key-status", c.handleKeyStatus)
	// Feature 6: bootstrap
	mux.HandleFunc("/bootstrap", c.handleBootstrap)
	mux.HandleFunc("/bootstrap/status", c.handleBootstrapStatus)

	addr := fmt.Sprintf(":%d", c.cfg.Coordinator.Port)
	c.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	c.logger.Info("coordinator listening", zap.String("addr", addr))
	return c.server.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server.
func (c *Coordinator) Stop() error {
	if c.server != nil {
		return c.server.Close()
	}
	return nil
}

func (c *Coordinator) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]interface{}{
		"status":    "ok",
		"site_id":   c.cfg.SiteID,
		"uptime_s":  int(time.Since(c.startedAt).Seconds()),
	}
	writeJSON(w, resp)
}

func (c *Coordinator) handleLag(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	writeJSON(w, map[string]interface{}{
		"site_id": c.cfg.SiteID,
		"peers":   c.consumer.PeerLags(ctx),
	})
}

func (c *Coordinator) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Prometheus metrics are exposed on the metrics port.
	// This endpoint returns a summary.
	resp := map[string]interface{}{
		"site_id":      c.cfg.SiteID,
		"peers":        c.cfg.Peers,
		"metrics_port": c.cfg.Metrics.Port,
		"message":      "Full metrics available at /metrics on the metrics port",
	}
	writeJSON(w, resp)
}

func (c *Coordinator) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Site string `json:"site"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Site == "" {
		http.Error(w, `{"error":"site field required"}`, http.StatusBadRequest)
		return
	}
	c.consumer.Pause(req.Site)
	writeJSON(w, map[string]interface{}{"status": "paused", "site": req.Site})
}

func (c *Coordinator) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Site string `json:"site"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Site == "" {
		http.Error(w, `{"error":"site field required"}`, http.StatusBadRequest)
		return
	}
	c.consumer.Resume(req.Site)
	writeJSON(w, map[string]interface{}{"status": "resumed", "site": req.Site})
}

func (c *Coordinator) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	redacted := c.cfg.Redacted()
	writeJSON(w, redacted)
}

// handleReconcile triggers an immediate reconciler cycle (Feature 1).
// POST /reconcile
func (c *Coordinator) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	queued := c.producer.TriggerReconcile()
	writeJSON(w, map[string]interface{}{
		"status":  "ok",
		"queued":  queued,
		"message": map[bool]string{true: "reconciler cycle triggered", false: "trigger already queued"}[queued],
	})
}

// handleReconcileStatus returns the last reconciler run statistics (Feature 1).
// GET /reconcile/status
func (c *Coordinator) handleReconcileStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	writeJSON(w, c.producer.GetReconcileStatus())
}

// handleSync force-publishes a specific key to the replication stream (Feature 1).
// POST /sync?key=<keyname>
func (c *Coordinator) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"key query parameter required"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	c.producer.SyncKey(ctx, key)
	writeJSON(w, map[string]interface{}{"status": "ok", "key": key, "message": "force-published to replication stream"})
}

// handleKeyStatus returns the replication state of a single key (Feature 3).
// GET /key-status?key=<keyname>
func (c *Coordinator) handleKeyStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"key query parameter required"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	ks, err := c.producer.KeyStatus(ctx, key)
	if err != nil {
		c.logger.Warn("key-status lookup failed", zap.String("key", key), zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, ks)
}

// handleBootstrap starts a full-scan bootstrap sync (Feature 6).
// POST /bootstrap          — start bootstrap
// POST /bootstrap?stop=1   — cancel a running bootstrap
func (c *Coordinator) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("stop") == "1" {
		c.producer.StopBootstrap()
		writeJSON(w, map[string]interface{}{"status": "ok", "message": "bootstrap cancellation requested"})
		return
	}
	if err := c.producer.StartBootstrap(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]interface{}{"status": "ok", "message": "bootstrap started — poll /bootstrap/status for progress"})
}

// handleBootstrapStatus returns current bootstrap progress (Feature 6).
// GET /bootstrap/status
func (c *Coordinator) handleBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if c.producer == nil {
		http.Error(w, `{"error":"producer not available (consumer-only role)"}`, http.StatusBadRequest)
		return
	}
	writeJSON(w, c.producer.GetBootstrapStatus())
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
