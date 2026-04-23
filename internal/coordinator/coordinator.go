package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nagaraju/redibridge/internal/config"
	"github.com/nagaraju/redibridge/internal/consumer"
	"go.uber.org/zap"
)

// Coordinator serves the management HTTP API.
type Coordinator struct {
	cfg      *config.Config
	consumer *consumer.Consumer
	startedAt time.Time
	logger   *zap.Logger
	server   *http.Server
}

// New creates a new Coordinator.
func New(cfg *config.Config, cons *consumer.Consumer, logger *zap.Logger) *Coordinator {
	return &Coordinator{
		cfg:       cfg,
		consumer:  cons,
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

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
