// Package observability provides the shared liveness, readiness and
// Prometheus endpoints required by every service (P3.4/P3.5).
package observability

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check is a readiness probe. It must return nil when the dependency is
// healthy and an error otherwise.
type Check func(ctx context.Context) error

// Ops bundles the health checks and the Prometheus registry of one service.
type Ops struct {
	registry *prometheus.Registry

	mu     sync.Mutex
	checks map[string]Check
}

func NewOps(registry *prometheus.Registry) *Ops {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}

	WithRuntimeMetrics(registry)

	return &Ops{
		registry: registry,
		checks:   make(map[string]Check),
	}
}

// WithRuntimeMetrics registers the standard Go runtime and process collectors
// onto a service registry.
func WithRuntimeMetrics(registry *prometheus.Registry) {
	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
}

func (o *Ops) Registry() *prometheus.Registry {
	return o.registry
}

// AddCheck registers a named readiness probe.
func (o *Ops) AddCheck(name string, fn Check) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.checks[name] = fn
}

// MetricsHandler serves Prometheus metrics for this service.
func (o *Ops) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(o.registry, promhttp.HandlerOpts{})
}

// LiveHandler reports process liveness; it never fails.
func LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// ReadyHandler runs every registered check and reports readiness.
func (o *Ops) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := make(map[string]string)

		o.mu.Lock()
		checks := make(map[string]Check, len(o.checks))
		for name, fn := range o.checks {
			checks[name] = fn
		}
		o.mu.Unlock()

		ready := true

		for name, fn := range checks {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := fn(ctx)
			cancel()

			if err != nil {
				ready = false
				results[name] = err.Error()
			} else {
				results[name] = "ok"
			}
		}

		w.Header().Set("Content-Type", "application/json")

		status := http.StatusOK
		body := map[string]any{
			"status": "ok",
			"checks": results,
		}

		if !ready {
			status = http.StatusServiceUnavailable
			body["status"] = "degraded"
		}

		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
}

// Handler returns the combined /metrics + /health/live + /health/ready mux
// for services that do not use a framework router.
func (o *Ops) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", o.MetricsHandler())
	mux.HandleFunc("/health/live", LiveHandler())
	mux.HandleFunc("/health/ready", o.ReadyHandler())
	return mux
}

// Run serves the ops endpoints, blocking until ctx is canceled, then drains
// with a bounded shutdown.
func (o *Ops) Run(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           o.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)

	go func() {
		log.Printf("ops server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err

	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
