package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestLiveAlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/live", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("live status = %d, want 200", rec.Code)
	}
}

func TestReadyPassingChecks(t *testing.T) {
	ops := NewOps(nil)
	ops.AddCheck("redis", func(context.Context) error { return nil })
	ops.AddCheck("postgres", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	ops.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	for _, dep := range []string{"redis", "postgres"} {
		if !strings.Contains(rec.Body.String(), "\""+dep+"\":\"ok\"") {
			t.Errorf("missing check %q in body: %s", dep, rec.Body.String())
		}
	}
}

func TestReadyFailingCheckReturns503(t *testing.T) {
	ops := NewOps(nil)
	ops.AddCheck("redis", func(context.Context) error { return errors.New("down") })

	rec := httptest.NewRecorder()
	ops.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want 503", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "degraded") {
		t.Errorf("body should report degraded: %s", rec.Body.String())
	}
}

func TestMetricsEndpointExposesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_metric_total",
	}))

	ops := NewOps(reg)

	rec := httptest.NewRecorder()
	handler := ops.Handler()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "test_metric_total") {
		t.Errorf("metrics body missing registered counter: %s", rec.Body.String())
	}
}

func TestHandlerRoutesHealthAndMetrics(t *testing.T) {
	ops := NewOps(nil)
	handler := ops.Handler()

	for _, path := range []string{"/health/live", "/health/ready", "/metrics"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, rec.Code)
		}
	}
}