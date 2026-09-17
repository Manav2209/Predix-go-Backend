package engine

import "github.com/prometheus/client_golang/prometheus"

// Metrics owns engine-internal instrumentation. Each engine instance gets its
// own registry so multiple engines can coexist in one process (tests); the
// HTTP exporter can serve any registry by wiring a custom promhttp.Handler.
type Metrics struct {
	OrdersProcessed prometheus.Counter
	OrderLatency    prometheus.Histogram

	// AcquiredPartitions is 1 while this engine owns the partition (P2.3).
	AcquiredPartitions *prometheus.GaugeVec

	// LeaseLosses counts ownership transfers away from this instance (P2.6).
	LeaseLosses *prometheus.CounterVec

	Registry *prometheus.Registry
}

func NewMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.NewRegistry())
}

func NewMetricsWithRegistry(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		OrdersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_processed_total",
			Help: "Total orders processed by the engine",
		}),
		OrderLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "engine_order_latency_seconds",
			Help:    "Order processing latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		}),
		AcquiredPartitions: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "engine_partition_owner",
				Help: "1 while this engine holds the partition lease",
			},
			[]string{"engine", "partition"},
		),
		LeaseLosses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "engine_lease_losses_total",
				Help: "Partition leases lost to another engine instance",
			},
			[]string{"engine", "partition"},
		),
		Registry: reg,
	}

	reg.MustRegister(
		m.OrdersProcessed,
		m.OrderLatency,
		m.AcquiredPartitions,
		m.LeaseLosses,
	)
	return m
}