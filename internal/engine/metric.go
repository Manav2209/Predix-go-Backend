package engine

import "github.com/prometheus/client_golang/prometheus"

// Metrics owns engine-internal instrumentation. Each engine instance gets its
// own registry so multiple engines can coexist in one process (tests); the
// HTTP exporter can serve any registry by wiring a custom promhttp.Handler.
type Metrics struct {
	OrdersProcessed prometheus.Counter
	OrderLatency    prometheus.Histogram

	// P3.4 — order/trade lifecycle observations.
	OrdersAccepted prometheus.Counter
	OrdersRejected prometheus.Counter
	OrdersCanceled prometheus.Counter
	OrdersFilled   prometheus.Counter
	TradesExecuted prometheus.Counter

	// P3.4 — latency observations.
	MatchingLatency prometheus.Histogram
	ReplayLatency   prometheus.Histogram

	// AcquiredPartitions is 1 while this engine owns the partition (P2.3).
	AcquiredPartitions *prometheus.GaugeVec

	// LeaseAcquire counts successful partition acquisitions (P3.4).
	LeaseAcquire *prometheus.CounterVec

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
		OrdersAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_accepted_total",
			Help: "Orders accepted by the engine",
		}),
		OrdersRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_rejected_total",
			Help: "Orders rejected by the engine",
		}),
		OrdersCanceled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_canceled_total",
			Help: "Orders canceled by the engine",
		}),
		OrdersFilled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_filled_total",
			Help: "Orders fully filled by the engine",
		}),
		TradesExecuted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_trades_executed_total",
			Help: "Trades executed by the engine",
		}),
		MatchingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "engine_matching_latency_seconds",
			Help:    "Order matching latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		}),
		ReplayLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "engine_replay_latency_seconds",
			Help:    "Command stream replay latency at startup",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 30},
		}),
		AcquiredPartitions: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "engine_partition_owner",
				Help: "1 while this engine holds the partition lease",
			},
			[]string{"engine", "partition"},
		),
		LeaseAcquire: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "engine_lease_acquire_total",
				Help: "Partition leases acquired by this engine",
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
		m.OrdersAccepted,
		m.OrdersRejected,
		m.OrdersCanceled,
		m.OrdersFilled,
		m.TradesExecuted,
		m.MatchingLatency,
		m.ReplayLatency,
		m.AcquiredPartitions,
		m.LeaseAcquire,
		m.LeaseLosses,
	)
	return m
}
