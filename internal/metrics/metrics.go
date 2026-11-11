package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ReservationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "warego_reservation_duration_seconds",
			Help:    "Latency of reservation transactions in seconds",
			Buckets: []float64{0.001, 0.002, 0.005, 0.010, 0.015, 0.020, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0},
		},
		[]string{"strategy", "status"},
	)

	ReservationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "warego_reservations_total",
			Help: "Total reservation attempts partitioned by strategy and outcome status",
		},
		[]string{"strategy", "status"},
	)

	SemaphoreActiveSlots = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "warego_semaphore_active_slots",
			Help: "Current number of in-flight active slots in the admission semaphore (0-100)",
		},
	)

	SemaphoreRejectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "warego_semaphore_rejections_total",
			Help: "Total requests shed due to semaphore queue timeout (503 Service Unavailable)",
		},
	)

	ReaperReclaimedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "warego_reaper_reclaimed_total",
			Help: "Total expired orders reclaimed by the reaper daemon",
		},
	)

	ReaperCycleDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "warego_reaper_cycle_duration_seconds",
			Help:    "Time taken by the reaper to execute a cleanup cycle",
			Buckets: []float64{0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500},
		},
	)

	OutboxEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "warego_outbox_events_total",
			Help: "Total events processed by the outbox relay",
		},
		[]string{"status", "event_type"},
	)
)