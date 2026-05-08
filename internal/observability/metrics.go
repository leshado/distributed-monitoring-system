package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	ChecksScheduled prometheus.Counter
	ChecksExecuted  *prometheus.CounterVec
	CheckLatency    *prometheus.HistogramVec
	IncidentsOpened prometheus.Counter
	IncidentsClosed prometheus.Counter
	AlertsTriggered *prometheus.CounterVec
	AlertsDelivered *prometheus.CounterVec
	HandlerLatency  *prometheus.HistogramVec

	KafkaHandlerDuration *prometheus.HistogramVec
	KafkaRetriesTotal    *prometheus.CounterVec
	KafkaDLQTotal        *prometheus.CounterVec
}

func NewMetrics(service string) *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		ChecksScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "checks_scheduled_total",
			Help: "Total number of scheduled checks.",
		}),
		ChecksExecuted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "checks_executed_total",
			Help: "Total number of executed checks.",
		}, []string{"status", "type"}),
		CheckLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dms", Subsystem: service, Name: "check_latency_seconds",
			Help:    "External check latency.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"type"}),
		IncidentsOpened: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "incidents_opened_total",
			Help: "Total opened incidents.",
		}),
		IncidentsClosed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "incidents_closed_total",
			Help: "Total resolved incidents.",
		}),
		AlertsTriggered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "alerts_triggered_total",
			Help: "Total triggered alerts.",
		}, []string{"kind"}),
		AlertsDelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "alerts_delivered_total",
			Help: "Total delivered alerts.",
		}, []string{"status"}),
		HandlerLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dms", Subsystem: service, Name: "handler_latency_seconds",
			Help:    "HTTP handler latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "status"}),

		KafkaHandlerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dms", Subsystem: service, Name: "kafka_handler_duration_seconds",
			Help:    "Kafka message handler duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"topic"}),
		KafkaRetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "kafka_handler_retries_total",
			Help: "Number of handler retries due to transient errors.",
		}, []string{"topic"}),
		KafkaDLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms", Subsystem: service, Name: "kafka_dlq_total",
			Help: "Number of messages published to DLQ.",
		}, []string{"topic"}),
	}
	registry.MustRegister(
		m.ChecksScheduled,
		m.ChecksExecuted,
		m.CheckLatency,
		m.IncidentsOpened,
		m.IncidentsClosed,
		m.AlertsTriggered,
		m.AlertsDelivered,
		m.HandlerLatency,
		m.KafkaHandlerDuration,
		m.KafkaRetriesTotal,
		m.KafkaDLQTotal,
	)
	return m
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveHTTP(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		m.HandlerLatency.WithLabelValues(route, strconv.Itoa(rw.status)).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
