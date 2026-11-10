package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Deliveries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_deliveries_total",
		Help: "Delivery attempts by outcome: succeeded, failed (will retry), dead (sent to DLQ), duplicate (already delivered, skipped).",
	}, []string{"status"})
	DeliveryLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_delivery_duration_seconds",
		Help:    "HTTP round-trip time of a single delivery attempt.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
	EndToEnd = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_end_to_end_seconds",
		Help:    "Time from event publish to successful delivery.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	})
	Retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_retries_total",
		Help: "Delivery retries scheduled after a failed attempt.",
	})
	QueueLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_queue_lag_seconds",
		Help:    "Time a message spent in Kafka before a worker picked it up.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	RateLimited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_rate_limited_total",
		Help: "Times a delivery waited on a subscriber's token bucket.",
	})
	Published = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_events_published_total",
		Help: "Publish requests by result: created, duplicate, error.",
	}, []string{"result"})
	PublishLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_publish_duration_seconds",
		Help:    "Server-side publish latency (Redis + Postgres + Kafka produce).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"transport"})
	DeadLettered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_dead_lettered_total",
		Help: "Deliveries that exhausted retries and went to the dead-letter topic.",
	})
)
