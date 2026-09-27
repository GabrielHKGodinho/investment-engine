// internal/metrics/metrics.go
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts every request the api handles, by method,
	// path and final status code.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration tracks how long each api request took to handle.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets, // 0.005s .. 10s, good default spread for web APIs
		},
		[]string{"method", "path"},
	)

	// MessagesProcessedTotal counts consumer deliveries by outcome, using
	// the same three categories as failure.classifyFailure.
	MessagesProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "messages_processed_total",
			Help: "Total number of RabbitMQ deliveries processed by the consumer",
		},
		[]string{"result"}, // success | permanent_failure | transient_failure
	)

	// MessageProcessingDuration tracks how long the consumer takes to
	// handle one delivery, start to finish.
	MessageProcessingDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "message_processing_duration_seconds",
			Help:    "Time to process one RabbitMQ delivery, in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
)
