package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

// headerCarrier adapts an amqp.Table to implement propagation.TextMapCarrier.
type headerCarrier amqp.Table

func (h headerCarrier) Get(key string) string {
	if val, ok := h[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (h headerCarrier) Set(key, value string) {
	h[key] = value
}

func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext injects the current OpenTelemetry trace context from ctx
// into the provided RabbitMQ amqp.Table headers using the global propagator.
//
// amqp.Publishing.Headers defaults to nil, and writing to a nil map causes
// a panic. For this reason, the caller is responsible for allocating the headers
// table (for example, headers := amqp.Table{}) before passing it to InjectTraceContext.
func InjectTraceContext(ctx context.Context, headers amqp.Table) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(headers))
}

// ExtractTraceContext reads an OpenTelemetry trace context from RabbitMQ
// message headers into ctx using the global propagator. If headers carries
// no valid traceparent, ctx is returned unchanged, and the caller's next
// span becomes a new trace root instead of joining the publisher's trace.
func ExtractTraceContext(ctx context.Context, headers amqp.Table) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier(headers))
}
