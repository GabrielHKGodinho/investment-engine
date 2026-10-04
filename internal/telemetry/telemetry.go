// Package telemetry configures OpenTelemetry tracing for a process. It knows
// nothing about any domain: every binary calls Init once at startup.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init installs the global propagator and, when an OTLP endpoint is
// configured, a tracer provider that exports spans to it.
//
// Tracing is opt-in through configuration: the exporter reads its destination
// from OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).
// When neither is set, Init keeps OpenTelemetry's built-in no-op tracer
// provider, so spans are never recorded and no exporter keeps retrying
// against a collector that does not exist.
//
// The returned shutdown function flushes buffered spans and must be called
// before the process exits. It is a no-op when tracing is disabled.
func Init(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	// Installed even when tracing is disabled: non-recording spans still
	// carry the incoming trace context, so a process without an exporter
	// keeps forwarding traceparent to the next hop instead of breaking it.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	if !endpointConfigured() {
		slog.Info("tracing disabled: no OTLP endpoint configured")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to build resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)

	return provider.Shutdown, nil
}

// endpointConfigured reports whether either of the standard OTLP endpoint
// variables read by otlptracehttp is set.
func endpointConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}
