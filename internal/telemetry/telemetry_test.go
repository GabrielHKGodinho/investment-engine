package telemetry

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestInit_DisabledWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	shutdown, err := Init(context.Background(), "test")
	if err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown() error = %v, want nil", err)
	}

	// No exporter: spans must not be recorded.
	_, span := otel.Tracer("test").Start(context.Background(), "probe")
	defer span.End()
	if span.IsRecording() {
		t.Error("span is recording, want a non-recording span when tracing is disabled")
	}

	// The propagator must still be installed so trace context is forwarded.
	if fields := otel.GetTextMapPropagator().Fields(); !slices.Contains(fields, "traceparent") {
		t.Errorf("propagator fields = %v, want them to include traceparent", fields)
	}
}
