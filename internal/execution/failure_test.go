package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

// TestClassifyFailure checks the classification rules on their own, with
// hand-made errors. Errors are wrapped with %w on purpose, the way the real
// code wraps them, to prove errors.Is and status.Code see through the wrapping.
func TestClassifyFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want failureKind
	}{
		{
			name: "invalid event",
			err:  fmt.Errorf("handling delivery: %w", errInvalidEvent),
			want: permanentFailure,
		},
		{
			name: "order does not exist",
			err:  fmt.Errorf("execute order: %w", order.ErrOrderNotFound),
			want: permanentFailure,
		},
		{
			name: "gRPC NotFound (unknown symbol)",
			err:  fmt.Errorf("getting price: %w", status.Error(codes.NotFound, "unknown symbol")),
			want: permanentFailure,
		},
		{
			name: "gRPC InvalidArgument",
			err:  fmt.Errorf("getting price: %w", status.Error(codes.InvalidArgument, "empty symbol")),
			want: permanentFailure,
		},
		{
			name: "gRPC Unavailable (price service down)",
			err:  fmt.Errorf("getting price: %w", status.Error(codes.Unavailable, "connection refused")),
			want: transientFailure,
		},
		{
			name: "gRPC DeadlineExceeded",
			err:  fmt.Errorf("getting price: %w", status.Error(codes.DeadlineExceeded, "too slow")),
			want: transientFailure,
		},
		{
			name: "context deadline exceeded",
			err:  fmt.Errorf("execute order: %w", context.DeadlineExceeded),
			want: transientFailure,
		},
		{
			name: "context canceled",
			err:  fmt.Errorf("execute order: %w", context.Canceled),
			want: transientFailure,
		},
		{
			// Documents the default: what we do not recognize is retried.
			name: "unrecognized error",
			err:  errors.New("something unexpected"),
			want: transientFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyFailure(tt.err); got != tt.want {
				t.Errorf("classifyFailure(%v) = %s, want %s", tt.err, got, tt.want)
			}
		})
	}
}

// TestHandleOrderCreated_FailureKinds checks the two halves together: the
// real errors produced by handleOrderCreated must be classified as intended.
// It catches drift, e.g. someone adding a new failure path and forgetting to
// mark it as invalid-event.
func TestHandleOrderCreated_FailureKinds(t *testing.T) {
	validBody, _ := validEventJSON(t)

	tests := []struct {
		name   string
		body   []byte
		store  *fakeExecutionStore
		prices *fakePriceGetter
		want   failureKind
	}{
		{
			name:   "malformed JSON",
			body:   []byte(`{not valid json}`),
			store:  &fakeExecutionStore{},
			prices: &fakePriceGetter{},
			want:   permanentFailure,
		},
		{
			name:   "event with missing required fields",
			body:   []byte(`{}`),
			store:  &fakeExecutionStore{},
			prices: &fakePriceGetter{},
			want:   permanentFailure,
		},
		{
			name:   "unknown symbol",
			body:   validBody,
			store:  &fakeExecutionStore{},
			prices: &fakePriceGetter{err: fmt.Errorf("getting price: %w", status.Error(codes.NotFound, "unknown symbol"))},
			want:   permanentFailure,
		},
		{
			name:   "price service unavailable",
			body:   validBody,
			store:  &fakeExecutionStore{},
			prices: &fakePriceGetter{err: fmt.Errorf("getting price: %w", status.Error(codes.Unavailable, "connection refused"))},
			want:   transientFailure,
		},
		{
			name:   "order does not exist",
			body:   validBody,
			store:  &fakeExecutionStore{err: order.ErrOrderNotFound},
			prices: &fakePriceGetter{price: 38.42},
			want:   permanentFailure,
		},
		{
			name:   "database failure",
			body:   validBody,
			store:  &fakeExecutionStore{err: errors.New("connection reset by peer")},
			prices: &fakePriceGetter{price: 38.42},
			want:   transientFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := handleOrderCreated(context.Background(), tt.store, tt.prices, tt.body)
			if err == nil {
				t.Fatal("handleOrderCreated() returned no error, want one")
			}
			if got := classifyFailure(err); got != tt.want {
				t.Errorf("classifyFailure(%v) = %s, want %s", err, got, tt.want)
			}
		})
	}
}
