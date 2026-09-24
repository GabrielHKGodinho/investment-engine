package execution

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

// failureKind tells the consumer what a failed delivery deserves.
type failureKind int

const (
	// permanentFailure means retrying can never succeed, because the problem
	// is in the message itself or in what it refers to (bad payload, unknown
	// order, unknown symbol). Such a message goes straight to the dead-letter
	// queue.
	permanentFailure failureKind = iota

	// transientFailure means the cause is temporary (a dependency is down or
	// slow), so the same message may succeed if it is tried again later.
	transientFailure
)

func (k failureKind) String() string {
	if k == permanentFailure {
		return "permanent"
	}
	return "transient"
}

// errInvalidEvent marks a failure caused by the message itself: it could not
// be decoded or it broke the event contract. Handlers wrap it with %w.
var errInvalidEvent = errors.New("execution: invalid event")

// classifyFailure decides whether an error returned by handleOrderCreated is
// permanent or transient.
//
// Anything it does not recognize is treated as transient. Retries will be
// bounded, so the worst case is a few wasted attempts before the message
// reaches the dead-letter queue; calling a temporary blip "permanent" would
// throw good work away.
func classifyFailure(err error) failureKind {
	if errors.Is(err, errInvalidEvent) || errors.Is(err, order.ErrOrderNotFound) {
		return permanentFailure
	}

	// status.Code sees through errors wrapped with %w, so the "getting price: %w"
	// wrapper added by the price client does not hide the gRPC code.
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument:
		return permanentFailure
	}

	return transientFailure
}
