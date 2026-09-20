package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

// simulatedWorkDuration imitates the read-only part of execution (the price
// lookup). It goes away when the real price lookup is integrated.
const simulatedWorkDuration = 5 * time.Second

// ExecutionStore is what the execution package needs from persistence. It is
// declared here, next to its only user, so this package does not depend on a
// concrete store: *order.PostgresOrderStore satisfies it implicitly.
type ExecutionStore interface {
	MarkExecuted(ctx context.Context, id uuid.UUID) (bool, error)
}

// handleOrderCreated decodes, validates and processes one order created
// message body. It knows nothing about AMQP, so it can be tested without a broker.
func handleOrderCreated(ctx context.Context, store ExecutionStore, body []byte) error {
	event, err := decodeOrderCreated(body)
	if err != nil {
		return err
	}

	if err := event.Validate(); err != nil {
		return fmt.Errorf("execution: invalid order created event: %w", err)
	}

	return processOrderCreated(ctx, store, event)
}

func decodeOrderCreated(body []byte) (order.OrderCreatedEvent, error) {
	var event order.OrderCreatedEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return order.OrderCreatedEvent{}, fmt.Errorf("execution: failed to decode order created event: %w", err)
	}
	return event, nil
}

// processOrderCreated executes the order. The read-only part (price lookup)
// is simulated for now; the status transition is real and idempotent: only
// the call that finds the order PENDING performs it, so a duplicate delivery
// is skipped. MarkExecuted runs AFTER the work: today the work has no
// persisted effect, and when the portfolio writes arrive they will share one
// transaction with this transition.
func processOrderCreated(ctx context.Context, store ExecutionStore, event order.OrderCreatedEvent) error {
	log.Printf("executing order %s: %s %d x %s", event.OrderID, event.Side, event.Quantity, event.AssetSymbol)
	time.Sleep(simulatedWorkDuration)

	executed, err := store.MarkExecuted(ctx, event.OrderID)
	if err != nil {
		return fmt.Errorf("execution: failed to mark order %s as executed: %w", event.OrderID, err)
	}
	if !executed {
		log.Printf("order %s is not PENDING anymore, skipping (duplicate delivery or already resolved)", event.OrderID)
		return nil
	}

	log.Printf("order %s executed", event.OrderID)
	return nil
}
