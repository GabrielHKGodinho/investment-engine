package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

// ExecutionStore is what the execution package needs from persistence. It is
// declared here, next to its only user, so this package does not depend on a
// concrete store: *order.PostgresOrderStore satisfies it implicitly.
type ExecutionStore interface {
	ExecuteOrder(ctx context.Context, id uuid.UUID, price float64, quantity int) (bool, error)
}

// PriceGetter is what the execution package needs from the price service. It
// is declared here, next to its only user, so this package does not depend
// on a concrete gRPC client: *priceservice.Client satisfies it implicitly.
type PriceGetter interface {
	GetPrice(ctx context.Context, symbol string) (float64, error)
}

// handleOrderCreated decodes, validates and processes one order created
// message body. It knows nothing about AMQP, so it can be tested without a broker.
func handleOrderCreated(ctx context.Context, store ExecutionStore, prices PriceGetter, body []byte) error {
	event, err := decodeOrderCreated(body)
	if err != nil {
		return err
	}

	if err := event.Validate(); err != nil {
		return fmt.Errorf("%w: %w", errInvalidEvent, err)
	}

	return processOrderCreated(ctx, store, prices, event)
}

func decodeOrderCreated(body []byte) (order.OrderCreatedEvent, error) {
	var event order.OrderCreatedEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return order.OrderCreatedEvent{}, fmt.Errorf("%w: failed to decode: %w", errInvalidEvent, err)
	}
	return event, nil
}

// processOrderCreated executes the order: it looks up the current price and
// then, in a single atomic transition, records the execution effect and
// moves the order to EXECUTED. ExecuteOrder is idempotent: only the call
// that finds the order PENDING performs the transition and records the
// effect, so a duplicate delivery is skipped without side effects.
func processOrderCreated(ctx context.Context, store ExecutionStore, prices PriceGetter, event order.OrderCreatedEvent) error {
	log.Printf("executing order %s: %s %d x %s", event.OrderID, event.Side, event.Quantity, event.AssetSymbol)

	price, err := prices.GetPrice(ctx, event.AssetSymbol)
	if err != nil {
		return fmt.Errorf("execution: failed to get price for order %s: %w", event.OrderID, err)
	}

	executed, err := store.ExecuteOrder(ctx, event.OrderID, price, event.Quantity)
	if err != nil {
		return fmt.Errorf("execution: failed to execute order %s: %w", event.OrderID, err)
	}
	if !executed {
		log.Printf("order %s is not PENDING anymore, skipping (duplicate delivery or already resolved)", event.OrderID)
		return nil
	}

	log.Printf("order %s executed at price %.2f", event.OrderID, price)
	return nil
}
