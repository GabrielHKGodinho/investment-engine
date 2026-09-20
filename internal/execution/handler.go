package execution

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
)

// simulatedWorkDuration imitates real execution time. It goes away when the
// real execution (price lookup + status update) is integrated.
const simulatedWorkDuration = 5 * time.Second

// handleOrderCreated decodes, validates and processes one order created
// message body. It knows nothing about AMQP, so it can be tested without a broker.
func handleOrderCreated(body []byte) error {
	event, err := decodeOrderCreated(body)
	if err != nil {
		return err
	}

	if err := event.Validate(); err != nil {
		return fmt.Errorf("execution: invalid order created event: %w", err)
	}

	return processOrderCreated(event)
}

func decodeOrderCreated(body []byte) (order.OrderCreatedEvent, error) {
	var event order.OrderCreatedEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return order.OrderCreatedEvent{}, fmt.Errorf("execution: failed to decode order created event: %w", err)
	}
	return event, nil
}

// processOrderCreated executes the order. The execution is simulated for
// now. The error return is the seam for the real implementation.
func processOrderCreated(event order.OrderCreatedEvent) error {
	log.Printf("executing order %s: %s %d x %s", event.OrderID, event.Side, event.Quantity, event.AssetSymbol)
	time.Sleep(simulatedWorkDuration)
	log.Printf("order %s executed (simulated)", event.OrderID)
	return nil
}
