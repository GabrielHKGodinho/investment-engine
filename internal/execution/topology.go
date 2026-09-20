package execution

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

// OrderQueue is the durable queue the order execution consumer reads from.
const OrderQueue = "order_execution"

// SetupTopology declares everything the execution consumer depends on: the
// order events exchange, its own queue, and the binding between them.
// Must run once at startup, before consuming.
func SetupTopology(channel *amqp.Channel) error {
	// The API also declares this exchange, but the consumer cannot assume it
	// started first: binding to a missing exchange is a channel-level error.
	// Re-declaring with identical parameters is a no-op, so both sides can
	// declare it safely. Reusing order.SetupMessaging keeps those parameters
	// defined in a single place.
	if err := order.SetupMessaging(channel); err != nil {
		return fmt.Errorf("execution: failed to declare order events exchange: %w", err)
	}

	if err := rabbitmq.DeclareDurableQueue(channel, OrderQueue); err != nil {
		return fmt.Errorf("execution: failed to declare order queue: %w", err)
	}

	if err := rabbitmq.BindQueue(channel, OrderQueue, order.EventsExchange, order.CreatedRoutingKey); err != nil {
		return fmt.Errorf("execution: failed to bind order queue: %w", err)
	}

	return nil
}
