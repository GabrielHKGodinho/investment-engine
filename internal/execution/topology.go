package execution

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GabrielHKGodinho/investment-engine/internal/order"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

const (
	// OrderQueue is the durable queue the order execution consumer reads from.
	OrderQueue = "order_execution"

	// DeadLetterExchange receives the messages RabbitMQ dead-letters from
	// OrderQueue: the ones the consumer rejects without requeue.
	DeadLetterExchange = "order_execution.dlx"

	// DeadLetterQueue keeps those messages for inspection instead of losing them.
	DeadLetterQueue = "order_execution.dlq"

	deadLetterRoutingKey = "order_execution.dlq"
)

// SetupTopology declares everything the execution consumer depends on: the
// order events exchange, the dead letter exchange and queue, its own work
// queue, and the bindings between them. Must run once at startup, before
// consuming.
//
// Messages the consumer rejects without requeue are dead-lettered by RabbitMQ
// to the dead letter queue instead of being discarded (see ADR-009). Queue
// arguments are immutable: redeclaring an existing queue with different ones
// fails with PRECONDITION_FAILED, so the old queue has to be deleted first.
func SetupTopology(channel *amqp.Channel) error {
	// The API also declares this exchange, but the consumer cannot assume it
	// started first: binding to a missing exchange is a channel-level error.
	// Re-declaring with identical parameters is a no-op, so both sides can
	// declare it safely. Reusing order.SetupMessaging keeps those parameters
	// defined in a single place.
	if err := order.SetupMessaging(channel); err != nil {
		return fmt.Errorf("execution: failed to declare order events exchange: %w", err)
	}

	if err := rabbitmq.DeclareDirectExchange(channel, DeadLetterExchange); err != nil {
		return fmt.Errorf("execution: failed to declare dead letter exchange: %w", err)
	}

	if err := rabbitmq.DeclareDurableQueue(channel, DeadLetterQueue, nil); err != nil {
		return fmt.Errorf("execution: failed to declare dead letter queue: %w", err)
	}

	if err := rabbitmq.BindQueue(channel, DeadLetterQueue, DeadLetterExchange, deadLetterRoutingKey); err != nil {
		return fmt.Errorf("execution: failed to bind dead letter queue: %w", err)
	}

	// The dead letter destination is declared before OrderQueue, so it already
	// exists by the time any message can be dead-lettered.
	orderQueueArguments := amqp.Table{
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": deadLetterRoutingKey,
	}
	if err := rabbitmq.DeclareDurableQueue(channel, OrderQueue, orderQueueArguments); err != nil {
		return fmt.Errorf("execution: failed to declare order queue: %w", err)
	}

	if err := rabbitmq.BindQueue(channel, OrderQueue, order.EventsExchange, order.CreatedRoutingKey); err != nil {
		return fmt.Errorf("execution: failed to bind order queue: %w", err)
	}

	return nil
}
