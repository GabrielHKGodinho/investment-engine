package execution

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

// prefetchCount is how many unacknowledged deliveries RabbitMQ may keep in
// flight on the channel. The consumer handles one delivery at a time, so a
// bigger window would only pile deliveries up in memory.
const prefetchCount = 1

// ErrDeliveriesClosed means the deliveries channel closed: the AMQP channel or
// connection died, or the broker cancelled the consumer (e.g. queue deleted).
var ErrDeliveriesClosed = errors.New("execution: deliveries channel closed")

// Consumer reads order-created events from the execution queue.
type Consumer struct {
	conn *rabbitmq.Connection
}

func NewConsumer(conn *rabbitmq.Connection) *Consumer {
	return &Consumer{conn: conn}
}

// Run declares the topology and processes deliveries. Returns nil
// whith a clean shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	channel, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("execution: failed to open consumer channel: %w", err)
	}
	defer channel.Close()

	if err := SetupTopology(channel); err != nil {
		return err
	}

	// Must be set before Consume, so it applies to the consumer we start below.
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return fmt.Errorf("execution: failed to set channel prefetch: %w", err)
	}

	deliveries, err := channel.Consume(
		OrderQueue,
		"",    // consumer tag: let the broker generate a unique one
		false, // autoAck: false, we ack manually
		false, // exclusive: false, allow multiple consumers on the queue
		false, // noLocal: not supported by RabbitMQ
		false, // noWait: wait for the broker's confirmation
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("execution: failed to start consuming from %s: %w", OrderQueue, err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown requested: not taking new deliveries")
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return ErrDeliveriesClosed
			}

			// A delivery can arrive at the same moment as the shutdown request.
			// When both cases are ready, select picks one at random, so re-check.
			if ctx.Err() != nil {
				// Left unacked on purpose: RabbitMQ requeues it when the channel closes.
				log.Println("shutdown requested: leaving a received delivery unacked, it returns to the queue")
				return nil
			}

			log.Printf("received delivery: redelivered=%t", delivery.Redelivered)

			if handleErr := handleOrderCreated(delivery.Body); handleErr != nil {
				log.Printf("failed to handle delivery, rejecting without requeue: %v", handleErr)
				if err := delivery.Reject(false); err != nil {
					log.Printf("failed to reject delivery: %v", err)
				}
				continue
			}

			// Ack only AFTER handling: a crash mid-processing leaves the delivery
			// unacked, so RabbitMQ redelivers it.
			if err := delivery.Ack(false); err != nil {
				log.Printf("failed to ack delivery: %v", err)
			}
		}
	}
}
