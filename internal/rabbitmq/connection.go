// internal/rabbitmq/connection.go
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection wraps the connection to RabbitMQ. It knows nothing about
// exchanges, queues, or any specific domain — only connection lifecycle.
type Connection struct {
	conn *amqp.Connection
}

// Dial connects to RabbitMQ. It declares nothing — exchange/queue setup
// is domain-specific and belongs to whichever package owns that topology.
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: failed to connect: %w", err)
	}
	return &Connection{conn: conn}, nil
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	channel, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: failed to open channel: %w", err)
	}
	return channel, nil
}

func (c *Connection) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("rabbitmq: failed to close connection: %w", err)
	}
	return nil
}

// DeclareDirectExchange declares a durable, non-auto-deleted direct
// exchange — the project's standard defaults. Callers only pick the name.
func DeclareDirectExchange(channel *amqp.Channel, name string) error {
	err := channel.ExchangeDeclare(name, "direct", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: failed to declare exchange %s: %w", name, err)
	}
	return nil
}

// DeclareDurableQueue declares a durable, non-exclusive, non-auto-deleted
// queue — the project's standard defaults for work queues. Callers only
// pick the name.
func DeclareDurableQueue(channel *amqp.Channel, name string) error {
	_, err := channel.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("rabbitmq: failed to declare queue %s: %w", name, err)
	}
	return nil
}

// BindQueue binds a queue to an existing exchange for a routing key.
func BindQueue(channel *amqp.Channel, queue, exchange, routingKey string) error {
	if err := channel.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		return fmt.Errorf(
			"rabbitmq: failed to bind queue %s to exchange %s with key %s: %w",
			queue, exchange, routingKey, err,
		)
	}
	return nil
}
