package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

const (
	// EventsExchange is the RabbitMQ exchange the order domain publishes
	// lifecycle events to (see ADR-005).
	EventsExchange = "order_events"

	// CreatedRoutingKey is the routing key used when an order is created.
	CreatedRoutingKey = "order.created"
)

// SetupMessaging declares the order domain's exchange. Must run once at
// startup, before any order-created event is published.
func SetupMessaging(channel *amqp.Channel) error {
	return rabbitmq.DeclareDirectExchange(channel, EventsExchange)
}

// OrderCreatedEvent is the wire contract published to order_events when an
// order is created. It is a separate type from Order on purpose — the event
// payload is a public contract for other services, and should not silently
// change just because Order's internal fields change.
type OrderCreatedEvent struct {
	OrderID       uuid.UUID     `json:"order_id"`
	UserID        uuid.UUID     `json:"user_id"`
	AssetSymbol   string        `json:"asset_symbol"`
	Quantity      int           `json:"quantity"`
	Side          Side          `json:"side"`
	ExecutionType ExecutionType `json:"execution_type"`
	LimitPrice    *float64      `json:"limit_price,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
}

// NewOrderCreatedEvent builds the event payload from a freshly created Order.
func NewOrderCreatedEvent(o Order) OrderCreatedEvent {
	return OrderCreatedEvent{
		OrderID:       o.ID,
		UserID:        o.UserID,
		AssetSymbol:   o.AssetSymbol,
		Quantity:      o.Quantity,
		Side:          o.Side,
		ExecutionType: o.ExecutionType,
		LimitPrice:    o.LimitPrice,
		CreatedAt:     o.CreatedAt,
	}
}

// Validate checks the invariants of the event contract. A successful JSON
// decode only guarantees well-formed JSON with matching types: absent fields
// silently become zero values, so consumers must call this before trusting
// the event. It returns on the first violation found.
func (e OrderCreatedEvent) Validate() error {
	if e.OrderID == uuid.Nil {
		return errors.New("order_id is required")
	}
	if e.UserID == uuid.Nil {
		return errors.New("user_id is required")
	}
	if e.AssetSymbol == "" {
		return errors.New("asset_symbol is required")
	}
	if e.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	if !isValidSide(e.Side) {
		return fmt.Errorf("invalid side %q", e.Side)
	}
	if !isValidExecutionType(e.ExecutionType) {
		return fmt.Errorf("invalid execution_type %q", e.ExecutionType)
	}

	switch e.ExecutionType {
	case ExecutionLimit:
		if e.LimitPrice == nil || *e.LimitPrice <= 0 {
			return errors.New("limit_price must be present and positive for LIMIT orders")
		}
	case ExecutionMarket:
		if e.LimitPrice != nil {
			return errors.New("limit_price must be absent for MARKET orders")
		}
	}

	return nil
}

// Publisher publishes order domain events to RabbitMQ.
type Publisher struct {
	conn *rabbitmq.Connection
}

func NewPublisher(conn *rabbitmq.Connection) *Publisher {
	return &Publisher{conn: conn}
}

// PublishOrderCreated publishes an OrderCreatedEvent to the order_events
// exchange under the order.created routing key. Publishing with mandatory
// asks the broker to return the message instead of silently dropping it if
// no queue is bound to route it (closing the gap left open by ADR-005).
//
// The caller is not blocked waiting to find out whether the message was
// routed — same grava-depois-publica principle as before: this returns nil
// as soon as the publish itself succeeds, and a late "unroutable" return
// from the broker is watched for and logged in the background.
func (p *Publisher) PublishOrderCreated(ctx context.Context, event OrderCreatedEvent) error {
	channel, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("order: failed to open channel to publish order created event: %w", err)
	}

	returns := channel.NotifyReturn(make(chan amqp.Return, 1))

	body, err := json.Marshal(event)
	if err != nil {
		channel.Close()
		return fmt.Errorf("order: failed to marshal order created event: %w", err)
	}

	err = channel.PublishWithContext(
		ctx,
		EventsExchange,
		CreatedRoutingKey,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		channel.Close()
		return fmt.Errorf("order: failed to publish order created event: %w", err)
	}

	go awaitReturn(channel, returns, event.OrderID)

	return nil
}

// awaitReturn waits briefly for the broker to report this specific publish
// as unroutable. It owns channel from this point on, and is responsible for
// closing it once it's done waiting.
func awaitReturn(channel *amqp.Channel, returns <-chan amqp.Return, orderID uuid.UUID) {
	defer channel.Close()
	select {
	case returned := <-returns:
		log.Printf("order: order created event for %s was not routed: code=%d reason=%q", orderID, returned.ReplyCode, returned.ReplyText)
	case <-time.After(1 * time.Second):
		// No return within the window: the message was routed successfully.
	}
}
