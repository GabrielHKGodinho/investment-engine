// internal/order/events_integration_test.go
package order

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
)

// TestPublishOrderCreated_Unroutable exercises mandatory + NotifyReturn
// against a real broker: it publishes to a routing key with no matching
// binding, and asserts the broker's basic.return is actually observed and
// logged, closing the ADR-005 gap for real. This needs a real RabbitMQ, so
// it's skipped unless RABBITMQ_URL is set — same pattern as the rest of the
// project's env-var-driven config.
func TestPublishOrderCreated_Unroutable(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL not set, skipping integration test")
	}

	conn, err := rabbitmq.Dial(url)
	if err != nil {
		t.Fatalf("failed to dial rabbitmq: %v", err)
	}
	defer conn.Close()

	setupChannel, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open setup channel: %v", err)
	}
	if err := SetupMessaging(setupChannel); err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}
	setupChannel.Close()

	event := OrderCreatedEvent{
		OrderID:       uuid.New(),
		UserID:        uuid.New(),
		AssetSymbol:   "PETR4",
		Quantity:      10,
		Side:          SideBuy,
		ExecutionType: ExecutionMarket,
		CreatedAt:     time.Now(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal test event: %v", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}

	returns := channel.NotifyReturn(make(chan amqp.Return, 1))

	err = channel.PublishWithContext(
		context.Background(),
		EventsExchange,
		"routing.key.with.no.binding",
		true,
		false,
		amqp.Publishing{ContentType: "application/json", Body: body},
	)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case ret := <-returns:
		if ret.ReplyCode == 0 {
			t.Error("received a Return, but ReplyCode was zero — unexpected")
		}
		t.Logf("received expected Return: code=%d reason=%q", ret.ReplyCode, ret.ReplyText)
	case <-time.After(3 * time.Second):
		t.Fatal("expected a Return for an unroutable mandatory publish, got none within 3s")
	}

	channel.Close()
}
