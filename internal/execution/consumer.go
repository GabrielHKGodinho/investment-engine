package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GabrielHKGodinho/investment-engine/internal/metrics"
	"github.com/GabrielHKGodinho/investment-engine/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/GabrielHKGodinho/investment-engine/internal/execution")

// prefetchCount is how many unacknowledged deliveries RabbitMQ may keep in
// flight on the channel. The consumer handles one delivery at a time, so a
// bigger window would only pile deliveries up in memory.
const prefetchCount = 1

// ErrDeliveriesClosed means the deliveries channel closed: the AMQP channel or
// connection died, or the broker cancelled the consumer (e.g. queue deleted).
var ErrDeliveriesClosed = errors.New("execution: deliveries channel closed")

// handleTimeout bounds the handling of one delivery. It stays below Docker's
// 10s stop grace period, so a shutdown that waits for the delivery in progress
// still fits in it.
const handleTimeout = 8 * time.Second

// Consumer reads order-created events from the execution queue.
type Consumer struct {
	conn   *rabbitmq.Connection
	store  ExecutionStore
	prices PriceGetter
}

func NewConsumer(conn *rabbitmq.Connection, store ExecutionStore, prices PriceGetter) *Consumer {
	return &Consumer{conn: conn, store: store, prices: prices}
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
			slog.Info("shutdown requested: not taking new deliveries")
			return nil

		case delivery, ok := <-deliveries:
			if !ok {
				return ErrDeliveriesClosed
			}

			// A delivery can arrive at the same moment as the shutdown request.
			// When both cases are ready, select picks one at random, so re-check.
			if ctx.Err() != nil {
				// Left unacked on purpose: RabbitMQ requeues it when the channel closes.
				slog.Info("shutdown requested: leaving a received delivery unacked, it returns to the queue")
				return nil
			}

			slog.Debug("received delivery", "redelivered", delivery.Redelivered, "delivery_tag", delivery.DeliveryTag)

			// ctx is the shutdown signal: it means "stop taking new work", not
			// "abort what is in progress" (ADR-007). The handler gets its own
			// context, detached from the signal, with a timeout.
			handleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handleTimeout)
			handleCtx = rabbitmq.ExtractTraceContext(handleCtx, delivery.Headers)
			handleCtx, span := tracer.Start(
				handleCtx,
				OrderQueue+" process",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination.name", OrderQueue),
					attribute.Bool("messaging.rabbitmq.message.redelivered", delivery.Redelivered),
				),
			)

			start := time.Now()
			handleErr := handleOrderCreated(handleCtx, c.store, c.prices, delivery.Body)
			cancel()

			if handleErr != nil {
				span.RecordError(handleErr)
				span.SetStatus(codes.Error, handleErr.Error())
			}
			span.End()

			metrics.MessageProcessingDuration.Observe(time.Since(start).Seconds())

			if handleErr != nil {
				failure := classifyFailure(handleErr)
				metrics.MessagesProcessedTotal.WithLabelValues(failure.String()).Inc()

				// attempt is how many times this message has already failed (0 on its
				// very first failure). This failure is attempt+1; if that already
				// reaches maxDeliveryAttempts, there is no next try left to schedule.
				attempt := retryAttempt(delivery.Headers)
				outOfRetries := attempt+1 >= maxDeliveryAttempts

				if failure == permanentFailure || outOfRetries {
					slog.Error("failed to handle delivery, rejecting without requeue",
						"error", handleErr, "delivery_tag", delivery.DeliveryTag,
						"failure", failure.String(), "attempt", attempt+1)
					if err := delivery.Reject(false); err != nil {
						slog.Error("failed to reject delivery", "error", err, "delivery_tag", delivery.DeliveryTag)
					}
					continue
				}

				// Copy the headers into a fresh map before mutating: delivery.Headers
				// is the original delivery's own map, and writing into it in place
				// would silently change what the original delivery carries.
				retryHeaders := amqp.Table{}
				for k, v := range delivery.Headers {
					retryHeaders[k] = v
				}
				retryHeaders[retryCountHeader] = attempt + 1

				retryMsg := amqp.Publishing{
					Headers:      retryHeaders,
					ContentType:  "application/json",
					DeliveryMode: amqp.Persistent,
					Body:         delivery.Body,
				}

				// Publish the retry copy BEFORE acking the original: if the ack came
				// first and this publish then failed, the original would already be
				// gone from OrderQueue with no copy anywhere — the message would be
				// lost, not just delayed.
				if err := channel.PublishWithContext(ctx, "", RetryQueue, true, false, retryMsg); err != nil {
					slog.Error("failed to send delivery to retry queue, requeueing original instead",
						"error", err, "delivery_tag", delivery.DeliveryTag)
					if err := delivery.Reject(true); err != nil {
						slog.Error("failed to requeue delivery after retry publish failure", "error", err, "delivery_tag", delivery.DeliveryTag)
					}
					continue
				}

				slog.Error("failed to handle delivery, sent a copy to retry queue",
					"error", handleErr, "delivery_tag", delivery.DeliveryTag, "attempt", attempt+1)
				if err := delivery.Ack(false); err != nil {
					slog.Error("failed to ack delivery after retry publish succeeded", "error", err, "delivery_tag", delivery.DeliveryTag)
				}
				continue
			}

			metrics.MessagesProcessedTotal.WithLabelValues("success").Inc()

			if err := delivery.Ack(false); err != nil {
				slog.Error("failed to ack delivery", "error", err, "delivery_tag", delivery.DeliveryTag)
			}
		}
	}
}

// retryAttempt reads how many times this delivery has already failed.
// Absent or unexpected header value means this is the first attempt (0).
// A value written by this same code always arrives back as int32 — that
// is how amqp091-go decodes the wire's 32-bit integer tag on redelivery.
func retryAttempt(headers amqp.Table) int {
	if val, ok := headers[retryCountHeader]; ok {
		if n, ok := val.(int32); ok {
			return int(n)
		}
	}
	return 0
}
