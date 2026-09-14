# ADR-001: Use RabbitMQ for asynchronous order execution

## Status
Accepted

## Context
When an order is created, it needs to be executed asynchronously: the API
must respond to the client immediately after persisting the order, without
waiting for execution to complete. Execution involves fetching a current
price (via the PriceService) and updating the order/portfolio state.

This requires a message broker between the Order API (producer) and the
Order Consumer (single consumer processing execution). The workload is
task-oriented: each order execution must happen exactly once, with retry
on failure and a dead-letter path for messages that fail repeatedly. There
is currently no requirement for multiple independent services reacting to
the same event, nor for replaying historical events.

## Decision
We will use RabbitMQ as the message broker between the Order API and the
Order Consumer.

## Consequences
- Gained: built-in retry semantics, automatic dead-lettering for messages
  that fail repeatedly, and a simple push-based delivery model that fits
  our single-consumer task-processing use case. Fan-out to additional
  consumers (e.g. a future analytics service reacting to the same
  "order created" event) is achievable via a fanout/topic exchange if
  needed later.
- Accepted trade-off: messages are deleted once acknowledged, so there is
  no way to replay historical events. If the consumer processes a message
  incorrectly (e.g. a bug in the execution logic), the original event
  cannot be recovered — we are limited to whatever state was already
  written to PostgreSQL. A consumer added in the future will only see
  messages published after it starts listening; it cannot retroactively
  process events from before it existed. If event replay or historical
  reprocessing becomes a real requirement, this decision should be
  revisited in favor of a log-based broker such as Kafka.

# ADR-002: Use gRPC for internal PriceService communication

## Status
Accepted

## Context
Before an order can be executed, the Order Consumer must fetch the
current price for the asset from the PriceService. This call happens on
every order execution, so as volume grows the system needs to sustain
many order executions per minute without price lookups becoming a
bottleneck. The PriceService is called exclusively by internal
components (the Order Consumer) — no external client or browser ever
calls it directly.

## Decision
We will expose the PriceService over gRPC (Protocol Buffers), rather
than as a REST/JSON endpoint.

## Consequences
- Gained: a typed contract enforced by the .proto file (compile-time
  safety instead of runtime JSON parsing errors), lower serialization
  overhead than JSON at high call volume, and HTTP/2 multiplexing over a
  single connection.
- Accepted trade-off: gRPC is harder to inspect and debug ad hoc than
  REST — plain curl doesn't work, and even a purpose-built tool like
  grpcurl required troubleshooting due to shell-specific quoting issues
  (encountered firsthand on 2026-09-11). If the PriceService ever needs
  to be called by an external client or a browser, it would require an
  additional REST gateway layer rather than being consumed directly.

# ADR-003: Simulate order execution against an external price, no real order book

## Status
Accepted

## Context
A complete order book requires implementing a real-time matching engine
for buy and sell orders, along with continuous management of order
depth and price-time priority queues. This demands significant
development time on the algorithmic complexity of a trading engine
itself. The primary goal of this project is to master event-driven
messaging patterns, resilience, observability, and distributed
infrastructure — not to build a trading engine.

## Decision
Order execution will be simulated against a price fetched from the
PriceService (mocked internally or sourced from a public API), rather
than matched against a real order book of competing buy/sell orders.

## Consequences
- Gained: development time stays focused on the messaging, resilience,
  and observability patterns this project is meant to teach, rather
  than on matching-engine algorithms unrelated to those goals.
- Accepted trade-off: the project does not demonstrate trading-system-
  specific skills — matching algorithms, price-time priority, partial
  fills, or order book depth management. Anyone reading the code without
  this ADR could mistake the simplification for a knowledge gap rather
  than a deliberate scope decision; this should be called out explicitly
  in the README and in interviews when discussing the project.